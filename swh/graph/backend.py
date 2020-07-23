# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import asyncio
import contextlib
import io
import os
import struct
import subprocess
import sys
import tempfile

from py4j.java_gateway import JavaGateway

from swh.graph.config import check_config
from swh.graph.pid import NodeToPidMap, PidToNodeMap
from swh.model.identifiers import PID_TYPES

BUF_SIZE = 64 * 1024
BIN_FMT = ">q"  # 64 bit integer, big endian
PATH_SEPARATOR_ID = -1
NODE2PID_EXT = "node2pid.bin"
PID2NODE_EXT = "pid2node.bin"


def _get_pipe_stderr():
    # Get stderr if possible, or pipe to stdout if running with Jupyter.
    try:
        sys.stderr.fileno()
    except io.UnsupportedOperation:
        return subprocess.STDOUT
    else:
        return sys.stderr


class Backend:
    def __init__(self, graph_path, config=None):
        self.gateway = None
        self.entry = None
        self.graph_path = graph_path
        self.config = check_config(config or {})

    def __enter__(self):
        self.gateway = JavaGateway.launch_gateway(
            java_path=None,
            javaopts=self.config["java_tool_options"].split(),
            classpath=self.config["classpath"],
            die_on_exit=True,
            redirect_stdout=sys.stdout,
            redirect_stderr=_get_pipe_stderr(),
        )
        self.entry = self.gateway.jvm.org.softwareheritage.graph.Entry()
        self.entry.load_graph(self.graph_path)
        self.node2pid = NodeToPidMap(self.graph_path + "." + NODE2PID_EXT)
        self.pid2node = PidToNodeMap(self.graph_path + "." + PID2NODE_EXT)
        self.stream_proxy = JavaStreamProxy(self.entry)
        return self

    def __exit__(self, exc_type, exc_value, tb):
        self.gateway.shutdown()

    def stats(self):
        return self.entry.stats()

    def count(self, ttype, direction, edges_fmt, src):
        method = getattr(self.entry, "count_" + ttype)
        return method(direction, edges_fmt, src)

    async def simple_traversal(self, ttype, direction, edges_fmt, src):
        assert ttype in ("leaves", "neighbors", "visit_nodes")
        method = getattr(self.stream_proxy, ttype)
        async for node_id in method(direction, edges_fmt, src):
            yield node_id

    async def walk(self, direction, edges_fmt, algo, src, dst):
        if dst in PID_TYPES:
            it = self.stream_proxy.walk_type(direction, edges_fmt, algo, src, dst)
        else:
            it = self.stream_proxy.walk(direction, edges_fmt, algo, src, dst)
        async for node_id in it:
            yield node_id

    async def random_walk(self, direction, edges_fmt, retries, src, dst):
        if dst in PID_TYPES:
            it = self.stream_proxy.random_walk_type(
                direction, edges_fmt, retries, src, dst
            )
        else:
            it = self.stream_proxy.random_walk(direction, edges_fmt, retries, src, dst)
        async for node_id in it:  # TODO return 404 if path is empty
            yield node_id

    async def visit_edges(self, direction, edges_fmt, src):
        it = self.stream_proxy.visit_edges(direction, edges_fmt, src)
        # convert stream a, b, c, d -> (a, b), (c, d)
        prevNode = None
        async for node in it:
            if prevNode is not None:
                yield (prevNode, node)
                prevNode = None
            else:
                prevNode = node

    async def visit_paths(self, direction, edges_fmt, src):
        path = []
        async for node in self.stream_proxy.visit_paths(direction, edges_fmt, src):
            if node == PATH_SEPARATOR_ID:
                yield path
                path = []
            else:
                path.append(node)


class JavaStreamProxy:
    """A proxy class for the org.softwareheritage.graph.Entry Java class that
    takes care of the setup and teardown of the named-pipe FIFO communication
    between Python and Java.

    Initialize JavaStreamProxy using:

        proxy = JavaStreamProxy(swh_entry_class_instance)

    Then you can call an Entry method and iterate on the FIFO results like
    this:

        async for value in proxy.java_method(arg1, arg2):
            print(value)
    """

    def __init__(self, entry):
        self.entry = entry

    async def read_node_ids(self, fname):
        loop = asyncio.get_event_loop()
        open_thread = loop.run_in_executor(None, open, fname, "rb")

        # Since the open() call on the FIFO is blocking until it is also opened
        # on the Java side, we await it with a timeout in case there is an
        # exception that prevents the write-side open().
        with (await asyncio.wait_for(open_thread, timeout=2)) as f:
            while True:
                data = await loop.run_in_executor(None, f.read, BUF_SIZE)
                if not data:
                    break
                for data in struct.iter_unpack(BIN_FMT, data):
                    yield data[0]

    class _HandlerWrapper:
        def __init__(self, handler):
            self._handler = handler

        def __getattr__(self, name):
            func = getattr(self._handler, name)

            async def java_call(*args, **kwargs):
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, lambda: func(*args, **kwargs))

            def java_task(*args, **kwargs):
                return asyncio.create_task(java_call(*args, **kwargs))

            return java_task

    @contextlib.contextmanager
    def get_handler(self):
        with tempfile.TemporaryDirectory(prefix="swh-graph-") as tmpdirname:
            cli_fifo = os.path.join(tmpdirname, "swh-graph.fifo")
            os.mkfifo(cli_fifo)
            reader = self.read_node_ids(cli_fifo)
            query_handler = self.entry.get_handler(cli_fifo)
            handler = self._HandlerWrapper(query_handler)
            yield (handler, reader)

    def __getattr__(self, name):
        async def java_call_iterator(*args, **kwargs):
            with self.get_handler() as (handler, reader):
                java_task = getattr(handler, name)(*args, **kwargs)
                try:
                    async for value in reader:
                        yield value
                except asyncio.TimeoutError:
                    # If the read-side open() timeouts, an exception on the
                    # Java side probably happened that prevented the
                    # write-side open(). We propagate this exception here if
                    # that is the case.
                    task_exc = java_task.exception()
                    if task_exc:
                        raise task_exc
                    raise
                await java_task

        return java_call_iterator
