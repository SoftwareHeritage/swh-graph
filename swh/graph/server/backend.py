# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import asyncio
import contextlib
import json
import os
import pathlib
import struct
import sys
import tempfile

from py4j.java_gateway import JavaGateway

from swh.graph.pid import IntToPidMap, PidToIntMap

BUF_SIZE = 64*1024
BIN_FMT = '>q'  # 64 bit integer, big endian
NODE2PID_EXT = 'node2pid.bin'
PID2NODE_EXT = 'pid2node.bin'


def find_graph_jar():
    swh_graph_root = pathlib.Path(__file__).parents[3]
    try_paths = [
        swh_graph_root / 'java/server/target/',
        pathlib.Path(sys.prefix) / 'share/swh-graph/',
    ]
    for path in try_paths:
        glob = list(path.glob('swh-graph-*.jar'))
        if glob:
            return str(glob[0])
    raise RuntimeError("swh-graph-*.jar not found. Have you run `make java`?")


class Backend:
    def __init__(self, graph_path):
        self.gateway = None
        self.entry = None
        self.graph_path = graph_path

    def __enter__(self):
        self.gateway = JavaGateway.launch_gateway(
            java_path=None,
            classpath=find_graph_jar(),
            die_on_exit=True,
            redirect_stdout=sys.stdout,
            redirect_stderr=sys.stderr,
        )
        self.entry = self.gateway.jvm.org.softwareheritage.graph.Entry()
        self.entry.load_graph(self.graph_path)
        self.node2pid = IntToPidMap(self.graph_path + '.' + NODE2PID_EXT)
        self.pid2node = PidToIntMap(self.graph_path + '.' + PID2NODE_EXT)
        self.stream_proxy = JavaStreamProxy(self.entry)

    def __exit__(self, exc_type, exc_value, tb):
        self.gateway.shutdown()

    def stats(self):
        return self.entry.stats()

    async def _simple_traversal(self, ttype, direction, edges_fmt, src):
        assert ttype in ('leaves', 'neighbors', 'visit_nodes', 'visit_paths')
        src_id = self.pid2node[src]
        method = getattr(self.stream_proxy, ttype)
        async for node_id in method(direction, edges_fmt, src_id):
            if node_id == -1:  # Path separator
                yield None
            else:
                yield self.node2pid[node_id]

    async def leaves(self, *args):
        async for res_pid in self._simple_traversal('leaves', *args):
            yield res_pid

    async def neighbors(self, *args):
        async for res_pid in self._simple_traversal('neighbors', *args):
            yield res_pid

    async def visit_nodes(self, *args):
        async for res_pid in self._simple_traversal('visit_nodes', *args):
            yield res_pid

    async def walk(self, direction, edges_fmt, algo, src, dst):
        src_id = self.pid2node[src]
        if dst in ('cnt', 'dir', 'rel', 'rev', 'snp', 'ori'):
            it = self.stream_proxy.walk_type(direction, edges_fmt, algo,
                                             src_id, dst)
        else:
            dst_id = self.pid2node[dst]
            it = self.stream_proxy.walk(direction, edges_fmt, algo,
                                        src_id, dst_id)

        async for node_id in it:
            yield self.node2pid[node_id]

    async def visit_paths(self, *args):
        buffer = []
        async for res_pid in self._simple_traversal('visit_paths', *args):
            if res_pid is None:  # Path separator, flush
                yield json.dumps(buffer)
                buffer = []
            else:
                buffer.append(res_pid)


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
        with (await loop.run_in_executor(None, open, fname, 'rb')) as f:
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
        with tempfile.TemporaryDirectory(prefix='swh-graph-') as tmpdirname:
            cli_fifo = os.path.join(tmpdirname, 'swh-graph.fifo')
            os.mkfifo(cli_fifo)
            reader = self.read_node_ids(cli_fifo)
            query_handler = self.entry.get_handler(cli_fifo)
            handler = self._HandlerWrapper(query_handler)
            yield (handler, reader)

    def __getattr__(self, name):
        async def java_call_iterator(*args, **kwargs):
            with self.get_handler() as (handler, reader):
                java_task = getattr(handler, name)(*args, **kwargs)
                async for value in reader:
                    yield value
                await java_task
        return java_call_iterator
