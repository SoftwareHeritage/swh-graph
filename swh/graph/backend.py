# Copyright (C) 2019-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import asyncio
import contextlib
import io
import os
import re
import subprocess
import sys
import tempfile

from py4j.java_gateway import JavaGateway
from py4j.protocol import Py4JJavaError

from swh.graph.config import check_config

BUF_LINES = 1024


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

    def start_gateway(self):
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
        self.stream_proxy = JavaStreamProxy(self.entry)

    def stop_gateway(self):
        self.gateway.shutdown()

    def __enter__(self):
        self.start_gateway()
        return self

    def __exit__(self, exc_type, exc_value, tb):
        self.stop_gateway()

    def stats(self):
        return self.entry.stats()

    def check_swhid(self, swhid):
        try:
            self.entry.check_swhid(swhid)
        except Py4JJavaError as e:
            m = re.search(r"malformed SWHID: (\w+)", str(e))
            if m:
                raise ValueError(f"malformed SWHID: {m[1]}")
            m = re.search(r"Unknown SWHID: (\w+)", str(e))
            if m:
                raise NameError(f"Unknown SWHID: {m[1]}")
            raise

    def count(self, ttype, *args):
        method = getattr(self.entry, "count_" + ttype)
        return method(*args)

    async def traversal(self, ttype, *args):
        method = getattr(self.stream_proxy, ttype)
        async for line in method(*args):
            yield line.decode().rstrip("\n")


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

            def read_n_lines(f, n):
                buf = []
                for _ in range(n):
                    try:
                        buf.append(next(f))
                    except StopIteration:
                        break
                return buf

            while True:
                lines = await loop.run_in_executor(None, read_n_lines, f, BUF_LINES)
                if not lines:
                    break
                for line in lines:
                    yield line

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
