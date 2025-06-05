# Copyright (C) 2019-2025  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import contextlib
import logging
import multiprocessing
import os
import signal
import socket
import subprocess
import threading

from aiohttp.test_utils import TestClient, TestServer, loop_context
import grpc
import pytest

from swh.graph.example_dataset import DATASET_DIR
from swh.graph.grpc.swhgraph_pb2_grpc import TraversalServiceStub
from swh.graph.grpc_server import ExecutableNotFound
from swh.graph.http_client import RemoteGraphClient
from swh.graph.http_naive_client import NaiveClient

logger = logging.getLogger(__name__)


# The default on Python < 3.14 on Linux is "fork", which is not safe
# as forking a multithreaded process is problematic
class GraphServerProcess(multiprocessing.context.ForkServerProcess):
    def __init__(self, config, *args, **kwargs):
        self.config = config
        self.q = multiprocessing.get_context("forkserver").Queue()
        super().__init__(*args, **kwargs)

    def run(self):
        # Lazy import to allow debian packaging
        from swh.graph.http_rpc_server import make_app

        try:
            with loop_context() as loop:

                async def make_client():
                    return TestClient(TestServer(app))

                app = make_app(config=self.config)
                client = loop.run_until_complete(make_client())
                loop.run_until_complete(client.start_server())
                url = client.make_url("/graph/")
                self.q.put(
                    {
                        "server_url": url,
                        "rpc_url": app["rpc_url"],
                        "pid": app["local_server"].pid,
                    }
                )
                loop.run_forever()
        except Exception as e:
            if isinstance(e, ExecutableNotFound):
                # hack to add a bit more context and help to the user,
                # especially when this is used from another swh package...
                # XXX on py>=3.11 we could use e.add_note() instead
                e.args = (
                    *e.args,
                    "This probably means you need to build the rust grpc server "
                    'for swh-graph. Check the "Minimal setup for tests" section in '
                    "the rust/README.md file in the swh-graph "
                    "source code directory.",
                )
            logger.exception(e)
            self.q.put(e)

    def start(self, *args, **kwargs):
        super().start()
        self.result = self.q.get()

    def stop(self):
        try:
            self.kill()
            # ensure to terminate swh-graph-grpc-serve process spawned
            # by graph HTTP server process
            if not isinstance(self.result, Exception):
                os.kill(self.result["pid"], signal.SIGKILL)
        except AttributeError:
            # server was never started
            pass


class StatsdServer:
    def __init__(self):
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._sock.bind(("127.0.0.1", 0))
        self._sock.settimeout(0.1)
        (self.host, self.port) = self._sock.getsockname()
        self._closing = False
        self._thread = threading.Thread(target=self._listen)
        self._thread.start()
        self.datagrams = []
        self.new_datagram = threading.Event()
        """Woken up every time a datagram is added to self.datagrams."""

    def _listen(self):
        while not self._closing:
            try:
                (datagram, addr) = self._sock.recvfrom(4096)
            except TimeoutError:
                continue
            self.datagrams.append(datagram)
            self.new_datagram.set()
        self._sock.close()

    def close(self):
        self._closing = True


@pytest.fixture(scope="session")
def graph_statsd_server():
    with contextlib.closing(StatsdServer()) as statsd_server:
        yield statsd_server


@pytest.fixture(scope="session", params=["rust"])
def graph_grpc_backend_implementation(request):
    return request.param


@pytest.fixture(scope="session")
def graph_grpc_server_config(graph_grpc_backend_implementation, graph_statsd_server):
    return {
        "graph": {
            "cls": f"local_{graph_grpc_backend_implementation}",
            "grpc_server": {
                "path": DATASET_DIR / "compressed/example",
                "debug": True,
                "statsd_host": graph_statsd_server.host,
                "statsd_port": graph_statsd_server.port,
            },
            "http_rpc_server": {"debug": True},
        }
    }


@pytest.fixture(scope="session")
def graph_grpc_server_process(graph_grpc_server_config, graph_statsd_server):
    server = GraphServerProcess(graph_grpc_server_config)
    yield server
    server.stop()


@pytest.fixture(scope="session")
def graph_grpc_server_started(graph_grpc_server_process):
    server = graph_grpc_server_process
    server.start()
    if isinstance(server.result, Exception):
        raise server.result
    yield server
    server.stop()


@pytest.fixture(scope="module")
def graph_grpc_stub(graph_grpc_server):
    with grpc.insecure_channel(graph_grpc_server) as channel:
        stub = TraversalServiceStub(channel)
        yield stub


@pytest.fixture(scope="module")
def graph_grpc_server(graph_grpc_server_started):
    yield graph_grpc_server_started.result["rpc_url"]


@pytest.fixture(scope="module")
def remote_graph_client_url(graph_grpc_server_started):
    yield str(graph_grpc_server_started.result["server_url"])


@pytest.fixture(scope="module")
def remote_graph_client(graph_grpc_server_started):
    yield RemoteGraphClient(str(graph_grpc_server_started.result["server_url"]))


@pytest.fixture(scope="module")
def naive_graph_client():
    def zstdcat(*files):
        p = subprocess.run(["zstdcat", *files], stdout=subprocess.PIPE)
        return p.stdout.decode()

    edges_dataset = DATASET_DIR / "edges"
    edge_files = edges_dataset.glob("*/*.edges.csv.zst")
    node_files = edges_dataset.glob("*/*.nodes.csv.zst")

    nodes = set(zstdcat(*node_files).strip().split("\n"))
    edge_lines = [line.split() for line in zstdcat(*edge_files).strip().split("\n")]
    edges = [(src, dst) for src, dst, *_ in edge_lines]
    for src, dst in edges:
        nodes.add(src)
        nodes.add(dst)

    yield NaiveClient(nodes=list(nodes), edges=edges)


@pytest.fixture(scope="module", params=["remote", "naive"])
def graph_client(request):
    if request.param == "remote":
        yield request.getfixturevalue("remote_graph_client")
    else:
        yield request.getfixturevalue("naive_graph_client")
