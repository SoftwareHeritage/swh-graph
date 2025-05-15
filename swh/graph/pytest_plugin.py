# Copyright (C) 2019-2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import contextlib
import logging
import multiprocessing
import socket
import subprocess
import sys
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


class GraphServerProcess(multiprocessing.Process):
    def __init__(self, config, *args, **kwargs):
        self.config = config
        self.q = multiprocessing.Queue()
        super().__init__(*args, **kwargs)

    def run(self):
        # Lazy import to allow debian packaging
        from swh.graph.http_rpc_server import make_app

        print("GraphServerProcess.run", file=sys.stderr)

        try:
            with loop_context() as loop:
                print("GraphServerProcess.run -> loop_context", file=sys.stderr)
                app = make_app(config=self.config)
                print("GraphServerProcess.run -> make_app", file=sys.stderr)
                client = TestClient(TestServer(app), loop=loop)
                print("GraphServerProcess.run -> TestClient", file=sys.stderr)
                loop.run_until_complete(client.start_server())
                print(
                    "GraphServerProcess.run -> loop.run_until_complete", file=sys.stderr
                )
                url = client.make_url("/graph/")
                print("GraphServerProcess.run -> client.make_url", file=sys.stderr)
                self.q.put(
                    {
                        "server_url": url,
                        "rpc_url": app["rpc_url"],
                        "pid": app["local_server"].pid,
                    }
                )
                print("GraphServerProcess.run -> self.q.put", file=sys.stderr)
                loop.run_forever()
                print("GraphServerProcess.run -> loop.run_forever", file=sys.stderr)
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
            print("GraphServerProcess.run -> exc", file=sys.stderr)
            logger.exception(e)
            print("GraphServerProcess.run -> logger.exception", file=sys.stderr)
            self.q.put(e)
        print("GraphServerProcess.run -> done", file=sys.stderr)

    def start(self, *args, **kwargs):
        super().start()
        self.result = self.q.get()


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

    try:
        server.kill()
    except AttributeError:
        # server was never started
        pass


@pytest.fixture(scope="session")
def graph_grpc_server_started(graph_grpc_server_process):
    server = graph_grpc_server_process
    server.start()
    if isinstance(server.result, Exception):
        raise server.result
    yield server
    server.kill()


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
