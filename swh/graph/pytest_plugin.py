# Copyright (C) 2019-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
import multiprocessing
import subprocess

from aiohttp.test_utils import TestClient, TestServer, loop_context
import grpc
import pytest

from swh.graph.example_dataset import DATASET_DIR
from swh.graph.grpc.swhgraph_pb2_grpc import TraversalServiceStub
from swh.graph.http_client import RemoteGraphClient
from swh.graph.http_naive_client import NaiveClient

logger = logging.getLogger(__name__)


class GraphServerProcess(multiprocessing.Process):
    def __init__(self, *args, **kwargs):
        self.q = multiprocessing.Queue()
        super().__init__(*args, **kwargs)

    def run(self):
        # Lazy import to allow debian packaging
        from swh.graph.http_rpc_server import make_app

        try:
            config = {
                "graph": {
                    "cls": "local",
                    "grpc_server": {"path": DATASET_DIR / "compressed/example"},
                    "http_rpc_server": {"debug": True},
                }
            }
            with loop_context() as loop:
                app = make_app(config=config)
                client = TestClient(TestServer(app), loop=loop)
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
            logger.exception(e)
            self.q.put(e)

    def start(self, *args, **kwargs):
        super().start()
        self.result = self.q.get()


@pytest.fixture(scope="module")
def graph_grpc_server_process():
    server = GraphServerProcess()

    yield server

    try:
        server.kill()
    except AttributeError:
        # server was never started
        pass


@pytest.fixture(scope="module")
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
