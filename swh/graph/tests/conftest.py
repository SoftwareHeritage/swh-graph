# Copyright (C) 2019-2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import csv
import multiprocessing
from pathlib import Path

from aiohttp.test_utils import TestClient, TestServer, loop_context
import pytest

from swh.graph.client import RemoteGraphClient
from swh.graph.naive_client import NaiveClient

SWH_GRAPH_TESTS_ROOT = Path(__file__).parents[0]
TEST_GRAPH_PATH = SWH_GRAPH_TESTS_ROOT / "dataset/output/example"


class GraphServerProcess(multiprocessing.Process):
    def __init__(self, q, *args, **kwargs):
        self.q = q
        super().__init__(*args, **kwargs)

    def run(self):
        # Lazy import to allow debian packaging
        from swh.graph.backend import Backend
        from swh.graph.server.app import make_app

        try:
            backend = Backend(graph_path=str(TEST_GRAPH_PATH))
            with loop_context() as loop:
                app = make_app(backend=backend, debug=True)
                client = TestClient(TestServer(app), loop=loop)
                loop.run_until_complete(client.start_server())
                url = client.make_url("/graph/")
                self.q.put(url)
                loop.run_forever()
        except Exception as e:
            self.q.put(e)


@pytest.fixture(scope="module", params=["remote", "naive"])
def graph_client(request):
    if request.param == "remote":
        queue = multiprocessing.Queue()
        server = GraphServerProcess(queue)
        server.start()
        res = queue.get()
        if isinstance(res, Exception):
            raise res
        yield RemoteGraphClient(str(res))
        server.terminate()
    else:
        with open(SWH_GRAPH_TESTS_ROOT / "dataset/example.nodes.csv") as fd:
            nodes = [node for (node,) in csv.reader(fd, delimiter=" ")]
        with open(SWH_GRAPH_TESTS_ROOT / "dataset/example.edges.csv") as fd:
            edges = list(csv.reader(fd, delimiter=" "))
        yield NaiveClient(nodes=nodes, edges=edges)
