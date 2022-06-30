# Copyright (C) 2019-2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import multiprocessing
from pathlib import Path
import subprocess

from aiohttp.test_utils import TestClient, TestServer, loop_context
import pytest

from swh.graph.http_client import RemoteGraphClient
from swh.graph.http_naive_client import NaiveClient

SWH_GRAPH_TESTS_ROOT = Path(__file__).parents[0]
TEST_GRAPH_PATH = SWH_GRAPH_TESTS_ROOT / "dataset/compressed/example"


class GraphServerProcess(multiprocessing.Process):
    def __init__(self, q, *args, **kwargs):
        self.q = q
        super().__init__(*args, **kwargs)

    def run(self):
        # Lazy import to allow debian packaging
        from swh.graph.http_server import make_app

        try:
            config = {"graph": {"path": TEST_GRAPH_PATH}}
            with loop_context() as loop:
                app = make_app(config=config, debug=True)
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

        def zstdcat(*files):
            p = subprocess.run(["zstdcat", *files], stdout=subprocess.PIPE)
            return p.stdout.decode()

        edges_dataset = SWH_GRAPH_TESTS_ROOT / "dataset/edges"
        edge_files = edges_dataset.glob("*/*.edges.csv.zst")
        node_files = edges_dataset.glob("*/*.nodes.csv.zst")

        nodes = set(zstdcat(*node_files).strip().split("\n"))
        edge_lines = [line.split() for line in zstdcat(*edge_files).strip().split("\n")]
        edges = [(src, dst) for src, dst, *_ in edge_lines]
        for src, dst in edges:
            nodes.add(src)
            nodes.add(dst)

        yield NaiveClient(nodes=list(nodes), edges=edges)
