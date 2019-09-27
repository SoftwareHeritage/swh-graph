import multiprocessing
import pytest
from pathlib import Path

from aiohttp.test_utils import TestServer, TestClient, loop_context

from swh.graph.client import RemoteGraphClient
from swh.graph.server.backend import Backend
from swh.graph.server.app import make_app

SWH_GRAPH_ROOT = Path(__file__).parents[3]
TEST_GRAPH_PATH = SWH_GRAPH_ROOT / 'tests/dataset/output/example'


class GraphServerProcess(multiprocessing.Process):
    def __init__(self, q, *args, **kwargs):
        self.q = q
        super().__init__(*args, **kwargs)

    def run(self):
        backend = Backend(graph_path=str(TEST_GRAPH_PATH))
        with backend:
            with loop_context() as loop:
                self.loop = loop
                app = make_app(backend=backend)
                client = TestClient(TestServer(app), loop=loop)
                loop.run_until_complete(client.start_server())
                url = client.make_url('/graph/')
                self.q.put(url)
                loop.run_forever()


@pytest.fixture(scope="module")
def graph_client():
    queue = multiprocessing.Queue()
    server = GraphServerProcess(queue)
    server.start()
    url = queue.get()
    yield RemoteGraphClient(str(url))
    server.terminate()
