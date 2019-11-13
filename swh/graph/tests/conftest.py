import multiprocessing
import pytest

from aiohttp.test_utils import TestServer, TestClient, loop_context
from pathlib import Path

from swh.graph.graph import load as graph_load
from swh.graph.client import RemoteGraphClient
from swh.graph.backend import Backend
from swh.graph.server.app import make_app

SWH_GRAPH_TESTS_ROOT = Path(__file__).parents[0]
TEST_GRAPH_PATH = SWH_GRAPH_TESTS_ROOT / 'dataset/output/example'


class GraphServerProcess(multiprocessing.Process):
    def __init__(self, q, *args, **kwargs):
        self.q = q
        super().__init__(*args, **kwargs)

    def run(self):
        try:
            backend = Backend(graph_path=str(TEST_GRAPH_PATH))
            with backend:
                with loop_context() as loop:
                    app = make_app(backend=backend, debug=True)
                    client = TestClient(TestServer(app), loop=loop)
                    loop.run_until_complete(client.start_server())
                    url = client.make_url('/graph/')
                    self.q.put(url)
                    loop.run_forever()
        except Exception as e:
            self.q.put(e)


@pytest.fixture(scope="module")
def graph_client():
    queue = multiprocessing.Queue()
    server = GraphServerProcess(queue)
    server.start()
    res = queue.get()
    if isinstance(res, Exception):
        raise res
    yield RemoteGraphClient(str(res))
    server.terminate()


@pytest.fixture(scope="module")
def graph():
    with graph_load(str(TEST_GRAPH_PATH)) as g:
        yield g
