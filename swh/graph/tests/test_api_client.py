from pathlib import Path
import subprocess
import time

import aiohttp.test_utils
import pytest

from swh.graph.client import RemoteGraphClient
from swh.graph.tests import SWH_GRAPH_VERSION


@pytest.fixture(scope='module')
def graph_client():
    swh_graph_root = Path(__file__).parents[3]
    java_dir = swh_graph_root / 'java/server'

    # Compile Java server using maven
    pom_path = java_dir / 'pom.xml'
    subprocess.run(
        ['mvn', '-f', str(pom_path), 'compile', 'assembly:single'], check=True)

    port = aiohttp.test_utils.unused_port()

    # Start Java server
    jar_file = 'swh-graph-{}-jar-with-dependencies.jar'.format(
        SWH_GRAPH_VERSION)
    jar_path = java_dir / 'target' / jar_file
    graph_path = java_dir / 'src/test/dataset/output/example'
    server = subprocess.Popen([
        'java', '-cp', str(jar_path),
        'org.softwareheritage.graph.App', str(graph_path), '-p', str(port)
    ])

    # Make sure the server is entirely started before running the client
    time.sleep(1)

    # Start Python client
    localhost = 'http://0.0.0.0:{}'.format(port)
    client = RemoteGraphClient(localhost)

    yield client

    print('Service teardown')
    server.kill()


class TestEndpoints:
    @pytest.fixture(autouse=True)
    def init_graph_client(self, graph_client):
        self.client = graph_client

    @staticmethod
    def assert_endpoint_output(actual, expected):
        assert set(actual.keys()) == {'result', 'meta'}
        assert set(actual['result']) == set(expected['result'])
        assert actual['meta'] == expected['meta']

    def test_leaves(self):
        actual = self.client.leaves(
            'swh:1:ori:0000000000000000000000000000000000000021'
        )
        expected = {
            'result': [
                'swh:1:cnt:0000000000000000000000000000000000000001',
                'swh:1:cnt:0000000000000000000000000000000000000004',
                'swh:1:cnt:0000000000000000000000000000000000000005',
                'swh:1:cnt:0000000000000000000000000000000000000007'
            ],
            'meta': {
                'nb_edges_accessed': 13
            }
        }
        TestEndpoints.assert_endpoint_output(actual, expected)

    def test_neighbors(self):
        actual = self.client.neighbors(
            'swh:1:rev:0000000000000000000000000000000000000009',
            direction='backward'
        )
        expected = {
            'result': [
                'swh:1:snp:0000000000000000000000000000000000000020',
                'swh:1:rel:0000000000000000000000000000000000000010',
                'swh:1:rev:0000000000000000000000000000000000000013'
            ],
            'meta': {
                'nb_edges_accessed': 3
            }
        }
        TestEndpoints.assert_endpoint_output(actual, expected)

    def test_stats(self):
        stats = self.client.stats()

        assert set(stats.keys()) == {'counts', 'ratios', 'indegree',
                                     'outdegree'}

        assert set(stats['counts'].keys()) == {'nodes', 'edges'}
        assert set(stats['ratios'].keys()) == {'compression', 'bits_per_node',
                                               'bits_per_edge', 'avg_locality'}
        assert set(stats['indegree'].keys()) == {'min', 'max', 'avg'}
        assert set(stats['outdegree'].keys()) == {'min', 'max', 'avg'}

        assert stats['counts']['nodes'] == 21
        assert stats['counts']['edges'] == 23
        assert isinstance(stats['ratios']['compression'], float)
        assert isinstance(stats['ratios']['bits_per_node'], float)
        assert isinstance(stats['ratios']['bits_per_edge'], float)
        assert isinstance(stats['ratios']['avg_locality'], float)
        assert stats['indegree']['min'] == 0
        assert stats['indegree']['max'] == 3
        assert isinstance(stats['indegree']['avg'], float)
        assert stats['outdegree']['min'] == 0
        assert stats['outdegree']['max'] == 3
        assert isinstance(stats['outdegree']['avg'], float)

    def test_visit_nodes(self):
        actual = self.client.visit_nodes(
            'swh:1:rel:0000000000000000000000000000000000000010',
            edges='rel:rev,rev:rev'
        )
        expected = {
            'result': [
                'swh:1:rel:0000000000000000000000000000000000000010',
                'swh:1:rev:0000000000000000000000000000000000000009',
                'swh:1:rev:0000000000000000000000000000000000000003'
            ],
            'meta': {
                'nb_edges_accessed': 4
            }
        }
        TestEndpoints.assert_endpoint_output(actual, expected)

    def test_visit_paths(self):
        actual = self.client.visit_paths(
                      'swh:1:snp:0000000000000000000000000000000000000020',
                      edges='snp:*,rev:*')
        actual['result'] = [tuple(path) for path in actual['result']]
        expected = {
            'result': [
                (
                    'swh:1:snp:0000000000000000000000000000000000000020',
                    'swh:1:rev:0000000000000000000000000000000000000009',
                    'swh:1:rev:0000000000000000000000000000000000000003',
                    'swh:1:dir:0000000000000000000000000000000000000002'
                ),
                (
                    'swh:1:snp:0000000000000000000000000000000000000020',
                    'swh:1:rev:0000000000000000000000000000000000000009',
                    'swh:1:dir:0000000000000000000000000000000000000008'
                ),
                (
                    'swh:1:snp:0000000000000000000000000000000000000020',
                    'swh:1:rel:0000000000000000000000000000000000000010'
                )
            ],
            'meta': {
                'nb_edges_accessed': 10
            }
        }
        TestEndpoints.assert_endpoint_output(actual, expected)

    def test_walk(self):
        actual = self.client.walk(
            'swh:1:dir:0000000000000000000000000000000000000016', 'rel',
            edges='dir:dir,dir:rev,rev:*',
            direction='backward',
            traversal='bfs'
        )
        expected = {
            'result': [
                'swh:1:dir:0000000000000000000000000000000000000016',
                'swh:1:dir:0000000000000000000000000000000000000017',
                'swh:1:rev:0000000000000000000000000000000000000018',
                'swh:1:rel:0000000000000000000000000000000000000019'
            ],
            'meta': {
                'nb_edges_accessed': 3
            }
        }
        TestEndpoints.assert_endpoint_output(actual, expected)
