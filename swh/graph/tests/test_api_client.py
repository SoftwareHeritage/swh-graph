def test_stats(graph_client):
    stats = graph_client.stats()

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


def test_leaves(graph_client):
    actual = list(graph_client.leaves(
        'swh:1:ori:0000000000000000000000000000000000000021'
    ))
    expected = [
        'swh:1:cnt:0000000000000000000000000000000000000001',
        'swh:1:cnt:0000000000000000000000000000000000000004',
        'swh:1:cnt:0000000000000000000000000000000000000005',
        'swh:1:cnt:0000000000000000000000000000000000000007'
    ]
    assert set(actual) == set(expected)


def test_neighbors(graph_client):
    actual = list(graph_client.neighbors(
        'swh:1:rev:0000000000000000000000000000000000000009',
        direction='backward'
    ))
    expected = [
        'swh:1:snp:0000000000000000000000000000000000000020',
        'swh:1:rel:0000000000000000000000000000000000000010',
        'swh:1:rev:0000000000000000000000000000000000000013'
    ]
    assert set(actual) == set(expected)


def test_visit_nodes(graph_client):
    actual = list(graph_client.visit_nodes(
        'swh:1:rel:0000000000000000000000000000000000000010',
        edges='rel:rev,rev:rev'
    ))
    expected = [
        'swh:1:rel:0000000000000000000000000000000000000010',
        'swh:1:rev:0000000000000000000000000000000000000009',
        'swh:1:rev:0000000000000000000000000000000000000003'
    ]
    assert set(actual) == set(expected)


def test_visit_paths(graph_client):
    actual = list(graph_client.visit_paths(
        'swh:1:snp:0000000000000000000000000000000000000020',
        edges='snp:*,rev:*'))
    actual = [tuple(path) for path in actual]
    expected = [
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
    ]
    assert set(actual) == set(expected)


def test_walk(graph_client):
    actual = list(graph_client.walk(
        'swh:1:dir:0000000000000000000000000000000000000016', 'rel',
        edges='dir:dir,dir:rev,rev:*',
        direction='backward',
        traversal='bfs'
    ))
    expected = [
        'swh:1:dir:0000000000000000000000000000000000000016',
        'swh:1:dir:0000000000000000000000000000000000000017',
        'swh:1:rev:0000000000000000000000000000000000000018',
        'swh:1:rel:0000000000000000000000000000000000000019'
    ]
    assert set(actual) == set(expected)


def test_count(graph_client):
    print(graph_client)
    actual = graph_client.count_leaves(
        'swh:1:ori:0000000000000000000000000000000000000021'
    )
    assert actual == 4
    actual = graph_client.count_visit_nodes(
        'swh:1:rel:0000000000000000000000000000000000000010',
        edges='rel:rev,rev:rev'
    )
    assert actual == 3
    actual = graph_client.count_neighbors(
        'swh:1:rev:0000000000000000000000000000000000000009',
        direction='backward'
    )
    assert actual == 3