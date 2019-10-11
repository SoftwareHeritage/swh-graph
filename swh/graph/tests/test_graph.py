import pytest


def test_graph(graph):
    assert len(graph) == 21

    obj = 'swh:1:dir:0000000000000000000000000000000000000008'
    node = graph[obj]

    assert str(node) == obj
    assert len(node.children()) == 3
    assert len(node.parents()) == 2

    actual = {p.pid for p in node.children()}
    expected = {
        'swh:1:cnt:0000000000000000000000000000000000000001',
        'swh:1:dir:0000000000000000000000000000000000000006',
        'swh:1:cnt:0000000000000000000000000000000000000007'
    }
    assert expected == actual

    actual = {p.pid for p in node.parents()}
    expected = {
        'swh:1:rev:0000000000000000000000000000000000000009',
        'swh:1:dir:0000000000000000000000000000000000000012',
    }
    assert expected == actual


def test_invalid_pid(graph):
    with pytest.raises(IndexError):
        graph[1337]

    with pytest.raises(IndexError):
        graph[len(graph) + 1]

    with pytest.raises(KeyError):
        graph['swh:1:dir:0000000000000000000000000000000420000012']


def test_leaves(graph):
    actual = list(graph['swh:1:ori:0000000000000000000000000000000000000021']
                  .leaves())
    actual = [p.pid for p in actual]
    expected = [
        'swh:1:cnt:0000000000000000000000000000000000000001',
        'swh:1:cnt:0000000000000000000000000000000000000004',
        'swh:1:cnt:0000000000000000000000000000000000000005',
        'swh:1:cnt:0000000000000000000000000000000000000007'
    ]
    assert set(actual) == set(expected)


def test_visit_nodes(graph):
    actual = list(graph['swh:1:rel:0000000000000000000000000000000000000010']
                  .visit_nodes(edges='rel:rev,rev:rev'))
    actual = [p.pid for p in actual]
    expected = [
        'swh:1:rel:0000000000000000000000000000000000000010',
        'swh:1:rev:0000000000000000000000000000000000000009',
        'swh:1:rev:0000000000000000000000000000000000000003'
    ]
    assert set(actual) == set(expected)


def test_visit_paths(graph):
    actual = list(graph['swh:1:snp:0000000000000000000000000000000000000020']
                  .visit_paths(edges='snp:*,rev:*'))
    actual = [tuple(n.pid for n in path) for path in actual]
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


def test_walk(graph):
    actual = list(graph['swh:1:dir:0000000000000000000000000000000000000016']
                  .walk('rel',
                        edges='dir:dir,dir:rev,rev:*',
                        direction='backward',
                        traversal='bfs'))
    actual = [p.pid for p in actual]
    expected = [
        'swh:1:dir:0000000000000000000000000000000000000016',
        'swh:1:dir:0000000000000000000000000000000000000017',
        'swh:1:rev:0000000000000000000000000000000000000018',
        'swh:1:rel:0000000000000000000000000000000000000019'
    ]
    assert set(actual) == set(expected)


def test_count(graph):
    assert (graph['swh:1:ori:0000000000000000000000000000000000000021']
            .count_leaves() == 4)
    assert (graph['swh:1:rel:0000000000000000000000000000000000000010']
            .count_visit_nodes(edges='rel:rev,rev:rev') == 3)
    assert (graph['swh:1:rev:0000000000000000000000000000000000000009']
            .count_neighbors(direction='backward') == 3)


def test_iter_type(graph):
    rev_list = list(graph.iter_type('rev'))
    actual = [n.pid for n in rev_list]
    expected = ['swh:1:rev:0000000000000000000000000000000000000003',
                'swh:1:rev:0000000000000000000000000000000000000009',
                'swh:1:rev:0000000000000000000000000000000000000013',
                'swh:1:rev:0000000000000000000000000000000000000018']
    assert expected == actual
