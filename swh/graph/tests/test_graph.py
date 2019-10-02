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
