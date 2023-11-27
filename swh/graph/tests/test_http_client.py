# Copyright (c) 2022 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import hashlib
import logging

import pytest
from pytest import raises

from swh.core.api import RemoteException
from swh.graph.http_client import GraphArgumentException

TEST_ORIGIN_ID = "swh:1:ori:{}".format(
    hashlib.sha1(b"https://example.com/swh/graph").hexdigest()
)


def test_remote_graph_client_with_bad_url(remote_graph_client_url):
    from ..http_client import RemoteGraphClient

    with pytest.raises(ValueError, match="URL is incorrect"):
        _ = RemoteGraphClient(f"{remote_graph_client_url}/GARBAGE/")


def test_remote_graph_client_with_service_down():
    from ..http_client import GraphAPIError, RemoteGraphClient

    with pytest.raises(GraphAPIError):
        _ = RemoteGraphClient("http://localhost:9/graph/")


def test_remote_graph_client_errors_on_unexpected_stats(
    mocker, remote_graph_client_url
):
    from ..http_client import RemoteGraphClient

    mocker.patch.object(RemoteGraphClient, "stats", return_value={})
    with pytest.raises(ValueError, match="unexpected results"):
        _ = RemoteGraphClient(remote_graph_client_url)


def test_remote_graph_client_log_export_started_at(
    mocker, caplog, remote_graph_client_url
):
    from ..http_client import RemoteGraphClient

    caplog.set_level(logging.DEBUG, logger="swh.graph.http_client")
    mocker.patch.object(
        RemoteGraphClient,
        "stats",
        return_value={"num_nodes": 42, "export_started_at": 1700000000},
    )
    _ = RemoteGraphClient(remote_graph_client_url)
    assert "Graph export started at 2023-11-14T22:13:20+00:00 (42 nodes)" in [
        rec.message for rec in caplog.records
    ]


def test_stats(graph_client):
    stats = graph_client.stats()
    assert stats["num_nodes"] == 24
    assert stats["num_edges"] == 28
    assert isinstance(stats["compression_ratio"], float)
    assert isinstance(stats["bits_per_node"], float)
    assert isinstance(stats["bits_per_edge"], float)
    assert isinstance(stats["avg_locality"], float)
    assert stats["indegree_min"] == 0
    assert stats["indegree_max"] == 4
    assert isinstance(stats["indegree_avg"], float)
    assert stats["outdegree_min"] == 0
    assert stats["outdegree_max"] == 3
    assert isinstance(stats["outdegree_avg"], float)
    assert stats["export_started_at"] == 1669888200
    assert stats["export_ended_at"] == 1669899600


def test_leaves(graph_client):
    actual = list(graph_client.leaves(TEST_ORIGIN_ID))
    expected = [
        "swh:1:cnt:0000000000000000000000000000000000000001",
        "swh:1:cnt:0000000000000000000000000000000000000004",
        "swh:1:cnt:0000000000000000000000000000000000000005",
        "swh:1:cnt:0000000000000000000000000000000000000007",
    ]
    assert set(actual) == set(expected)


@pytest.mark.parametrize("max_matching_nodes", [0, 1, 2, 3, 4, 5, 10, 1 << 31])
def test_leaves_with_limit(graph_client, max_matching_nodes):
    actual = list(
        graph_client.leaves(TEST_ORIGIN_ID, max_matching_nodes=max_matching_nodes)
    )
    expected = [
        "swh:1:cnt:0000000000000000000000000000000000000001",
        "swh:1:cnt:0000000000000000000000000000000000000004",
        "swh:1:cnt:0000000000000000000000000000000000000005",
        "swh:1:cnt:0000000000000000000000000000000000000007",
    ]

    if max_matching_nodes == 0:
        assert set(actual) == set(expected)
    else:
        assert set(actual) <= set(expected)
        assert len(actual) == min(4, max_matching_nodes)


def test_empty_neighbors(graph_client):
    # https://gitlab.softwareheritage.org/swh/devel/swh-graph/-/issues/4790
    actual = list(
        graph_client.neighbors(
            "swh:1:rev:0000000000000000000000000000000000000009",
            direction="forward",
            # We are guaranteed no results because no ori→snp edges exist from a rev
            edges="ori:snp",
        )
    )
    assert actual == []


def test_neighbors(graph_client):
    actual = list(
        graph_client.neighbors(
            "swh:1:rev:0000000000000000000000000000000000000009", direction="backward"
        )
    )
    expected = [
        "swh:1:snp:0000000000000000000000000000000000000022",
        "swh:1:snp:0000000000000000000000000000000000000020",
        "swh:1:rel:0000000000000000000000000000000000000010",
        "swh:1:rev:0000000000000000000000000000000000000013",
    ]
    assert set(actual) == set(expected)


def test_visit_nodes(graph_client):
    actual = list(
        graph_client.visit_nodes(
            "swh:1:rel:0000000000000000000000000000000000000010",
            edges="rel:rev,rev:rev",
        )
    )
    expected = [
        "swh:1:rel:0000000000000000000000000000000000000010",
        "swh:1:rev:0000000000000000000000000000000000000009",
        "swh:1:rev:0000000000000000000000000000000000000003",
    ]
    assert set(actual) == set(expected)


@pytest.mark.parametrize("max_matching_nodes", [0, 1, 2, 3, 4, 5, 10, 1 << 31])
def test_visit_nodes_limit(graph_client, max_matching_nodes):
    actual = list(
        graph_client.visit_nodes(
            "swh:1:rel:0000000000000000000000000000000000000010",
            edges="rel:rev,rev:rev",
            max_matching_nodes=max_matching_nodes,
        )
    )
    expected = [
        "swh:1:rel:0000000000000000000000000000000000000010",
        "swh:1:rev:0000000000000000000000000000000000000009",
        "swh:1:rev:0000000000000000000000000000000000000003",
    ]
    if max_matching_nodes == 0:
        assert set(actual) == set(expected)
    else:
        assert set(actual) <= set(expected)
        assert len(actual) == min(3, max_matching_nodes)


def test_visit_nodes_filtered(graph_client):
    actual = list(
        graph_client.visit_nodes(
            "swh:1:rel:0000000000000000000000000000000000000010",
            return_types="dir",
        )
    )
    expected = [
        "swh:1:dir:0000000000000000000000000000000000000002",
        "swh:1:dir:0000000000000000000000000000000000000008",
        "swh:1:dir:0000000000000000000000000000000000000006",
    ]
    assert set(actual) == set(expected)


@pytest.mark.parametrize("max_matching_nodes", [0, 1, 2, 3, 4, 5, 10, 1 << 31])
def test_visit_nodes_filtered_limit(graph_client, max_matching_nodes):
    actual = list(
        graph_client.visit_nodes(
            "swh:1:rel:0000000000000000000000000000000000000010",
            return_types="dir",
            max_matching_nodes=max_matching_nodes,
        )
    )
    expected = [
        "swh:1:dir:0000000000000000000000000000000000000002",
        "swh:1:dir:0000000000000000000000000000000000000008",
        "swh:1:dir:0000000000000000000000000000000000000006",
    ]
    if max_matching_nodes == 0:
        assert set(actual) == set(expected)
    else:
        assert set(actual) <= set(expected)
        assert len(actual) == min(3, max_matching_nodes)


def test_visit_nodes_filtered_star(graph_client):
    actual = list(
        graph_client.visit_nodes(
            "swh:1:rel:0000000000000000000000000000000000000010",
            return_types="*",
        )
    )
    expected = [
        "swh:1:rel:0000000000000000000000000000000000000010",
        "swh:1:rev:0000000000000000000000000000000000000009",
        "swh:1:rev:0000000000000000000000000000000000000003",
        "swh:1:dir:0000000000000000000000000000000000000002",
        "swh:1:cnt:0000000000000000000000000000000000000001",
        "swh:1:dir:0000000000000000000000000000000000000008",
        "swh:1:cnt:0000000000000000000000000000000000000007",
        "swh:1:dir:0000000000000000000000000000000000000006",
        "swh:1:cnt:0000000000000000000000000000000000000004",
        "swh:1:cnt:0000000000000000000000000000000000000005",
    ]
    assert set(actual) == set(expected)


def test_visit_edges(graph_client):
    actual = list(
        graph_client.visit_edges(
            "swh:1:rel:0000000000000000000000000000000000000010",
            edges="rel:rev,rev:rev,rev:dir",
        )
    )
    expected = [
        (
            "swh:1:rel:0000000000000000000000000000000000000010",
            "swh:1:rev:0000000000000000000000000000000000000009",
        ),
        (
            "swh:1:rev:0000000000000000000000000000000000000009",
            "swh:1:rev:0000000000000000000000000000000000000003",
        ),
        (
            "swh:1:rev:0000000000000000000000000000000000000009",
            "swh:1:dir:0000000000000000000000000000000000000008",
        ),
        (
            "swh:1:rev:0000000000000000000000000000000000000003",
            "swh:1:dir:0000000000000000000000000000000000000002",
        ),
    ]
    assert set(actual) == set(expected)


def test_visit_edges_limited(graph_client):
    actual = list(
        graph_client.visit_edges(
            "swh:1:rel:0000000000000000000000000000000000000010",
            max_edges=4,
            edges="rel:rev,rev:rev,rev:dir",
        )
    )
    expected = [
        (
            "swh:1:rel:0000000000000000000000000000000000000010",
            "swh:1:rev:0000000000000000000000000000000000000009",
        ),
        (
            "swh:1:rev:0000000000000000000000000000000000000009",
            "swh:1:rev:0000000000000000000000000000000000000003",
        ),
        (
            "swh:1:rev:0000000000000000000000000000000000000009",
            "swh:1:dir:0000000000000000000000000000000000000008",
        ),
        (
            "swh:1:rev:0000000000000000000000000000000000000003",
            "swh:1:dir:0000000000000000000000000000000000000002",
        ),
    ]

    # As there are four valid answers (up to reordering), we cannot check for
    # equality. Instead, we check the client returned either
    # * all edges but one, or
    # * all edges
    # and the right answer depends on which edges were traversed, which is
    # non-deterministic
    assert set(actual).issubset(set(expected))
    assert 3 <= len(actual) <= 4


def test_visit_edges_diamond_pattern(graph_client):
    actual = list(
        graph_client.visit_edges(
            "swh:1:rev:0000000000000000000000000000000000000009",
            edges="*",
        )
    )
    expected = [
        (
            "swh:1:rev:0000000000000000000000000000000000000009",
            "swh:1:rev:0000000000000000000000000000000000000003",
        ),
        (
            "swh:1:rev:0000000000000000000000000000000000000009",
            "swh:1:dir:0000000000000000000000000000000000000008",
        ),
        (
            "swh:1:rev:0000000000000000000000000000000000000003",
            "swh:1:dir:0000000000000000000000000000000000000002",
        ),
        (
            "swh:1:dir:0000000000000000000000000000000000000002",
            "swh:1:cnt:0000000000000000000000000000000000000001",
        ),
        (
            "swh:1:dir:0000000000000000000000000000000000000008",
            "swh:1:cnt:0000000000000000000000000000000000000001",
        ),
        (
            "swh:1:dir:0000000000000000000000000000000000000008",
            "swh:1:cnt:0000000000000000000000000000000000000007",
        ),
        (
            "swh:1:dir:0000000000000000000000000000000000000008",
            "swh:1:dir:0000000000000000000000000000000000000006",
        ),
        (
            "swh:1:dir:0000000000000000000000000000000000000006",
            "swh:1:cnt:0000000000000000000000000000000000000004",
        ),
        (
            "swh:1:dir:0000000000000000000000000000000000000006",
            "swh:1:cnt:0000000000000000000000000000000000000005",
        ),
    ]
    assert set(actual) == set(expected)


@pytest.mark.skip(reason="currently disabled due to T1969")
def test_walk(graph_client):
    args = ("swh:1:dir:0000000000000000000000000000000000000016", "rel")
    kwargs = {
        "edges": "dir:dir,dir:rev,rev:*",
        "direction": "backward",
        "traversal": "bfs",
    }

    actual = list(graph_client.walk(*args, **kwargs))
    expected = [
        "swh:1:dir:0000000000000000000000000000000000000016",
        "swh:1:dir:0000000000000000000000000000000000000017",
        "swh:1:rev:0000000000000000000000000000000000000018",
        "swh:1:rel:0000000000000000000000000000000000000019",
    ]
    assert set(actual) == set(expected)

    kwargs2 = kwargs.copy()
    kwargs2["limit"] = -1
    actual = list(graph_client.walk(*args, **kwargs2))
    expected = ["swh:1:rel:0000000000000000000000000000000000000019"]
    assert set(actual) == set(expected)

    kwargs2 = kwargs.copy()
    kwargs2["limit"] = 2
    actual = list(graph_client.walk(*args, **kwargs2))
    expected = [
        "swh:1:dir:0000000000000000000000000000000000000016",
        "swh:1:dir:0000000000000000000000000000000000000017",
    ]
    assert set(actual) == set(expected)


@pytest.mark.skip(reason="Random walk is deprecated")
def test_random_walk_dst_is_type(graph_client):
    """as the walk is random, we test a visit from a cnt node to a release
    reachable from every single path in the backward graph, and only check the
    final node of the path (i.e., the release)
    """
    args = ("swh:1:cnt:0000000000000000000000000000000000000015", "rel")
    kwargs = {"direction": "backward"}
    expected_root = "swh:1:rel:0000000000000000000000000000000000000019"

    actual = list(graph_client.random_walk(*args, **kwargs))
    assert len(actual) > 1  # no release directly links to a content
    assert actual[0] == args[0]
    assert actual[-1] == expected_root

    kwargs2 = kwargs.copy()
    kwargs2["limit"] = -1
    actual = list(graph_client.random_walk(*args, **kwargs2))
    assert actual == [expected_root]

    kwargs2["limit"] = -2
    actual = list(graph_client.random_walk(*args, **kwargs2))
    assert len(actual) == 2
    assert actual[-1] == expected_root

    kwargs2["limit"] = 3
    actual = list(graph_client.random_walk(*args, **kwargs2))
    assert len(actual) == 3


@pytest.mark.skip(reason="Random walk is deprecated")
def test_random_walk_dst_is_node(graph_client):
    """Same as test_random_walk_dst_is_type, but we target the specific release
    node instead of a type
    """
    args = (
        "swh:1:cnt:0000000000000000000000000000000000000015",
        "swh:1:rel:0000000000000000000000000000000000000019",
    )
    kwargs = {"direction": "backward"}
    expected_root = "swh:1:rel:0000000000000000000000000000000000000019"

    actual = list(graph_client.random_walk(*args, **kwargs))
    assert len(actual) > 1  # no origin directly links to a content
    assert actual[0] == args[0]
    assert actual[-1] == expected_root

    kwargs2 = kwargs.copy()
    kwargs2["limit"] = -1
    actual = list(graph_client.random_walk(*args, **kwargs2))
    assert actual == [expected_root]

    kwargs2["limit"] = -2
    actual = list(graph_client.random_walk(*args, **kwargs2))
    assert len(actual) == 2
    assert actual[-1] == expected_root

    kwargs2["limit"] = 3
    actual = list(graph_client.random_walk(*args, **kwargs2))
    assert len(actual) == 3


def test_count(graph_client):
    actual = graph_client.count_leaves(TEST_ORIGIN_ID)
    assert actual == 4
    actual = graph_client.count_visit_nodes(
        "swh:1:rel:0000000000000000000000000000000000000010", edges="rel:rev,rev:rev"
    )
    assert actual == 3
    actual = graph_client.count_neighbors(
        "swh:1:rev:0000000000000000000000000000000000000009", direction="backward"
    )
    assert actual == 4


@pytest.mark.parametrize("max_matching_nodes", [0, 1, 2, 3, 4, 5, 10, 1 << 31])
def test_count_with_limit(graph_client, max_matching_nodes):
    actual = graph_client.count_leaves(
        TEST_ORIGIN_ID, max_matching_nodes=max_matching_nodes
    )
    if max_matching_nodes == 0:
        assert actual == 4
    else:
        assert actual == min(4, max_matching_nodes)


def test_param_validation(graph_client):
    with raises(GraphArgumentException) as exc_info:  # SWHID not found
        list(graph_client.leaves("swh:1:rel:00ffffffff000000000000000000000000000010"))
    if exc_info.value.response:
        assert exc_info.value.response.status_code == 404

    with raises(GraphArgumentException) as exc_info:  # malformed SWHID
        list(
            graph_client.neighbors("swh:1:rel:00ffffffff00000000zzzzzzz000000000000010")
        )
    if exc_info.value.response:
        assert exc_info.value.response.status_code == 400

    with raises(GraphArgumentException) as exc_info:  # malformed edge specificaiton
        list(
            graph_client.visit_nodes(
                "swh:1:dir:0000000000000000000000000000000000000016",
                edges="dir:notanodetype,dir:rev,rev:*",
                direction="backward",
            )
        )
    if exc_info.value.response:
        assert exc_info.value.response.status_code == 400

    with raises(GraphArgumentException) as exc_info:  # malformed direction
        list(
            graph_client.visit_nodes(
                "swh:1:dir:0000000000000000000000000000000000000016",
                edges="dir:dir,dir:rev,rev:*",
                direction="notadirection",
            )
        )
    if exc_info.value.response:
        assert exc_info.value.response.status_code == 400


@pytest.mark.skip(reason="currently disabled due to T1969")
def test_param_validation_walk(graph_client):
    """test validation of walk-specific parameters only"""
    with raises(RemoteException) as exc_info:  # malformed traversal order
        list(
            graph_client.walk(
                "swh:1:dir:0000000000000000000000000000000000000016",
                "rel",
                edges="dir:dir,dir:rev,rev:*",
                direction="backward",
                traversal="notatraversalorder",
            )
        )
    assert exc_info.value.response.status_code == 400
