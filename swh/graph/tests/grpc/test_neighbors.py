# Copyright (c) 2022-2023 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import hashlib

from swh.graph.grpc.swhgraph_pb2 import GraphDirection, TraversalRequest

TEST_ORIGIN_ID = "swh:1:ori:{}".format(
    hashlib.sha1(b"https://example.com/swh/graph").hexdigest()
)
TEST_ORIGIN_ID2 = "swh:1:ori:{}".format(
    hashlib.sha1(b"https://example.com/swh/graph2").hexdigest()
)


def traverse_nodes(graph_grpc_stub, src, direction=None, edges=None):
    request = graph_grpc_stub.Traverse(
        TraversalRequest(
            src=src,
            min_depth=1,
            max_depth=1,
            direction=direction,
            edges=edges,
        )
    )
    return [node.swhid for node in request]


def test_zero_neighbor(graph_grpc_stub):
    expected_nodes = []

    actual_nodes = traverse_nodes(
        graph_grpc_stub, [TEST_ORIGIN_ID], direction=GraphDirection.BACKWARD
    )
    assert set(actual_nodes) == set(expected_nodes)

    actual_nodes = traverse_nodes(
        graph_grpc_stub, ["swh:1:cnt:0000000000000000000000000000000000000004"]
    )
    assert set(actual_nodes) == set(expected_nodes)

    actual_nodes = traverse_nodes(
        graph_grpc_stub, ["swh:1:cnt:0000000000000000000000000000000000000015"]
    )
    assert set(actual_nodes) == set(expected_nodes)

    actual_nodes = traverse_nodes(
        graph_grpc_stub,
        ["swh:1:rel:0000000000000000000000000000000000000019"],
        direction=GraphDirection.BACKWARD,
    )
    assert set(actual_nodes) == set(expected_nodes)

    actual_nodes = traverse_nodes(
        graph_grpc_stub,
        ["swh:1:dir:0000000000000000000000000000000000000008"],
        edges="snp:*,rev:*,rel:*",
    )
    assert set(actual_nodes) == set(expected_nodes)


def test_one_neighbor(graph_grpc_stub):
    expected_nodes = []

    actual_nodes = traverse_nodes(
        graph_grpc_stub, ["swh:1:rev:0000000000000000000000000000000000000003"]
    )
    expected_nodes = ["swh:1:dir:0000000000000000000000000000000000000002"]
    assert set(actual_nodes) == set(expected_nodes)

    actual_nodes = traverse_nodes(
        graph_grpc_stub,
        ["swh:1:dir:0000000000000000000000000000000000000017"],
        edges="dir:cnt",
    )
    expected_nodes = ["swh:1:cnt:0000000000000000000000000000000000000014"]
    assert set(actual_nodes) == set(expected_nodes)

    actual_nodes = traverse_nodes(
        graph_grpc_stub,
        ["swh:1:dir:0000000000000000000000000000000000000012"],
        direction=GraphDirection.BACKWARD,
    )
    expected_nodes = ["swh:1:rev:0000000000000000000000000000000000000013"]
    assert set(actual_nodes) == set(expected_nodes)

    actual_nodes = traverse_nodes(
        graph_grpc_stub,
        ["swh:1:rev:0000000000000000000000000000000000000009"],
        direction=GraphDirection.BACKWARD,
        edges="rev:rev",
    )
    expected_nodes = ["swh:1:rev:0000000000000000000000000000000000000013"]
    assert set(actual_nodes) == set(expected_nodes)

    actual_nodes = traverse_nodes(
        graph_grpc_stub,
        ["swh:1:snp:0000000000000000000000000000000000000020"],
        direction=GraphDirection.BACKWARD,
    )
    expected_nodes = [TEST_ORIGIN_ID]
    assert set(actual_nodes) == set(expected_nodes)


def test_two_neighbors(graph_grpc_stub):
    expected_nodes = []

    actual_nodes = traverse_nodes(
        graph_grpc_stub, ["swh:1:snp:0000000000000000000000000000000000000020"]
    )
    expected_nodes = [
        "swh:1:rel:0000000000000000000000000000000000000010",
        "swh:1:rev:0000000000000000000000000000000000000009",
    ]
    assert set(actual_nodes) == set(expected_nodes)

    actual_nodes = traverse_nodes(
        graph_grpc_stub,
        ["swh:1:dir:0000000000000000000000000000000000000008"],
        edges="dir:cnt",
    )
    expected_nodes = [
        "swh:1:cnt:0000000000000000000000000000000000000001",
        "swh:1:cnt:0000000000000000000000000000000000000007",
    ]
    assert set(actual_nodes) == set(expected_nodes)

    actual_nodes = traverse_nodes(
        graph_grpc_stub,
        ["swh:1:cnt:0000000000000000000000000000000000000001"],
        direction=GraphDirection.BACKWARD,
    )
    expected_nodes = [
        "swh:1:dir:0000000000000000000000000000000000000008",
        "swh:1:dir:0000000000000000000000000000000000000002",
    ]
    assert set(actual_nodes) == set(expected_nodes)

    actual_nodes = traverse_nodes(
        graph_grpc_stub,
        ["swh:1:rev:0000000000000000000000000000000000000009"],
        direction=GraphDirection.BACKWARD,
        edges="rev:snp,rev:rel",
    )
    expected_nodes = [
        "swh:1:snp:0000000000000000000000000000000000000020",
        "swh:1:rel:0000000000000000000000000000000000000010",
        "swh:1:snp:0000000000000000000000000000000000000022",
    ]
    assert set(actual_nodes) == set(expected_nodes)


def test_three_neighbors(graph_grpc_stub):
    expected_nodes = []

    actual_nodes = traverse_nodes(
        graph_grpc_stub, ["swh:1:dir:0000000000000000000000000000000000000008"]
    )
    expected_nodes = [
        "swh:1:dir:0000000000000000000000000000000000000006",
        "swh:1:cnt:0000000000000000000000000000000000000001",
        "swh:1:cnt:0000000000000000000000000000000000000007",
    ]
    assert set(actual_nodes) == set(expected_nodes)

    actual_nodes = traverse_nodes(
        graph_grpc_stub,
        ["swh:1:rev:0000000000000000000000000000000000000009"],
        direction=GraphDirection.BACKWARD,
    )
    expected_nodes = [
        "swh:1:snp:0000000000000000000000000000000000000020",
        "swh:1:rel:0000000000000000000000000000000000000010",
        "swh:1:rev:0000000000000000000000000000000000000013",
        "swh:1:snp:0000000000000000000000000000000000000022",
    ]
    assert set(actual_nodes) == set(expected_nodes)
