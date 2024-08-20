# Copyright (c) 2022-2023 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import hashlib

import pytest

from swh.graph.grpc.swhgraph_pb2 import GraphDirection, NodeFilter, TraversalRequest

TEST_ORIGIN_ID = "swh:1:ori:{}".format(
    hashlib.sha1(b"https://example.com/swh/graph").hexdigest()
)
TEST_ORIGIN_ID2 = "swh:1:ori:{}".format(
    hashlib.sha1(b"https://example.com/swh/graph2").hexdigest()
)


def get_leaves(
    graph_grpc_stub, src, direction=None, edges=None, max_matching_nodes=None
):
    request = graph_grpc_stub.Traverse(
        TraversalRequest(
            src=src,
            direction=direction,
            edges=edges,
            max_matching_nodes=max_matching_nodes,
            return_nodes=NodeFilter(
                max_traversal_successors=0,
            ),
        )
    )
    return [node.swhid for node in request]


def _check_forward_from_snp(limit, actual_leaves):
    expected_leaves = [
        "swh:1:cnt:0000000000000000000000000000000000000001",
        "swh:1:cnt:0000000000000000000000000000000000000004",
        "swh:1:cnt:0000000000000000000000000000000000000005",
        "swh:1:cnt:0000000000000000000000000000000000000007",
    ]

    if limit == 0:
        assert set(expected_leaves) == set(actual_leaves)
    else:
        assert set(actual_leaves) <= set(expected_leaves)
        assert len(actual_leaves) == min(limit, 4)


def test_forward_from_snp(graph_grpc_stub):
    actual_leaves = get_leaves(
        graph_grpc_stub,
        ["swh:1:snp:0000000000000000000000000000000000000020"],
        GraphDirection.FORWARD,
    )
    _check_forward_from_snp(0, actual_leaves)


@pytest.mark.parametrize("limit", [0, 1, 2, 4, 51 << 31 - 1])
def test_forward_from_snp_with_limit(graph_grpc_stub, limit):
    actual_leaves = get_leaves(
        graph_grpc_stub,
        ["swh:1:snp:0000000000000000000000000000000000000020"],
        max_matching_nodes=limit,
    )
    _check_forward_from_snp(limit, actual_leaves)


def test_forward_from_rel(graph_grpc_stub):
    actual_leaves = get_leaves(
        graph_grpc_stub, ["swh:1:rel:0000000000000000000000000000000000000019"]
    )
    expected_leaves = [
        "swh:1:cnt:0000000000000000000000000000000000000015",
        "swh:1:cnt:0000000000000000000000000000000000000014",
        "swh:1:cnt:0000000000000000000000000000000000000001",
        "swh:1:cnt:0000000000000000000000000000000000000004",
        "swh:1:cnt:0000000000000000000000000000000000000005",
        "swh:1:cnt:0000000000000000000000000000000000000007",
        "swh:1:cnt:0000000000000000000000000000000000000011",
    ]
    assert set(expected_leaves) == set(actual_leaves)


def test_backward_from_leaf(graph_grpc_stub):
    actual_leaves = get_leaves(
        graph_grpc_stub,
        ["swh:1:cnt:0000000000000000000000000000000000000015"],
        direction=GraphDirection.BACKWARD,
    )
    expected_leaves = [
        "swh:1:rel:0000000000000000000000000000000000000019",
        TEST_ORIGIN_ID2,
    ]
    assert set(expected_leaves) == set(actual_leaves)

    actual_leaves2 = get_leaves(
        graph_grpc_stub,
        ["swh:1:cnt:0000000000000000000000000000000000000004"],
        direction=GraphDirection.BACKWARD,
    )
    expected_leaves2 = [
        TEST_ORIGIN_ID,
        TEST_ORIGIN_ID2,
        "swh:1:rel:0000000000000000000000000000000000000019",
    ]
    assert set(expected_leaves2) == set(actual_leaves2)


def test_forward_rev_to_rev_only(graph_grpc_stub):
    actual_leaves = get_leaves(
        graph_grpc_stub,
        ["swh:1:rev:0000000000000000000000000000000000000018"],
        edges="rev:rev",
    )
    expected_leaves = ["swh:1:rev:0000000000000000000000000000000000000003"]
    assert set(expected_leaves) == set(actual_leaves)


def test_forward_dir_to_all(graph_grpc_stub):
    actual_leaves = get_leaves(
        graph_grpc_stub,
        ["swh:1:dir:0000000000000000000000000000000000000008"],
        edges="dir:*",
    )
    expected_leaves = [
        "swh:1:cnt:0000000000000000000000000000000000000004",
        "swh:1:cnt:0000000000000000000000000000000000000005",
        "swh:1:cnt:0000000000000000000000000000000000000001",
        "swh:1:cnt:0000000000000000000000000000000000000007",
    ]
    assert set(expected_leaves) == set(actual_leaves)


def test_backward_cnt_to_dir_dir_to_dir(graph_grpc_stub):
    actual_leaves = get_leaves(
        graph_grpc_stub,
        ["swh:1:cnt:0000000000000000000000000000000000000005"],
        edges="cnt:dir,dir:dir",
        direction=GraphDirection.BACKWARD,
    )
    expected_leaves = ["swh:1:dir:0000000000000000000000000000000000000012"]
    assert set(expected_leaves) == set(actual_leaves)


@pytest.mark.parametrize("limit", [0, 1, 2, 1 << 31 - 1])
def test_backward_cnt_to_dir_dir_to_dir_with_limit(graph_grpc_stub, limit):
    actual_leaves = get_leaves(
        graph_grpc_stub,
        ["swh:1:cnt:0000000000000000000000000000000000000005"],
        edges="cnt:dir,dir:dir",
        direction=GraphDirection.BACKWARD,
        max_matching_nodes=limit,
    )
    expected_leaves = ["swh:1:dir:0000000000000000000000000000000000000012"]
    assert set(expected_leaves) == set(actual_leaves)
