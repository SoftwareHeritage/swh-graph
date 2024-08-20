# Copyright (c) 2022-2023 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import hashlib

import grpc
import pytest

from swh.graph.grpc.swhgraph_pb2 import FindPathBetweenRequest, GraphDirection

TEST_ORIGIN_ID = "swh:1:ori:{}".format(
    hashlib.sha1(b"https://example.com/swh/graph").hexdigest()
)
TEST_ORIGIN_ID2 = "swh:1:ori:{}".format(
    hashlib.sha1(b"https://example.com/swh/graph2").hexdigest()
)


def get_path(graph_grpc_stub, src, dst, **kwargs):
    request = graph_grpc_stub.FindPathBetween(
        FindPathBetweenRequest(
            src=src,
            dst=dst,
            **kwargs,
        )
    )
    return ([node.swhid for node in request.node], request.midpoint_index)


def test_src_errors(graph_grpc_stub):
    with pytest.raises(grpc.RpcError) as exc_info:
        get_path(
            graph_grpc_stub,
            ["swh:1:cnt:0000000000000000000000000000000000000194"],
            [TEST_ORIGIN_ID],
        )
    assert exc_info.value.code() == grpc.StatusCode.NOT_FOUND

    with pytest.raises(grpc.RpcError) as exc_info:
        get_path(
            graph_grpc_stub,
            [TEST_ORIGIN_ID],
            ["swh:1:cnt:0000000000000000000000000000000000000194"],
        )
    assert exc_info.value.code() == grpc.StatusCode.NOT_FOUND

    with pytest.raises(grpc.RpcError) as exc_info:
        get_path(
            graph_grpc_stub,
            [TEST_ORIGIN_ID],
            ["swh:1:lol:0000000000000000000000000000000000000001"],
        )
    assert exc_info.value.code() == grpc.StatusCode.INVALID_ARGUMENT

    with pytest.raises(grpc.RpcError) as exc_info:
        get_path(
            graph_grpc_stub,
            ["swh:1:lol:0000000000000000000000000000000000000001"],
            [TEST_ORIGIN_ID],
        )
    assert exc_info.value.code() == grpc.StatusCode.INVALID_ARGUMENT

    with pytest.raises(grpc.RpcError) as exc_info:
        get_path(
            graph_grpc_stub,
            ["swh:1:cnt:000000000000000000000000000000000000000z"],
            [TEST_ORIGIN_ID],
        )
    assert exc_info.value.code() == grpc.StatusCode.INVALID_ARGUMENT

    with pytest.raises(grpc.RpcError) as exc_info:
        get_path(
            graph_grpc_stub,
            [TEST_ORIGIN_ID],
            ["swh:1:cnt:000000000000000000000000000000000000000z"],
        )
    assert exc_info.value.code() == grpc.StatusCode.INVALID_ARGUMENT


def test_edge_errors(graph_grpc_stub):
    with pytest.raises(grpc.RpcError) as exc_info:
        get_path(
            graph_grpc_stub,
            [TEST_ORIGIN_ID],
            [TEST_ORIGIN_ID],
            edges="batracien:reptile",
        )
    assert exc_info.value.code() == grpc.StatusCode.INVALID_ARGUMENT


def test_forward_root_to_leaf(graph_grpc_stub):
    """Test path between ori 1 and cnt 4 (forward graph)"""
    (actual, _midpoint) = get_path(
        graph_grpc_stub,
        [TEST_ORIGIN_ID],
        ["swh:1:cnt:0000000000000000000000000000000000000004"],
    )
    expected = [
        TEST_ORIGIN_ID,
        "swh:1:snp:0000000000000000000000000000000000000020",
        "swh:1:rev:0000000000000000000000000000000000000009",
        "swh:1:dir:0000000000000000000000000000000000000008",
        "swh:1:dir:0000000000000000000000000000000000000006",
        "swh:1:cnt:0000000000000000000000000000000000000004",
    ]
    assert expected == actual


def test_forward_rev_to_rev(graph_grpc_stub):
    """Test path between rev 18 and rev 3 (forward graph)"""
    (actual, _midpoint) = get_path(
        graph_grpc_stub,
        ["swh:1:rev:0000000000000000000000000000000000000018"],
        ["swh:1:rev:0000000000000000000000000000000000000003"],
    )
    expected = [
        "swh:1:rev:0000000000000000000000000000000000000018",
        "swh:1:rev:0000000000000000000000000000000000000013",
        "swh:1:rev:0000000000000000000000000000000000000009",
        "swh:1:rev:0000000000000000000000000000000000000003",
    ]
    assert expected == actual


def test_backward_rev_to_rev(graph_grpc_stub):
    """Test path between rev 3 and rev 18 (backward graph)"""
    (actual, _midpoint) = get_path(
        graph_grpc_stub,
        ["swh:1:rev:0000000000000000000000000000000000000003"],
        ["swh:1:rev:0000000000000000000000000000000000000018"],
        direction=GraphDirection.BACKWARD,
    )
    expected = [
        "swh:1:rev:0000000000000000000000000000000000000003",
        "swh:1:rev:0000000000000000000000000000000000000009",
        "swh:1:rev:0000000000000000000000000000000000000013",
        "swh:1:rev:0000000000000000000000000000000000000018",
    ]
    assert expected == actual


def test_forward_cnt_to_itself(graph_grpc_stub):
    """Test path between cnt 4 and itself (forward graph)"""
    (actual, _midpoint) = get_path(
        graph_grpc_stub,
        ["swh:1:cnt:0000000000000000000000000000000000000004"],
        ["swh:1:cnt:0000000000000000000000000000000000000004"],
    )
    expected = ["swh:1:cnt:0000000000000000000000000000000000000004"]
    assert expected == actual


def test_forward_multiple_sources_dest(graph_grpc_stub):
    """Start from ori and rel 19 and find cnt 14 or cnt 7 (forward graph)"""
    (actual, _midpoint) = get_path(
        graph_grpc_stub,
        ["swh:1:rel:0000000000000000000000000000000000000019", TEST_ORIGIN_ID],
        [
            "swh:1:cnt:0000000000000000000000000000000000000014",
            "swh:1:cnt:0000000000000000000000000000000000000007",
        ],
    )
    expected = [
        "swh:1:rel:0000000000000000000000000000000000000019",
        "swh:1:rev:0000000000000000000000000000000000000018",
        "swh:1:dir:0000000000000000000000000000000000000017",
        "swh:1:cnt:0000000000000000000000000000000000000014",
    ]
    assert expected == actual


def test_backward_multiple_sources_dest(graph_grpc_stub):
    "Start from cnt 4 and cnt 11 and find rev 13 or rev 9 (backward graph)" ""
    (actual, _midpoint) = get_path(
        graph_grpc_stub,
        [
            "swh:1:cnt:0000000000000000000000000000000000000004",
            "swh:1:cnt:0000000000000000000000000000000000000011",
        ],
        [
            "swh:1:rev:0000000000000000000000000000000000000013",
            "swh:1:rev:0000000000000000000000000000000000000009",
        ],
        direction=GraphDirection.BACKWARD,
    )
    expected = [
        "swh:1:cnt:0000000000000000000000000000000000000011",
        "swh:1:dir:0000000000000000000000000000000000000012",
        "swh:1:rev:0000000000000000000000000000000000000013",
    ]
    assert expected == actual


def test_backward_multiple_sources_all_dir_to_ori(graph_grpc_stub):
    """Start from all directories and find the origin (backward graph)"""
    (actual, _midpoint) = get_path(
        graph_grpc_stub,
        [
            "swh:1:dir:0000000000000000000000000000000000000002",
            "swh:1:dir:0000000000000000000000000000000000000006",
            "swh:1:dir:0000000000000000000000000000000000000008",
            "swh:1:dir:0000000000000000000000000000000000000012",
            "swh:1:dir:0000000000000000000000000000000000000016",
            "swh:1:dir:0000000000000000000000000000000000000017",
        ],
        [TEST_ORIGIN_ID],
        direction=GraphDirection.BACKWARD,
    )
    expected = [
        "swh:1:dir:0000000000000000000000000000000000000008",
        "swh:1:rev:0000000000000000000000000000000000000009",
        "swh:1:snp:0000000000000000000000000000000000000020",
        TEST_ORIGIN_ID,
    ]
    assert expected == actual


def test_backward_cnt_to_any_rev(graph_grpc_stub):
    """Start from cnt 4 and find any rev (backward graph)"""
    (actual, _midpoint) = get_path(
        graph_grpc_stub,
        ["swh:1:cnt:0000000000000000000000000000000000000004"],
        [
            "swh:1:rev:0000000000000000000000000000000000000003",
            "swh:1:rev:0000000000000000000000000000000000000009",
            "swh:1:rev:0000000000000000000000000000000000000013",
            "swh:1:rev:0000000000000000000000000000000000000018",
        ],
        direction=GraphDirection.BACKWARD,
    )
    expected = [
        "swh:1:cnt:0000000000000000000000000000000000000004",
        "swh:1:dir:0000000000000000000000000000000000000006",
        "swh:1:dir:0000000000000000000000000000000000000008",
        "swh:1:rev:0000000000000000000000000000000000000009",
    ]
    assert expected == actual


def test_forward_impossible_path(graph_grpc_stub):
    """Impossible path between rev 9 and cnt 14"""
    with pytest.raises(grpc.RpcError) as rpc_error:
        get_path(
            graph_grpc_stub,
            ["swh:1:rev:0000000000000000000000000000000000000009"],
            ["swh:1:cnt:0000000000000000000000000000000000000014"],
        )
    assert rpc_error.value.code() == grpc.StatusCode.NOT_FOUND

    # Reverse direction
    with pytest.raises(grpc.RpcError) as rpc_error:
        get_path(
            graph_grpc_stub,
            ["swh:1:cnt:0000000000000000000000000000000000000014"],
            ["swh:1:rev:0000000000000000000000000000000000000009"],
            direction=GraphDirection.BACKWARD,
        )
    assert rpc_error.value.code() == grpc.StatusCode.NOT_FOUND


def test_common_ancestor_backward_backward(graph_grpc_stub):
    """Common ancestor between cnt 4 and cnt 15 : rev 18"""
    (actual, midpoint) = get_path(
        graph_grpc_stub,
        ["swh:1:cnt:0000000000000000000000000000000000000004"],
        ["swh:1:cnt:0000000000000000000000000000000000000015"],
        direction=GraphDirection.BACKWARD,
        direction_reverse=GraphDirection.BACKWARD,
    )
    expected = [
        "swh:1:cnt:0000000000000000000000000000000000000004",
        "swh:1:dir:0000000000000000000000000000000000000006",
        "swh:1:dir:0000000000000000000000000000000000000008",
        "swh:1:rev:0000000000000000000000000000000000000009",
        "swh:1:rev:0000000000000000000000000000000000000013",
        "swh:1:rev:0000000000000000000000000000000000000018",
        "swh:1:dir:0000000000000000000000000000000000000017",
        "swh:1:dir:0000000000000000000000000000000000000016",
        "swh:1:cnt:0000000000000000000000000000000000000015",
    ]
    assert expected == actual
    assert actual[midpoint] == "swh:1:rev:0000000000000000000000000000000000000018"


def test_common_descendant_forward_forward(graph_grpc_stub):
    """Common descendant between rev 13 and rev 3 : cnt 1 (with rev:dir,dir:dir,dir:cnt)"""
    (actual, midpoint) = get_path(
        graph_grpc_stub,
        ["swh:1:rev:0000000000000000000000000000000000000013"],
        ["swh:1:rev:0000000000000000000000000000000000000003"],
        direction=GraphDirection.FORWARD,
        direction_reverse=GraphDirection.FORWARD,
        edges="rev:dir,dir:dir,dir:cnt",
    )
    expected = "swh:1:cnt:0000000000000000000000000000000000000001"
    assert expected == actual[midpoint]


def test_common_descendant_forward_forward_edges_reverse(graph_grpc_stub):
    """Common descendant between rev 13 and rev 3 : cnt 1 (with rev:dir,dir:dir,dir:cnt)"""
    (actual, midpoint) = get_path(
        graph_grpc_stub,
        ["swh:1:rev:0000000000000000000000000000000000000013"],
        ["swh:1:rev:0000000000000000000000000000000000000003"],
        direction=GraphDirection.FORWARD,
        direction_reverse=GraphDirection.FORWARD,
        edges="rev:dir,dir:dir,dir:cnt",
        edges_reverse="rev:dir,dir:dir,dir:cnt",
    )
    expected = "swh:1:cnt:0000000000000000000000000000000000000001"
    assert expected == actual[midpoint]


def test_max_depth(graph_grpc_stub):
    """Path between rel 19 and cnt 15 with various max depths"""
    # Works with max_depth = 2
    (actual, _midpoint) = get_path(
        graph_grpc_stub,
        ["swh:1:rel:0000000000000000000000000000000000000019"],
        ["swh:1:cnt:0000000000000000000000000000000000000015"],
        max_depth=2,
    )
    expected = [
        "swh:1:rel:0000000000000000000000000000000000000019",
        "swh:1:rev:0000000000000000000000000000000000000018",
        "swh:1:dir:0000000000000000000000000000000000000017",
        "swh:1:dir:0000000000000000000000000000000000000016",
        "swh:1:cnt:0000000000000000000000000000000000000015",
    ]
    assert expected == actual

    # Check that it throws NOT_FOUND with max depth = 1
    with pytest.raises(grpc.RpcError) as rpc_error:
        print(
            get_path(
                graph_grpc_stub,
                ["swh:1:rel:0000000000000000000000000000000000000019"],
                ["swh:1:cnt:0000000000000000000000000000000000000015"],
                max_depth=1,
            )
        )
    assert rpc_error.value.code() == grpc.StatusCode.NOT_FOUND


def test_max_edges(graph_grpc_stub):
    """Path between rel 19 and cnt 15 with various max edges"""
    # Works with max_edges = 3
    (actual, _midpoint) = get_path(
        graph_grpc_stub,
        ["swh:1:rel:0000000000000000000000000000000000000019"],
        ["swh:1:cnt:0000000000000000000000000000000000000015"],
        max_edges=3,
    )
    expected = [
        "swh:1:rel:0000000000000000000000000000000000000019",
        "swh:1:rev:0000000000000000000000000000000000000018",
        "swh:1:dir:0000000000000000000000000000000000000017",
        "swh:1:dir:0000000000000000000000000000000000000016",
        "swh:1:cnt:0000000000000000000000000000000000000015",
    ]
    assert expected == actual

    # Check that it throws NOT_FOUND with max_edges = 2
    with pytest.raises(grpc.RpcError) as rpc_error:
        get_path(
            graph_grpc_stub,
            ["swh:1:rel:0000000000000000000000000000000000000019"],
            ["swh:1:cnt:0000000000000000000000000000000000000015"],
            max_edges=2,
        )
    assert rpc_error.value.code() == grpc.StatusCode.NOT_FOUND
