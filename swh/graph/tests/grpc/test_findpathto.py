# Copyright (c) 2022-2023 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import hashlib

from google.protobuf.field_mask_pb2 import FieldMask
import grpc
import pytest

from swh.graph.grpc.swhgraph_pb2 import (
    FindPathToRequest,
    GraphDirection,
    Node,
    NodeFilter,
    OriginData,
    Path,
    RevisionData,
)

TEST_ORIGIN_ID = "swh:1:ori:{}".format(
    hashlib.sha1(b"https://example.com/swh/graph").hexdigest()
)
TEST_ORIGIN_ID2 = "swh:1:ori:{}".format(
    hashlib.sha1(b"https://example.com/swh/graph2").hexdigest()
)


def get_path(graph_grpc_stub, src, target_types, **kwargs):
    request = graph_grpc_stub.FindPathTo(
        FindPathToRequest(
            src=src,
            target=NodeFilter(types=target_types),
            **kwargs,
        )
    )
    return [node.swhid for node in request.node]


def test_src_errors(graph_grpc_stub):
    with pytest.raises(grpc.RpcError) as exc_info:
        get_path(
            graph_grpc_stub,
            ["swh:1:cnt:0000000000000000000000000000000000000194"],
            "rel",
        )
    assert exc_info.value.code() == grpc.StatusCode.NOT_FOUND

    with pytest.raises(grpc.RpcError) as exc_info:
        get_path(
            graph_grpc_stub,
            ["swh:1:lol:0000000000000000000000000000000000000001"],
            "rev",
        )
    assert exc_info.value.code() == grpc.StatusCode.INVALID_ARGUMENT

    with pytest.raises(grpc.RpcError) as exc_info:
        get_path(
            graph_grpc_stub,
            ["swh:1:cnt:000000000000000000000000000000000000000z"],
            "dir",
        )
    assert exc_info.value.code() == grpc.StatusCode.INVALID_ARGUMENT


def test_edge_errors(graph_grpc_stub):
    with pytest.raises(grpc.RpcError) as exc_info:
        get_path(graph_grpc_stub, [TEST_ORIGIN_ID], "batracien:reptile")
    assert exc_info.value.code() == grpc.StatusCode.INVALID_ARGUMENT


def test_target_errors(graph_grpc_stub):
    with pytest.raises(grpc.RpcError) as exc_info:
        get_path(graph_grpc_stub, [TEST_ORIGIN_ID], "argoumante,eglomatique")
    assert exc_info.value.code() == grpc.StatusCode.INVALID_ARGUMENT


def test_forward_ori_to_first_dir(graph_grpc_stub):
    """Test path between ori 1 and any dir (forward graph)"""
    actual = get_path(graph_grpc_stub, [TEST_ORIGIN_ID], "dir")
    expected = [
        TEST_ORIGIN_ID,
        "swh:1:snp:0000000000000000000000000000000000000020",
        "swh:1:rev:0000000000000000000000000000000000000009",
        "swh:1:dir:0000000000000000000000000000000000000008",
    ]
    assert expected == actual


def test_minimal_fields(graph_grpc_stub):
    """Test path between ori 1 and any dir (forward graph)"""
    request = graph_grpc_stub.FindPathTo(
        FindPathToRequest(
            src=[TEST_ORIGIN_ID],
            target=NodeFilter(types="dir"),
            mask=FieldMask(paths=["node.swhid"]),
        )
    )
    assert request == Path(
        node=[
            Node(
                swhid=TEST_ORIGIN_ID,
            ),
            Node(
                swhid="swh:1:snp:0000000000000000000000000000000000000020",
            ),
            Node(
                swhid="swh:1:rev:0000000000000000000000000000000000000009",
            ),
            Node(
                swhid="swh:1:dir:0000000000000000000000000000000000000008",
            ),
        ]
    )


def test_fields_ori(graph_grpc_stub):
    """Test path between ori 1 and any dir (forward graph)"""
    request = graph_grpc_stub.FindPathTo(
        FindPathToRequest(
            src=[TEST_ORIGIN_ID],
            target=NodeFilter(types="dir"),
            mask=FieldMask(paths=["node.swhid", "node.ori"]),
        )
    )
    assert request == Path(
        node=[
            Node(
                swhid=TEST_ORIGIN_ID,
                ori=OriginData(
                    url="https://example.com/swh/graph",
                ),
            ),
            Node(
                swhid="swh:1:snp:0000000000000000000000000000000000000020",
            ),
            Node(
                swhid="swh:1:rev:0000000000000000000000000000000000000009",
            ),
            Node(
                swhid="swh:1:dir:0000000000000000000000000000000000000008",
            ),
        ]
    )


def test_fields_rev_message(graph_grpc_stub):
    """Test path between ori 1 and any dir (forward graph)"""
    request = graph_grpc_stub.FindPathTo(
        FindPathToRequest(
            src=[TEST_ORIGIN_ID],
            target=NodeFilter(types="dir"),
            mask=FieldMask(paths=["node.swhid", "node.rev.message"]),
        )
    )
    assert request == Path(
        node=[
            Node(
                swhid=TEST_ORIGIN_ID,
            ),
            Node(
                swhid="swh:1:snp:0000000000000000000000000000000000000020",
            ),
            Node(
                swhid="swh:1:rev:0000000000000000000000000000000000000009",
                rev=RevisionData(
                    message=b"Add parser",
                ),
            ),
            Node(
                swhid="swh:1:dir:0000000000000000000000000000000000000008",
            ),
        ]
    )


def test_forward_rel_to_first_cnt(graph_grpc_stub):
    """Test path between rel 19 and any cnt (forward graph)"""
    actual = get_path(
        graph_grpc_stub, ["swh:1:rel:0000000000000000000000000000000000000019"], "cnt"
    )
    expected = [
        "swh:1:rel:0000000000000000000000000000000000000019",
        "swh:1:rev:0000000000000000000000000000000000000018",
        "swh:1:dir:0000000000000000000000000000000000000017",
        "swh:1:cnt:0000000000000000000000000000000000000014",
    ]
    assert expected == actual


def test_backward_dir_to_first_rel(graph_grpc_stub):
    """Test path between dir 16 and any rel (backward graph)"""
    actual = get_path(
        graph_grpc_stub,
        ["swh:1:dir:0000000000000000000000000000000000000016"],
        "rel",
        direction=GraphDirection.BACKWARD,
    )
    expected = [
        "swh:1:dir:0000000000000000000000000000000000000016",
        "swh:1:dir:0000000000000000000000000000000000000017",
        "swh:1:rev:0000000000000000000000000000000000000018",
        "swh:1:rel:0000000000000000000000000000000000000021",  # FIXME: rel:0019 is valid too
    ]
    assert expected == actual


def test_forward_cnt_to_itself(graph_grpc_stub):
    """Test path between cnt 4 and itself (forward graph)"""
    actual = get_path(
        graph_grpc_stub, ["swh:1:cnt:0000000000000000000000000000000000000004"], "cnt"
    )
    expected = ["swh:1:cnt:0000000000000000000000000000000000000004"]
    assert expected == actual


def test_forward_multiple_sources(graph_grpc_stub):
    """Start from ori and rel 19 and find any cnt (forward graph)"""
    actual = get_path(
        graph_grpc_stub, ["swh:1:rel:0000000000000000000000000000000000000019"], "cnt"
    )
    expected = [
        "swh:1:rel:0000000000000000000000000000000000000019",
        "swh:1:rev:0000000000000000000000000000000000000018",
        "swh:1:dir:0000000000000000000000000000000000000017",
        "swh:1:cnt:0000000000000000000000000000000000000014",
    ]
    assert expected == actual


def test_backward_multiple_sources(graph_grpc_stub):
    """Start from cnt 4 and cnt 11 and find any rev (backward graph)"""
    actual = get_path(
        graph_grpc_stub,
        [
            "swh:1:cnt:0000000000000000000000000000000000000004",
            "swh:1:cnt:0000000000000000000000000000000000000011",
        ],
        "rev",
        direction=GraphDirection.BACKWARD,
    )
    expected = [
        "swh:1:cnt:0000000000000000000000000000000000000011",
        "swh:1:dir:0000000000000000000000000000000000000012",
        "swh:1:rev:0000000000000000000000000000000000000013",
    ]
    assert expected == actual


def test_backward_multiple_sources_all_dir_to_ori(graph_grpc_stub):
    """Start from all directories and find any origin (backward graph)"""
    actual = get_path(
        graph_grpc_stub,
        [
            "swh:1:dir:0000000000000000000000000000000000000002",
            "swh:1:dir:0000000000000000000000000000000000000006",
            "swh:1:dir:0000000000000000000000000000000000000008",
            "swh:1:dir:0000000000000000000000000000000000000012",
            "swh:1:dir:0000000000000000000000000000000000000016",
            "swh:1:dir:0000000000000000000000000000000000000017",
        ],
        "ori",
        direction=GraphDirection.BACKWARD,
    )
    expected1 = [
        "swh:1:dir:0000000000000000000000000000000000000008",
        "swh:1:rev:0000000000000000000000000000000000000009",
        "swh:1:snp:0000000000000000000000000000000000000020",
        TEST_ORIGIN_ID,
    ]
    expected2 = [
        "swh:1:dir:0000000000000000000000000000000000000008",
        "swh:1:rev:0000000000000000000000000000000000000009",
        "swh:1:snp:0000000000000000000000000000000000000022",
        TEST_ORIGIN_ID2,
    ]
    expected = [expected1, expected2]
    assert actual in expected


def test_forward_impossible_path(graph_grpc_stub):
    """Impossible path between rev 9 and any release (forward graph)"""
    with pytest.raises(grpc.RpcError) as exc_info:
        get_path(
            graph_grpc_stub,
            ["swh:1:rev:0000000000000000000000000000000000000009"],
            "rel",
        )
    assert exc_info.value.code() == grpc.StatusCode.NOT_FOUND


def test_max_depth(graph_grpc_stub):
    """Path from cnt 15 to any rel with various max depths"""
    actual = get_path(
        graph_grpc_stub,
        ["swh:1:cnt:0000000000000000000000000000000000000015"],
        "rel",
        direction=GraphDirection.BACKWARD,
        max_depth=4,
    )
    expected = [
        "swh:1:cnt:0000000000000000000000000000000000000015",
        "swh:1:dir:0000000000000000000000000000000000000016",
        "swh:1:dir:0000000000000000000000000000000000000017",
        "swh:1:rev:0000000000000000000000000000000000000018",
        "swh:1:rel:0000000000000000000000000000000000000021",  # FIXME: rel:0019 is valid too
    ]
    assert expected == actual

    # Check that it throws NOT_FOUND with max depth = 1
    with pytest.raises(grpc.RpcError) as exc_info:
        get_path(
            graph_grpc_stub,
            ["swh:1:cnt:0000000000000000000000000000000000000015"],
            "rel",
            direction=GraphDirection.BACKWARD,
            max_depth=3,
        )
    assert exc_info.value.code() == grpc.StatusCode.NOT_FOUND


def test_max_edges(graph_grpc_stub):
    """Path from cnt 15 to any rel with various max edges"""
    # FIXME: Number of edges traversed but backtracked from. it changes
    # nondeterministically every time the test dataset is changed.
    backtracked_edges = 0

    actual = get_path(
        graph_grpc_stub,
        ["swh:1:cnt:0000000000000000000000000000000000000015"],
        "rel",
        direction=GraphDirection.BACKWARD,
        max_edges=4 + backtracked_edges,
    )
    expected = [
        "swh:1:cnt:0000000000000000000000000000000000000015",
        "swh:1:dir:0000000000000000000000000000000000000016",
        "swh:1:dir:0000000000000000000000000000000000000017",
        "swh:1:rev:0000000000000000000000000000000000000018",
        "swh:1:rel:0000000000000000000000000000000000000021",  # FIXME: rel:0019 is valid too
    ]
    assert expected == actual

    # Check that it throws NOT_FOUND with max_edges = 3 + backtracked_edges
    with pytest.raises(grpc.RpcError) as exc_info:
        get_path(
            graph_grpc_stub,
            ["swh:1:cnt:0000000000000000000000000000000000000015"],
            "rel",
            direction=GraphDirection.BACKWARD,
            max_edges=3 + backtracked_edges,
        )
    assert exc_info.value.code() == grpc.StatusCode.NOT_FOUND
