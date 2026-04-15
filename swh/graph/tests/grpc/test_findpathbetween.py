# Copyright (C) 2022-2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import hashlib

from google.protobuf.field_mask_pb2 import FieldMask
import grpc
import pytest

from swh.graph.grpc.swhgraph_pb2 import (
    EdgeLabel,
    FindPathBetweenRequest,
    GraphDirection,
    LabeledNode,
    Node,
)

TEST_ORIGIN_ID = "swh:1:ori:{}".format(
    hashlib.sha1(b"https://example.com/swh/graph").hexdigest()
)
TEST_ORIGIN_ID2 = "swh:1:ori:{}".format(
    hashlib.sha1(b"https://example.com/swh/graph2").hexdigest()
)

parametrize = pytest.mark.parametrize(
    "labeled", [pytest.param(False, id="unlabeled"), pytest.param(True, id="labeled")]
)


@parametrize
def get_path(graph_grpc_stub, src, dst, labeled, **kwargs):
    request = graph_grpc_stub.FindPathBetween(
        FindPathBetweenRequest(
            src=src,
            dst=dst,
            **kwargs,
        )
    )
    if not labeled:
        return ([node.swhid for node in request.node], request.midpoint_index)
    else:
        return (
            [labeled_node.node.swhid for labeled_node in request.labeled_node],
            request.midpoint_index,
        )


@parametrize
def test_src_errors(graph_grpc_stub, labeled):
    with pytest.raises(grpc.RpcError) as exc_info:
        get_path(
            graph_grpc_stub,
            ["swh:1:cnt:0000000000000000000000000000000000000194"],
            [TEST_ORIGIN_ID],
            labeled=labeled,
        )
    assert exc_info.value.code() == grpc.StatusCode.NOT_FOUND

    with pytest.raises(grpc.RpcError) as exc_info:
        get_path(
            graph_grpc_stub,
            [TEST_ORIGIN_ID],
            ["swh:1:cnt:0000000000000000000000000000000000000194"],
            labeled=labeled,
        )
    assert exc_info.value.code() == grpc.StatusCode.NOT_FOUND

    with pytest.raises(grpc.RpcError) as exc_info:
        get_path(
            graph_grpc_stub,
            [TEST_ORIGIN_ID],
            ["swh:1:lol:0000000000000000000000000000000000000001"],
            labeled=labeled,
        )
    assert exc_info.value.code() == grpc.StatusCode.INVALID_ARGUMENT

    with pytest.raises(grpc.RpcError) as exc_info:
        get_path(
            graph_grpc_stub,
            ["swh:1:lol:0000000000000000000000000000000000000001"],
            [TEST_ORIGIN_ID],
            labeled=labeled,
        )
    assert exc_info.value.code() == grpc.StatusCode.INVALID_ARGUMENT

    with pytest.raises(grpc.RpcError) as exc_info:
        get_path(
            graph_grpc_stub,
            ["swh:1:cnt:000000000000000000000000000000000000000z"],
            [TEST_ORIGIN_ID],
            labeled=labeled,
        )
    assert exc_info.value.code() == grpc.StatusCode.INVALID_ARGUMENT

    with pytest.raises(grpc.RpcError) as exc_info:
        get_path(
            graph_grpc_stub,
            [TEST_ORIGIN_ID],
            ["swh:1:cnt:000000000000000000000000000000000000000z"],
            labeled=labeled,
        )
    assert exc_info.value.code() == grpc.StatusCode.INVALID_ARGUMENT


@parametrize
def test_edge_errors(graph_grpc_stub, labeled):
    with pytest.raises(grpc.RpcError) as exc_info:
        get_path(
            graph_grpc_stub,
            [TEST_ORIGIN_ID],
            [TEST_ORIGIN_ID],
            labeled=labeled,
            edges="batracien:reptile",
        )
    assert exc_info.value.code() == grpc.StatusCode.INVALID_ARGUMENT


@parametrize
def test_forward_root_to_leaf(graph_grpc_stub, labeled):
    """Test path between ori 1 and cnt 4 (forward graph)"""
    (actual, _midpoint) = get_path(
        graph_grpc_stub,
        [TEST_ORIGIN_ID],
        ["swh:1:cnt:0000000000000000000000000000000000000004"],
        labeled=labeled,
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


def test_labels_forward_root_to_leaf(graph_grpc_stub):
    """Test labeled path between ori 1 and cnt 4 (forward graph)"""
    request = graph_grpc_stub.FindPathBetween(
        FindPathBetweenRequest(
            src=[TEST_ORIGIN_ID],
            dst=["swh:1:cnt:0000000000000000000000000000000000000004"],
            mask=FieldMask(paths=["labeled_node.node.swhid", "labeled_node.label"]),
        )
    )
    expected = [
        LabeledNode(
            node=Node(swhid=TEST_ORIGIN_ID),
            label=[EdgeLabel(visit_timestamp=1367900441, is_full_visit=True)],
        ),
        LabeledNode(
            node=Node(swhid="swh:1:snp:0000000000000000000000000000000000000020"),
            label=[EdgeLabel(name=b"refs/heads/master")],
        ),
        LabeledNode(
            node=Node(swhid="swh:1:rev:0000000000000000000000000000000000000009"),
            label=[],
        ),
        LabeledNode(
            node=Node(swhid="swh:1:dir:0000000000000000000000000000000000000008"),
            label=[EdgeLabel(name=b"tests", permission=33261)],
        ),
        LabeledNode(
            node=Node(swhid="swh:1:dir:0000000000000000000000000000000000000006"),
            label=[EdgeLabel(name=b"README.rst", permission=33188)],
        ),
        LabeledNode(
            node=Node(swhid="swh:1:cnt:0000000000000000000000000000000000000004"),
            label=[],
        ),
    ]
    assert expected == list(request.labeled_node)


def test_labels_backward_root_to_leaf(graph_grpc_stub):
    """Test labeled path between cnt 4 and ori 1 (backward graph)"""
    request = graph_grpc_stub.FindPathBetween(
        FindPathBetweenRequest(
            src=["swh:1:cnt:0000000000000000000000000000000000000004"],
            dst=[TEST_ORIGIN_ID],
            direction=GraphDirection.BACKWARD,
            mask=FieldMask(paths=["labeled_node.node.swhid", "labeled_node.label"]),
        )
    )
    expected = [
        LabeledNode(
            node=Node(swhid="swh:1:cnt:0000000000000000000000000000000000000004"),
            label=[EdgeLabel(name=b"README.rst", permission=33188)],
        ),
        LabeledNode(
            node=Node(swhid="swh:1:dir:0000000000000000000000000000000000000006"),
            label=[EdgeLabel(name=b"tests", permission=33261)],
        ),
        LabeledNode(
            node=Node(swhid="swh:1:dir:0000000000000000000000000000000000000008"),
            label=[],
        ),
        LabeledNode(
            node=Node(swhid="swh:1:rev:0000000000000000000000000000000000000009"),
            label=[EdgeLabel(name=b"refs/heads/master")],
        ),
        LabeledNode(
            node=Node(swhid="swh:1:snp:0000000000000000000000000000000000000020"),
            label=[EdgeLabel(visit_timestamp=1367900441, is_full_visit=True)],
        ),
        LabeledNode(
            node=Node(swhid=TEST_ORIGIN_ID),
            label=[],
        ),
    ]
    assert expected == list(request.labeled_node)


@parametrize
def test_forward_rev_to_rev(graph_grpc_stub, labeled):
    """Test path between rev 18 and rev 3 (forward graph)"""
    (actual, _midpoint) = get_path(
        graph_grpc_stub,
        ["swh:1:rev:0000000000000000000000000000000000000018"],
        ["swh:1:rev:0000000000000000000000000000000000000003"],
        labeled=labeled,
    )
    expected = [
        "swh:1:rev:0000000000000000000000000000000000000018",
        "swh:1:rev:0000000000000000000000000000000000000013",
        "swh:1:rev:0000000000000000000000000000000000000009",
        "swh:1:rev:0000000000000000000000000000000000000003",
    ]
    assert expected == actual


def test_labels_forward_rev_to_rev(graph_grpc_stub):
    """Test labeled path between rev 18 and rev 3 (forward graph)"""
    request = graph_grpc_stub.FindPathBetween(
        FindPathBetweenRequest(
            src=["swh:1:rev:0000000000000000000000000000000000000018"],
            dst=["swh:1:rev:0000000000000000000000000000000000000003"],
            mask=FieldMask(paths=["labeled_node.node.swhid", "labeled_node.label"]),
        )
    )
    expected = [
        LabeledNode(
            node=Node(swhid="swh:1:rev:0000000000000000000000000000000000000018"),
            label=[],
        ),
        LabeledNode(
            node=Node(swhid="swh:1:rev:0000000000000000000000000000000000000013"),
            label=[],
        ),
        LabeledNode(
            node=Node(swhid="swh:1:rev:0000000000000000000000000000000000000009"),
            label=[],
        ),
        LabeledNode(
            node=Node(swhid="swh:1:rev:0000000000000000000000000000000000000003"),
            label=[],
        ),
    ]
    assert expected == list(request.labeled_node)


@parametrize
def test_backward_rev_to_rev(graph_grpc_stub, labeled):
    """Test path between rev 3 and rev 18 (backward graph)"""
    (actual, _midpoint) = get_path(
        graph_grpc_stub,
        ["swh:1:rev:0000000000000000000000000000000000000003"],
        ["swh:1:rev:0000000000000000000000000000000000000018"],
        labeled=labeled,
        direction=GraphDirection.BACKWARD,
    )
    expected = [
        "swh:1:rev:0000000000000000000000000000000000000003",
        "swh:1:rev:0000000000000000000000000000000000000009",
        "swh:1:rev:0000000000000000000000000000000000000013",
        "swh:1:rev:0000000000000000000000000000000000000018",
    ]
    assert expected == actual


def test_labels_backward_rev_to_rev(graph_grpc_stub):
    """Test labeled path between rev 18 and rev 3 (backward graph)"""
    request = graph_grpc_stub.FindPathBetween(
        FindPathBetweenRequest(
            src=["swh:1:rev:0000000000000000000000000000000000000003"],
            dst=["swh:1:rev:0000000000000000000000000000000000000018"],
            direction=GraphDirection.BACKWARD,
            mask=FieldMask(paths=["labeled_node.node.swhid", "labeled_node.label"]),
        )
    )
    expected = [
        LabeledNode(
            node=Node(swhid="swh:1:rev:0000000000000000000000000000000000000003"),
            label=[],
        ),
        LabeledNode(
            node=Node(swhid="swh:1:rev:0000000000000000000000000000000000000009"),
            label=[],
        ),
        LabeledNode(
            node=Node(swhid="swh:1:rev:0000000000000000000000000000000000000013"),
            label=[],
        ),
        LabeledNode(
            node=Node(swhid="swh:1:rev:0000000000000000000000000000000000000018"),
            label=[],
        ),
    ]
    assert expected == list(request.labeled_node)


@parametrize
def test_forward_cnt_to_itself(graph_grpc_stub, labeled):
    """Test path between cnt 4 and itself (forward graph)"""
    (actual, _midpoint) = get_path(
        graph_grpc_stub,
        ["swh:1:cnt:0000000000000000000000000000000000000004"],
        ["swh:1:cnt:0000000000000000000000000000000000000004"],
        labeled=labeled,
    )
    expected = ["swh:1:cnt:0000000000000000000000000000000000000004"]
    assert expected == actual


def test_labels_forward_cnt_to_itself(graph_grpc_stub):
    """Test labeled path between rev 18 and rev 3 (backward graph)"""
    request = graph_grpc_stub.FindPathBetween(
        FindPathBetweenRequest(
            src=["swh:1:cnt:0000000000000000000000000000000000000004"],
            dst=["swh:1:cnt:0000000000000000000000000000000000000004"],
            mask=FieldMask(paths=["labeled_node.node.swhid", "labeled_node.label"]),
        )
    )
    expected = [
        LabeledNode(
            node=Node(swhid="swh:1:cnt:0000000000000000000000000000000000000004"),
            label=[],
        ),
    ]
    assert expected == list(request.labeled_node)


@parametrize
def test_forward_multiple_sources_dest(graph_grpc_stub, labeled):
    """Start from ori and rel 19 and find cnt 14 or cnt 7 (forward graph)"""
    (actual, _midpoint) = get_path(
        graph_grpc_stub,
        ["swh:1:rel:0000000000000000000000000000000000000019", TEST_ORIGIN_ID],
        [
            "swh:1:cnt:0000000000000000000000000000000000000014",
            "swh:1:cnt:0000000000000000000000000000000000000007",
        ],
        labeled=labeled,
    )
    expected = [
        "swh:1:rel:0000000000000000000000000000000000000019",
        "swh:1:rev:0000000000000000000000000000000000000018",
        "swh:1:dir:0000000000000000000000000000000000000017",
        "swh:1:cnt:0000000000000000000000000000000000000014",
    ]
    assert expected == actual


def test_labels_forward_multiple_sources_dest(graph_grpc_stub):
    """Start from ori and rel 19 and find cnt 14 or cnt 7 (forward graph)"""
    request = graph_grpc_stub.FindPathBetween(
        FindPathBetweenRequest(
            src=["swh:1:rel:0000000000000000000000000000000000000019", TEST_ORIGIN_ID],
            dst=[
                "swh:1:cnt:0000000000000000000000000000000000000014",
                "swh:1:cnt:0000000000000000000000000000000000000007",
            ],
            mask=FieldMask(paths=["labeled_node.node.swhid", "labeled_node.label"]),
        )
    )
    expected = [
        LabeledNode(
            node=Node(swhid="swh:1:rel:0000000000000000000000000000000000000019"),
            label=[],
        ),
        LabeledNode(
            node=Node(swhid="swh:1:rev:0000000000000000000000000000000000000018"),
            label=[],
        ),
        LabeledNode(
            node=Node(swhid="swh:1:dir:0000000000000000000000000000000000000017"),
            label=[EdgeLabel(name=b"TODO.txt", permission=33188)],
        ),
        LabeledNode(
            node=Node(swhid="swh:1:cnt:0000000000000000000000000000000000000014"),
            label=[],
        ),
    ]
    assert expected == list(request.labeled_node)


@parametrize
def test_backward_multiple_sources_dest(graph_grpc_stub, labeled):
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
        labeled=labeled,
        direction=GraphDirection.BACKWARD,
    )
    expected = [
        "swh:1:cnt:0000000000000000000000000000000000000011",
        "swh:1:dir:0000000000000000000000000000000000000012",
        "swh:1:rev:0000000000000000000000000000000000000013",
    ]
    assert expected == actual


def test_labels_backward_multiple_sources_dest(graph_grpc_stub):
    "Start from cnt 4 and cnt 11 and find rev 13 or rev 9 (backward graph)" ""
    request = graph_grpc_stub.FindPathBetween(
        FindPathBetweenRequest(
            src=[
                "swh:1:cnt:0000000000000000000000000000000000000004",
                "swh:1:cnt:0000000000000000000000000000000000000011",
            ],
            dst=[
                "swh:1:rev:0000000000000000000000000000000000000013",
                "swh:1:rev:0000000000000000000000000000000000000009",
            ],
            direction=GraphDirection.BACKWARD,
            mask=FieldMask(paths=["labeled_node.node.swhid", "labeled_node.label"]),
        )
    )
    expected = [
        LabeledNode(
            node=Node(swhid="swh:1:cnt:0000000000000000000000000000000000000011"),
            label=[EdgeLabel(name=b"README.md", permission=33188)],
        ),
        LabeledNode(
            node=Node(swhid="swh:1:dir:0000000000000000000000000000000000000012"),
            label=[],
        ),
        LabeledNode(
            node=Node(swhid="swh:1:rev:0000000000000000000000000000000000000013"),
            label=[],
        ),
    ]
    assert expected == list(request.labeled_node)


@parametrize
def test_backward_multiple_sources_all_dir_to_ori(graph_grpc_stub, labeled):
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
        labeled=labeled,
        direction=GraphDirection.BACKWARD,
    )
    expected = [
        "swh:1:dir:0000000000000000000000000000000000000008",
        "swh:1:rev:0000000000000000000000000000000000000009",
        "swh:1:snp:0000000000000000000000000000000000000020",
        TEST_ORIGIN_ID,
    ]
    assert expected == actual


def test_labels_backward_multiple_sources_all_dir_to_ori(graph_grpc_stub):
    """Start from all directories and find the origin (backward graph)"""
    request = graph_grpc_stub.FindPathBetween(
        FindPathBetweenRequest(
            src=[
                "swh:1:dir:0000000000000000000000000000000000000002",
                "swh:1:dir:0000000000000000000000000000000000000006",
                "swh:1:dir:0000000000000000000000000000000000000008",
                "swh:1:dir:0000000000000000000000000000000000000012",
                "swh:1:dir:0000000000000000000000000000000000000016",
                "swh:1:dir:0000000000000000000000000000000000000017",
            ],
            dst=[TEST_ORIGIN_ID],
            direction=GraphDirection.BACKWARD,
            mask=FieldMask(paths=["labeled_node.node.swhid", "labeled_node.label"]),
        )
    )
    expected = [
        LabeledNode(
            node=Node(swhid="swh:1:dir:0000000000000000000000000000000000000008"),
            label=[],
        ),
        LabeledNode(
            node=Node(swhid="swh:1:rev:0000000000000000000000000000000000000009"),
            label=[EdgeLabel(name=b"refs/heads/master")],
        ),
        LabeledNode(
            node=Node(swhid="swh:1:snp:0000000000000000000000000000000000000020"),
            label=[EdgeLabel(visit_timestamp=1367900441, is_full_visit=True)],
        ),
        LabeledNode(
            node=Node(swhid=TEST_ORIGIN_ID),
            label=[],
        ),
    ]
    assert expected == list(request.labeled_node)


@parametrize
def test_backward_cnt_to_any_rev(graph_grpc_stub, labeled):
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
        labeled=labeled,
        direction=GraphDirection.BACKWARD,
    )
    expected = [
        "swh:1:cnt:0000000000000000000000000000000000000004",
        "swh:1:dir:0000000000000000000000000000000000000006",
        "swh:1:dir:0000000000000000000000000000000000000008",
        "swh:1:rev:0000000000000000000000000000000000000009",
    ]
    assert expected == actual


def test_labels_backward_cnt_to_any_rev(graph_grpc_stub):
    """Start from cnt 4 and find any rev (backward graph)"""
    request = graph_grpc_stub.FindPathBetween(
        FindPathBetweenRequest(
            src=["swh:1:cnt:0000000000000000000000000000000000000004"],
            dst=[
                "swh:1:rev:0000000000000000000000000000000000000003",
                "swh:1:rev:0000000000000000000000000000000000000009",
                "swh:1:rev:0000000000000000000000000000000000000013",
                "swh:1:rev:0000000000000000000000000000000000000018",
            ],
            direction=GraphDirection.BACKWARD,
            mask=FieldMask(paths=["labeled_node.node.swhid", "labeled_node.label"]),
        )
    )
    expected = [
        LabeledNode(
            node=Node(swhid="swh:1:cnt:0000000000000000000000000000000000000004"),
            label=[EdgeLabel(name=b"README.rst", permission=33188)],
        ),
        LabeledNode(
            node=Node(swhid="swh:1:dir:0000000000000000000000000000000000000006"),
            label=[EdgeLabel(name=b"tests", permission=33261)],
        ),
        LabeledNode(
            node=Node(swhid="swh:1:dir:0000000000000000000000000000000000000008"),
            label=[],
        ),
        LabeledNode(
            node=Node(swhid="swh:1:rev:0000000000000000000000000000000000000009"),
            label=[],
        ),
    ]
    assert expected == list(request.labeled_node)


@parametrize
def test_forward_impossible_path(graph_grpc_stub, labeled):
    """Impossible path between rev 9 and cnt 14"""
    with pytest.raises(grpc.RpcError) as rpc_error:
        get_path(
            graph_grpc_stub,
            ["swh:1:rev:0000000000000000000000000000000000000009"],
            ["swh:1:cnt:0000000000000000000000000000000000000014"],
            labeled=labeled,
        )
    assert rpc_error.value.code() == grpc.StatusCode.NOT_FOUND

    # Reverse direction
    with pytest.raises(grpc.RpcError) as rpc_error:
        get_path(
            graph_grpc_stub,
            ["swh:1:cnt:0000000000000000000000000000000000000014"],
            ["swh:1:rev:0000000000000000000000000000000000000009"],
            labeled=labeled,
            direction=GraphDirection.BACKWARD,
        )
    assert rpc_error.value.code() == grpc.StatusCode.NOT_FOUND


@parametrize
def test_common_ancestor_backward_backward(graph_grpc_stub, labeled):
    """Common ancestor between cnt 4 and cnt 15 : rev 18"""
    (actual, midpoint) = get_path(
        graph_grpc_stub,
        ["swh:1:cnt:0000000000000000000000000000000000000004"],
        ["swh:1:cnt:0000000000000000000000000000000000000015"],
        labeled=labeled,
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


def test_labels_common_ancestor_backward_backward(graph_grpc_stub):
    """Common ancestor between cnt 4 and cnt 15 : rev 18"""
    request = graph_grpc_stub.FindPathBetween(
        FindPathBetweenRequest(
            src=["swh:1:cnt:0000000000000000000000000000000000000004"],
            dst=["swh:1:cnt:0000000000000000000000000000000000000015"],
            direction=GraphDirection.BACKWARD,
            direction_reverse=GraphDirection.BACKWARD,
            mask=FieldMask(paths=["labeled_node.node.swhid", "labeled_node.label"]),
        )
    )
    expected = [
        LabeledNode(
            node=Node(swhid="swh:1:cnt:0000000000000000000000000000000000000004"),
            label=[EdgeLabel(name=b"README.rst", permission=33188)],
        ),
        LabeledNode(
            node=Node(swhid="swh:1:dir:0000000000000000000000000000000000000006"),
            label=[EdgeLabel(name=b"tests", permission=33261)],
        ),
        LabeledNode(
            node=Node(swhid="swh:1:dir:0000000000000000000000000000000000000008"),
            label=[],
        ),
        LabeledNode(
            node=Node(swhid="swh:1:rev:0000000000000000000000000000000000000009"),
            label=[],
        ),
        LabeledNode(
            node=Node(swhid="swh:1:rev:0000000000000000000000000000000000000013"),
            label=[],
        ),
        LabeledNode(
            node=Node(swhid="swh:1:rev:0000000000000000000000000000000000000018"),
            label=[],
        ),
        LabeledNode(
            node=Node(swhid="swh:1:dir:0000000000000000000000000000000000000017"),
            label=[EdgeLabel(name=b"old", permission=33261)],
        ),
        LabeledNode(
            node=Node(swhid="swh:1:dir:0000000000000000000000000000000000000016"),
            label=[EdgeLabel(name=b"TODO.txt", permission=33188)],
        ),
        LabeledNode(
            node=Node(swhid="swh:1:cnt:0000000000000000000000000000000000000015"),
            label=[],
        ),
    ]
    assert expected == list(request.labeled_node)
    assert (
        list(request.labeled_node)[request.midpoint_index].node.swhid
        == "swh:1:rev:0000000000000000000000000000000000000018"
    )


@parametrize
def test_common_descendant_forward_forward(graph_grpc_stub, labeled):
    """Common descendant between rev 13 and rev 3 : cnt 1 (with rev:dir,dir:dir,dir:cnt)"""
    (actual, midpoint) = get_path(
        graph_grpc_stub,
        ["swh:1:rev:0000000000000000000000000000000000000013"],
        ["swh:1:rev:0000000000000000000000000000000000000003"],
        labeled=labeled,
        direction=GraphDirection.FORWARD,
        direction_reverse=GraphDirection.FORWARD,
        edges="rev:dir,dir:dir,dir:cnt",
    )
    expected = "swh:1:cnt:0000000000000000000000000000000000000001"
    assert expected == actual[midpoint]


def test_labels_common_descendant_forward_forward(graph_grpc_stub):
    """Common descendant between rev 13 and rev 3 : cnt 1 (with rev:dir,dir:dir,dir:cnt)"""
    request = graph_grpc_stub.FindPathBetween(
        FindPathBetweenRequest(
            src=["swh:1:rev:0000000000000000000000000000000000000013"],
            dst=["swh:1:rev:0000000000000000000000000000000000000003"],
            direction=GraphDirection.FORWARD,
            direction_reverse=GraphDirection.FORWARD,
            edges="rev:dir,dir:dir,dir:cnt",
            mask=FieldMask(paths=["labeled_node.node.swhid", "labeled_node.label"]),
        )
    )
    assert (
        list(request.labeled_node)[request.midpoint_index].node.swhid
        == "swh:1:cnt:0000000000000000000000000000000000000001"
    )
    expected = [
        LabeledNode(
            node=Node(swhid="swh:1:rev:0000000000000000000000000000000000000013"),
            label=[],
        ),
        LabeledNode(
            node=Node(swhid="swh:1:dir:0000000000000000000000000000000000000012"),
            label=[EdgeLabel(name=b"oldproject", permission=33261)],
        ),
        LabeledNode(
            node=Node(swhid="swh:1:dir:0000000000000000000000000000000000000008"),
            label=[EdgeLabel(name=b"README.md", permission=33188)],
        ),
        LabeledNode(
            node=Node(swhid="swh:1:cnt:0000000000000000000000000000000000000001"),
            label=[EdgeLabel(name=b"README.md", permission=33188)],
        ),
        LabeledNode(
            node=Node(swhid="swh:1:dir:0000000000000000000000000000000000000002"),
            label=[],
        ),
        LabeledNode(
            node=Node(swhid="swh:1:rev:0000000000000000000000000000000000000003"),
            label=[],
        ),
    ]
    assert list(request.labeled_node) == expected


@parametrize
def test_common_descendant_forward_forward_edges_reverse(graph_grpc_stub, labeled):
    """Common descendant between rev 13 and rev 3 : cnt 1 (with rev:dir,dir:dir,dir:cnt)"""
    (actual, midpoint) = get_path(
        graph_grpc_stub,
        ["swh:1:rev:0000000000000000000000000000000000000013"],
        ["swh:1:rev:0000000000000000000000000000000000000003"],
        labeled=labeled,
        direction=GraphDirection.FORWARD,
        direction_reverse=GraphDirection.FORWARD,
        edges="rev:dir,dir:dir,dir:cnt",
        edges_reverse="rev:dir,dir:dir,dir:cnt",
    )
    expected = "swh:1:cnt:0000000000000000000000000000000000000001"
    assert expected == actual[midpoint]


def test_labels_common_descendant_forward_forward_edges_reverse(graph_grpc_stub):
    """Common descendant between rev 13 and rev 3 : cnt 1 (with rev:dir,dir:dir,dir:cnt)"""
    request = graph_grpc_stub.FindPathBetween(
        FindPathBetweenRequest(
            src=["swh:1:rev:0000000000000000000000000000000000000013"],
            dst=["swh:1:rev:0000000000000000000000000000000000000003"],
            direction=GraphDirection.FORWARD,
            direction_reverse=GraphDirection.FORWARD,
            edges="rev:dir,dir:dir,dir:cnt",
            edges_reverse="rev:dir,dir:dir,dir:cnt",
            mask=FieldMask(paths=["labeled_node.node.swhid", "labeled_node.label"]),
        )
    )
    assert (
        list(request.labeled_node)[request.midpoint_index].node.swhid
        == "swh:1:cnt:0000000000000000000000000000000000000001"
    )
    expected = [
        LabeledNode(
            node=Node(swhid="swh:1:rev:0000000000000000000000000000000000000013"),
            label=[],
        ),
        LabeledNode(
            node=Node(swhid="swh:1:dir:0000000000000000000000000000000000000012"),
            label=[EdgeLabel(name=b"oldproject", permission=33261)],
        ),
        LabeledNode(
            node=Node(swhid="swh:1:dir:0000000000000000000000000000000000000008"),
            label=[EdgeLabel(name=b"README.md", permission=33188)],
        ),
        LabeledNode(
            node=Node(swhid="swh:1:cnt:0000000000000000000000000000000000000001"),
            label=[EdgeLabel(name=b"README.md", permission=33188)],
        ),
        LabeledNode(
            node=Node(swhid="swh:1:dir:0000000000000000000000000000000000000002"),
            label=[],
        ),
        LabeledNode(
            node=Node(swhid="swh:1:rev:0000000000000000000000000000000000000003"),
            label=[],
        ),
    ]
    assert list(request.labeled_node) == expected


@parametrize
def test_max_depth(graph_grpc_stub, labeled):
    """Labeled path between rel 19 and cnt 15 with various max depths"""
    # Works with max_depth = 2
    mask = (
        FieldMask(paths=["labeled_node.node.swhid", "labeled_node.label"])
        if labeled
        else FieldMask(paths=["node.swhid"])
    )
    request = graph_grpc_stub.FindPathBetween(
        FindPathBetweenRequest(
            src=["swh:1:rel:0000000000000000000000000000000000000019"],
            dst=["swh:1:cnt:0000000000000000000000000000000000000015"],
            max_depth=2,
            mask=mask,
        )
    )
    expected_unlabeled = [
        Node(swhid="swh:1:rel:0000000000000000000000000000000000000019"),
        Node(swhid="swh:1:rev:0000000000000000000000000000000000000018"),
        Node(swhid="swh:1:dir:0000000000000000000000000000000000000017"),
        Node(swhid="swh:1:dir:0000000000000000000000000000000000000016"),
        Node(swhid="swh:1:cnt:0000000000000000000000000000000000000015"),
    ]
    expected_labeled = [
        LabeledNode(
            node=Node(swhid="swh:1:rel:0000000000000000000000000000000000000019"),
            label=[],
        ),
        LabeledNode(
            node=Node(swhid="swh:1:rev:0000000000000000000000000000000000000018"),
            label=[],
        ),
        LabeledNode(
            node=Node(swhid="swh:1:dir:0000000000000000000000000000000000000017"),
            label=[EdgeLabel(name=b"old", permission=33261)],
        ),
        LabeledNode(
            node=Node(swhid="swh:1:dir:0000000000000000000000000000000000000016"),
            label=[EdgeLabel(name=b"TODO.txt", permission=33188)],
        ),
        LabeledNode(
            node=Node(swhid="swh:1:cnt:0000000000000000000000000000000000000015"),
            label=[],
        ),
    ]
    if labeled:
        assert expected_labeled == list(request.labeled_node)
    else:
        assert expected_unlabeled == list(request.node)

    # Check that it throws NOT_FOUND with max depth = 1
    with pytest.raises(grpc.RpcError) as rpc_error:
        print(
            graph_grpc_stub.FindPathBetween(
                FindPathBetweenRequest(
                    src=["swh:1:rel:0000000000000000000000000000000000000019"],
                    dst=["swh:1:cnt:0000000000000000000000000000000000000015"],
                    max_depth=1,
                    mask=mask,
                )
            )
        )
    assert rpc_error.value.code() == grpc.StatusCode.NOT_FOUND


@parametrize
def test_max_edges(graph_grpc_stub, labeled):
    """Labeled path between rel 19 and cnt 15 with various max edges"""
    # Works with max_edges = 3
    mask = (
        FieldMask(paths=["labeled_node.node.swhid", "labeled_node.label"])
        if labeled
        else FieldMask(paths=["node.swhid"])
    )
    request = graph_grpc_stub.FindPathBetween(
        FindPathBetweenRequest(
            src=["swh:1:rel:0000000000000000000000000000000000000019"],
            dst=["swh:1:cnt:0000000000000000000000000000000000000015"],
            max_edges=3,
            mask=mask,
        )
    )
    expected_unlabeled = [
        Node(swhid="swh:1:rel:0000000000000000000000000000000000000019"),
        Node(swhid="swh:1:rev:0000000000000000000000000000000000000018"),
        Node(swhid="swh:1:dir:0000000000000000000000000000000000000017"),
        Node(swhid="swh:1:dir:0000000000000000000000000000000000000016"),
        Node(swhid="swh:1:cnt:0000000000000000000000000000000000000015"),
    ]
    expected_labeled = [
        LabeledNode(
            node=Node(swhid="swh:1:rel:0000000000000000000000000000000000000019"),
            label=[],
        ),
        LabeledNode(
            node=Node(swhid="swh:1:rev:0000000000000000000000000000000000000018"),
            label=[],
        ),
        LabeledNode(
            node=Node(swhid="swh:1:dir:0000000000000000000000000000000000000017"),
            label=[EdgeLabel(name=b"old", permission=33261)],
        ),
        LabeledNode(
            node=Node(swhid="swh:1:dir:0000000000000000000000000000000000000016"),
            label=[EdgeLabel(name=b"TODO.txt", permission=33188)],
        ),
        LabeledNode(
            node=Node(swhid="swh:1:cnt:0000000000000000000000000000000000000015"),
            label=[],
        ),
    ]
    if labeled:
        assert expected_labeled == list(request.labeled_node)
    else:
        assert expected_unlabeled == list(request.node)

    # Check that it throws NOT_FOUND with max_edges = 2
    with pytest.raises(grpc.RpcError) as rpc_error:
        graph_grpc_stub.FindPathBetween(
            FindPathBetweenRequest(
                src=["swh:1:rel:0000000000000000000000000000000000000019"],
                dst=["swh:1:cnt:0000000000000000000000000000000000000015"],
                max_edges=2,
                mask=mask,
            )
        )
    assert rpc_error.value.code() == grpc.StatusCode.NOT_FOUND


@parametrize
def test_ignore_node_forward(graph_grpc_stub, labeled):
    """Ignore a directory node, forcing an alternate path"""
    (actual, _midpoint) = get_path(
        graph_grpc_stub,
        ["swh:1:rel:0000000000000000000000000000000000000010"],
        ["swh:1:cnt:0000000000000000000000000000000000000001"],
        labeled=labeled,
        ignore_node=["swh:1:dir:0000000000000000000000000000000000000008"],
    )
    expected = [
        "swh:1:rel:0000000000000000000000000000000000000010",
        "swh:1:rev:0000000000000000000000000000000000000009",
        "swh:1:rev:0000000000000000000000000000000000000003",
        "swh:1:dir:0000000000000000000000000000000000000002",
        "swh:1:cnt:0000000000000000000000000000000000000001",
    ]
    assert expected == actual


@parametrize
def test_ignore_node_makes_path_impossible(graph_grpc_stub, labeled):
    """Ignore a node that blocks all paths"""
    with pytest.raises(grpc.RpcError) as rpc_error:
        get_path(
            graph_grpc_stub,
            ["swh:1:rel:0000000000000000000000000000000000000010"],
            ["swh:1:cnt:0000000000000000000000000000000000000001"],
            labeled=labeled,
            ignore_node=[
                "swh:1:rev:0000000000000000000000000000000000000003",
                "swh:1:dir:0000000000000000000000000000000000000008",
            ],
        )
    assert rpc_error.value.code() == grpc.StatusCode.NOT_FOUND


@parametrize
def test_ignore_node_common_ancestor(graph_grpc_stub, labeled):
    """Ignore a node in common ancestor search"""
    (actual, midpoint) = get_path(
        graph_grpc_stub,
        ["swh:1:cnt:0000000000000000000000000000000000000004"],
        ["swh:1:cnt:0000000000000000000000000000000000000015"],
        labeled=labeled,
        direction=GraphDirection.BACKWARD,
        direction_reverse=GraphDirection.BACKWARD,
        ignore_node=["swh:1:rev:0000000000000000000000000000000000000009"],
    )
    expected = [
        "swh:1:cnt:0000000000000000000000000000000000000004",
        "swh:1:dir:0000000000000000000000000000000000000006",
        "swh:1:dir:0000000000000000000000000000000000000008",
        "swh:1:dir:0000000000000000000000000000000000000012",
        "swh:1:rev:0000000000000000000000000000000000000013",
        "swh:1:rev:0000000000000000000000000000000000000018",
        "swh:1:dir:0000000000000000000000000000000000000017",
        "swh:1:dir:0000000000000000000000000000000000000016",
        "swh:1:cnt:0000000000000000000000000000000000000015",
    ]
    assert expected == actual
    assert actual[midpoint] == "swh:1:rev:0000000000000000000000000000000000000018"
