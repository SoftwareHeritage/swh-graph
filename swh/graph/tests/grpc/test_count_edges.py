# Copyright (c) 2022-2023 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import hashlib

from google.protobuf.field_mask_pb2 import FieldMask
import grpc
import pytest

from swh.graph.grpc.swhgraph_pb2 import GraphDirection, TraversalRequest

TEST_ORIGIN_ID = "swh:1:ori:{}".format(
    hashlib.sha1(b"https://example.com/swh/graph").hexdigest()
)
TEST_ORIGIN_ID2 = "swh:1:ori:{}".format(
    hashlib.sha1(b"https://example.com/swh/graph2").hexdigest()
)


def test_src_errors(graph_grpc_stub):
    with pytest.raises(grpc.RpcError) as exc_info:
        graph_grpc_stub.CountEdges(
            TraversalRequest(
                src=["swh:1:cnt:0000000000000000000000000000000000000404"],
                direction=GraphDirection.FORWARD,
            )
        )
    assert exc_info.value.code() == grpc.StatusCode.NOT_FOUND

    with pytest.raises(grpc.RpcError) as exc_info:
        graph_grpc_stub.CountEdges(
            TraversalRequest(
                src=["swh:1:lol:0000000000000000000000000000000000000001"],
                direction=GraphDirection.FORWARD,
            )
        )
    assert exc_info.value.code() == grpc.StatusCode.INVALID_ARGUMENT

    with pytest.raises(grpc.RpcError) as exc_info:
        graph_grpc_stub.CountEdges(
            TraversalRequest(
                src=["swh:1:cnt:000000000000000000000000000000000000000z"],
                direction=GraphDirection.FORWARD,
            )
        )
    assert exc_info.value.code() == grpc.StatusCode.INVALID_ARGUMENT


def test_forward_from_root(graph_grpc_stub):
    traversal_request = graph_grpc_stub.CountEdges(
        TraversalRequest(
            src=[TEST_ORIGIN_ID],
            mask=FieldMask(paths=["swhid"]),
        )
    )
    assert traversal_request.count == 13


@pytest.mark.parametrize("limit", [0, 1, 2, 13, 14, 15, 1 << 63 - 1])
def test_forward_from_root_with_limit(graph_grpc_stub, limit):
    traversal_request = graph_grpc_stub.CountEdges(
        TraversalRequest(
            src=[TEST_ORIGIN_ID],
            max_matching_nodes=limit,
            mask=FieldMask(paths=["swhid"]),
        )
    )

    if limit == 1:
        assert traversal_request.count == 1
    elif limit == 2:
        assert traversal_request.count == 3
    else:
        assert traversal_request.count == 13


def test_forward_from_middle(graph_grpc_stub):
    traversal_request = graph_grpc_stub.CountEdges(
        TraversalRequest(
            src=["swh:1:dir:0000000000000000000000000000000000000012"],
            mask=FieldMask(paths=["swhid"]),
        )
    )
    assert traversal_request.count == 7


def test_forward_rel_rev(graph_grpc_stub):
    traversal_request = graph_grpc_stub.CountEdges(
        TraversalRequest(
            src=["swh:1:rel:0000000000000000000000000000000000000010"],
            edges="rel:rev,rev:rev",
            mask=FieldMask(paths=["swhid"]),
        )
    )
    assert traversal_request.count == 2


def test_backward_from_middle(graph_grpc_stub):
    traversal_request = graph_grpc_stub.CountEdges(
        TraversalRequest(
            src=["swh:1:dir:0000000000000000000000000000000000000012"],
            direction=GraphDirection.BACKWARD,
            mask=FieldMask(paths=["swhid"]),
        )
    )
    assert traversal_request.count == 6


def test_backward_from_leaf(graph_grpc_stub):
    traversal_request = graph_grpc_stub.CountEdges(
        TraversalRequest(
            src=["swh:1:cnt:0000000000000000000000000000000000000004"],
            direction=GraphDirection.BACKWARD,
            mask=FieldMask(paths=["swhid"]),
        )
    )
    assert traversal_request.count == 17


def test_backward_rev_to_rev_rev_to_rel(graph_grpc_stub):
    traversal_request = graph_grpc_stub.CountEdges(
        TraversalRequest(
            src=["swh:1:rev:0000000000000000000000000000000000000003"],
            edges="rev:rev,rev:rel",
            direction=GraphDirection.BACKWARD,
            mask=FieldMask(paths=["swhid"]),
        )
    )
    assert traversal_request.count == 6


def test_with_empty_mask(graph_grpc_stub):
    traversal_request = graph_grpc_stub.CountEdges(
        TraversalRequest(
            src=["swh:1:dir:0000000000000000000000000000000000000012"],
            mask=FieldMask(),
        )
    )
    assert traversal_request.count == 7


def test_max_depth(graph_grpc_stub):
    traversal_request = graph_grpc_stub.CountEdges(
        TraversalRequest(
            src=["swh:1:rel:0000000000000000000000000000000000000019"],
            max_depth=1,
            mask=FieldMask(),
        )
    )
    # should count rel:19 -> rev:18
    assert traversal_request.count == 1
