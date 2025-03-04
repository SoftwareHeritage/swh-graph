# Copyright (c) 2022-2024 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import hashlib
import re
import time

import grpc
import pytest

from swh.graph.grpc.swhgraph_pb2 import GraphDirection, NodeFilter, TraversalRequest

TEST_ORIGIN_ID = "swh:1:ori:{}".format(
    hashlib.sha1(b"https://example.com/swh/graph").hexdigest()
)
TEST_ORIGIN_ID2 = "swh:1:ori:{}".format(
    hashlib.sha1(b"https://example.com/swh/graph2").hexdigest()
)


def test_src_errors(graph_grpc_stub):
    request = graph_grpc_stub.Traverse(
        TraversalRequest(
            src=["swh:1:cnt:0000000000000000000000000000000000000404"],
            direction=GraphDirection.FORWARD,
        )
    )
    with pytest.raises(grpc.RpcError) as exc_info:
        list(request)
    assert exc_info.value.code() == grpc.StatusCode.NOT_FOUND

    request = graph_grpc_stub.Traverse(
        TraversalRequest(
            src=["swh:1:lol:0000000000000000000000000000000000000001"],
            direction=GraphDirection.FORWARD,
        )
    )
    with pytest.raises(grpc.RpcError) as exc_info:
        list(request)
    assert exc_info.value.code() == grpc.StatusCode.INVALID_ARGUMENT

    request = graph_grpc_stub.Traverse(
        TraversalRequest(
            src=["swh:1:cnt:000000000000000000000000000000000000000z"],
            direction=GraphDirection.FORWARD,
        )
    )
    with pytest.raises(grpc.RpcError) as exc_info:
        list(request)
    assert exc_info.value.code() == grpc.StatusCode.INVALID_ARGUMENT


def test_forward_from_root(graph_grpc_stub):
    request = graph_grpc_stub.Traverse(
        TraversalRequest(
            src=[TEST_ORIGIN_ID],
            direction=GraphDirection.FORWARD,
        )
    )
    actual = [node.swhid for node in request]
    expected = [
        "swh:1:cnt:0000000000000000000000000000000000000001",
        "swh:1:cnt:0000000000000000000000000000000000000004",
        "swh:1:cnt:0000000000000000000000000000000000000005",
        "swh:1:cnt:0000000000000000000000000000000000000007",
        "swh:1:dir:0000000000000000000000000000000000000002",
        "swh:1:dir:0000000000000000000000000000000000000006",
        "swh:1:dir:0000000000000000000000000000000000000008",
        "swh:1:rel:0000000000000000000000000000000000000010",
        "swh:1:rev:0000000000000000000000000000000000000003",
        "swh:1:rev:0000000000000000000000000000000000000009",
        "swh:1:snp:0000000000000000000000000000000000000020",
        TEST_ORIGIN_ID,
    ]
    assert set(actual) == set(expected)


def test_traverse_statsd(graph_grpc_stub, graph_statsd_server):
    graph_statsd_server.datagrams = []
    graph_statsd_server.new_datagram.clear()

    list(
        graph_grpc_stub.Traverse(
            TraversalRequest(
                src=[TEST_ORIGIN_ID],
                direction=GraphDirection.FORWARD,
            )
        )
    )

    for _ in range(5):  # usually a single iteration is needed, but may need up to 5
        graph_statsd_server.new_datagram.wait(timeout=1)
        datagrams = [dg.decode() for dg in sorted(graph_statsd_server.datagrams)]
        if len(datagrams) == 5:
            break
        time.sleep(0.01)

    datagrams_by_key = dict(datagram.split(":", 1) for datagram in datagrams)
    assert set(datagrams_by_key) == {
        "swh_graph_grpc_server.frames_total",
        "swh_graph_grpc_server.requests_total",
        "swh_graph_grpc_server.response_wall_time_ms",
        "swh_graph_grpc_server.streaming_wall_time_ms",
        "swh_graph_grpc_server.traversal_returned_nodes_total",
    }

    assert re.match(
        "[0-9]|c|#path:/swh.graph.TraversalService/Traverse,status:200",
        datagrams_by_key["swh_graph_grpc_server.frames_total"],
    )

    assert datagrams_by_key["swh_graph_grpc_server.requests_total"] == (
        "1|c|#path:/swh.graph.TraversalService/Traverse,status:200"
    )

    assert re.match(
        "[0-9]{1,2}|ms|#path:/swh.graph.TraversalService/Traverse,status:200",
        datagrams_by_key["swh_graph_grpc_server.response_wall_time_ms"],
    )

    assert re.match(
        "[0-9]{1,2}|ms|#path:/swh.graph.TraversalService/Traverse,status:200",
        datagrams_by_key["swh_graph_grpc_server.streaming_wall_time_ms"],
    )

    assert (
        datagrams_by_key["swh_graph_grpc_server.traversal_returned_nodes_total"]
        == "12|c"
    )


def test_forward_from_middle(graph_grpc_stub):
    request = graph_grpc_stub.Traverse(
        TraversalRequest(
            src=["swh:1:dir:0000000000000000000000000000000000000012"],
            direction=GraphDirection.FORWARD,
        )
    )
    actual = [node.swhid for node in request]
    expected = [
        "swh:1:cnt:0000000000000000000000000000000000000001",
        "swh:1:cnt:0000000000000000000000000000000000000004",
        "swh:1:cnt:0000000000000000000000000000000000000005",
        "swh:1:cnt:0000000000000000000000000000000000000007",
        "swh:1:cnt:0000000000000000000000000000000000000011",
        "swh:1:dir:0000000000000000000000000000000000000006",
        "swh:1:dir:0000000000000000000000000000000000000008",
        "swh:1:dir:0000000000000000000000000000000000000012",
    ]
    assert set(actual) == set(expected)


def test_forward_rel_rev(graph_grpc_stub):
    request = graph_grpc_stub.Traverse(
        TraversalRequest(
            src=["swh:1:rel:0000000000000000000000000000000000000010"],
            direction=GraphDirection.FORWARD,
            edges="rel:rev,rev:rev",
        )
    )
    actual = [node.swhid for node in request]
    expected = [
        "swh:1:rel:0000000000000000000000000000000000000010",
        "swh:1:rev:0000000000000000000000000000000000000009",
        "swh:1:rev:0000000000000000000000000000000000000003",
    ]
    assert set(actual) == set(expected)


def test_forward_filter_returned_nodes_dir(graph_grpc_stub):
    request = graph_grpc_stub.Traverse(
        TraversalRequest(
            src=["swh:1:rel:0000000000000000000000000000000000000010"],
            return_nodes=NodeFilter(types="dir"),
        )
    )
    actual = [node.swhid for node in request]
    expected = [
        "swh:1:dir:0000000000000000000000000000000000000002",
        "swh:1:dir:0000000000000000000000000000000000000006",
        "swh:1:dir:0000000000000000000000000000000000000008",
    ]
    assert set(actual) == set(expected)


def test_backward_from_root(graph_grpc_stub):
    request = graph_grpc_stub.Traverse(
        TraversalRequest(
            src=[TEST_ORIGIN_ID],
            direction=GraphDirection.BACKWARD,
        )
    )
    actual = [node.swhid for node in request]
    expected = [TEST_ORIGIN_ID]
    assert set(actual) == set(expected)


def test_backward_from_middle(graph_grpc_stub):
    request = graph_grpc_stub.Traverse(
        TraversalRequest(
            src=["swh:1:dir:0000000000000000000000000000000000000012"],
            direction=GraphDirection.BACKWARD,
        )
    )
    actual = [node.swhid for node in request]
    expected = [
        "swh:1:dir:0000000000000000000000000000000000000012",
        "swh:1:rel:0000000000000000000000000000000000000019",
        "swh:1:rev:0000000000000000000000000000000000000013",
        "swh:1:rev:0000000000000000000000000000000000000018",
        "swh:1:rel:0000000000000000000000000000000000000021",
        "swh:1:snp:0000000000000000000000000000000000000022",
        TEST_ORIGIN_ID2,
    ]
    assert set(actual) == set(expected)


def test_backward_from_leaf(graph_grpc_stub):
    request = graph_grpc_stub.Traverse(
        TraversalRequest(
            src=["swh:1:cnt:0000000000000000000000000000000000000004"],
            direction=GraphDirection.BACKWARD,
        )
    )
    actual = [node.swhid for node in request]
    expected = [
        TEST_ORIGIN_ID,
        TEST_ORIGIN_ID2,
        "swh:1:cnt:0000000000000000000000000000000000000004",
        "swh:1:dir:0000000000000000000000000000000000000006",
        "swh:1:dir:0000000000000000000000000000000000000008",
        "swh:1:dir:0000000000000000000000000000000000000012",
        "swh:1:rel:0000000000000000000000000000000000000010",
        "swh:1:rel:0000000000000000000000000000000000000019",
        "swh:1:rev:0000000000000000000000000000000000000009",
        "swh:1:rev:0000000000000000000000000000000000000013",
        "swh:1:rev:0000000000000000000000000000000000000018",
        "swh:1:snp:0000000000000000000000000000000000000020",
        "swh:1:rel:0000000000000000000000000000000000000021",
        "swh:1:snp:0000000000000000000000000000000000000022",
    ]
    assert set(actual) == set(expected)


def test_forward_snp_to_rev(graph_grpc_stub):
    request = graph_grpc_stub.Traverse(
        TraversalRequest(
            src=["swh:1:snp:0000000000000000000000000000000000000020"],
            edges="snp:rev",
        )
    )
    actual = [node.swhid for node in request]
    expected = [
        "swh:1:rev:0000000000000000000000000000000000000009",
        "swh:1:snp:0000000000000000000000000000000000000020",
    ]
    assert set(actual) == set(expected)


def test_forward_rel_to_rev_rev_to_rev(graph_grpc_stub):
    request = graph_grpc_stub.Traverse(
        TraversalRequest(
            src=["swh:1:rel:0000000000000000000000000000000000000010"],
            edges="rel:rev,rev:rev",
        )
    )
    actual = [node.swhid for node in request]
    expected = [
        "swh:1:rel:0000000000000000000000000000000000000010",
        "swh:1:rev:0000000000000000000000000000000000000003",
        "swh:1:rev:0000000000000000000000000000000000000009",
    ]
    assert set(actual) == set(expected)


def test_forward_rev_to_all_dir_to_all(graph_grpc_stub):
    request = graph_grpc_stub.Traverse(
        TraversalRequest(
            src=["swh:1:rev:0000000000000000000000000000000000000013"],
            edges="rev:*,dir:*",
        )
    )
    actual = [node.swhid for node in request]
    expected = [
        "swh:1:cnt:0000000000000000000000000000000000000001",
        "swh:1:cnt:0000000000000000000000000000000000000004",
        "swh:1:cnt:0000000000000000000000000000000000000005",
        "swh:1:cnt:0000000000000000000000000000000000000007",
        "swh:1:cnt:0000000000000000000000000000000000000011",
        "swh:1:dir:0000000000000000000000000000000000000002",
        "swh:1:dir:0000000000000000000000000000000000000006",
        "swh:1:dir:0000000000000000000000000000000000000008",
        "swh:1:dir:0000000000000000000000000000000000000012",
        "swh:1:rev:0000000000000000000000000000000000000003",
        "swh:1:rev:0000000000000000000000000000000000000009",
        "swh:1:rev:0000000000000000000000000000000000000013",
    ]
    assert set(actual) == set(expected)


def test_forward_snp_to_all_rev_to_all(graph_grpc_stub):
    request = graph_grpc_stub.Traverse(
        TraversalRequest(
            src=["swh:1:snp:0000000000000000000000000000000000000020"],
            edges="snp:*,rev:*",
        )
    )
    actual = [node.swhid for node in request]
    expected = [
        "swh:1:dir:0000000000000000000000000000000000000002",
        "swh:1:dir:0000000000000000000000000000000000000008",
        "swh:1:rel:0000000000000000000000000000000000000010",
        "swh:1:rev:0000000000000000000000000000000000000003",
        "swh:1:rev:0000000000000000000000000000000000000009",
        "swh:1:snp:0000000000000000000000000000000000000020",
    ]
    assert set(actual) == set(expected)


def test_forward_no_edges(graph_grpc_stub):
    request = graph_grpc_stub.Traverse(
        TraversalRequest(
            src=["swh:1:snp:0000000000000000000000000000000000000020"],
            edges="",
        )
    )
    actual = [node.swhid for node in request]
    expected = ["swh:1:snp:0000000000000000000000000000000000000020"]
    assert set(actual) == set(expected)


def test_backward_rev_to_rev_rev_to_rel(graph_grpc_stub):
    request = graph_grpc_stub.Traverse(
        TraversalRequest(
            src=["swh:1:rev:0000000000000000000000000000000000000003"],
            edges="rev:rev,rev:rel",
            direction=GraphDirection.BACKWARD,
        )
    )
    actual = [node.swhid for node in request]
    expected = [
        "swh:1:rel:0000000000000000000000000000000000000010",
        "swh:1:rel:0000000000000000000000000000000000000019",
        "swh:1:rev:0000000000000000000000000000000000000003",
        "swh:1:rev:0000000000000000000000000000000000000009",
        "swh:1:rev:0000000000000000000000000000000000000013",
        "swh:1:rev:0000000000000000000000000000000000000018",
        "swh:1:rel:0000000000000000000000000000000000000021",
    ]
    assert set(actual) == set(expected)


def test_forward_from_root_nodes_only(graph_grpc_stub):
    request = graph_grpc_stub.Traverse(
        TraversalRequest(
            src=[TEST_ORIGIN_ID],
        )
    )
    actual = [node.swhid for node in request]
    expected = [
        TEST_ORIGIN_ID,
        "swh:1:cnt:0000000000000000000000000000000000000001",
        "swh:1:cnt:0000000000000000000000000000000000000004",
        "swh:1:cnt:0000000000000000000000000000000000000005",
        "swh:1:cnt:0000000000000000000000000000000000000007",
        "swh:1:dir:0000000000000000000000000000000000000002",
        "swh:1:dir:0000000000000000000000000000000000000006",
        "swh:1:dir:0000000000000000000000000000000000000008",
        "swh:1:rel:0000000000000000000000000000000000000010",
        "swh:1:rev:0000000000000000000000000000000000000003",
        "swh:1:rev:0000000000000000000000000000000000000009",
        "swh:1:snp:0000000000000000000000000000000000000020",
    ]
    assert set(actual) == set(expected)


def test_backward_rev_to_all_nodes_only(graph_grpc_stub):
    request = graph_grpc_stub.Traverse(
        TraversalRequest(
            src=["swh:1:rev:0000000000000000000000000000000000000003"],
            direction=GraphDirection.BACKWARD,
            edges="rev:*",
        )
    )
    actual = [node.swhid for node in request]
    expected = [
        "swh:1:rel:0000000000000000000000000000000000000010",
        "swh:1:rel:0000000000000000000000000000000000000019",
        "swh:1:rev:0000000000000000000000000000000000000003",
        "swh:1:rev:0000000000000000000000000000000000000009",
        "swh:1:rev:0000000000000000000000000000000000000013",
        "swh:1:rev:0000000000000000000000000000000000000018",
        "swh:1:snp:0000000000000000000000000000000000000020",
        "swh:1:rel:0000000000000000000000000000000000000021",
        "swh:1:snp:0000000000000000000000000000000000000022",
    ]
    assert set(actual) == set(expected)


def test_forward_max_depth(graph_grpc_stub):
    request = graph_grpc_stub.Traverse(
        TraversalRequest(
            src=[
                "swh:1:rel:0000000000000000000000000000000000000019",
            ],
            max_depth=1,
        )
    )
    actual = [node.swhid for node in request]
    expected = [
        "swh:1:rel:0000000000000000000000000000000000000019",
        "swh:1:rev:0000000000000000000000000000000000000018",
    ]
    assert set(actual) == set(expected)


def test_forward_multiple_sources(graph_grpc_stub):
    request = graph_grpc_stub.Traverse(
        TraversalRequest(
            src=[
                "swh:1:snp:0000000000000000000000000000000000000020",
                "swh:1:rel:0000000000000000000000000000000000000019",
            ],
            max_depth=1,
        )
    )
    actual = [node.swhid for node in request]
    expected = [
        "swh:1:snp:0000000000000000000000000000000000000020",
        "swh:1:rel:0000000000000000000000000000000000000019",
        "swh:1:rel:0000000000000000000000000000000000000010",
        "swh:1:rev:0000000000000000000000000000000000000009",
        "swh:1:rev:0000000000000000000000000000000000000018",
    ]
    assert set(actual) == set(expected)


def test_backward_multiple_sources(graph_grpc_stub):
    request = graph_grpc_stub.Traverse(
        TraversalRequest(
            src=[
                "swh:1:cnt:0000000000000000000000000000000000000005",
                "swh:1:dir:0000000000000000000000000000000000000016",
            ],
            max_depth=2,
            direction=GraphDirection.BACKWARD,
        )
    )
    actual = [node.swhid for node in request]
    expected = [
        "swh:1:cnt:0000000000000000000000000000000000000005",
        "swh:1:dir:0000000000000000000000000000000000000016",
        "swh:1:dir:0000000000000000000000000000000000000006",
        "swh:1:dir:0000000000000000000000000000000000000008",
        "swh:1:dir:0000000000000000000000000000000000000017",
        "swh:1:rev:0000000000000000000000000000000000000018",
    ]
    assert set(actual) == set(expected)


def test_max_depth_0(graph_grpc_stub):
    request = graph_grpc_stub.Traverse(
        TraversalRequest(
            src=["swh:1:rel:0000000000000000000000000000000000000019"],
            max_depth=0,
        )
    )
    actual = [node.swhid for node in request]
    expected = [
        "swh:1:rel:0000000000000000000000000000000000000019",
    ]
    assert set(actual) == set(expected)


def test_max_depth_1(graph_grpc_stub):
    request = graph_grpc_stub.Traverse(
        TraversalRequest(
            src=["swh:1:rel:0000000000000000000000000000000000000019"],
            max_depth=1,
        )
    )
    actual = [node.swhid for node in request]
    expected = [
        "swh:1:rel:0000000000000000000000000000000000000019",
        "swh:1:rev:0000000000000000000000000000000000000018",
    ]
    assert set(actual) == set(expected)


def test_max_depth_2(graph_grpc_stub):
    request = graph_grpc_stub.Traverse(
        TraversalRequest(
            src=["swh:1:rel:0000000000000000000000000000000000000019"],
            max_depth=2,
        )
    )
    actual = [node.swhid for node in request]
    expected = [
        "swh:1:rel:0000000000000000000000000000000000000019",
        "swh:1:rev:0000000000000000000000000000000000000018",
        "swh:1:rev:0000000000000000000000000000000000000013",
        "swh:1:dir:0000000000000000000000000000000000000017",
    ]
    assert set(actual) == set(expected)


def test_max_depth_3(graph_grpc_stub):
    request = graph_grpc_stub.Traverse(
        TraversalRequest(
            src=["swh:1:rel:0000000000000000000000000000000000000019"],
            max_depth=3,
        )
    )
    actual = [node.swhid for node in request]
    expected = [
        "swh:1:rel:0000000000000000000000000000000000000019",
        "swh:1:rev:0000000000000000000000000000000000000018",
        "swh:1:rev:0000000000000000000000000000000000000013",
        "swh:1:dir:0000000000000000000000000000000000000017",
        "swh:1:rev:0000000000000000000000000000000000000009",
        "swh:1:dir:0000000000000000000000000000000000000012",
        "swh:1:dir:0000000000000000000000000000000000000016",
        "swh:1:cnt:0000000000000000000000000000000000000014",
    ]
    assert set(actual) == set(expected)


def test_max_edges_1(graph_grpc_stub):
    request = graph_grpc_stub.Traverse(
        TraversalRequest(
            src=["swh:1:rel:0000000000000000000000000000000000000019"],
            max_edges=1,
        )
    )
    actual = [node.swhid for node in request]
    expected = [
        "swh:1:rel:0000000000000000000000000000000000000019",
        "swh:1:rev:0000000000000000000000000000000000000018",
    ]
    assert set(actual) == set(expected)


def test_max_edges_3(graph_grpc_stub):
    request = graph_grpc_stub.Traverse(
        TraversalRequest(
            src=["swh:1:rel:0000000000000000000000000000000000000019"],
            max_edges=3,
        )
    )
    actual = [node.swhid for node in request]
    expected = [
        "swh:1:rel:0000000000000000000000000000000000000019",
        "swh:1:rev:0000000000000000000000000000000000000018",
        "swh:1:rev:0000000000000000000000000000000000000013",
        "swh:1:dir:0000000000000000000000000000000000000017",
    ]
    assert set(actual) == set(expected)


def test_max_edges_7(graph_grpc_stub):
    request = graph_grpc_stub.Traverse(
        TraversalRequest(
            src=["swh:1:rel:0000000000000000000000000000000000000019"],
            max_edges=7,
        )
    )
    actual = [node.swhid for node in request]
    expected = [
        "swh:1:rel:0000000000000000000000000000000000000019",
        "swh:1:rev:0000000000000000000000000000000000000018",
        "swh:1:rev:0000000000000000000000000000000000000013",
        "swh:1:dir:0000000000000000000000000000000000000017",
        "swh:1:rev:0000000000000000000000000000000000000009",
        "swh:1:dir:0000000000000000000000000000000000000012",
        "swh:1:dir:0000000000000000000000000000000000000016",
        "swh:1:cnt:0000000000000000000000000000000000000014",
    ]
    assert set(actual) == set(expected)
