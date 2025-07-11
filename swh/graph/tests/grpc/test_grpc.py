# Copyright (c) 2022-2025 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from collections import Counter
import hashlib
import re
import time

from google.protobuf.field_mask_pb2 import FieldMask

from swh.graph.example_dataset import DATASET
from swh.graph.grpc.swhgraph_pb2 import (
    GraphDirection,
    NodeFilter,
    StatsRequest,
    TraversalRequest,
)

TEST_ORIGIN_ID = "swh:1:ori:{}".format(
    hashlib.sha1(b"https://example.com/swh/graph").hexdigest()
)


def test_stats_statsd(graph_grpc_stub, graph_statsd_server):
    graph_statsd_server.datagrams = []
    graph_statsd_server.new_datagram.clear()

    graph_grpc_stub.Stats(StatsRequest())

    for _ in range(4):  # usually a single iteration is needed, but may need up to 5
        graph_statsd_server.new_datagram.wait(timeout=1)
        datagrams = [dg.decode() for dg in sorted(graph_statsd_server.datagrams)]
        if len(datagrams) == 4:
            break
        time.sleep(0.01)

    datagrams_by_key = dict(datagram.split(":", 1) for datagram in datagrams)
    assert set(datagrams_by_key) == {
        "swh_graph_grpc_server.frames_total",
        "swh_graph_grpc_server.requests_total",
        "swh_graph_grpc_server.response_wall_time_ms",
        "swh_graph_grpc_server.streaming_wall_time_ms",
    }

    assert datagrams_by_key["swh_graph_grpc_server.frames_total"] == (
        "2|c|#path:/swh.graph.TraversalService/Stats,status:200"
    )

    assert datagrams_by_key["swh_graph_grpc_server.requests_total"] == (
        "1|c|#path:/swh.graph.TraversalService/Stats,status:200"
    )

    assert re.match(
        "[0-9]{1,2}|ms|#path:/swh.graph.TraversalService/Stats,status:200",
        datagrams_by_key["swh_graph_grpc_server.response_wall_time_ms"],
    )

    assert re.match(
        "[0-9]{1,2}|ms|#path:/swh.graph.TraversalService/Stats,status:200",
        datagrams_by_key["swh_graph_grpc_server.streaming_wall_time_ms"],
    )


def test_stats(graph_grpc_stub):
    stats = graph_grpc_stub.Stats(StatsRequest())
    assert stats.num_nodes == 24
    assert stats.num_edges == 28
    assert isinstance(stats.compression_ratio, float)
    assert isinstance(stats.bits_per_node, float)
    assert isinstance(stats.bits_per_edge, float)
    assert isinstance(stats.avg_locality, float)
    assert stats.indegree_min == 0
    assert stats.indegree_max == 4
    assert isinstance(stats.indegree_avg, float)
    assert stats.outdegree_min == 0
    assert stats.outdegree_max == 3
    assert isinstance(stats.outdegree_avg, float)
    assert stats.export_started_at == 1669888200
    assert stats.export_ended_at == 1669899600
    assert stats.num_nodes_by_type == dict(
        Counter(
            node.swhid().object_type.value for node in DATASET if hasattr(node, "swhid")
        )
    )
    # TODO: auto-generate this
    assert stats.num_arcs_by_type == {
        "ori:snp": 2,
        "snp:rel": 3,
        "snp:rev": 2,
        "rel:rev": 3,
        "rev:rev": 3,
        "rev:dir": 4,
        "dir:dir": 3,
        "dir:cnt": 8,
    }


def test_leaves(graph_grpc_stub):
    request = graph_grpc_stub.Traverse(
        TraversalRequest(
            src=[TEST_ORIGIN_ID],
            mask=FieldMask(paths=["swhid"]),
            return_nodes=NodeFilter(types="cnt"),
        )
    )
    actual = [node.swhid for node in request]
    expected = [
        "swh:1:cnt:0000000000000000000000000000000000000001",
        "swh:1:cnt:0000000000000000000000000000000000000004",
        "swh:1:cnt:0000000000000000000000000000000000000005",
        "swh:1:cnt:0000000000000000000000000000000000000007",
    ]
    assert set(actual) == set(expected)


def test_neighbors(graph_grpc_stub):
    request = graph_grpc_stub.Traverse(
        TraversalRequest(
            src=["swh:1:rev:0000000000000000000000000000000000000009"],
            direction=GraphDirection.BACKWARD,
            mask=FieldMask(paths=["swhid"]),
            min_depth=1,
            max_depth=1,
        )
    )
    actual = [node.swhid for node in request]
    expected = [
        "swh:1:snp:0000000000000000000000000000000000000022",
        "swh:1:snp:0000000000000000000000000000000000000020",
        "swh:1:rel:0000000000000000000000000000000000000010",
        "swh:1:rev:0000000000000000000000000000000000000013",
    ]
    assert set(actual) == set(expected)


def test_visit_nodes(graph_grpc_stub):
    request = graph_grpc_stub.Traverse(
        TraversalRequest(
            src=["swh:1:rel:0000000000000000000000000000000000000010"],
            mask=FieldMask(paths=["swhid"]),
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


def test_visit_nodes_filtered(graph_grpc_stub):
    request = graph_grpc_stub.Traverse(
        TraversalRequest(
            src=["swh:1:rel:0000000000000000000000000000000000000010"],
            mask=FieldMask(paths=["swhid"]),
            return_nodes=NodeFilter(types="dir"),
        )
    )
    actual = [node.swhid for node in request]
    expected = [
        "swh:1:dir:0000000000000000000000000000000000000002",
        "swh:1:dir:0000000000000000000000000000000000000008",
        "swh:1:dir:0000000000000000000000000000000000000006",
    ]
    assert set(actual) == set(expected)


def test_visit_nodes_filtered_star(graph_grpc_stub):
    request = graph_grpc_stub.Traverse(
        TraversalRequest(
            src=["swh:1:rel:0000000000000000000000000000000000000010"],
            mask=FieldMask(paths=["swhid"]),
        )
    )
    actual = [node.swhid for node in request]
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
