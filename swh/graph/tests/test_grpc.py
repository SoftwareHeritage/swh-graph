# Copyright (c) 2022 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import hashlib

from google.protobuf.field_mask_pb2 import FieldMask

from swh.graph.grpc.swhgraph_pb2 import (
    GraphDirection,
    NodeFilter,
    StatsRequest,
    TraversalRequest,
)

TEST_ORIGIN_ID = "swh:1:ori:{}".format(
    hashlib.sha1(b"https://example.com/swh/graph").hexdigest()
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
