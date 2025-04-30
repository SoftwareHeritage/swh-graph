# Copyright (c) 2022-2025 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from pathlib import Path

import pytest

from swh.graph.example_dataset import DATASET_DIR
from swh.graph.grpc.swhgraph_pb2 import GetNodeRequest, GraphDirection

# locally "redefine" all fixtures that depend on the session-scoped
# graph_grpc_server_config, because we need pytest to call them again.
from swh.graph.pytest_plugin import (  # noqa
    graph_grpc_server_process,
    graph_grpc_server_started,
)

from .test_getnode import TEST_ORIGIN_ID, test_invalid_swhid, test_not_found  # noqa
from .test_neighbors import traverse_nodes


@pytest.fixture(scope="session")
def graph_grpc_server_config(
    graph_grpc_backend_implementation, graph_statsd_server, tmpdir_factory
):
    full_graph_dir = DATASET_DIR / "compressed"
    partial_graph_path = Path(tmpdir_factory.mktemp("partial-graph"))
    for filepath in full_graph_dir.iterdir():
        # copy everything but property files
        if filepath.name.startswith("example.property."):
            continue
        symlink_path = partial_graph_path / filepath.name
        symlink_path.symlink_to(filepath)

    return {
        "graph": {
            "cls": f"local_{graph_grpc_backend_implementation}",
            "grpc_server": {
                "path": partial_graph_path / "example",
                "debug": True,
                "statsd_host": graph_statsd_server.host,
                "statsd_port": graph_statsd_server.port,
            },
            "http_rpc_server": {"debug": True},
        }
    }


def test_cnt(graph_grpc_stub):
    swhid = "swh:1:cnt:0000000000000000000000000000000000000001"

    node = graph_grpc_stub.GetNode(GetNodeRequest(swhid=swhid))
    if node.HasField("cnt"):
        assert not node.cnt.HasField("length")
        assert not node.cnt.HasField("is_skipped")


def test_rev(graph_grpc_stub):
    swhid = "swh:1:rev:0000000000000000000000000000000000000003"

    node = graph_grpc_stub.GetNode(GetNodeRequest(swhid=swhid))
    assert not node.rev.HasField("message")
    assert not node.rev.HasField("author")
    assert not node.rev.HasField("author_date")
    assert not node.rev.HasField("author_date_offset")
    assert not node.rev.HasField("committer")
    assert not node.rev.HasField("committer_date")
    assert not node.rev.HasField("committer_date_offset")


@pytest.mark.parametrize("rel_id", [10, 19])
def test_rel(rel_id, graph_grpc_stub):
    swhid = f"swh:1:rel:00000000000000000000000000000000000000{rel_id}"

    node = graph_grpc_stub.GetNode(GetNodeRequest(swhid=swhid))
    assert not node.rel.HasField("message")
    assert not node.rel.HasField("author")
    assert not node.rel.HasField("author_date")
    assert not node.rel.HasField("author_date_offset")


def test_ori(graph_grpc_stub):
    swhid = TEST_ORIGIN_ID

    node = graph_grpc_stub.GetNode(GetNodeRequest(swhid=swhid))
    assert not node.ori.HasField("url")


def test_neighbors(graph_grpc_stub):
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
