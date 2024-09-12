# Copyright (c) 2022-2024 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import copy
import hashlib

import grpc
import pytest

from swh.graph import pytest_plugin
from swh.graph.grpc.swhgraph_pb2 import GetNodeRequest, NodeFilter, TraversalRequest

TEST_ORIGIN_ID = "swh:1:ori:{}".format(
    hashlib.sha1(b"https://example.com/swh/graph").hexdigest()
)
TEST_ORIGIN_ID2 = "swh:1:ori:{}".format(
    hashlib.sha1(b"https://example.com/swh/graph2").hexdigest()
)


@pytest.fixture(scope="session")
def graph_grpc_server_config(graph_grpc_server_config, tmp_path_factory):
    masked_swhids_path = tmp_path_factory.mktemp("test_masking") / "masked_nodes.txt"
    masked_swhids_path.write_text(
        "\n".join(
            [
                TEST_ORIGIN_ID,
                "swh:1:cnt:0000000000000000000000000000000000000005",
                "swh:1:rel:0000000000000000000000000000000000000010",
                "swh:1:snp:ffffffffffffffffffffffffffffffffffffffff",  # not in the graph
            ]
        )
    )
    config = copy.deepcopy(graph_grpc_server_config)
    config["graph"]["grpc_server"]["masked_nodes"] = str(masked_swhids_path)
    yield config


@pytest.fixture(scope="module")
def graph_grpc_server_process(graph_grpc_server_config, graph_statsd_server):
    # override session-scoped fixture to force a new graph process with the new config
    yield from pytest_plugin.graph_grpc_server_process.__wrapped__(
        graph_grpc_server_config, graph_statsd_server
    )


@pytest.fixture(scope="module")
def graph_grpc_server_started(graph_grpc_server_process):
    # override session-scoped fixture to force a new graph process with the new config
    yield from pytest_plugin.graph_grpc_server_started.__wrapped__(
        graph_grpc_server_process
    )


def test_not_found(graph_grpc_stub, graph_grpc_backend_implementation):
    with pytest.raises(grpc.RpcError) as excinfo:
        graph_grpc_stub.GetNode(
            GetNodeRequest(swhid="swh:1:cnt:0000000000000000000000000000000000000194")
        )
    assert excinfo.value.code() == grpc.StatusCode.NOT_FOUND

    with pytest.raises(grpc.RpcError) as excinfo:
        graph_grpc_stub.GetNode(
            GetNodeRequest(swhid="swh:1:snp:ffffffffffffffffffffffffffffffffffffffff")
        )
    assert excinfo.value.code() == grpc.StatusCode.NOT_FOUND


def test_invalid_swhid(graph_grpc_stub, graph_grpc_backend_implementation):
    with pytest.raises(grpc.RpcError) as excinfo:
        graph_grpc_stub.GetNode(
            GetNodeRequest(swhid="swh:1:lol:0000000000000000000000000000000000000001")
        )
    assert excinfo.value.code() == grpc.StatusCode.INVALID_ARGUMENT

    with pytest.raises(grpc.RpcError) as excinfo:
        graph_grpc_stub.GetNode(
            GetNodeRequest(swhid="swh:1:cnt:000000000000000000000000000000000000000z")
        )
    assert excinfo.value.code() == grpc.StatusCode.INVALID_ARGUMENT


def test_masked_swhid(graph_grpc_stub, graph_grpc_backend_implementation):
    with pytest.raises(grpc.RpcError) as excinfo:
        graph_grpc_stub.GetNode(GetNodeRequest(swhid=TEST_ORIGIN_ID))
    assert excinfo.value.code() == grpc.StatusCode.NOT_FOUND
    assert "Unavailable node" in excinfo.value.details()

    with pytest.raises(grpc.RpcError) as excinfo:
        graph_grpc_stub.GetNode(
            GetNodeRequest(swhid="swh:1:cnt:0000000000000000000000000000000000000005")
        )
    assert excinfo.value.code() == grpc.StatusCode.NOT_FOUND


def test_getnode(graph_grpc_stub, graph_grpc_backend_implementation):
    expected_cnts = [1, 4, 7, 11, 14, 15]

    for cnt_id in expected_cnts:
        graph_grpc_stub.GetNode(GetNodeRequest(swhid=f"swh:1:cnt:{cnt_id:040}"))


def test_traverse(graph_grpc_stub, graph_grpc_backend_implementation):
    request = graph_grpc_stub.Traverse(
        TraversalRequest(
            src=["swh:1:dir:0000000000000000000000000000000000000008"],
            return_nodes=NodeFilter(max_traversal_successors=0),
        )
    )
    assert {node.swhid for node in request} == {
        "swh:1:cnt:0000000000000000000000000000000000000001",
        "swh:1:cnt:0000000000000000000000000000000000000004",
        # "swh:1:cnt:0000000000000000000000000000000000000005",  # masked
        "swh:1:cnt:0000000000000000000000000000000000000007",
    }


def test_traverse_from_masked(graph_grpc_stub, graph_grpc_backend_implementation):
    with pytest.raises(grpc.RpcError) as excinfo:
        list(
            graph_grpc_stub.Traverse(
                TraversalRequest(
                    src=["swh:1:rel:0000000000000000000000000000000000000010"]
                )
            )
        )
    assert excinfo.value.code() == grpc.StatusCode.NOT_FOUND
    assert "Unavailable node" in excinfo.value.details()
