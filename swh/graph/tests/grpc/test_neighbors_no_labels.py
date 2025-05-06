# Copyright (c) 2022-2025 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from pathlib import Path

from google.protobuf.field_mask_pb2 import FieldMask
import pytest

from swh.graph.example_dataset import DATASET_DIR
from swh.graph.grpc.swhgraph_pb2 import (
    GraphDirection,
    Node,
    Successor,
    TraversalRequest,
)

# locally "redefine" all fixtures that depend on the session-scoped
# graph_grpc_server_config, because we need pytest to call them again.
from swh.graph.pytest_plugin import (  # noqa
    graph_grpc_server_process,
    graph_grpc_server_started,
)

from .test_getnode import (  # noqa
    TEST_ORIGIN_ID,
    TEST_ORIGIN_ID2,
    test_invalid_swhid,
    test_not_found,
)
from .test_neighbors import traverse_nodes


@pytest.fixture(scope="session")
def graph_grpc_server_config(
    graph_grpc_backend_implementation, graph_statsd_server, tmpdir_factory
):
    full_graph_dir = DATASET_DIR / "compressed"
    partial_graph_path = Path(tmpdir_factory.mktemp("partial-graph"))
    for filepath in full_graph_dir.iterdir():
        # copy everything but labels
        if "-labelled" in filepath.name:
            continue
        symlink_path = partial_graph_path / filepath.name
        symlink_path.symlink_to(filepath)

    return {
        "graph": {
            "cls": f"local_{graph_grpc_backend_implementation}",
            "grpc_server": {
                "path": partial_graph_path / "example",
                "extra_options": ["--labels", "none"],
                "debug": True,
                "statsd_host": graph_statsd_server.host,
                "statsd_port": graph_statsd_server.port,
            },
            "http_rpc_server": {"debug": True},
        }
    }


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


def test_traverse_forward_labels(graph_grpc_stub):
    request = graph_grpc_stub.Traverse(
        TraversalRequest(
            src=[TEST_ORIGIN_ID],
            mask=FieldMask(paths=["swhid", "successor.swhid", "successor.label"]),
            direction=GraphDirection.FORWARD,
        )
    )
    expected = [
        Node(
            swhid="swh:1:cnt:0000000000000000000000000000000000000001",
            successor=None,
            cnt=None,
        ),
        Node(
            swhid="swh:1:cnt:0000000000000000000000000000000000000004",
            successor=None,
            cnt=None,
        ),
        Node(
            swhid="swh:1:cnt:0000000000000000000000000000000000000005",
            successor=None,
            cnt=None,
        ),
        Node(
            swhid="swh:1:cnt:0000000000000000000000000000000000000007",
            successor=None,
            cnt=None,
        ),
        Node(
            swhid="swh:1:dir:0000000000000000000000000000000000000002",
            successor=[
                Successor(
                    swhid="swh:1:cnt:0000000000000000000000000000000000000001",
                    label=None,
                )
            ],
        ),
        Node(
            swhid="swh:1:dir:0000000000000000000000000000000000000006",
            successor=[
                Successor(
                    swhid="swh:1:cnt:0000000000000000000000000000000000000004",
                    label=None,
                ),
                Successor(
                    swhid="swh:1:cnt:0000000000000000000000000000000000000005",
                    label=None,
                ),
            ],
        ),
        Node(
            swhid="swh:1:dir:0000000000000000000000000000000000000008",
            successor=[
                Successor(
                    swhid="swh:1:cnt:0000000000000000000000000000000000000001",
                    label=None,
                ),
                Successor(
                    swhid="swh:1:cnt:0000000000000000000000000000000000000007",
                    label=None,
                ),
                Successor(
                    swhid="swh:1:dir:0000000000000000000000000000000000000006",
                    label=None,
                ),
            ],
        ),
        Node(
            swhid=TEST_ORIGIN_ID,
            successor=[
                Successor(
                    swhid="swh:1:snp:0000000000000000000000000000000000000020",
                    label=None,
                ),
            ],
            ori=None,
        ),
        Node(
            swhid="swh:1:rel:0000000000000000000000000000000000000010",
            successor=[
                Successor(
                    swhid="swh:1:rev:0000000000000000000000000000000000000009",
                    label=[],
                ),
            ],
            rel=None,
        ),
        Node(
            swhid="swh:1:rev:0000000000000000000000000000000000000003",
            successor=[
                Successor(
                    swhid="swh:1:dir:0000000000000000000000000000000000000002",
                    label=[],
                ),
            ],
            rev=None,
        ),
        Node(
            swhid="swh:1:rev:0000000000000000000000000000000000000009",
            successor=[
                Successor(
                    swhid="swh:1:dir:0000000000000000000000000000000000000008",
                    label=[],
                ),
                Successor(
                    swhid="swh:1:rev:0000000000000000000000000000000000000003",
                    label=[],
                ),
            ],
            rev=None,
        ),
        Node(
            swhid="swh:1:snp:0000000000000000000000000000000000000020",
            successor=[
                Successor(
                    swhid="swh:1:rel:0000000000000000000000000000000000000010",
                    label=None,
                ),
                Successor(
                    swhid="swh:1:rev:0000000000000000000000000000000000000009",
                    label=None,
                ),
            ],
        ),
    ]
    actual = list(request)
    actual.sort(key=lambda node: node.swhid)
    for node in actual:
        node.successor.sort(key=lambda successor: successor.swhid)
    assert actual == expected


def test_traverse_backward_labels(graph_grpc_stub):
    request = graph_grpc_stub.Traverse(
        TraversalRequest(
            src=["swh:1:cnt:0000000000000000000000000000000000000015"],
            mask=FieldMask(paths=["swhid", "successor.swhid", "successor.label"]),
            direction=GraphDirection.BACKWARD,
        )
    )
    expected = [
        Node(
            swhid="swh:1:cnt:0000000000000000000000000000000000000015",
            successor=[
                Successor(
                    swhid="swh:1:dir:0000000000000000000000000000000000000016",
                    label=None,
                )
            ],
            cnt=None,
        ),
        Node(
            swhid="swh:1:dir:0000000000000000000000000000000000000016",
            successor=[
                Successor(
                    swhid="swh:1:dir:0000000000000000000000000000000000000017",
                    label=None,
                ),
            ],
        ),
        Node(
            swhid="swh:1:dir:0000000000000000000000000000000000000017",
            successor=[
                Successor(
                    swhid="swh:1:rev:0000000000000000000000000000000000000018",
                    label=[],
                ),
            ],
        ),
        Node(
            swhid=TEST_ORIGIN_ID2,
            successor=None,
            ori=None,
        ),
        Node(
            swhid="swh:1:rel:0000000000000000000000000000000000000019",
            successor=[],
            rel=None,
        ),
        Node(
            swhid="swh:1:rel:0000000000000000000000000000000000000021",
            successor=[
                Successor(
                    swhid="swh:1:snp:0000000000000000000000000000000000000022",
                    label=None,
                ),
            ],
            rel=None,
        ),
        Node(
            swhid="swh:1:rev:0000000000000000000000000000000000000018",
            successor=[
                Successor(
                    swhid="swh:1:rel:0000000000000000000000000000000000000019",
                    label=None,
                ),
                Successor(
                    swhid="swh:1:rel:0000000000000000000000000000000000000021",
                    label=None,
                ),
            ],
            rev=None,
        ),
        Node(
            swhid="swh:1:snp:0000000000000000000000000000000000000022",
            successor=[
                Successor(
                    swhid=TEST_ORIGIN_ID2,
                    label=None,
                ),
            ],
        ),
    ]
    actual = list(request)
    actual.sort(key=lambda node: node.swhid)
    for node in actual:
        node.successor.sort(key=lambda successor: successor.swhid)
    assert actual == expected
