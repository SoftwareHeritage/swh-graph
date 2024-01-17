# Copyright (c) 2022-2023 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import hashlib

from google.protobuf.field_mask_pb2 import FieldMask
import pytest

from swh.graph.grpc.swhgraph_pb2 import (
    ContentData,
    EdgeLabel,
    GraphDirection,
    Node,
    OriginData,
    ReleaseData,
    RevisionData,
    Successor,
    TraversalRequest,
)

TEST_ORIGIN_ID = "swh:1:ori:{}".format(
    hashlib.sha1(b"https://example.com/swh/graph").hexdigest()
)
TEST_ORIGIN_ID2 = "swh:1:ori:{}".format(
    hashlib.sha1(b"https://example.com/swh/graph2").hexdigest()
)


def test_traverse_forward_labels(graph_grpc_stub, graph_grpc_backend_implementation):
    if graph_grpc_backend_implementation == "rust":
        raise pytest.skip("Labels are not supported by the Rust backend yet")
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
            cnt=ContentData(),
        ),
        Node(
            swhid="swh:1:cnt:0000000000000000000000000000000000000004",
            successor=None,
            cnt=ContentData(),
        ),
        Node(
            swhid="swh:1:cnt:0000000000000000000000000000000000000005",
            successor=None,
            cnt=ContentData(),
        ),
        Node(
            swhid="swh:1:cnt:0000000000000000000000000000000000000007",
            successor=None,
            cnt=ContentData(),
        ),
        Node(
            swhid="swh:1:dir:0000000000000000000000000000000000000002",
            successor=[
                Successor(
                    swhid="swh:1:cnt:0000000000000000000000000000000000000001",
                    label=[EdgeLabel(name=b"README.md", permission=0o100644)],
                )
            ],
        ),
        Node(
            swhid="swh:1:dir:0000000000000000000000000000000000000006",
            successor=[
                Successor(
                    swhid="swh:1:cnt:0000000000000000000000000000000000000004",
                    label=[EdgeLabel(name=b"README.md", permission=0o100644)],
                ),
                Successor(
                    swhid="swh:1:cnt:0000000000000000000000000000000000000005",
                    label=[EdgeLabel(name=b"parser.c", permission=0o100644)],
                ),
            ],
        ),
        Node(
            swhid="swh:1:dir:0000000000000000000000000000000000000008",
            successor=[
                Successor(
                    swhid="swh:1:cnt:0000000000000000000000000000000000000001",
                    label=[EdgeLabel(name=b"README.md", permission=0o100644)],
                ),
                Successor(
                    swhid="swh:1:cnt:0000000000000000000000000000000000000007",
                    label=[EdgeLabel(name=b"parser.c", permission=0o100644)],
                ),
                Successor(
                    swhid="swh:1:dir:0000000000000000000000000000000000000006",
                    label=[EdgeLabel(name=b"tests", permission=0o100755)],
                ),
            ],
        ),
        Node(
            swhid=TEST_ORIGIN_ID,
            successor=[
                Successor(
                    swhid="swh:1:snp:0000000000000000000000000000000000000020",
                    label=[],
                ),
            ],
            ori=OriginData(),
        ),
        Node(
            swhid="swh:1:rel:0000000000000000000000000000000000000010",
            successor=[
                Successor(
                    swhid="swh:1:rev:0000000000000000000000000000000000000009",
                    label=[],
                ),
            ],
            rel=ReleaseData(),
        ),
        Node(
            swhid="swh:1:rev:0000000000000000000000000000000000000003",
            successor=[
                Successor(
                    swhid="swh:1:dir:0000000000000000000000000000000000000002",
                    label=[],
                ),
            ],
            rev=RevisionData(),
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
            rev=RevisionData(),
        ),
        Node(
            swhid="swh:1:snp:0000000000000000000000000000000000000020",
            successor=[
                Successor(
                    swhid="swh:1:rel:0000000000000000000000000000000000000010",
                    label=[
                        EdgeLabel(name=b"refs/tags/v1.0"),
                    ],
                ),
                Successor(
                    swhid="swh:1:rev:0000000000000000000000000000000000000009",
                    label=[
                        EdgeLabel(name=b"refs/heads/master"),
                    ],
                ),
            ],
        ),
    ]
    actual = list(request)
    actual.sort(key=lambda node: node.swhid)
    for node in actual:
        node.successor.sort(key=lambda successor: successor.swhid)
    assert actual == expected


def test_traverse_backward_labels(graph_grpc_stub, graph_grpc_backend_implementation):
    if graph_grpc_backend_implementation == "rust":
        raise pytest.skip("Labels are not supported by the Rust backend yet")
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
                    label=[EdgeLabel(name=b"TODO.txt", permission=0o100644)],
                )
            ],
            cnt=ContentData(),
        ),
        Node(
            swhid="swh:1:dir:0000000000000000000000000000000000000016",
            successor=[
                Successor(
                    swhid="swh:1:dir:0000000000000000000000000000000000000017",
                    label=[EdgeLabel(name=b"old", permission=0o100755)],
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
            ori=OriginData(),
        ),
        Node(
            swhid="swh:1:rel:0000000000000000000000000000000000000019",
            successor=[],
            rel=ReleaseData(),
        ),
        Node(
            swhid="swh:1:rel:0000000000000000000000000000000000000021",
            successor=[
                Successor(
                    swhid="swh:1:snp:0000000000000000000000000000000000000022",
                    label=[
                        EdgeLabel(name=b"refs/tags/v2.0-anonymous"),
                    ],
                ),
            ],
            rel=ReleaseData(),
        ),
        Node(
            swhid="swh:1:rev:0000000000000000000000000000000000000018",
            successor=[
                Successor(
                    swhid="swh:1:rel:0000000000000000000000000000000000000019",
                    label=[],
                ),
                Successor(
                    swhid="swh:1:rel:0000000000000000000000000000000000000021",
                    label=[],
                ),
            ],
            rev=RevisionData(),
        ),
        Node(
            swhid="swh:1:snp:0000000000000000000000000000000000000022",
            successor=[
                Successor(
                    swhid=TEST_ORIGIN_ID2,
                    label=[],
                ),
            ],
        ),
    ]
    actual = list(request)
    actual.sort(key=lambda node: node.swhid)
    for node in actual:
        node.successor.sort(key=lambda successor: successor.swhid)
    assert actual == expected
