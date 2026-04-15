# Copyright (C) 2022-2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from hashlib import sha1
import logging

import attr
from google.protobuf.field_mask_pb2 import FieldMask
import grpc

from swh.graph.grpc.swhgraph_pb2 import (
    FindPathBetweenRequest,
    FindPathToRequest,
    NodeFilter,
    TraversalRequest,
)
from swh.graph.grpc.swhgraph_pb2_grpc import TraversalServiceStub
from swh.model.cli import swhid_of_file
from swh.model.exceptions import ValidationError

# documentation: https://docs.softwareheritage.org/devel/apidoc/swh.model.swhids.html
from swh.model.swhids import CoreSWHID, ExtendedObjectType, ExtendedSWHID

GRAPH_GRPC_SERVER = "localhost:50091"

logger = logging.getLogger(__name__)


def fqswhid_of_traversal(response, verbose):
    """Build the fully qualified SWHID for a gRPC response.

    Args:
        response: Response from the gRPC server.
        verbose: Verbosity.

    Returns:
        The fully qualified SWHID corresponding to the response from the gRPC
        server, as a string.
    """
    path_items = []
    revision = None
    release = None
    snapshot = None
    origin = ""
    shortest_path = iter(response.labeled_node)
    target_labeled_node = next(shortest_path)
    swhid = CoreSWHID.from_string(target_labeled_node.node.swhid).to_qualified()
    core_swhid = target_node_swhid = ExtendedSWHID.from_string(
        target_labeled_node.node.swhid
    )

    for source_labeled_node in shortest_path:
        if verbose:
            print("Examining node: {target_labeled_node.node.swhid}")

        origin = source_labeled_node.node.ori.url if origin == "" else ""

        source_node_swhid = ExtendedSWHID.from_string(source_labeled_node.node.swhid)

        if target_node_swhid.object_type in (
            ExtendedObjectType.CONTENT,
            ExtendedObjectType.DIRECTORY,
        ):
            if source_node_swhid.object_type == ExtendedObjectType.DIRECTORY:
                if len(target_labeled_node.label) > 0:
                    pathid = target_labeled_node.label[0].name.decode()
                    path_items.insert(0, pathid)

        if target_node_swhid.object_type == ExtendedObjectType.REVISION:
            if revision is None:
                revision = target_labeled_node.node.swhid

        if target_node_swhid.object_type == ExtendedObjectType.RELEASE:
            if release is None:
                release = target_labeled_node.node.swhid

        if target_node_swhid.object_type == ExtendedObjectType.SNAPSHOT:
            snapshot = target_labeled_node.node.swhid

        target_labeled_node = source_labeled_node
        target_node_swhid = source_node_swhid

    visit = snapshot if core_swhid.object_type != ExtendedObjectType.SNAPSHOT else None

    if (
        core_swhid.object_type == ExtendedObjectType.CONTENT
        or core_swhid.object_type == ExtendedObjectType.DIRECTORY
    ):
        anchor = revision or release
        path = f"/{'/'.join(path_items)}"
    else:
        anchor = path = None

    fqswhid = attr.evolve(swhid, origin=origin, visit=visit, anchor=anchor, path=path)

    return str(fqswhid)


def main(
    content_swhid,
    origin_url,
    all_origins,
    random_origin,
    filename,
    graph_grpc_server,
    fqswhid,
    trace,
):

    # Check if content SWHID is valid
    try:
        CoreSWHID.from_string(content_swhid)
    except ValidationError:
        print(f"Error: '{content_swhid}' is not a valid SWHID")
        return

    with grpc.insecure_channel(graph_grpc_server) as channel:
        client = TraversalServiceStub(channel)

        field_mask_findpath = FieldMask(
            paths=[
                "labeled_node.node.swhid",
                "labeled_node.node.ori.url",
                "labeled_node.label.name",
            ]
        )

        field_mask_traverse = FieldMask(
            paths=[
                "node.swhid",
                "node.ori.url",
                "node.successor.swhid",
                "node.successor.label.name",
            ]
        )

        try:
            if filename:
                content_swhid = str(swhid_of_file(filename))

            # Traversal request: get all origins
            if all_origins:
                random_origin = False
                response = client.Traverse(
                    TraversalRequest(
                        src=[content_swhid],
                        edges="cnt:dir,dir:dir,dir:rev,rev:rev,rev:snp,rev:rel,rel:snp,snp:ori",
                        direction="BACKWARD",
                        return_nodes=NodeFilter(types="ori"),
                        mask=field_mask_traverse,
                    )
                )
                for node in response:
                    if fqswhid:
                        response = client.FindPathBetween(
                            FindPathBetweenRequest(
                                src=[content_swhid],
                                dst=[node.swhid],
                                direction="BACKWARD",
                                mask=field_mask_findpath,
                            )
                        )
                        print(fqswhid_of_traversal(response, verbose=trace))
                    else:
                        print(node.ori.url)

            # Traversal request to a (random) origin
            if random_origin:
                response = client.FindPathTo(
                    FindPathToRequest(
                        src=[content_swhid],
                        target=NodeFilter(types="ori"),
                        direction="BACKWARD",
                        mask=field_mask_findpath,
                    )
                )
                if fqswhid:
                    print(fqswhid_of_traversal(response, verbose=trace))
                else:
                    for labeled_node in response.labeled_node.node:
                        print(labeled_node.node.ori.url)

            # Traversal request to a given origin URL
            if origin_url:
                response = client.FindPathBetween(
                    FindPathBetweenRequest(
                        src=[content_swhid],
                        dst=[
                            str(
                                ExtendedSWHID(
                                    object_type=ExtendedObjectType.ORIGIN,
                                    object_id=bytes.fromhex(
                                        sha1(bytes(origin_url, "UTF-8")).hexdigest()
                                    ),
                                )
                            )
                        ],
                        direction="BACKWARD",
                        mask=field_mask_findpath,
                    )
                )
                print(fqswhid_of_traversal(response, verbose=trace))

        except grpc.RpcError as e:
            print("Error from the GRPC API call: {}".format(e.details()))
            if filename:
                print(filename + " has SWHID " + content_swhid)
        except Exception:
            logger.exception("Unexpected error occurred:")
