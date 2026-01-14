# Copyright (C) 2022-2025  The Software Heritage developers
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
    # Build the Fully qualified SWHID
    path_items = []
    revision = None
    release = None
    snapshot = None
    url = ""
    shortest_path = iter(response.node)
    target_node = next(shortest_path)
    swhid = target_node.swhid
    core_swhid = target_node_swhid = ExtendedSWHID.from_string(target_node.swhid)
    for source_node in shortest_path:
        # response contains the nodes in the order content -> dir -> revision
        # -> release -> snapshot -> origin
        if verbose:
            print(" examining node: " + target_node.swhid)
        if url == "":
            url = source_node.ori.url
        source_node_swhid = ExtendedSWHID.from_string(source_node.swhid)
        if target_node_swhid.object_type == ExtendedObjectType.CONTENT:
            pathid = next(
                (
                    label.name.decode()
                    for successor in target_node.successor
                    for label in successor.label
                    if successor.swhid == source_node.swhid
                    and source_node_swhid.object_type == ExtendedObjectType.DIRECTORY
                ),
                None,
            )

            # pathid might be empty if the content is referenced directly by a
            # revision/release/snapshot (eg. because we archive single patch files
            # for nix/guix)
            if pathid:
                path_items.insert(0, pathid)
        if target_node_swhid.object_type == ExtendedObjectType.DIRECTORY:
            pathid = next(
                (
                    label.name.decode()
                    for successor in target_node.successor
                    for label in successor.label
                    if successor.swhid == source_node.swhid
                    and source_node_swhid.object_type == ExtendedObjectType.DIRECTORY
                ),
                None,
            )
            # pathid is empty for a root directory
            if pathid:
                path_items.insert(0, pathid)

        if target_node_swhid.object_type == ExtendedObjectType.REVISION:
            if revision is None:
                revision = target_node.swhid
        if target_node_swhid.object_type == ExtendedObjectType.RELEASE:
            if release is None:
                release = target_node.swhid
        if target_node_swhid.object_type == ExtendedObjectType.SNAPSHOT:
            snapshot = target_node.swhid
        target_node = source_node
        target_node_swhid = source_node_swhid

    # Now we have all the elements to print a FQ SWHID
    if (
        core_swhid.object_type == ExtendedObjectType.CONTENT
        or core_swhid.object_type == ExtendedObjectType.DIRECTORY
    ):
        path = "/".join(path_items)
        anchor = revision or release
    else:
        path = anchor = None

    if core_swhid.object_type != ExtendedObjectType.SNAPSHOT:
        visit = snapshot
    else:
        visit = None

    fqswhid = attr.evolve(
        CoreSWHID.from_string(swhid).to_qualified(),
        path=f"/{path}" if path is not None else None,
        anchor=anchor,
        visit=visit,
        origin=url or None,
    )

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
        field_mask = FieldMask(
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
                        mask=field_mask,
                    )
                )
                for node in response:
                    if fqswhid:
                        response = client.FindPathBetween(
                            FindPathBetweenRequest(
                                src=[content_swhid],
                                dst=[node.swhid],
                                direction="BACKWARD",
                                mask=field_mask,
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
                        mask=field_mask,
                    )
                )
                if fqswhid:
                    print(fqswhid_of_traversal(response, verbose=trace))
                else:
                    for node in response.node:
                        print(node.ori.url)

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
                        mask=field_mask,
                    )
                )
                print(fqswhid_of_traversal(response, verbose=trace))

        except grpc.RpcError as e:
            print("Error from the GRPC API call: {}".format(e.details()))
            if filename:
                print(filename + " has SWHID " + content_swhid)
        except Exception:
            logger.exception("Unexpected error occurred:")
