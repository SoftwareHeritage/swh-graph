# Copyright (C) 2022-2025  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from hashlib import sha1

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


def fqswhid_of_traversal(response, verbose):
    # Build the Fully qualified SWHID
    fqswhid = []
    coreswhidtype = ""
    needrevision = True
    needrelease = True
    path = []
    url = ""
    for node in response.node:
        # response contains the nodes in the order content -> dir -> revision
        # -> release -> snapshot -> origin
        if verbose:
            print(" examining node: " + node.swhid)
        if url == "":
            url = node.ori.url
        parsedid = ExtendedSWHID.from_string(node.swhid)
        if parsedid.object_type == ExtendedObjectType.CONTENT:
            # print(parsedid.object_type)
            pathids = [
                label.name.decode()
                for successor in node.successor
                for label in successor.label
            ]
            path.insert(0, pathids[0])  # raises exception if pathids is empty!
            fqswhid.append(node.swhid)
            coreswhidtype = "cnt"
        if parsedid.object_type == ExtendedObjectType.DIRECTORY:
            # print(parsedid.object_type)
            if fqswhid:  # empty list signals coreswhid not found yet
                pathids = [
                    label.name.decode()
                    for successor in node.successor
                    for label in successor.label
                ]
                path.insert(0, pathids[0])  # raises exception if pathids is empty!
            else:
                fqswhid.append(node.swhid)
                coreswhidtype = "dir"
        if parsedid.object_type == ExtendedObjectType.REVISION:
            if fqswhid:  # empty list signals coreswhid not found yet
                if needrevision:
                    revision = node.swhid
                    needrevision = False
            else:
                fqswhid.append(node.swhid)
                coreswhidtype = "rev"
        if parsedid.object_type == ExtendedObjectType.RELEASE:
            if fqswhid:  # empty list signals coreswhid not found yet
                if needrelease:
                    release = node.swhid
                    needrelease = False
            else:
                fqswhid.append(node.swhid)
                coreswhidtype = "rel"
        if parsedid.object_type == ExtendedObjectType.SNAPSHOT:
            if fqswhid:  # empty list signals coreswhid not found yet
                snapshot = node.swhid
            else:
                fqswhid.append(node.swhid)
                coreswhidtype = "snp"
    # Now we have all the elements to print a FQ SWHID
    # We could also build and return a swh.model.swhids.QualifiedSWHID
    if coreswhidtype == "cnt" or coreswhidtype == "dir":
        fqswhid.append("path=/" + "/".join(path))
        if not needrevision:
            fqswhid.append("anchor=" + revision)
        elif not needrelease:
            fqswhid.append("anchor=" + release)
        if snapshot:
            fqswhid.append("visit=" + snapshot)
    elif coreswhidtype == "rev" or coreswhidtype == "rel":
        if snapshot:
            fqswhid.append("visit=" + snapshot)
    if url:
        fqswhid.append("origin=" + url)
    return ";".join(fqswhid)


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
        except Exception as e:
            print("Unexpected error occurred: {}".format(e))
