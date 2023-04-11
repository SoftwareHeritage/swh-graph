#!/usr/bin/env python3

__copyright__ = "Copyright (C) 2022 Roberto Di Cosmo"
__license__ = "GPL-3.0-or-later"

from hashlib import sha1

import click
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

# documentation: https://docs.softwareheritage.org/devel/apidoc/swh.model.swhids.html
from swh.model.swhids import ExtendedObjectType, ExtendedSWHID
from swh.web.client.client import WebAPIClient

# global variable holding headers parameters
headers = {}
swhcli = {}
verbose = False

GRAPH_GRPC_SERVER = "localhost:50091"


def fqswhid_of_traversal(response):
    # Build the Fully qualified SWHID
    global verbose
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
            # print(swhcli.get(node.swhid))
            fqswhid.append(node.swhid)
            lastid = node.swhid
            coreswhidtype = "cnt"
        if parsedid.object_type == ExtendedObjectType.DIRECTORY:
            # print(parsedid.object_type)
            # print(swhcli.get(node.swhid))
            if fqswhid:  # empty list signals coreswhid not found yet
                pathids = [
                    x["name"]
                    for x in swhcli.get(node.swhid)
                    if (str(x["target"]) == lastid)
                ]
                path.insert(0, pathids[0])  # raises exception if pathids is empty!
            else:
                fqswhid.append(node.swhid)
                coreswhidtype = "dir"
            lastid = node.swhid
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
        fqswhid.append("path=" + "/".join(path))
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


# Click docs: https://click.palletsprojects.com/en/8.0.x/options/
@click.command(
    help="""Utility to get the fully qualified SWHID for a given core SWHID.
            Uses the graph traversal to find the shortest path to an origin, and
            retains the first seen revision or release as anchor for cnt and dir types."""
)
@click.option(
    "-t",
    "--swh-bearer-token",
    default="",
    metavar="SWHTOKEN",
    show_default=True,
    help="bearer token to bypass SWH API rate limit",
)
@click.option(
    "-g",
    "--graph-grpc-server",
    default="localhost:50091",
    metavar="GRAPH_GRPC_SERVER",
    show_default=True,
    help="Graph RPC server address: as host:port",
)
@click.option(
    "-c",
    "--content-swhid",
    default="swh:1:cnt:3b997e8ef2e38d5b31fb353214a54686e72f0870",
    metavar="CNTSWHID",
    show_default=True,
    help="SWHID of the content",
)
@click.option(
    "-f",
    "--filename",
    default="",
    metavar="FILENAME",
    show_default=True,
    help="Name of file to search for",
)
@click.option(
    "-o",
    "--origin-url",
    default="",
    metavar="ORIGINURL",
    show_default=True,
    help="URL of the origin where we look for a content",
)
@click.option(
    "--all-origins/--no-all-origins",
    default=False,
    help="Compute fqswhid for all origins",
)
@click.option(
    "--fqswhid/--no-fqswhid",
    default=True,
    help="Compute fqswhid. If disabled, print only the origins.",
)
@click.option(
    "--trace/--no-trace",
    default=False,
    help="Print nodes examined while building fully qualified SWHID.",
)
@click.option(
    "--random-origin/--no-random-origin",
    default=True,
    help="Compute fqswhid for a random origin",
)
def main(
    swh_bearer_token,
    content_swhid,
    origin_url,
    all_origins,
    random_origin,
    filename,
    graph_grpc_server,
    fqswhid,
    trace,
):
    global headers
    global swhcli
    global verbose
    verbose = trace
    if swh_bearer_token:
        swhcli = WebAPIClient(
            api_url="https://archive.softwareheritage.org/api/1/",
            bearer_token=swh_bearer_token,
        )
    else:
        swhcli = WebAPIClient(api_url="https://archive.softwareheritage.org/api/1/")

    with grpc.insecure_channel(graph_grpc_server) as channel:
        client = TraversalServiceStub(channel)

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
                        mask=FieldMask(paths=["swhid", "ori.url"]),
                    )
                )
                for node in response:
                    if fqswhid:
                        response = client.FindPathBetween(
                            FindPathBetweenRequest(
                                src=[content_swhid],
                                dst=[node.swhid],
                                direction="BACKWARD",
                                mask=FieldMask(paths=["swhid", "ori.url"]),
                            )
                        )
                        print(fqswhid_of_traversal(response))
                    else:
                        print(node.ori.url)

            # Traversal request to a (random) origin
            if random_origin:
                response = client.FindPathTo(
                    FindPathToRequest(
                        src=[content_swhid],
                        target=NodeFilter(types="ori"),
                        direction="BACKWARD",
                        mask=FieldMask(paths=["swhid", "ori.url"]),
                    )
                )
                if fqswhid:
                    print(fqswhid_of_traversal(response))
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
                        mask=FieldMask(paths=["swhid", "ori.url"]),
                    )
                )
                print(fqswhid_of_traversal(response))

        except grpc.RpcError as e:
            print("Error from the GRPC API call: {}".format(e.details()))
            if filename:
                print(filename + " has SWHID " + content_swhid)
        except Exception as e:
            print("Unexpected error occurred: {}".format(e))


if __name__ == "__main__":
    main()
