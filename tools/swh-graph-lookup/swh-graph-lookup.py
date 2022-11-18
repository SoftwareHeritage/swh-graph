#!/usr/bin/env python3

__copyright__ = "Copyright (C) 2022 Roberto Di Cosmo"
__license__ = "GPL-3.0-or-later"

from swh.web.client.client import WebAPIClient
# documentation: https://docs.softwareheritage.org/devel/apidoc/swh.model.swhids.html
from swh.model.swhids import ExtendedSWHID
from swh.model.swhids import ExtendedObjectType
# from swh.model.swhids import CoreSWHID
# from swh.model.swhids import ObjectType
import grpc
import click
from hashlib import sha1

# global variable holding headers parameters
headers={}
swhcli={}

from swh.graph.grpc.swhgraph_pb2 import (
    NodeFilter,
    StatsRequest,
    TraversalRequest,
    FindPathToRequest,
    FindPathBetweenRequest
)

from swh.graph.grpc.swhgraph_pb2_grpc import TraversalServiceStub

from google.protobuf.field_mask_pb2 import FieldMask

GRAPH_GRPC_SERVER = 'localhost:50091'

def fqswhid_of_traversal(response):
    # Build the Fully qualified SWHID
    fqswhid=[]
    needrevision=True
    needrelease=True
    path=[]
    url=""
    for node in response.node:
        # response contains the nodes in the order content -> dir -> revision -> release -> snapshot -> origin
        if url == "" :
            url = node.ori.url
        parsedid=ExtendedSWHID.from_string(node.swhid)
        if parsedid.object_type == ExtendedObjectType.CONTENT :
            # print(parsedid.object_type)
            # print(swhcli.get(node.swhid))
            fqswhid.append(node.swhid)
            lastid=node.swhid
            lasttype='file'
        if parsedid.object_type == ExtendedObjectType.DIRECTORY :
            # print(parsedid.object_type)
            # print(swhcli.get(node.swhid))
            pathids=[x['name'] for x in swhcli.get(node.swhid) if (str(x['target']) == lastid)]
            path.insert(0,pathids[0]) # raises exception if pathids is empty!
            lastid=node.swhid
            lasttype='dir'
        if parsedid.object_type == ExtendedObjectType.REVISION :
            if needrevision :
                revision=node.swhid
                needrevision=False
        if parsedid.object_type == ExtendedObjectType.RELEASE :
            if needrelease :
                release=node.swhid
                needrelease=False
        if parsedid.object_type == ExtendedObjectType.SNAPSHOT :
            snapshot=node.swhid
    # Now we have all the elements to print a FQ
    # TODO build a swh.model.swhids.QualifiedSWHID
    fqswhid.append('path='+"/".join(path))
    if needrevision==False :
        fqswhid.append('anchor='+revision)
    elif  needrelease==False :
        fqswhid.append('anchor='+revision)
    if snapshot :
        fqswhid.append('visit='+snapshot)
    if url:
        fqswhid.append('origin='+url)
    return(";".join(fqswhid))



# Click docs: https://click.palletsprojects.com/en/8.0.x/options/
@click.command(
    help="""Utility to get the fully qualified SWHID for a given content core SWHID.
            Uses the graph traversal to find the shortest path to an origin, and
            retains the first seen revision or release as anchor."""
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
    "-c",
    "--content-swhid",
    default="swh:1:cnt:3b997e8ef2e38d5b31fb353214a54686e72f0870",
    metavar="CNTSWHID",
    show_default=True,
    help="SWHID of the content",
)
@click.option(
    "-o",
    "--origin-url",
    default="https://github.com/rdicosmo/parmap",
    metavar="ORIGINURL",
    show_default=True,
    help="URL of the origin where we look for a content",
)
def main(swh_bearer_token,content_swhid,origin_url):
    global headers
    global swhcli
    if (swh_bearer_token):
       swhcli = WebAPIClient(api_url="https://archive.softwareheritage.org/api/1/",
                             bearer_token=swh_bearer_token)
    else:
       swhcli = WebAPIClient(api_url="https://archive.softwareheritage.org/api/1/")

    with grpc.insecure_channel(GRAPH_GRPC_SERVER) as channel:
        client = TraversalServiceStub(channel)
        
        # Stats request
        if False:
            print(client.Stats(StatsRequest()))
        
        # Traversal request
        if False:
            request = TraversalRequest(
                src=["swh:1:cnt:3b997e8ef2e38d5b31fb353214a54686e72f0870"],
                edges="cnt:dir,dir:dir,dir:rev,rev:rev,rev:rel,rel:snp,snp:ori",
                direction="BACKWARD",
                return_nodes=NodeFilter(types="cnt,dir,rev,rel,snp,ori"),
                mask=FieldMask(paths=["swhid", "rel.message", "rel.author", "ori.url"]),
            )
            for node in client.Traverse(request):
                # print(node)
                print(node.swhid)
                
            print("-----------------")

        # Traversal request: get all origins
        request = TraversalRequest(
            src=["swh:1:cnt:8722d84d658e5e11519b807abb5c05bfbfc531f0"],
            edges="cnt:dir,dir:dir,dir:rev,rev:rev,rev:rel,rel:snp,snp:ori",
            direction="BACKWARD",
            return_nodes=NodeFilter(types="ori"),
            mask=FieldMask(paths=["swhid", "ori.url"]),
        )
        for node in client.Traverse(request):
            response = client.FindPathBetween(FindPathBetweenRequest(
                src=[content_swhid],
                dst=[node.swhid],
                direction="BACKWARD",
                mask=FieldMask(paths=["swhid","ori.url"]),
            ))
            print(fqswhid_of_traversal(response))
                
            print("-----------------")
            
        # Traversal request to an origin
        
        response = client.FindPathTo(FindPathToRequest(
            src=[content_swhid],
            target=NodeFilter(types="ori"),
            direction="BACKWARD",
            mask=FieldMask(paths=["swhid","ori.url"]),
        ))

        print(fqswhid_of_traversal(response))

        # Traversal request to a given origin URL
        
        response = client.FindPathBetween(FindPathBetweenRequest(
            src=[content_swhid],
            dst=[str(ExtendedSWHID(
                object_type=ExtendedObjectType.ORIGIN,
                object_id=bytes.fromhex(sha1(bytes(origin_url,'UTF-8')).hexdigest())
            ))],
            direction="BACKWARD",
            mask=FieldMask(paths=["swhid","ori.url"]),
        ))
        print(fqswhid_of_traversal(response))
        
if __name__ == "__main__":
    main()

        
