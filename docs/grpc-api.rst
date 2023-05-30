.. _swh-graph-grpc-api:

==================
Using the GRPC API
==================

The GRPC API is the core API used to query the graph remotely. It uses the
`GRPC framework <https://grpc.io/>`_ to provide high-performance graph
traversal methods with server streaming.

It is more expressive than the :ref:`HTTP API <swh-graph-api>` (which itself
uses the GRPC API under the hood to serve queries), however it can only be
used internally or with a local setup, and is never exposed publicly.

Its major features include: returning node and edge properties, performing BFS
traversals, including traversals with more than one starting node, finding
shortest paths, common ancestors, etc.

Quickstart
==========

Starting the server
-------------------

The GRPC server is automatically started on port 50091 when the HTTP server
is started with ``swh graph rpc-serve``. It can also be started directly with
Java, instead of going through the Python layer, by using the fat-jar shipped
with swh-graph:

.. code-block:: console

    $ java -cp swh-graph-XXX.jar org.softwareheritage.graph.rpc.GraphServer <graph_basename>

(See :ref:`swh-graph-java-api` and :ref:`swh-graph-memory` for more
information on Java process options and JVM tuning.)

Running queries
---------------

The `gRPC command line tool
<https://github.com/grpc/grpc/blob/master/doc/command_line_tool.md>`_
can be an easy way to query the GRPC API from the command line. It is
invoked with the ``grpc_cli`` command. Of course, it is also possible to use
a generated RPC client in any programming language supported by GRPC.

All RPC methods are defined in the service ``swh.graph.TraversalService``.
The available endpoints can be listed with ``ls``:

.. code-block:: console

    $ grpc_cli ls localhost:50091 swh.graph.TraversalService
    Traverse
    FindPathTo
    FindPathBetween
    CountNodes
    CountEdges
    Stats
    GetNode

A RPC method can be called with the ``call`` subcommand.

.. code-block:: console

    $ grpc_cli call localhost:50091 swh.graph.TraversalService.Stats ""
    connecting to localhost:50091
    num_nodes: 21
    num_edges: 23
    compression: 1.412
    bits_per_node: 8.524
    [...]
    Rpc succeeded with OK status

The ``--json-output`` flag can also be used to make the results easier to
parse.

.. code-block:: console

    $ grpc_cli --json_output call localhost:50091 swh.graph.TraversalService.Stats ""
    connecting to localhost:50091
    {
     "numNodes": "21",
     "numEdges": "23",
     [...]
    }
    Rpc succeeded with OK status


Or, in Python:

.. code-block:: python

    import grpc

    import swh.graph.grpc.swhgraph_pb2 as swhgraph
    import swh.graph.grpc.swhgraph_pb2_grpc as swhgraph_grpc

    GRAPH_GRPC_SERVER = "granet.internal.softwareheritage.org:50091"

    with grpc.insecure_channel(GRAPH_GRPC_SERVER) as channel:
        stub = swhgraph_grpc.TraversalServiceStub(channel)
        response = stub.Stats(swhgraph.StatsRequest())
        print(response)
        print("Compression ratio:", response.compression_ratio * 100, "%")


which prints:

.. code-block::

    num_nodes: 25340003875
    num_edges: 359467940510
    compression_ratio: 0.096
    bits_per_node: 43.993
    bits_per_edge: 3.101
    avg_locality: 1030367242.935
    indegree_max: 381552037
    indegree_avg: 14.185788695346046
    outdegree_max: 1033207
    outdegree_avg: 14.185788695346046
    export_started_at: 1669888200

    Compression ratio: 9.6 %


**Note**: grpc_cli's outputs in this document are slightly modified for
readability's sake.

Simple queries
==============

For a full documentation of all the endpoints, as well as the request and
response messages, see :ref:`swh-graph-grpc-api-protobuf`.

All Python examples below assume they are run in the following context:

.. code-block:: python

    import grpc

    from google.protobuf.field_mask_pb2 import FieldMask

    import swh.graph.grpc.swhgraph_pb2 as swhgraph
    import swh.graph.grpc.swhgraph_pb2_grpc as swhgraph_grpc

    GRAPH_GRPC_SERVER = "granet.internal.softwareheritage.org:50091"

    with grpc.insecure_channel(GRAPH_GRPC_SERVER) as channel:
        stub = swhgraph_grpc.TraversalServiceStub(channel)
        pass  # <insert snippet here>

Querying a single node
----------------------

The **GetNode** endpoint can be used to return information on a single
node of the graph, including all its node properties, from its SWHID. Here
are a few examples from the test graph:

Content
~~~~~~~

.. code-block:: console

    $ grpc_cli call localhost:50091 swh.graph.TraversalService.GetNode \
        'swhid: "swh:1:cnt:0000000000000000000000000000000000000001"'

.. code-block:: python

    swhid = "swh:1:cnt:0000000000000000000000000000000000000001"
    response = stub.GetNode(swhgraph.GetNodeRequest(swhid=swhid))
    print(response)
    # results will be in response.cnt.length and response.cnt.is_skipped

.. code-block:: javascript

    swhid: "swh:1:cnt:0000000000000000000000000000000000000001"
    cnt {
      length: 42
      is_skipped: false
    }

Revision
~~~~~~~~

.. code-block:: console

    $ grpc_cli call localhost:50091 swh.graph.TraversalService.GetNode \
        'swhid: "swh:1:rev:0000000000000000000000000000000000000009"'

.. code-block:: python

    swhid = "swh:1:rev:0000000000000000000000000000000000000009"
    response = stub.GetNode(swhgraph.GetNodeRequest(swhid=swhid))
    print(response)
    # results will be in response.rev.author, response.rev.author_date, ...

.. code-block:: javascript

    swhid: "swh:1:rev:0000000000000000000000000000000000000009"
    rev {
      author: 2
      author_date: 1111140840
      author_date_offset: 120
      committer: 2
      committer_date: 1111151950
      committer_date_offset: 120
      message: "Add parser"
    }

Note that author and committer names are not available in the compressed graph,
so you must use either the :swh_web:`public API <1/revision/>` or swh-storage
directly to access them.

Release
~~~~~~~

.. code-block:: console

    $ grpc_cli call localhost:50091 swh.graph.TraversalService.GetNode \
        'swhid: "swh:1:rel:0000000000000000000000000000000000000010"'

.. code-block:: python

    swhid = "swh:1:rel:0000000000000000000000000000000000000010"
    response = stub.GetNode(swhgraph.GetNodeRequest(swhid=swhid))
    print(response)
    # results will be in response.rel.author, response.rel.author_date, ...

.. code-block:: javascript

    swhid: "swh:1:rel:0000000000000000000000000000000000000010"
    rel {
      author: 0
      author_date: 1234564290
      author_date_offset: 120
      message: "Version 1.0"
    }

Origin
~~~~~~

.. code-block:: console

    $ grpc_cli call localhost:50091 swh.graph.TraversalService.GetNode \
        'swhid: "swh:1:ori:83404f995118bd25774f4ac14422a8f175e7a054"'

.. code-block:: python

    swhid = "swh:1:ori:83404f995118bd25774f4ac14422a8f175e7a054"
    response = stub.GetNode(swhgraph.GetNodeRequest(swhid=swhid))
    print(response)
    # results will be in response.ori.url

.. code-block:: javascript

    swhid: "swh:1:ori:83404f995118bd25774f4ac14422a8f175e7a054"
    ori {
      url: "https://example.com/swh/graph"
    }


Checking the presence of a node
-------------------------------

The **GetNode** endpoint can also be used to check if a node exists in the
graph. The RPC will return the ``INVALID_ARGUMENT`` code, and a detailed error
message.

With ``grpc_cli``:

.. code-block:: console

    $ grpc_cli call localhost:50091 swh.graph.TraversalService.GetNode \
        'swhid: "swh:1:ori:ffffffffffffffffffffffffffffffffffffffff"'
    Rpc failed with status code 3, error message: Unknown SWHID: swh:1:ori:ffffffffffffffffffffffffffffffffffffffff

    $ grpc_cli call localhost:50091 swh.graph.TraversalService.GetNode \
        'swhid: "invalidswhid"'
    Rpc failed with status code 3, error message: malformed SWHID: invalidswhid


With Python:

.. code-block::

    grpc._channel._InactiveRpcError: <_InactiveRpcError of RPC that terminated with:
        status = StatusCode.INVALID_ARGUMENT
        details = "Unknown SWHID: swh:1:ori:83404f995118bd25774f4ac14422a8f175e7a054"
        debug_error_string = "{"created":"@1666018913.304633417","description":"Error received from peer ipv4:192.168.100.51:50091","file":"src/core/lib/surface/call.cc","file_line":966,"grpc_message":"Unknown SWHID: swh:1:ori:83404f995118bd25774f4ac14422a8f175e7a054","grpc_status":3}"

    grpc._channel._InactiveRpcError: <_InactiveRpcError of RPC that terminated with:
        status = StatusCode.INVALID_ARGUMENT
        details = "malformed SWHID: malformedswhid"
        debug_error_string = "{"created":"@1666019057.270929623","description":"Error received from peer ipv4:192.168.100.51:50091","file":"src/core/lib/surface/call.cc","file_line":966,"grpc_message":"malformed SWHID: malformedswhid","grpc_status":3}"



Selecting returned fields with FieldMask
----------------------------------------

Many endpoints, including **GetNode**, contain a ``mask`` field of type
`FieldMask
<https://developers.google.com/protocol-buffers/docs/reference/java/com/google/protobuf/FieldMask.html>`_,
which can be used to select which fields should be returned in the response.

This is particularly interesting for traversal queries that return a large
number of nodes, because property access is quite costly from the compressed
graph (at least compared to regular node access). It is therefore recommended
that clients systematically use FieldMasks to only request the properties that
they will consume.

A FieldMask is represented as a set of "field paths" in dotted notation. For
instance, ``paths: ["swhid", "rev.message"]`` will only request the swhid and
the message of a given node. An empty mask will return an empty object.

Examples:

**Just the SWHID**:

.. code-block:: console

    $ grpc_cli call localhost:50091 swh.graph.TraversalService.GetNode \
        'swhid: "swh:1:rev:0000000000000000000000000000000000000009", mask: {paths: ["swhid"]}'

.. code-block:: python

    response = stub.GetNode(swhgraph.GetNodeRequest(
        swhid="swh:1:rev:0000000000000000000000000000000000000009",
        mask=FieldMask(paths=["swhid"])
    ))
    print(response)
    # Result is in response.swhid; other fields are omitted from the response as
    # they are not part of the FieldMask.

.. code-block:: javascript

    swhid: "swh:1:rev:0000000000000000000000000000000000000009"

**Multiple fields**:

.. code-block:: console

    $ grpc_cli call localhost:50091 swh.graph.TraversalService.GetNode \
        'swhid: "swh:1:rev:0000000000000000000000000000000000000009", mask: {paths: ["swhid", "rev.message", "rev.author"]}'


.. code-block:: python

    response = stub.GetNode(swhgraph.GetNodeRequest(
        swhid="swh:1:rev:0000000000000000000000000000000000000009",
        mask=FieldMask(paths=["swhid", "rev.message", "rev.author"])
    ))
    print(response)
    # Results are in response.swhid, response.rev.message, and response.rev.author;
    # other fields are omitted from the response as they are not part of the FieldMask.

.. code-block:: javascript

    swhid: "swh:1:rev:0000000000000000000000000000000000000009"
    rev {
      author: 2
      message: "Add parser"
    }

Getting statistics on the graph
-------------------------------

The **Stats** endpoint returns overall statistics on the entire compressed
graph. Most notably, the total number of nodes and edges, as well as the
range of indegrees and outdegrees, and some compression-related statistics.

.. code-block:: console

    $ grpc_cli --json_output call localhost:50091 swh.graph.TraversalService.Stats ""

.. code-block:: python

    response = stub.Stats(swhgraph.StatsRequest())
    print(response)

.. code-block:: python

    {
     "numNodes": "21",
     "numEdges": "23",
     "compression": 1.412,
     "bitsPerNode": 8.524,
     "bitsPerEdge": 7.783,
     "avgLocality": 2.522,
     "indegreeMax": "3",
     "indegreeAvg": 1.0952380952380953,
     "outdegreeMax": "3",
     "outdegreeAvg": 1.0952380952380953,
     "exportStartedAt": 1669888200,
     "exportEndedAt": 1669899600,
    }

``exportStartedAt`` and ``exportEndedAt`` are optional and might not be present
if the the information is not available to the server.

.. note::

   Objects inserted before ``exportStartedAt`` are guaranteed to be in the
   export. Objects inserted after ``exportEndedAt`` are guaranteed not to be
   in the export.

Graph traversals
================

Breadth-first traversal
-----------------------

The **Traverse** endpoint performs a breadth-first traversal from a set of
source nodes, and `streams
<https://grpc.io/docs/what-is-grpc/core-concepts/#server-streaming-rpc>`_ all
the nodes it encounters on the way. All the node properties are stored in the
result nodes. Additionally, the *edge properties* (e.g., directory entry names
and permissions) are stored as a list in the ``successor`` field of each node.

For instance, here we run a traversal from a directory that contains two
contents:

.. code-block:: console

    $ grpc_cli call localhost:50091 swh.graph.TraversalService.Traverse \
       "src: 'swh:1:dir:0000000000000000000000000000000000000006'"

.. code-block:: python

    response = stub.Traverse(swhgraph.TraversalRequest(
        src=["swh:1:dir:0000000000000000000000000000000000000006"]
    ))
    for item in response:
        print(item)

We get the following stream of nodes: first, the source directory (including
its properties, successor list and their labels), then the contents themselves
and their respective properties.

.. code-block:: javascript

    swhid: "swh:1:dir:0000000000000000000000000000000000000006"
    successor {
      swhid: "swh:1:cnt:0000000000000000000000000000000000000005"
      label {
        name: "parser.c"
        permission: 33188
      }
    }
    successor {
      swhid: "swh:1:cnt:0000000000000000000000000000000000000004"
      label {
        name: "README.md"
        permission: 33188
      }
    }
    num_successors: 2

.. code-block:: javascript

    swhid: "swh:1:cnt:0000000000000000000000000000000000000005"
    cnt {
      length: 1337
      is_skipped: false
    }

.. code-block:: javascript

    swhid: "swh:1:cnt:0000000000000000000000000000000000000004"
    cnt {
      length: 404
      is_skipped: false
    }

Again, it is possible to use a FieldMask to restrict which fields get returned.
For instance, if we only care about the SWHIDs:

.. code-block:: console

    $ grpc_cli call localhost:50091 swh.graph.TraversalService.Traverse \
        "src: 'swh:1:dir:0000000000000000000000000000000000000006', mask: {paths: ['swhid']}"

.. code-block:: python

    response = stub.Traverse(swhgraph.TraversalRequest(
        src=["swh:1:dir:0000000000000000000000000000000000000006"],
        mask=FieldMask(paths=["swhid"])
    ))
    for item in response:
        print(f'swhid: "{item.swhid}"')

.. code-block:: javascript

    swhid: "swh:1:dir:0000000000000000000000000000000000000006"
    swhid: "swh:1:cnt:0000000000000000000000000000000000000005"
    swhid: "swh:1:cnt:0000000000000000000000000000000000000004"


Graph direction
~~~~~~~~~~~~~~~

For many purposes, especially that of finding the provenance of software
artifacts, it is useful to query the backward (or transposed) graph instead,
which is the same as the forward graph except all the edges are reversed.
To achieve this, the ``direction`` field can be used to specify a direction
from the ``GraphDirection`` enum (either ``FORWARD`` or ``BACKWARD``).

This query returns all the nodes reachable from a given directory in the
*backward* (or "transposed") graph:

.. code-block:: console

    $ grpc_cli call localhost:50091 swh.graph.TraversalService.Traverse \
        "src: 'swh:1:dir:0000000000000000000000000000000000000006', direction: BACKWARD, mask: {paths: ['swhid']}"

.. code-block:: python

    response = stub.Traverse(swhgraph.TraversalRequest(
        src=["swh:1:dir:0000000000000000000000000000000000000006"],
        direction=swhgraph.GraphDirection.BACKWARD,
        mask=FieldMask(paths=["swhid"]),
    ))
    for item in response:
        print(f'swhid: "{item.swhid}"')

.. code-block:: javascript

    swhid: "swh:1:dir:0000000000000000000000000000000000000006"
    swhid: "swh:1:dir:0000000000000000000000000000000000000008"
    swhid: "swh:1:dir:0000000000000000000000000000000000000012"
    swhid: "swh:1:rev:0000000000000000000000000000000000000009"
    swhid: "swh:1:rev:0000000000000000000000000000000000000013"
    swhid: "swh:1:rel:0000000000000000000000000000000000000010"
    swhid: "swh:1:snp:0000000000000000000000000000000000000020"
    swhid: "swh:1:rev:0000000000000000000000000000000000000018"
    swhid: "swh:1:ori:83404f995118bd25774f4ac14422a8f175e7a054"
    swhid: "swh:1:rel:0000000000000000000000000000000000000019"


Edge restrictions
~~~~~~~~~~~~~~~~~

To constrain the types of edges that can be followed during the graph
traversal, it is possible to specify an edge restriction string in the ``edge``
field.  It is a comma-separated list of edge types that will be followed (e.g.
``"rev:dir,dir:cnt"`` to only follow revision → directory and directory →
content edges).
By default (or when ``"*"`` is provided), all edges can be followed.

This query traverses the parent revisions of a given revision only (i.e., it
outputs the *commit log* from a given commit):

.. code-block:: console

    $ grpc_cli call localhost:50091 swh.graph.TraversalService.Traverse \
        "src: 'swh:1:rev:0000000000000000000000000000000000000018', edges: 'rev:rev', mask: {paths: ['swhid']}"

.. code-block:: python

    response = stub.Traverse(swhgraph.TraversalRequest(
        src=["swh:1:rev:0000000000000000000000000000000000000018"],
        edges="rev:rev",
        mask=FieldMask(paths=["swhid"]),
    ))
    for item in response:
        print(f'swhid: "{item.swhid}"')

.. code-block:: javascript

    swhid: "swh:1:rev:0000000000000000000000000000000000000018"
    swhid: "swh:1:rev:0000000000000000000000000000000000000013"
    swhid: "swh:1:rev:0000000000000000000000000000000000000009"
    swhid: "swh:1:rev:0000000000000000000000000000000000000003"


Limiting the traversal
~~~~~~~~~~~~~~~~~~~~~~

To avoid using up too much memory or resources, a traversal can be limited
in three different ways:

- the ``max_depth`` attribute defines the maximum depth of the traversal.
- the ``max_edges`` attribute defines the maximum number of edges that can be
  fetched by the traversal.
- the ``max_matching_nodes`` attribute defines how many nodes matching the
  given constraints (see :ref:`swh-graph-grpc-api-return-nodes`) may be
  visited by the traversal before halting.
  This is typically used to limit the number of results in leaves requests.

When these limits are reached, the traversal will simply stop. While these
options have obvious use-cases for anti-abuse, they can also be semantically
useful: for instance, specifying ``max_depth: 1`` will only return the
*neighbors* of the source node.

.. _swh-graph-grpc-api-return-nodes:

Filtering returned nodes
~~~~~~~~~~~~~~~~~~~~~~~~

In many cases, clients might not want to get all the traversed nodes in the
response stream. With the ``return_nodes`` field (of type ``NodeFilter``), it
is possible to specify various *criteria* for which nodes should be sent to the
stream. By default, all nodes are returned.

One common filter is to only want specific *node types* to be returned, which
can be done with the ``types`` field of ``NodeFilter``. This field contains a
node type restriction string (e.g. "dir,cnt,rev"), and defaults to "*" (all).
For instance, to find the list of origins in which a given directory can be
found:

.. code-block:: console

    $ grpc_cli call localhost:50091 swh.graph.TraversalService.Traverse \
        "src: 'swh:1:dir:0000000000000000000000000000000000000006', return_nodes: {types: 'ori'}, direction: BACKWARD, mask: {paths: ['swhid']}"

.. code-block:: python

    response = stub.Traverse(swhgraph.TraversalRequest(
        src=["swh:1:dir:0000000000000000000000000000000000000006"],
        return_nodes=swhgraph.NodeFilter(types="ori"),
        direction=swhgraph.GraphDirection.BACKWARD,
        mask=FieldMask(paths=["swhid"]),
    ))
    for item in response:
        print(f'swhid: "{item.swhid}"')

.. code-block:: javascript

    swhid: "swh:1:ori:83404f995118bd25774f4ac14422a8f175e7a054"


Traversal from multiple sources
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Traversals can have multiple starting nodes, when multiple source nodes are
present in the ``src`` field. For instance, this BFS starts from two different
directories, and explores the graph in parallel from these multiple starting
points:

.. code-block:: console

    $ grpc_cli call localhost:50091 swh.graph.TraversalService.Traverse \
        "src: ['swh:1:dir:0000000000000000000000000000000000000006', 'swh:1:dir:0000000000000000000000000000000000000017'], mask: {paths: ['swhid']}"

.. code-block:: python

    response = stub.Traverse(swhgraph.TraversalRequest(
        src=[
            "swh:1:dir:0000000000000000000000000000000000000006",
            "swh:1:dir:0000000000000000000000000000000000000017",
        ],
        mask=FieldMask(paths=["swhid"]),
    ))
    for item in response:
        print(f'swhid: "{item.swhid}"')

.. code-block:: javascript

    swhid: "swh:1:dir:0000000000000000000000000000000000000006"
    swhid: "swh:1:dir:0000000000000000000000000000000000000017"
    swhid: "swh:1:cnt:0000000000000000000000000000000000000005"
    swhid: "swh:1:cnt:0000000000000000000000000000000000000004"
    swhid: "swh:1:cnt:0000000000000000000000000000000000000014"
    swhid: "swh:1:dir:0000000000000000000000000000000000000016"
    swhid: "swh:1:cnt:0000000000000000000000000000000000000015"


Finding a path to a node matching a criteria
--------------------------------------------

The **FindPathTo** endpoint searches for a shortest path between a set of
source nodes and any node that matches a specific *criteria*.
It does so by performing a breadth-first search from the source node,
until any node that matches the given criteria is found, then follows
back its parents to return a shortest path from the source set to that
node.

The criteria can be specified in the ``target`` field of the
``FindPathToRequest``, which is of type ``NodeFilter``.

As an example, a common use-case for content provenance is to find the shortest
path of a content to an origin in the transposed graph. This query can be
run like this:

.. code-block:: console

    $ grpc_cli call localhost:50091 swh.graph.TraversalService.FindPathTo \
        "src: 'swh:1:cnt:0000000000000000000000000000000000000001', target: {types: 'ori'}, direction: BACKWARD, mask: {paths: ['swhid']}"

.. code-block:: python

    response = stub.FindPathTo(swhgraph.FindPathToRequest(
        src=["swh:1:cnt:0000000000000000000000000000000000000001"],
        target=swhgraph.NodeFilter(types="ori"),
        direction=swhgraph.GraphDirection.BACKWARD,
        mask=FieldMask(paths=["swhid"]),
    ))
    for item in response.node:
        print(f'swhid: "{item.swhid}"')

.. code-block:: javascript

    swhid: "swh:1:cnt:0000000000000000000000000000000000000001"
    swhid: "swh:1:dir:0000000000000000000000000000000000000008"
    swhid: "swh:1:rev:0000000000000000000000000000000000000009"
    swhid: "swh:1:snp:0000000000000000000000000000000000000020"
    swhid: "swh:1:ori:83404f995118bd25774f4ac14422a8f175e7a054"

As soon as the request finds an origin, it stops and returns the path from the
source set to this origin.

Similar to the **Traverse** endpoint, it is possible to specify edge
restrictions, graph directions, as well as multiple source nodes.


Finding a path between two sets of nodes
----------------------------------------

The **FindPathBetween** endpoint searches for a shortest path between a set of
source nodes and a set of destination nodes.

It does so by performing a *bidirectional breadth-first search*, i.e.,
two parallel breadth-first searches, one from the source set ("src-BFS")
and one from the destination set ("dst-BFS"), until both searches find a
common node that joins their visited sets. This node is called the
"midpoint node".
The path returned is the path src -> ... -> midpoint -> ... -> dst,
which is always a shortest path between src and dst.

The graph direction of both BFS can be configured separately. By
default, the dst-BFS will use the graph in the opposite direction than
the src-BFS (if direction = FORWARD, by default direction_reverse =
BACKWARD, and vice-versa). The default behavior is thus to search for
a shortest path between two nodes in a given direction. However, one
can also specify FORWARD or BACKWARD for *both* the src-BFS and the
dst-BFS. This will search for a common descendant or a common ancestor
between the two sets, respectively. These will be the midpoints of the
returned path.

Similar to the **Traverse** endpoint, it is also possible to specify edge
restrictions.

**Example 1**: shortest path from a snapshot to a content (forward graph):

.. code-block:: console

    $ grpc_cli call localhost:50091 swh.graph.TraversalService.FindPathBetween \
        "src: 'swh:1:snp:0000000000000000000000000000000000000020', dst: 'swh:1:cnt:0000000000000000000000000000000000000004', mask: {paths: ['swhid']}"

.. code-block:: python

    response = stub.FindPathBetween(swhgraph.FindPathBetweenRequest(
        src=["swh:1:snp:0000000000000000000000000000000000000020"],
        dst=["swh:1:cnt:0000000000000000000000000000000000000004"],
        mask=FieldMask(paths=["swhid"]),
    ))
    for item in response.node:
        print(f'swhid: "{item.swhid}"')

.. code-block:: javascript

    swhid: "swh:1:snp:0000000000000000000000000000000000000020"
    swhid: "swh:1:rev:0000000000000000000000000000000000000009"
    swhid: "swh:1:dir:0000000000000000000000000000000000000008"
    swhid: "swh:1:dir:0000000000000000000000000000000000000006"
    swhid: "swh:1:cnt:0000000000000000000000000000000000000004"

**Example 2**: shortest path from a directory to a snapshot (backward graph):

.. code-block:: console

    $ grpc_cli call localhost:50091 swh.graph.TraversalService.FindPathBetween \
        "src: 'swh:1:dir:0000000000000000000000000000000000000006', dst: 'swh:1:rel:0000000000000000000000000000000000000019', direction: BACKWARD, mask: {paths: ['swhid']}"

.. code-block:: python

    response = stub.FindPathBetween(swhgraph.FindPathBetweenRequest(
        src=["swh:1:dir:0000000000000000000000000000000000000006"],
        dst=["swh:1:rel:0000000000000000000000000000000000000019"],
        direction=swhgraph.GraphDirection.BACKWARD,
        mask=FieldMask(paths=["swhid"]),
    ))
    for item in response.node:
        print(f'swhid: "{item.swhid}"')

.. code-block:: javascript

    swhid: "swh:1:dir:0000000000000000000000000000000000000006"
    swhid: "swh:1:dir:0000000000000000000000000000000000000008"
    swhid: "swh:1:dir:0000000000000000000000000000000000000012"
    swhid: "swh:1:rev:0000000000000000000000000000000000000013"
    swhid: "swh:1:rev:0000000000000000000000000000000000000018"
    swhid: "swh:1:rel:0000000000000000000000000000000000000019"

**Example 3**: common ancestor of two contents:

.. code-block:: console

    $ grpc_cli call localhost:50091 swh.graph.TraversalService.FindPathBetween \
        "src: 'swh:1:cnt:0000000000000000000000000000000000000004', dst: 'swh:1:cnt:0000000000000000000000000000000000000015', direction: BACKWARD, direction_reverse: BACKWARD, mask: {paths: ['swhid']}"

.. code-block:: python

    response = stub.FindPathBetween(swhgraph.FindPathBetweenRequest(
        src=["swh:1:cnt:0000000000000000000000000000000000000004"],
        dst=["swh:1:cnt:0000000000000000000000000000000000000015"],
        direction=swhgraph.GraphDirection.BACKWARD,
        direction_reverse=swhgraph.GraphDirection.BACKWARD,
        mask=FieldMask(paths=["swhid"]),
    ))
    for item in response.node:
        print(f'swhid: "{item.swhid}"')

.. code-block:: javascript

    swhid: "swh:1:cnt:0000000000000000000000000000000000000004"
    swhid: "swh:1:dir:0000000000000000000000000000000000000006"
    swhid: "swh:1:dir:0000000000000000000000000000000000000008"
    swhid: "swh:1:dir:0000000000000000000000000000000000000012"
    swhid: "swh:1:rev:0000000000000000000000000000000000000013"
    swhid: "swh:1:rev:0000000000000000000000000000000000000018"
    swhid: "swh:1:dir:0000000000000000000000000000000000000017"
    swhid: "swh:1:dir:0000000000000000000000000000000000000016"
    swhid: "swh:1:cnt:0000000000000000000000000000000000000015"
    midpoint_index: 5

Because ``midpoint_index = 5``, the common ancestor is
``swh:1:rev:0000000000000000000000000000000000000018``.


.. _swh-graph-grpc-api-protobuf:

Protobuf API Reference
======================

The GRPC API is specified in a single self-documenting
`protobuf <https://developers.google.com/protocol-buffers>`_ file, which is
available in the ``proto/swhgraph.proto`` file of the swh-graph repository:

https://forge.softwareheritage.org/source/swh-graph/browse/master/proto/swhgraph.proto

..
    .. literalinclude:: swhgraph.proto
       :language: protobuf
