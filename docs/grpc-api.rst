.. _swh-graph-grpc-api:

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
----------

Starting the server
~~~~~~~~~~~~~~~~~~~

The GRPC server is automatically started on port 50091 when the HTTP server
is started with ``swh graph rpc-serve``. It can also be started directly with
Java, instead of going through the Python layer, by using the fat-jar shipped
with swh-graph:

.. code-block:: console

    $ java -cp swh-graph-XXX.jar org.softwareheritage.graph.rpc.GraphServer <graph_basename>

(See :ref:`swh-graph-java-api` and :ref:`swh-graph-memory` for more
information on Java process options and JVM tuning.)

Running queries
~~~~~~~~~~~~~~~

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

A RPC method can be called with ``call``:

.. code-block:: console

    % grpc_cli --json_output call localhost:50091 swh.graph.TraversalService.Stats ""
    connecting to localhost:50091
    {
     "numNodes": "21",
     "numEdges": "23",
     [...]
    }
    Rpc succeeded with OK status


Endpoints
---------

For a full documentation of all the endpoints, as well as the request and
response messages, see :ref:`swh-graph-grpc-api-protobuf`.

Stats
~~~~~

Stats returns overall statistics on the entire compressed graph.

TODO

GetNode
~~~~~~~

TODO

Traverse
~~~~~~~~

TODO

FindPathTo
~~~~~~~~~~

TODO

FindPathBetween
~~~~~~~~~~~~~~~

TODO

CountNodes
~~~~~~~~~~

TODO

CountEdges
~~~~~~~~~~

TODO

.. _swh-graph-grpc-api-protobuf:

Protobuf API Reference
----------------------

The GRPC API is specified in a single self-documenting
`protobuf <https://developers.google.com/protocol-buffers>`_ file, reproduced
here verbatim.

.. literalinclude:: ../proto/swhgraph.proto
   :language: protobuf
   :linenos:
