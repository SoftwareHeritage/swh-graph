Graph REST API
==============


Terminology
-----------

This API uses the following notions:

- **Node**: a node in the `Software Heritage graph
  <https://docs.softwareheritage.org/devel/swh-model/data-model.html>`_,
  represented by a `persistent identifier
  <https://docs.softwareheritage.org/devel/swh-model/persistent-identifiers.html#persistent-identifiers>`_
  (abbreviated as *SWH PID*, or simply *PID*).

- **Node type**: the 3-letter specifier from the node PID (``cnt``, ``dir``,
  ``rel``, ``rev``, ``snp``, ``ori``), or ``*`` for all node types.

- **Edge type**: a pair ``src:dst`` where ``src`` and ``dst`` are either node
  types, or ``*`` to denote all node types.

- **Edge restrictions**: a textual specification of which edges can be followed
  during graph traversal. Either ``*`` to denote that all edges can be followed
  or a comma separated list of edge types to allow following only those edges.

  Note that when traversing the *backward* (i.e., transposed) graph, edge types
  are reversed too. So, for instance, ``ori:snp`` makes sense when traversing
  the forward graph, but useless (due to lack of matching edges in the graph)
  when traversing the backward graph; conversely ``snp:ori`` is useful when
  traversing the backward graph, but not in the forward one. For the same
  reason ``dir:dir` allows following edges from parent directories to
  sub-directories when traversing the forward graph, but the same restriction
  allows following edges from sub-directories to parent directories.


Examples
~~~~~~~~

- ``swh:1:cnt:94a9ed024d3859793618152ea559a168bbcbb5e2`` the PID of a node of
  type content containing the full text of the GPL3 license.
- ``swh:1:rev:f39d7d78b70e0f39facb1e4fab77ad3df5c52a35`` the PID of a node of
  type revision corresponding to the commit in Linux that merged the
  'x86/urgent' branch on 31 December 2017.
- ``"dir:dir,dir:cnt"`` node types allowing edges from directories to
  directories nodes, or directories to contents nodes.
- ``"rev:rev,dir:*"`` node types allowing edges from revisions to revisions
  nodes, or from directories nodes.
- ``"*:rel"`` node types allowing all edges to releases.


Leaves
------

.. http:get:: /graph/leaves/:src

    Performs a graph traversal and returns the leaves of the subgraph rooted at
    the specified source node.

    :param string src: source node specified as a SWH PID

    :query string edges: edges types the traversal can follow; default to
        ``"*"``
    :query string direction: direction in which graph edges will be followed;
        can be either ``forward`` or ``backward``, default to ``forward``

    :statuscode 200: success
    :statuscode 400: invalid query string provided
    :statuscode 404: starting node cannot be found

    .. sourcecode:: http

        HTTP/1.1 200 OK
        Content-Type: text/plain
        Transfer-Encoding: chunked

        swh:1:cnt:669ac7c32292798644b21dbb5a0dc657125f444d
        swh:1:cnt:da4cb28febe66172a9fdf1a235525ae6c00cde1d",
        ...


Neighbors
---------

.. http:get:: /graph/neighbors/:src

    Returns node direct neighbors (linked with exactly one edge) in the graph.

    :param string src: source node specified as a SWH PID

    :query string edges: edges types allowed to be listed as neighbors; default
        to ``"*"``
    :query string direction: direction in which graph edges will be followed;
        can be either ``forward`` or ``backward``, default to ``forward``

    :statuscode 200: success
    :statuscode 400: invalid query string provided
    :statuscode 404: starting node cannot be found

    .. sourcecode:: http

        HTTP/1.1 200 OK
        Content-Type: text/plain
        Transfer-Encoding: chunked

        swh:1:cnt:94a9ed024d3859793618152ea559a168bbcbb5e2
        swh:1:dir:d198bc9d7a6bcf6db04f476d29314f157507d505
        ...


Walk
----

..
   .. http:get:: /graph/walk/:src/:dst

       Performs a graph traversal and returns the first found path from source to
       destination (final destination node included).

       :param string src: starting node specified as a SWH PID
       :param string dst: destination node, either as a node PID or a node type.
	   The traversal will stop at the first node encountered matching the
	   desired destination.

       :query string edges: edges types the traversal can follow; default to
	   ``"*"``
       :query string traversal: traversal algorithm; can be either ``dfs`` or
	   ``bfs``, default to ``dfs``
       :query string direction: direction in which graph edges will be followed;
	   can be either ``forward`` or ``backward``, default to ``forward``

       :statuscode 200: success
       :statuscode 400: invalid query string provided
       :statuscode 404: starting node cannot be found

       .. sourcecode:: http

	   HTTP/1.1 200 OK
	   Content-Type: text/plain
	   Transfer-Encoding: chunked

	   swh:1:rev:f39d7d78b70e0f39facb1e4fab77ad3df5c52a35
	   swh:1:rev:52c90f2d32bfa7d6eccd66a56c44ace1f78fbadd
	   swh:1:rev:cea92e843e40452c08ba313abc39f59efbb4c29c
	   swh:1:rev:8d517bdfb57154b8a11d7f1682ecc0f79abf8e02
	   ...

.. http:get:: /graph/randomwalk/:src/:dst

    Performs a graph *random* traversal, i.e., picking one random successor
    node at each hop, from source to destination (final destination node
    included).

    :param string src: starting node specified as a SWH PID
    :param string dst: destination node, either as a node PID or a node type.
        The traversal will stop at the first node encountered matching the
        desired destination.

    :query string edges: edges types the traversal can follow; default to
        ``"*"``
    :query string direction: direction in which graph edges will be followed;
        can be either ``forward`` or ``backward``, default to ``forward``

    :statuscode 200: success
    :statuscode 400: invalid query string provided
    :statuscode 404: starting node cannot be found

    .. sourcecode:: http

        HTTP/1.1 200 OK
        Content-Type: text/plain
        Transfer-Encoding: chunked

        swh:1:rev:f39d7d78b70e0f39facb1e4fab77ad3df5c52a35
        swh:1:rev:52c90f2d32bfa7d6eccd66a56c44ace1f78fbadd
        swh:1:rev:cea92e843e40452c08ba313abc39f59efbb4c29c
        swh:1:rev:8d517bdfb57154b8a11d7f1682ecc0f79abf8e02
        ...


Visit
-----

.. http:get:: /graph/visit/nodes/:src
.. http:get:: /graph/visit/paths/:src

    Performs a graph traversal and returns explored nodes or paths (in the order
    of the traversal).

    :param string src: starting node specified as a SWH PID

    :query string edges: edges types the traversal can follow; default to
        ``"*"``
    :query string direction: direction in which graph edges will be followed;
        can be either ``forward`` or ``backward``, default to ``forward``

    :statuscode 200: success
    :statuscode 400: invalid query string provided
    :statuscode 404: starting node cannot be found

    .. sourcecode:: http

        GET /graph/visit/nodes/...
        HTTP/1.1 200 OK
        Content-Type: text/plain
        Transfer-Encoding: chunked

        swh:1:rev:f39d7d78b70e0f39facb1e4fab77ad3df5c52a35
        swh:1:rev:52c90f2d32bfa7d6eccd66a56c44ace1f78fbadd
        ...
        swh:1:rev:a31e58e129f73ab5b04016330b13ed51fde7a961
        ...

    .. sourcecode:: http

        GET /graph/visit/paths/...
        HTTP/1.1 200 OK
        Content-Type: application/x-ndjson
        Transfer-Encoding: chunked

        ["swh:1:rev:f39d7d78b70e0f39facb1e4fab77ad3df5c52a35", "swh:1:rev:52c90f2d32bfa7d6eccd66a56c44ace1f78fbadd", ...]
        ["swh:1:rev:f39d7d78b70e0f39facb1e4fab77ad3df5c52a35", "swh:1:rev:a31e58e129f73ab5b04016330b13ed51fde7a961", ...]
        ...


Counting results
----------------

The following method variants, with trailing `/count` added, behave like their
already discussed counterparts but, instead of returning results, return the
*amount* of results that would have been returned:

.. http:get:: /graph/leaves/count/:src

.. http:get:: /graph/neighbors/count/:src

.. http:get:: /graph/visit/nodes/count/:src


Returning last element only
---------------------------

The following method variants, with trailing `/last` added, behave like their
already discussed counterparts but, instead of returning full results, return
the *last* element of the result that would have been returned:

.. http:get:: /graph/walk/last/:src/:dst

.. http:get:: /graph/randomwalk/last/:src/:dst


Stats
-----

.. http:get:: /graph/stats

    Returns statistics on the compressed graph.

    :statuscode 200: success

    .. sourcecode:: http

        HTTP/1.1 200 OK
        Content-Type: application/json

        {
            "counts": {
                "nodes": 16222788,
                "edges": 9907464
            },
            "ratios": {
                "compression": 0.367,
                "bits_per_node": 5.846,
                "bits_per_edge": 9.573,
                "avg_locality": 270.369
            },
            "indegree": {
                "min": 0,
                "max": 12382,
                "avg": 0.6107127825377487
            },
            "outdegree": {
                "min": 0,
                "max": 1,
                "avg": 0.6107127825377487
            }
        }
