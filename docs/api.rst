Graph REST API
==============


Terminology
-----------

This API uses the following notions:

- **Node**: a node in the :ref:`Software Heritage graph <data-model>`,
  represented by a :ref:`SWHID <persistent-identifiers>`.

- **Node type**: the 3-letter specifier from the node SWHID (``cnt``, ``dir``,
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
  reason ``dir:dir`` allows following edges from parent directories to
  sub-directories when traversing the forward graph, but the same restriction
  allows following edges from sub-directories to parent directories.


Examples
~~~~~~~~

- ``swh:1:cnt:94a9ed024d3859793618152ea559a168bbcbb5e2`` the SWHID of a node of
  type content containing the full text of the GPL3 license.
- ``swh:1:rev:f39d7d78b70e0f39facb1e4fab77ad3df5c52a35`` the SWHID of a node of
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

    :param string src: source node specified as a SWHID

    :query string edges: edges types the traversal can follow; default to
        ``"*"``
    :query string direction: direction in which graph edges will be followed;
        can be either ``forward`` or ``backward``, default to ``forward``

    :statuscode 200: success
    :statuscode 400: invalid query string provided
    :statuscode 404: starting node cannot be found

    **Example:**

    .. sourcecode:: http

        GET /graph/leaves/swh:1:dir:432d1b21c1256f7408a07c577b6974bbdbcc1323 HTTP/1.1

        Content-Type: text/plain
        Transfer-Encoding: chunked

    .. sourcecode:: http

        HTTP/1.1 200 OK

        swh:1:cnt:540faad6b1e02e2db4f349a4845192db521ff2bd
        swh:1:cnt:630585fc6d34e5e121139e2aee0a64e83dc9aae6
        swh:1:cnt:f8634ced669f0a9155c8cab1b2621d57d778215e
        swh:1:cnt:ba6daa801ad3ea587904b1abe9161dceedb2e0bd
        ...


Neighbors
---------

.. http:get:: /graph/neighbors/:src

    Returns node direct neighbors (linked with exactly one edge) in the graph.

    :param string src: source node specified as a SWHID

    :query string edges: edges types allowed to be listed as neighbors; default
        to ``"*"``
    :query string direction: direction in which graph edges will be followed;
        can be either ``forward`` or ``backward``, default to ``forward``

    :statuscode 200: success
    :statuscode 400: invalid query string provided
    :statuscode 404: starting node cannot be found

    **Example:**

    .. sourcecode:: http

        GET /graph/neighbors/swh:1:rev:f39d7d78b70e0f39facb1e4fab77ad3df5c52a35 HTTP/1.1

        Content-Type: text/plain
        Transfer-Encoding: chunked

    .. sourcecode:: http

        HTTP/1.1 200 OK

        swh:1:rev:a31e58e129f73ab5b04016330b13ed51fde7a961
        swh:1:dir:b5d2aa0746b70300ebbca82a8132af386cc5986d
        swh:1:rev:52c90f2d32bfa7d6eccd66a56c44ace1f78fbadd
        ...


Walk
----

..
  .. http:get:: /graph/walk/:src/:dst

      Performs a graph traversal and returns the first found path from source to
      destination (final destination node included).

      :param string src: starting node specified as a SWHID
      :param string dst: destination node, either as a node SWHID or a node
          type.  The traversal will stop at the first node encountered matching
          the desired destination.

      :query string edges: edges types the traversal can follow; default to
          ``"*"``
      :query string traversal: traversal algorithm; can be either ``dfs`` or
          ``bfs``, default to ``dfs``
      :query string direction: direction in which graph edges will be followed;
          can be either ``forward`` or ``backward``, default to ``forward``

      :statuscode 200: success
      :statuscode 400: invalid query string provided
      :statuscode 404: starting node cannot be found

      **Example:**

      .. sourcecode:: http

          HTTP/1.1 200 OK

          swh:1:rev:f39d7d78b70e0f39facb1e4fab77ad3df5c52a35
          swh:1:rev:52c90f2d32bfa7d6eccd66a56c44ace1f78fbadd
          swh:1:rev:cea92e843e40452c08ba313abc39f59efbb4c29c
          swh:1:rev:8d517bdfb57154b8a11d7f1682ecc0f79abf8e02
          ...

.. http:get:: /graph/randomwalk/:src/:dst

    Performs a graph *random* traversal, i.e., picking one random successor
    node at each hop, from source to destination (final destination node
    included).

    :param string src: starting node specified as a SWHID
    :param string dst: destination node, either as a node SWHID or a node type.
        The traversal will stop at the first node encountered matching the
        desired destination.

    :query string edges: edges types the traversal can follow; default to
        ``"*"``
    :query string direction: direction in which graph edges will be followed;
        can be either ``forward`` or ``backward``, default to ``forward``
    :query int limit: limit the number of nodes returned. You can use positive
        numbers to get the first N results, or negative numbers to get the last
        N results starting from the tail;
        default to ``0``, meaning no limit.

    :statuscode 200: success
    :statuscode 400: invalid query string provided
    :statuscode 404: starting node cannot be found

    **Example:**

    .. sourcecode:: http

        GET /graph/randomwalk/swh:1:cnt:94a9ed024d3859793618152ea559a168bbcbb5e2/ori?direction=backward HTTP/1.1

        Content-Type: text/plain
        Transfer-Encoding: chunked

    .. sourcecode:: http

        HTTP/1.1 200 OK

        swh:1:cnt:94a9ed024d3859793618152ea559a168bbcbb5e2
        swh:1:dir:8de8a8823a0780524529c94464ee6ef60b98e2ed
        swh:1:dir:7146ea6cbd5ffbfec58cc8df5e0552da45e69cb7
        swh:1:rev:b12563e00026b48b817fd3532fc3df2db2a0f460
        swh:1:rev:13e8ebe80fb878bade776131e738d5772aa0ad1b
        swh:1:rev:cb39b849f167c70c1f86d4356f02d1285d49ee13
        ...
        swh:1:rev:ff70949f336593d6c59b18e4989edf24d7f0f254
        swh:1:snp:a511810642b7795e725033febdd82075064ed863
        swh:1:ori:98aa0e71f5c789b12673717a97f6e9fa20aa1161

    **Limit example:**

    .. sourcecode:: http

        GET /graph/randomwalk/swh:1:cnt:94a9ed024d3859793618152ea559a168bbcbb5e2/ori?direction=backward&limit=-2 HTTP/1.1

        Content-Type: text/plain
        Transfer-Encoding: chunked

    .. sourcecode:: http

        HTTP/1.1 200 OK

        swh:1:ori:98aa0e71f5c789b12673717a97f6e9fa20aa1161
        swh:1:snp:a511810642b7795e725033febdd82075064ed863


Visit
-----

.. http:get:: /graph/visit/nodes/:src
.. http:get:: /graph/visit/edges/:src
.. http:get:: /graph/visit/paths/:src

    Performs a graph traversal and returns explored nodes, edges or paths (in
    the order of the traversal).

    :param string src: starting node specified as a SWHID

    :query string edges: edges types the traversal can follow; default to
        ``"*"``
    :query string direction: direction in which graph edges will be followed;
        can be either ``forward`` or ``backward``, default to ``forward``

    :statuscode 200: success
    :statuscode 400: invalid query string provided
    :statuscode 404: starting node cannot be found

    **Example:**

    .. sourcecode:: http

        GET /graph/visit/nodes/swh:1:snp:40f9f177b8ab0b7b3d70ee14bbc8b214e2b2dcfc HTTP/1.1

        Content-Type: text/plain
        Transfer-Encoding: chunked

    .. sourcecode:: http

        HTTP/1.1 200 OK

        swh:1:snp:40f9f177b8ab0b7b3d70ee14bbc8b214e2b2dcfc
        swh:1:rev:cfab784723a6c2d33468c9ed8a566fd5e2abd8c9
        swh:1:rev:53e5df0e7a6b7bd4919074c081a173655c0da164
        swh:1:rev:f85647f14b8243532283eff3e08f4ee96c35945f
        swh:1:rev:fe5f9ef854715fc59b9ec22f9878f11498cfcdbf
        swh:1:dir:644dd466d8ad527ea3a609bfd588a3244e6dafcb
        swh:1:cnt:c8cece50beae7a954f4ea27e3ae7bf941dc6d0c0
        swh:1:dir:a358d0cf89821227d4c00b0ced5e0a8b3756b5db
        swh:1:cnt:cc407b7e24dd300d2e1a77d8f04af89b3f962a51
        swh:1:cnt:701bd0a63e11b3390a547ce8515d28c6bab8a201
        ...

    **Example:**

    .. sourcecode:: http

        GET /graph/visit/edges/swh:1:snp:40f9f177b8ab0b7b3d70ee14bbc8b214e2b2dcfc HTTP/1.1

        Content-Type: text/plain
        Transfer-Encoding: chunked

    .. sourcecode:: http

        HTTP/1.1 200 OK

        swh:1:snp:40f9f177b8ab0b7b3d70ee14bbc8b214e2b2dcfc swh:1:rev:61f92a7db95f5a6d1fcb94d2b897ed3797584d7b
        swh:1:snp:40f9f177b8ab0b7b3d70ee14bbc8b214e2b2dcfc swh:1:rev:00e81c89c29ff3e58745fdaf7abb68daa1389e85
        swh:1:snp:40f9f177b8ab0b7b3d70ee14bbc8b214e2b2dcfc swh:1:rev:7596fdc31c9aa00aed281ccb026a74cabf2383bb
        swh:1:snp:40f9f177b8ab0b7b3d70ee14bbc8b214e2b2dcfc swh:1:rev:ec7a2341ac3d9d8b571bbdfb90a089d4e54dea56
        swh:1:snp:40f9f177b8ab0b7b3d70ee14bbc8b214e2b2dcfc swh:1:rev:1c5b5eac61eda2454034a43eb124ab490885ef3a
        swh:1:snp:40f9f177b8ab0b7b3d70ee14bbc8b214e2b2dcfc swh:1:rev:4dfa88ca55e04e8afe05e8543ddddee32dde7236
        swh:1:snp:40f9f177b8ab0b7b3d70ee14bbc8b214e2b2dcfc swh:1:rev:d56ae79e43ff1b37534370911c8a78ec7f38d437
        swh:1:snp:40f9f177b8ab0b7b3d70ee14bbc8b214e2b2dcfc swh:1:rev:19ba5d6203a040a39ecc4a77b165d3f097c1e662
        swh:1:snp:40f9f177b8ab0b7b3d70ee14bbc8b214e2b2dcfc swh:1:rev:9c56102eefea23c95405533e1de23da4b873ecc4
        swh:1:snp:40f9f177b8ab0b7b3d70ee14bbc8b214e2b2dcfc swh:1:rev:3f54e816b46c2e179cd164e17fea93b3013a9db4
        ...

    **Example:**

    .. sourcecode:: http

        GET /graph/visit/paths/swh:1:dir:644dd466d8ad527ea3a609bfd588a3244e6dafcb HTTP/1.1

        Content-Type: application/x-ndjson
        Transfer-Encoding: chunked

    .. sourcecode:: http

        HTTP/1.1 200 OK

        ["swh:1:dir:644dd466d8ad527ea3a609bfd588a3244e6dafcb", "swh:1:cnt:acfb7cabd63b368a03a9df87670ece1488c8bce0"]
        ["swh:1:dir:644dd466d8ad527ea3a609bfd588a3244e6dafcb", "swh:1:cnt:2a0837708151d76edf28fdbb90dc3eabc676cff3"]
        ["swh:1:dir:644dd466d8ad527ea3a609bfd588a3244e6dafcb", "swh:1:cnt:eaf025ad54b94b2fdda26af75594cfae3491ec75"]
        ...
        ["swh:1:dir:644dd466d8ad527ea3a609bfd588a3244e6dafcb", "swh:1:dir:2ebd4b96fa5665ff74f2b27ae41aecdc43af4463", "swh:1:cnt:1d3b6575fb7bf2a147d228e78ffd77ea193c3639"]
        ...


Counting results
----------------

The following method variants, with trailing `/count` added, behave like their
already discussed counterparts but, instead of returning results, return the
*amount* of results that would have been returned:


.. http:get:: /graph/leaves/count/:src

   Return the amount of :http:get:`/graph/leaves/:src` results


.. http:get:: /graph/neighbors/count/:src

   Return the amount of :http:get:`/graph/neighbors/:src` results


.. http:get:: /graph/visit/nodes/count/:src

   Return the amount of :http:get:`/graph/visit/nodes/:src` results


Stats
-----

.. http:get:: /graph/stats

    Returns statistics on the compressed graph.

    :statuscode 200: success

    **Example**

    .. sourcecode:: http

        GET /graph/stats HTTP/1.1

        Content-Type: application/json

    .. sourcecode:: http

        HTTP/1.1 200 OK

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
