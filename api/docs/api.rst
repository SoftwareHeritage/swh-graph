Graph traversal API
===================

Visit
-----

.. http:get:: /graph/visit/:swh_id

    Performs a graph traversal and returns explored paths (in the order of the
    traversal).

    :param string swh_id: starting point for the traversal, represented as a
    `Software Heritage persitent identifier
    <https://docs.softwareheritage.org/devel/swh-model/persistent-identifiers.html#persistent-identifiers>`_

    :query string edges: edges types the traversal can follow, expressed as a
    comma-separated list of ``src:dst`` strings (e.g.: ``"rev:rev,rev:dir"``).
    If no string is provided, the traversal will use all types of edges.
    :query string traversal: default to ``dfs``
    :query string direction: can be either ``forward`` or ``backward``, default
    to ``forward``

    :statuscode 200: no error
    :statuscode 400: an invalid query string has been provided
    :statuscode 404: requested starting node cannot be found in the graph

    .. sourcecode:: http

    HTTP/1.1 200 OK
    Content-Type: application/json

    {
        "paths": [
            [
                "swh:1:cnt:94a9ed024d3859793618152ea559a168bbcbb5e2",
                ...
            ],
            [
                "swh:1:dir:d198bc9d7a6bcf6db04f476d29314f157507d505",
                ...
            ],
            ...
        ]
    }

Stats
-----

.. http:get:: /graph/stats/:src_type/:dst_type

    Returns statistics on the compressed graph.

    :param string src_type: can either be ``dir``, ``ori``, ``rel``, ``rev``, or
    ``snp``
    :param string dst_type: can either be ``cnt``, ``dir``, ``obj``, ``rev``, or
    ``snp``

    :statuscode 200: no error
    :statuscode 404: requested subgraph cannot be found

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
