Graph Docker environment
========================

Build
-----

.. code:: bash

    $ git clone https://forge.softwareheritage.org/source/swh-graph.git
    $ cd swh-graph
    $ docker build --tag swh-graph dockerfiles

Run
---

Given a graph specified by:

- ``g.edges.csv.gz``: gzip-compressed csv file with one edge per line, as a
  "SRC_ID SPACE DST_ID" string, where identifiers are the `persistent identifier
  <https://docs.softwareheritage.org/devel/swh-model/persistent-identifiers.html#persistent-identifiers>`_
  of each node.
- ``g.nodes.csv.gz``: sorted list of unique node identifiers appearing in the
  corresponding ``g.edges.csv.gz`` file. The format is a gzip-compressed csv
  file with one persistent identifier per line.

.. code:: bash

    $ docker run                                        \
        --volume /path/to/graph/:/graph                 \
        --volume /path/to/output/:/graph/compressed     \
        --name swh-graph --tty --interactive            \
        swh-graph:latest bash

Where ``/path/to/graph`` is a directory containing the ``g.edges.csv.gz`` and
``g.nodes.csv.gz`` files.

Graph compression
~~~~~~~~~~~~~~~~~

To start graph compression:

.. code:: bash

    $ ./scripts/compress_graph.sh           \
        --input /graph/g                    \
        --output /graph/compressed          \
        --lib /swh/graph-lib                \
        --tmp /graph/compressed/tmp         \
        --stdout /graph/compressed/stdout   \
        --stderr /graph/compressed/stderr

Warning: very large graphs may need a bigger batch size parameter for WebGraph
internals (you can specify a value when running the compression script using:
``--batch-size 1000000000``).

Node ids mapping
~~~~~~~~~~~~~~~~

To dump the mapping files:

.. code:: bash

    $ java -cp /swh/app/swh-graph.jar       \
        org.softwareheritage.graph.backend.Setup /graph/compressed/g

This command outputs:

- ``g.nodeToSwhMap.csv``: long node id to string persistent identifier.
- ``g.swhToNodeMap.csv``: string persistent identifier to long node id.

REST API
~~~~~~~~

To start the REST API web-service:

.. code:: bash

    $ java -cp /swh/app/swh-graph.jar       \
        org.softwareheritage.graph.App /graph/compressed/g
