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

Given a graph ``g`` specified by:

- ``g.edges.csv.gz``: gzip-compressed csv file with one edge per line, as a
  "SRC_ID SPACE DST_ID" string, where identifiers are the
  :ref:`persistent-identifiers` of each node.
- ``g.nodes.csv.gz``: sorted list of unique node identifiers appearing in the
  corresponding ``g.edges.csv.gz`` file. The format is a gzip-compressed csv
  file with one persistent identifier per line.

.. code:: bash

    $ docker run -ti \
        --volume /PATH/TO/GRAPH/:/srv/softwareheritage/graph/data \
        swh-graph:latest \
	bash

Where ``/PATH/TO/GRAPH`` is a directory containing the ``g.edges.csv.gz`` and
``g.nodes.csv.gz`` files.


Graph compression
~~~~~~~~~~~~~~~~~

To compress the graph:

.. code:: bash

    $ app/scripts/compress_graph.sh --lib lib/ --input data/g

Warning: very large graphs may need a bigger batch size parameter for WebGraph
internals (you can specify a value when running the compression script using:
``--batch-size 1000000000``).


Node identifier mappings
~~~~~~~~~~~~~~~~~~~~~~~~

To dump the mapping files:

.. code:: bash

    $ java -cp /srv/softwareheritage/graph/app/swh-graph.jar \
        org.softwareheritage.graph.backend.Setup /graph/data/compressed/g

This command outputs:

- ``g.node2pid.csv``: long node id to string persistent identifier.
- ``g.pid2node.csv``: string persistent identifier to long node id.


Graph server
~~~~~~~~~~~~

To start the swh-graph server:

.. code:: bash

    $ java -cp /srv/softwareheritage/graph/app/swh-graph.jar \
        org.softwareheritage.graph.App /graph/data/compressed/g
