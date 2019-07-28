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
        --publish 127.0.0.1:5009:5009 \
        swh-graph:latest \
	bash

Where ``/PATH/TO/GRAPH`` is a directory containing the ``g.edges.csv.gz`` and
``g.nodes.csv.gz`` files.  By default, when entering the container the current
working directory will be ``/srv/softwareheritage/graph``; all relative paths
found below are intended to be relative to that dir.


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

To dump the mapping files (i.e., various node id <-> other info mapping files,
in either ``.csv.gz`` or ad-hoc ``.map`` format):

.. code:: bash

    $ java -cp app/swh-graph.jar \
        org.softwareheritage.graph.backend.Setup data/compressed/g


Graph server
~~~~~~~~~~~~

To start the swh-graph server:

.. code:: bash

    $ java -cp app/swh-graph.jar \
        org.softwareheritage.graph.App data/compressed/g
