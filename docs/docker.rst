Docker environment
==================


Build
-----

.. code:: bash

    $ git clone https://forge.softwareheritage.org/source/swh-graph.git
    $ cd swh-graph
    $ docker build --tag swh-graph docker/


Run
---

Given a graph ``g`` specified by:

- ``g.edges.csv.zst``: zstd-compressed CSV file with one edge per line, as a
  "SRC_ID SPACE DST_ID" string, where identifiers are the
  :ref:`persistent-identifiers` of each node.
- ``g.nodes.csv.zst``: sorted list of unique node identifiers appearing in the
  corresponding ``g.edges.csv.zst`` file. The format is a zst-compressed CSV
  file (single column) with one persistent identifier per line.

.. code:: bash

    $ docker run -ti \
        --volume /PATH/TO/GRAPH/:/srv/softwareheritage/graph/data \
        --publish 127.0.0.1:5009:5009 \
        swh-graph:latest \
        bash

Where ``/PATH/TO/GRAPH`` is a directory containing the ``g.edges.csv.zst`` and
``g.nodes.csv.zst`` files.  By default, when entering the container the current
working directory will be ``/srv/softwareheritage/graph``; all relative paths
found below are intended to be relative to that dir.


Graph compression
~~~~~~~~~~~~~~~~~

To compress the graph:

.. code:: bash

    $ swh graph compress --graph data/g --outdir data/compressed


Graph server
~~~~~~~~~~~~

To start the swh-graph server:

.. code:: bash

    $ swh graph rpc-serve --graph data/compressed/g
