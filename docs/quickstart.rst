Quickstart
==========

This quick tutorial shows how to compress and browse a graph using `swh.graph`.

It does not cover the technical details behind the graph compression techniques
(refer to :ref:`Graph compression <compression>`).


Dependencies
------------

In order to run the `swh.graph` tool, you will need Python (>= 3.7) and Java
JRE, you do not need the JDK if you install the package from pypi, but may want
to install it if you want to hack the code or install it from this git
repository. To compress a graph, you will need zstd_ compression tools.

It is highly recommended to install this package in a virtualenv.

On a Debian stable (buster) system:

.. code:: bash

   $ sudo apt install python3-virtualenv default-jre zstd


.. _zstd: https://facebook.github.io/zstd/


Install
-------

Create a virtualenv and activate it:

.. code:: bash

   ~/tmp$ mkdir swh-graph-tests
   ~/tmp$ cd swh-graph-tests
   ~/t/swh-graph-tests$ virtualenv swhenv
   ~/t/swh-graph-tests$ . swhenv/bin/activate

Install the `swh.graph` python package:

.. code:: bash

   (swhenv) ~/t/swh-graph-tests$ pip install swh.graph
   [...]
   (swhenv) ~/t/swh-graph-tests swh graph --help
   Usage: swh graph [OPTIONS] COMMAND [ARGS]...

     Software Heritage graph tools.

   Options:
     -C, --config-file FILE  YAML configuration file
     -h, --help              Show this message and exit.

   Commands:
     api-client  client for the graph REST service
     cachemount  Cache the mmapped files of the compressed graph in a tmpfs.
     compress    Compress a graph using WebGraph Input: a pair of files...
     map         Manage swh-graph on-disk maps
     rpc-serve   run the graph REST service

Compression
-----------

Existing datasets
^^^^^^^^^^^^^^^^^

You can directly use compressed graph datasets provided by Software Heritage.
Here is a small and realistic dataset (3.1GB):

  https://annex.softwareheritage.org/public/dataset/graph/latest/popular-3k-python/python3kcompress.tar

.. code:: bash

   (swhenv) ~/t/swh-graph-tests$ curl -O https://annex.softwareheritage.org/public/dataset/graph/latest/popular-3k-python/python3kcompress.tar
   (swhenv) ~/t/swh-graph-tests$ tar xvf python3kcompress.tar
   (swhenv) ~/t/swh-graph-tests$ touch python3kcompress/*.obl # fix the mtime of cached offset files to allow faster loading

Note: not for the faint heart, but the full dataset is available at:

  https://annex.softwareheritage.org/public/dataset/graph/latest/compressed/

Own datasets
^^^^^^^^^^^^

A graph is described as both its adjacency list and the set of nodes identifiers
in plain text format. Such graph example can be found in the
`swh/graph/tests/dataset/` folder. Depending on the machine you are using, you
might want to tune parameters down for lower RAM usage. Parameters are
configured in a separate YAML file:

.. code:: yaml

    graph:
    compress:
        batch_size: 1000

Then, we can run the compression:

.. code:: bash


   (swhenv) ~/t/swh-graph-tests$ swh graph -C config.yml compress --graph swh/graph/tests/dataset/example --outdir output/

   [...]

   (swhenv) ~/t/swh-graph-tests$ ls output/
    example-bv.properties  example.mph             example.obl      example.outdegree   example.swhid2node.bin    example-transposed.offsets
    example.graph          example.node2swhid.bin  example.offsets  example.properties  example-transposed.graph  example-transposed.properties
    example.indegree       example.node2type.map   example.order    example.stats       example-transposed.obl


API server
----------

To start a `swh.graph` API server of a compressed graph dataset, run:

.. code:: bash

   (swhenv) ~/t/swh-graph-tests$ swh graph rpc-serve -g output/example
   Loading graph output/example ...
   Graph loaded.
   ======== Running on http://0.0.0.0:5009 ========
   (Press CTRL+C to quit)

From there you can use this endpoint to query the compressed graph, for example
with httpie_ (`sudo apt install`) from another terminal:

.. _httpie: https://httpie.org


.. code:: bash

   ~/tmp$ http :5009/graph/visit/nodes/swh:1:rel:0000000000000000000000000000000000000010
    HTTP/1.1 200 OK
    Content-Type: text/plain
    Date: Tue, 15 Sep 2020 08:33:25 GMT
    Server: Python/3.8 aiohttp/3.6.2
    Transfer-Encoding: chunked

    swh:1:rel:0000000000000000000000000000000000000010
    swh:1:rev:0000000000000000000000000000000000000009
    swh:1:rev:0000000000000000000000000000000000000003
    swh:1:dir:0000000000000000000000000000000000000002
    swh:1:cnt:0000000000000000000000000000000000000001
    swh:1:dir:0000000000000000000000000000000000000008
    swh:1:dir:0000000000000000000000000000000000000006
    swh:1:cnt:0000000000000000000000000000000000000004
    swh:1:cnt:0000000000000000000000000000000000000005
    swh:1:cnt:0000000000000000000000000000000000000007


Running the existing `python3kcompress` dataset:

.. code:: bash

   (swhenv) ~/t/swh-graph-tests$ swh graph rpc-serve -g python3kcompress/python3k
   Loading graph python3kcompress/python3k ...
   Graph loaded.
   ======== Running on http://0.0.0.0:5009 ========
   (Press CTRL+C to quit)


   ~/tmp$ http :5009/graph/leaves/swh:1:dir:432d1b21c1256f7408a07c577b6974bbdbcc1323
   HTTP/1.1 200 OK
   Content-Type: text/plain
   Date: Tue, 15 Sep 2020 08:35:19 GMT
   Server: Python/3.8 aiohttp/3.6.2
   Transfer-Encoding: chunked

   swh:1:cnt:33af56e02dd970873d8058154bf016ec73b35dfb
   swh:1:cnt:b03b4ffd7189ae5457d8e1c2ee0490b1938fd79f
   swh:1:cnt:74d127c2186f7f0e8b14a27249247085c49d548a
   swh:1:cnt:c0139aa8e79b338e865a438326629fa22fa8f472
   [...]
   swh:1:cnt:a6b60e797063fef707bbaa4f90cfb4a2cbbddd4a
   swh:1:cnt:cc0a1deca559c1dd2240c08156d31cde1d8ed406


See the documentation of the :ref:`API <swh-graph-api>` for more details.
