Quickstart
==========

This quick tutorial shows how to use a compressed graph dataset like the ones
provided by Software Heritage, and make it browsable using the `swh.graph` API
server.

It does not cover the technical details behind the graph compression techniques
nor how to generate these compressed graph files.


Dependencies
------------

In order to run the `swh.graph` tool, you will need Python (>= 3.7) and Java
JRE, you do not need the JDK if you install the package from pypi, but may want
to install it if you want to hack the code or install it from this git
repository.

It is highly recommended to install this package in a virtualenv.

On a Debian stable (buster) system:

.. code:: bash

   $ sudo apt install python3-virtualenv default-jre


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


API server
----------

To start a `swh.graph` API server, you need a compressed graph dataset. You can
download a small dataset here:

  https://annex.softwareheritage.org/public/dataset/graph/latest/popular-3k-python/python3kcompress.tar

And use it as dataset for the `swh.graph` API:

.. code:: bash

   (swhenv) ~/t/swh-graph-tests$ curl -O https://annex.softwareheritage.org/public/dataset/graph/latest/popular-3k-python/python3kcompress.tar
   (swhenv) ~/t/swh-graph-tests$ tar xvf python3kcompress.tar
   (swhenv) ~/t/swh-graph-tests$ touch python3kcompress/*.obl # fix the mtime of cached offset files to allow faster loading
   (swhenv) ~/t/swh-graph-tests$ swh graph rpc-serve -g python3kcompress/python3k
   Loading graph python3kcompress/python3k ...
   Graph loaded.
   ======== Running on http://0.0.0.0:5009 ========
   (Press CTRL+C to quit)

Note: not for the faint heart, but the full dataset is available at:

  https://annex.softwareheritage.org/public/dataset/graph/latest/compressed/

From there you can use this endpoint to query the compressed graph, for example
with httpie_ (`sudo apt install`) from another terminal:

.. _httpie: https://httpie.org


.. code:: bash

   ~/tmp$ http :5009/graph/leaves/swh:1:dir:432d1b21c1256f7408a07c577b6974bbdbcc1323
   HTTP/1.1 200 OK
   Content-Type: text/plain
   Date: Thu, 03 Sep 2020 12:12:58 GMT
   Server: Python/3.8 aiohttp/3.6.2
   Transfer-Encoding: chunked

   swh:1:cnt:33af56e02dd970873d8058154bf016ec73b35dfb
   swh:1:cnt:b03b4ffd7189ae5457d8e1c2ee0490b1938fd79f
   swh:1:cnt:74d127c2186f7f0e8b14a27249247085c49d548a
   swh:1:cnt:c0139aa8e79b338e865a438326629fa22fa8f472
   [...]
   swh:1:cnt:a6b60e797063fef707bbaa4f90cfb4a2cbbddd4a
   swh:1:cnt:cc0a1deca559c1dd2240c08156d31cde1d8ed406
   ~/tmp$


See the documentation of the :ref:`API <swh-graph-api>` for more details.
