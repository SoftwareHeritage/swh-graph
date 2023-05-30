.. _swh-graph-quickstart:

Quickstart
==========

This quick tutorial shows how to start the ``swh.graph`` service to query
an existing compressed graph with the high-level HTTP API.

Dependencies
------------

In order to run the ``swh.graph`` tool, you will need Python (>= 3.7) and Java
JRE. On a Debian system:

.. code:: console

   $ sudo apt install python3 python3-venv default-jre

Installing swh.graph
--------------------

Create a virtualenv and activate it:

.. code:: console

   $ python3 -m venv .venv
   $ source .venv/bin/activate

Install the ``swh.graph`` python package:

.. code:: console

   (venv) $ pip install swh.graph
   [...]
   (venv) $ swh graph --help
   Usage: swh graph [OPTIONS] COMMAND [ARGS]...

     Software Heritage graph tools.

   Options:
     -C, --config-file FILE  YAML configuration file
     -h, --help              Show this message and exit.

   Commands:
     compress    Compress a graph using WebGraph Input: a pair of files...
     rpc-serve   run the graph RPC service


.. _swh-graph-retrieving-compressed:

Retrieving a compressed graph
-----------------------------

Software Heritage provides a list of off-the-shelf datasets that can be used
for various research or prototyping purposes. Most of them are available in
*compressed* representation, i.e., in a format suitable to be loaded and
queried by the ``swh-graph`` library.

All the publicly available datasets are documented on this page:
https://docs.softwareheritage.org/devel/swh-dataset/graph/dataset.html

A good way of retrieving these datasets is to use the `AWS S3 CLI
<https://docs.aws.amazon.com/cli/latest/reference/s3/>`_.

Here is an example with the dataset ``2021-03-23-popular-3k-python``, which has
a relatively reasonable size (~15 GiB including property data, with
the compressed graph itself being less than 700 MiB):

.. code:: console

    (venv) $ pip install awscli
    [...]
    (venv) $ mkdir -p 2021-03-23-popular-3k-python/compressed
    (venv) $ cd 2021-03-23-popular-3k-python/
    (venv) $ aws s3 cp --recursive s3://softwareheritage/graph/2021-03-23-popular-3k-python/compressed/ compressed


You can also retrieve larger graphs, but note that these graphs are generally
intended to be loaded fully in RAM, and do not fit on ordinary desktop
machines. The server we use in production to run the graph service has more
than 700 GiB of RAM. These memory considerations are discussed in more details
in :ref:`swh-graph-memory`.

.. note::

   For testing purposes, a :ref:`synthetic test dataset <swh-graph-example-dataset>`
   is available in the ``swh-graph`` repository,
   with just a few dozen nodes. Its basename is
   ``swh-graph/swh/graph/example_dataset/compressed/example``.


API server
----------

To start a ``swh.graph`` API server of a compressed graph dataset, you need to
use the ``rpc-serve`` command with the basename of the graph, which is the path prefix
of all the graph files (e.g., with the basename ``compressed/graph``, it will
attempt to load the files located at
``compressed/graph.{graph,properties,offsets,...}``.

In our example:

.. code:: console

   (venv) $ swh graph rpc-serve -g compressed/graph
   Loading graph compressed/graph ...
   Graph loaded.
   ======== Running on http://0.0.0.0:5009 ========
   (Press CTRL+C to quit)

From there you can use this endpoint to query the compressed graph, for example
with httpie_ (``sudo apt install httpie``):

.. _httpie: https://httpie.org


.. code:: bash

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

See the documentation of the :ref:`API <swh-graph-api>` for more details on how
to use the HTTP graph querying API.
