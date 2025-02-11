.. _swh-graph-quickstart:

Quickstart
==========

This quick tutorial shows how to start the ``swh.graph`` service to query
an existing compressed graph with the high-level HTTP API.

Dependencies
------------

In order to run the ``swh.graph`` tool, you will need Python (>= 3.9),
Rust (>= 1.79), and zstd. On a Debian system:

.. code:: console

   $ sudo apt install build-essential libclang-dev python3 python3-venv \
       default-jre zstd protobuf-compiler pv
   $ curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

Rustup will ask you a few questions, you can pick the defaults. Or select
"Customize installation" and to install a ``minimal`` distribution if you are
not planning to edit the code and want to save some disk space.

Installing swh.graph
--------------------

Install the ``swh_graph`` rust package:

.. code:: console

   $ RUSTFLAGS="-C target-cpu=native" cargo install --locked --git https://gitlab.softwareheritage.org/swh/devel/swh-graph.git swh-graph-grpc-server

Or:

.. code:: console

   $ git clone https://gitlab.softwareheritage.org/swh/devel/swh-graph.git
   $ cd swh-graph
   $ cargo build --release -p swh-graph-grpc-server

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
     compress    Compress a graph using WebGraph
     download    Downloads a compressed SWH graph to the given target directory
     grpc-serve  start the graph GRPC service
     luigi       Internal command
     rpc-serve   run the graph RPC service


Alternatively, if you want to edit the swh-graph code, use these commands:

.. code:: console

   # get the code
   $ git clone https://gitlab.softwareheritage.org/swh/devel/swh-graph.git
   $ cd swh-graph

   # build Rust backend (only if you need to modify the Rust code,
   # or did not run `cargo install` above)
   $ cargo build --release -p swh-graph-grpc-server

   # install Python package
   $ python3 -m venv .venv
   $ source .venv/bin/activate
   $ pip install -e .

.. _swh-graph-retrieving-compressed:

Retrieving a compressed graph
-----------------------------

Software Heritage provides a list of off-the-shelf datasets that can be used
for various research or prototyping purposes. Most of them are available in
*compressed* representation, i.e., in a format suitable to be loaded and
queried by the ``swh-graph`` library.

All the publicly available datasets are documented on this page:
https://docs.softwareheritage.org/devel/swh-export/graph/dataset.html

A good way of retrieving these datasets is to use the `AWS S3 CLI
<https://docs.aws.amazon.com/cli/latest/reference/s3/>`_.

Here is an example with the dataset ``2021-03-23-popular-3k-python``, which has
a relatively reasonable size (~15 GiB including property data, with
the compressed graph itself being less than 700 MiB):

.. code:: console

    (venv) $ swh graph download --name 2021-03-23-popular-3k-python 2021-03-23-popular-3k-python/compressed


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
   Started GRPC using dataset from swh/graph/example_dataset/compressed/example
   ['/home/dev/.cargo/bin/swh-graph-grpc-serve', '--bind', '[::]:50867', 'compressed/graph']
   INFO:swh.graph.grpc_server:Starting gRPC server: /home/dev/.cargo/bin/swh-graph-grpc-serve --bind '[::]:50867' compressed/graph
   2024-06-18T09:12:40+02:00 - INFO - Loading graph
   2024-06-18T09:12:40+02:00 - INFO - Loading properties
   2024-06-18T09:12:40+02:00 - INFO - Loading labels
   2024-06-18T09:12:40+02:00 - INFO - Starting server
   ======== Running on http://0.0.0.0:5009 ========
   (Press CTRL+C to quit)

If you get any error about a missing file ``.cmph``, ``.bin``, ``.bits``, ``.ef``
file (typically for graphs before 2024), you need to generate it with:

.. code:: console

   RUSTFLAGS="-C target-cpu=native" cargo install --locked swh-graph
   swh graph reindex compressed/graph

Additionally, the `.ef` format may change from time to time. If you get an error
like this:

.. code:: console

    Error: Cannot map Elias-Fano pointer list ../swh/graph/example_dataset/compressed/example.ef

    Caused by:
        Wrong type hash. Expected: 0x47e8ca1ab8fa94f1 Actual: 0x890ce77a9258940c.
        You are trying to deserialize a file with the wrong type.
        The serialized type is 'sux::dict::elias_fano::EliasFano<sux::rank_sel::select_fixed2::SelectFixed2<sux::bits::bit_vec::CountBitVec, alloc::vec::Vec<u64>, 8>>' and the deserialized type is 'sux::dict::elias_fano::EliasFano<sux::rank_sel::select_adapt_const::SelectAdaptConst<sux::bits::bit_vec::BitVec<alloc::boxed::Box<[usize]>>, alloc::boxed::Box<[usize]>, 12, 4>>'.

it means your swh-graph expects a different version of the ``.ef`` files as the one
you have locally. You need to regenerate them for your version:

.. code:: console

   RUSTFLAGS="-C target-cpu=native" cargo install --locked swh-graph
   swh graph reindex --ef compressed/graph

Then try again.

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
