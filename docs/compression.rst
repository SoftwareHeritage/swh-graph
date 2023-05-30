.. _graph-compression:

=================
Graph compression
=================

The compression pipeline is implemented on top of the `WebGraph framework
<http://webgraph.di.unimi.it/>`_. It takes an ORC Graph Dataset as an input,
such as the ones found in the :ref:`Graph Dataset List <swh-dataset-list>`,
and generates a compressed graph suitable for high intensity analyses on
large servers.


Running the compression pipeline
================================

Dependencies
------------

To compress a graph, you will need to install the ``swh.graph`` tool as well as
a recent JRE, as described in the :ref:`swh-graph-quickstart` page.

You will also need the zstd_ compression tool::

    $ sudo apt install zstd

.. _zstd: https://facebook.github.io/zstd/


Hardware Requirements
---------------------

The compression pipeline is even more demanding than the graph server in terms
of hardware requirements, especially RAM. Notably, the BFS compression step
loads a graph compressed in random order in memory, which is usually more than
a TiB for the full graph. While it is possible to do this step with a memory
mapping, our experiments show that this could take a very long time (several
months) on hard drives.

The LLP compression step requires 13 bytes of RAM per node, which could amount
to storing hundreds of gigabytes in RAM in addition to loading the graph
itself.

Some steps also involve sorting the entire set of edges and their labels, by
using large on-disk buffer files, sometimes reaching the size of the input
dataself itself.

The machine we used to compress the entire graph (dataset version 2022-04-25)
has the following hardware specs:

- 2 TiB of RAM (DDR4 ECC 2400Mhz)
- 64 vCPUs (Dual AMD EPYC 7302 16-Core)
- 24 TiB of SSD (NVMe)

The server we rented is from the
`HGR-HCI-4 <https://www.ovhcloud.com/en/bare-metal/high-grade/hgr-hci-4/>`_
series from OVH.


Input dataset
-------------

First, you need to retrieve a graph to compress, in ORC format. The :ref:`Graph
Dataset List <swh-dataset-list>` has a list of datasets made available by the
Software Heritage archive, including "teaser" subdatasets which have a more
manageable size and are thus very useful for prototyping with less hardware
resources.

The datasets can be retrieved from S3 or the annex, in a similar fashion to
what is described in :ref:`swh-graph-retrieving-compressed`, by simply
replacing "compressed" by "orc":

.. code:: console

    (venv) $ mkdir -p 2021-03-23-popular-3k-python/orc
    (venv) $ cd 2021-03-23-popular-3k-python/
    (venv) $ aws s3 cp --recursive s3://softwareheritage/graph/2021-03-23-popular-3k-python/orc/ orc

Alternatively, any custom ORC dataset can be used as long as it respects
:ref:`the schema <swh-dataset-schema>` of the Software Heritage Graph Dataset.

**Note:** for testing purposes, a fake test dataset is available in the
``swh-graph`` repository, with just a few dozen nodes. The ORC tables are
available in ``swh-graph/swh/graph/example_dataset/orc/``.


Compression
-----------

You can compress your dataset by using the ``swh graph compress`` command. It
will run all the various steps of the pipeline in the right order.

.. code:: console


    (venv) $ swh graph compress --input-dataset orc/ --outdir compressed/
    [...]
    (venv) $ ls compressed/
    graph.edges.count.txt
    graph.edges.stats.txt
    graph.graph
    graph.indegree
    graph-labelled.labeloffsets
    graph-labelled.labels
    [...]
    graph-transposed.obl
    graph-transposed.offsets
    graph-transposed.properties


(The purpose of each of these files is detailed in the
:ref:`swh-graph-java-api` page.

For sufficiently large graphs, this command can take entire weeks. It is highly
recommended to run it in a systemd service or in a tmux session.

It is also possible to run single steps or step ranges from the CLI:

.. code:: bash

    swh graph compress -i orc/ -o compressed/ --steps mph-bfs

See ``swh graph compress --help`` for syntax and usage details.

For some steps, Java will try to allocate terabytes of memory in sub-gigabyte
segments, so you should increase the number of allowed allocations first:

.. code:: bash

    sudo sysctl -w vm.max_map_count=1966080

.. _swh-graph-compression-steps:

Compression steps
=================

The compression pipeline consists of the following steps:

.. figure:: images/compression_steps.png
    :align: center
    :alt: Compression steps
    :scale: 20%

    Compression steps

Each of these steps is briefly described below. For more details see the
original Software Heritage graph compression paper [SWHGraphCompression2020]_,
as well as chapters 9 and 10 of Antoine Pietri's PhD thesis
[PietriThesis2021]_.

.. [SWHGraphCompression2020]
    | Paolo Boldi, Antoine Pietri, Sebastiano Vigna, Stefano Zacchiroli.
    | `Ultra-Large-Scale Repository Analysis via Graph Compression
      <https://upsilon.cc/~zack/research/publications/saner-2020-swh-graph.pdf>`_.
    | In proceedings of `SANER 2020 <https://saner2020.csd.uwo.ca/>`_: The 27th
      IEEE International Conference on Software Analysis, Evolution and
      Reengineering. IEEE 2020.
    | Links: `preprint
      <https://upsilon.cc/~zack/research/publications/saner-2020-swh-graph.pdf>`_,
      `bibtex
      <https://upsilon.cc/~zack/research/publications/saner-2020-swh-graph.bib>`_.



.. [PietriThesis2021]
   | Antoine Pietri
   | `Organizing the graph of public software development for large-scale mining
     <https://hal.archives-ouvertes.fr/tel-03515795/>`_.
   | Doctoral dissertation. Inria, 2021.


1. EXTRACT_NODES
----------------

This step reads a graph dataset and extract all the unique node SWHIDs it
contains, including the ones that are not stored as actual objects in the
graph, but only *referred to* by the edges.  Additionally, it extracts the set
of all unique edge labels in the graph.

**Rationale:** Because the graph can contain holes, loose objects and dangling
objects, some nodes that are referred to as destinations in the edge
relationships might not actually be stored in the graph itself. However, to
compress the graph using a graph compressio library, it is necessary to have a
list of *all* the nodes in the graph, including the ones that are simply
referred to by the edges but not actually stored as concrete objects.

This step reads the entire graph dataset, and uses ``sort -u`` to extract the
set of all the unique nodes and unique labels that will be needed as an input
for the compression process. It also write object count statistics in various
files:

- The set of nodes is written in ``graph.nodes.csv.zst``, as a zst-compressed
  sorted list of SWHIDs, one per line.
- The set of edge labels is written in ``graph.labels.csv.zst``, as a
  zst-compressed sorted list of labels encoded in base64, one per line.
- The number of unique nodes referred to in the graph is written in a text
  file, ``graph.nodes.count.txt``
- The number of unique edges referred to in the graph is written in a text
  file, ``graph.edges.count.txt``
- The number of unique edge labels is written in a text file,
  ``graph.labels.count.txt``
- Statistics on the number of nodes of each type are written in a text file,
  ``graph.nodes.stats.txt``
- Statistics on the number of edges of each type are written in a text file,
  ``graph.edges.stats.txt``


2. MPH
------

As discussed in :ref:`swh-graph-java-basics`, a node in the Software Heritage
:ref:`data model <data-model>` is identified by its SWHID (see :ref:`persistent
identifiers <persistent-identifiers>`), but WebGraph internally uses integers
to refer to node ids.

To create a mapping between integer node IDs and SWHIDs, we use the
`GOVMinimalPerfectHashFunction
<http://sux.di.unimi.it/docs/it/unimi/dsi/sux4j/mph/GOVMinimalPerfectHashFunction.html>`_
class of the `Sux4J <http://sux.di.unimi.it/>`_ library, which maps N keys to N
consecutive integers.

We run this function on the list of SWHIDs stored in the
``graph.nodes.csv.zst`` file generated in the previous step.
This allows us to generate a bijection from the set of all the *n* SWHIDs in the
graph to the set of integers :math:`[0, n - 1]`.

The step produces a ``graph.mph`` file (MPH stands for *Minimal Perfect
Hash-function*), containing a function which takes a SWHID (as a bytestring)
and returns its associated node ID.


3. BV compress
--------------

This is the first actual compression step, where we build a compressed version
of the input graph dataset.

We use a ScatteredArcsORCGraph to load the dataset
(implementation inspired of the `ScatteredArcsASCIIGraph
<http://webgraph.di.unimi.it/docs-big/it/unimi/dsi/big/webgraph/ScatteredArcsASCIIGraph.html>`_
class in WebGraph).
This class wraps the ORC Graph dataset and exposes a *virtual* ImmutableGraph,
whose nodes and edges can be iterated sequentially as if it was any other
standard graph. To do so, it puts all the edges in batches and sorts them in an
aggressively parallel fashion, then stores them as ``.bitstream`` files, and
returns a `BatchGraph
<https://webgraph.di.unimi.it/docs-big/it/unimi/dsi/big/webgraph/Transform.BatchGraph.html>`_
created from these batches.

Finally, it uses the ``BVGraph.store()`` method, which compresses the input
graph as a `BVGraph
<https://webgraph.di.unimi.it/docs-big/it/unimi/dsi/big/webgraph/BVGraph.html>`_,
using the compression techniques described in the article *The WebGraph
Framework I: Compression Techniques* cited above.

The resulting BV graph is stored as a set of files:

- ``graph-base.graph``: the compressed graph in the BV format
- ``graph-base.offsets``: offsets values to read the bit stream graph file
- ``graph-base.properties``: entries used to correctly decode graph and offset
  files


4. BFS
------

In [LLP]_, the paper authors empirically demonstrate that a high graph
compression ratio can be achieved for the graph of the Web by ordering nodes
such that vertices from the same host are close to each other.

In Software Heritage, there is no notion of "host" that can be used to generate
these compression-friendly orderings, because the identifiers are just
uniformly random cryptographic hashes. However, we can generate these orderings
by running algorithms to inform us on which nodes are close to each other.

In this step, we run a BFS traversal on the entire graph to get a more
compression-friendly ordering of nodes. We use the `BFS
<http://law.di.unimi.it/software/law-docs/it/unimi/dsi/law/big/graph/BFS.html>`_
class from the `LAW <http://law.di.unimi.it/>`_ library.

The resulting ordering is stored in a ``graph-bfs.order`` file, which contains
all the node IDs in the order of traversal.


5. PERMUTE_BFS
--------------

Once the BFS order is computed, we permute the initial "base" graph using the
this new ordering. The permutation uses the `Transform
<http://webgraph.di.unimi.it/docs-big/it/unimi/dsi/big/webgraph/Transform.html>`_
class from WebGraph framework.

The BFS-compressed graph is stored in the files
``graph-bfs.{graph,offsets,properties}``.

6. TRANSPOSE_BFS
----------------

We transpose the BFS-compressed graph, using the `Transform
<http://webgraph.di.unimi.it/docs-big/it/unimi/dsi/big/webgraph/Transform.html>`_
class from WebGraph.
This step is a prerequisite for LLP compression.

7. SIMPLIFY
-----------

This step creates a loopless and symmetric version of the BFS-compressed graph,
using the `Transform
<http://webgraph.di.unimi.it/docs-big/it/unimi/dsi/big/webgraph/Transform.html>`_
class from WebGraph.
This step is a prerequisite for LLP compression.

8. LLP
------

Better compression ratios can be achieved by the Layered Label Propagation
(LLP) algorithm to reorder nodes. This algorithm is described in [LLP]_.
The LLP algorithm finds locality-preserving orders by clustering together nodes
in close proximity. Similar to the BFS, this algorithm is particularly
interesting for our use case as it is unsupervised, and does not rely on prior
information on the clusters present in the graph. The idea behind the
clustering algorithm is to randomly distribute communities to the nodes in the
graph, then iteratively assign to each node the community most represented in
its neighbors.

.. [LLP] Paolo Boldi, Marco Rosa, Massimo Santini, Sebastiano Vigna.
    *Layered label propagation: a multiresolution coordinate-free ordering for compressing social networks.*
    WWW 2011: 587-596
    DOI: https://doi.org/10.1145/1963405.1963488
    preprint: https://arxiv.org/abs/1011.5425

LLP is more costly than simple BFS-based compression in both time and memory.
Even though the algorithm has a linear time complexity, it does multiple
iterations on the graph and is significantly slower than the BFS which is just
one single traversal. Moreover, keeping track of the communities requires a
total of 13 bytes per node, which increases the RAM requirements.
Because of these constraints, it is unrealistic to run the LLP algorithm on the
uncompressed version of the graph; this is why we do an intermediate
compression with the BFS ordering first, then compress the entire graph *again*
with an even better ordering.

The LLP algorithm takes a simplified (loopless, symmetric) graph as an input,
which we already computed in the previous steps.

The algorithm is also parameterized by a list of γ values, a "resolution" parameter
which defines the shapes of the clustering it produces: either small, but
denser pieces, or larger, but unavoidably sparser pieces. The algorithm then
combines the different clusterings together to generate the output reordering.
γ values are given to the algorithm in the form :math:`\frac{j}{2^k}`; by
default, 12 different values of γ are used.  However, the combination procedure
is very slow, and using that many γ values could take several months in our
case.
We thus narrowed down a smaller set of γ values that empirically give good
compression results, which are used by default in the pipeline. In general,
smaller values of γ seem to generate better compression ratios. The effect of a
given γ is that the density of the sparsest cluster is at least γ γ+1, so large
γ values imply small, more dense clusters. It is reasonable to assume that
since the graph is very sparse to start with, such clusters are not that
useful.

The resulting ordering is stored in a ``graph-llp.order`` file.

9. PERMUTE_LLP
--------------

Once the LLP order is computed, we permute the BFS-compressed graph using the
this new ordering. The LLP-compressed graph, which is our final compressed
graph, is stored in the files ``graph.{graph,offsets,properties}``.

10. OBL
-------

Cache the BVGraph offsets of the forward graph to make loading faster. The
resulting offset big list is stored in the ``graph.obl`` file.

11. COMPOSE_ORDERS
------------------

To be able to translate the initial MPH inputs to their resulting rank in the
LLP-compressed graph, we need to use the two order permutations successively:
the base → BFS permutation, then the BFS → LLP permutation.

To make this less wasteful, we *compose* the two permutations into a single
one. We use the `composePermutationsInPlace
<https://dsiutils.di.unimi.it/docs/it/unimi/dsi/Util.html#composePermutationsInPlace(long%5B%5D%5B%5D,long%5B%5D%5B%5D)>`_
function of the dsiutils library. The resulting permutation is stored as a
``graph.order`` file. Hashing a SWHID with the ``graph.mph`` function, then
permuting the result using the ``graph.order`` permutation yields the integer
node ID matching the input SWHID in the graph.

12. STATS
---------

This step computes various statistics on the compressed graph:

- ``.stats``: statistics such as number of nodes, edges, avg/min/max degree,
  average locality, etc.
- ``.indegree``: graph indegree distribution
- ``.outdegree``: graph outdegree distribution

This step uses the `Stats
<http://webgraph.di.unimi.it/docs-big/it/unimi/dsi/big/webgraph/Stats.html>`_
class from WebGraph.


13. TRANSPOSE
-------------

Transpose the graph to allow backward traversal, using the `Transform
<http://webgraph.di.unimi.it/docs-big/it/unimi/dsi/big/webgraph/Transform.html>`_
class from WebGraph. The resulting transposed graph is stored as the
``graph-transposed.{graph,offsets,properties}`` files.


14. TRANSPOSE_OBL
-----------------

Same as OBL, but for the transposed graph. The resulting offset big list is
stored in the ``graph-transposed.obl`` file.


15. MAPS
--------

This steps generates the *node mappings* described in
:ref:`swh-graph-java-node-mappings`. In particular, it generates:

- ``graph.node2swhid.bin``: a compact binary representation of all the
  SWHIDs in the graph, ordered by their rank in the graph file.
- ``graph.node2type.bin``: a `LongBigArrayBitVector
  <https://dsiutils.di.unimi.it/docs/it/unimi/dsi/bits/LongBigArrayBitVector.html>`_
  which stores the type of each node.

It does so by reading all the SWHIDs in the ``graph.nodes.csv.zst`` file generated in the
EXTRACT_NODES step, then getting their corresponding node IDs (using the
``.mph`` and ``.order`` files), then sorting all the SWHIDs according to
their node ID. It then writes these SWHIDs in order, in a compact but seekable
binary format, which can be used to return the SWHID corresponding to any given
node in O(1).


16. EXTRACT_PERSONS
-------------------

This step reads the ORC graph dataset and extracts all the unique persons it
contains. Here, "persons" are defined as the set of unique pairs of name +
email, potentially pseudonymized, found either as revision authors, revision
committers or release authors.

The ExtractPersons class reads all the persons from revision and release
tables, then uses ``sort -u`` to get a sorted list without any duplicates. The
resulting sorted list of authors is stored in the ``graph.persons.csv.zst``
file.


17. MPH_PERSONS
---------------

This step computes a Minimal Perfect Hash function on the set of all the unique
persons extracted in the EXTRACT_PERSONS step. Each individual person is mapped
to a unique integer in :math:`[0, n-1]` where *n* is the total number of
persons. The resulting function is serialized and stored in the
``graph.persons.mph`` file.


18. NODE_PROPERTIES
-------------------

This step generates the *node property files*, as described in
:ref:`swh-graph-java-node-properties`.
The nodes in the Software Heritage Graph each have associated *properties*
(e.g., commit timestamps, authors, messages, ...). The values of these
properties for each node in the graph are compressed and stored in files
alongside the compressed graph.

The WriteNodeProperties class reads all the properties from the ORC Graph
Dataset, then serializes each of them in a representation suitable for
efficient random access (e.g., large binary arrays) and stores them on disk.

For persons (authors, committers etc), the MPH computed in the MPH_PERSONS step
is used to store them as a single pseudonymized integer ID, which uniquely
represents a full name + email.

The results are stored in the following list of files:

- ``graph.property.author_id.bin``
- ``graph.property.author_timestamp.bin``
- ``graph.property.author_timestamp_offset.bin``
- ``graph.property.committer_id.bin``
- ``graph.property.committer_timestamp.bin``
- ``graph.property.committer_timestamp_offset.bin``
- ``graph.property.content.is_skipped.bin``
- ``graph.property.content.length.bin``
- ``graph.property.message.bin``
- ``graph.property.message.offset.bin``
- ``graph.property.tag_name.bin``
- ``graph.property.tag_name.offset.bin``


19. MPH_LABELS
--------------

This step computes a **monotone** Minimal Perfect Hash function on the set of
all the unique *arc label names* extracted in the EXTRACT_NODES step. Each
individual arc label name (i.e., directory entry names and snapshot branch
names) is monotonely mapped to a unique integer in :math:`[0, n-1]`, where *n*
is the total number of unique arc label names, which corresponds to their
**lexical rank** in the set of all arc labels.

In other words, this MPH being monotone means that the hash of the *k*-th item
in the sorted input list of arc labels will always be *k*.
We use the `LcpMonotoneMinimalPerfectHashFunction
<https://sux.di.unimi.it/docs/it/unimi/dsi/sux4j/mph/LcpMonotoneMinimalPerfectHashFunction.html>`_
of Sux4J to generate this function.

The rationale for using a monotone function here is that it will allow us to
quickly get back the arc label from its hash without having to store an
additional permutation.
The resulting MPH function is serialized and stored in the ``graph.labels.mph``
file.


20. FCL_LABELS
--------------

This step computes a *reverse-mapping* for arc labels, i.e., a way to
efficiently get the arc label name from its hash computed with the monotone MPH
of the MPH_LABELS step.

Thanks to the MPH being monotone, this boils down to storing all the labels in
lexicographic order in a string list format that allows O(1) access to its
elements. For this purpose, we use the `MappedFrontCodedStringBigList
<https://dsiutils.di.unimi.it/docs/it/unimi/dsi/big/util/MappedFrontCodedStringBigList.html>`_
class from the dsiutils library, using the ``graph.labels.csv.zst`` file as its
input. It stores the label names in a compact way by using front-coding
compression, which is particularly efficient here because the strings are
already in lexicographic order. The resulting FCL files are stored as
``graph.labels.fcl.*``, and they can be loaded using memory mapping.


21. EDGE_LABELS
---------------


This step generates the *edge property files*, as described in
:ref:`swh-graph-java-edge-properties`. These files allow us to get the *edge
labels* as we iterate on the edges of the graph. The files essentially contain
compressed sorted triplets of the form (source, destination, label), with
additional offsets to allow random access.

To generate these files, the LabelMapBuilder class starts by reading in
parallel the labelled edges in the ORC dataset, which can be thought of as
quadruplets containing the source SWHID, the destination SWHID, the label name
and the entry permission if applicable:

.. code-block:: text

    swh:1:snp:4548a5… swh:1:rev:0d6834… cmVmcy9oZWFkcy9tYXN0ZXI=
    swh:1:dir:05faa1… swh:1:cnt:a35136… dGVzdC5j 33188
    swh:1:dir:05faa1… swh:1:dir:d0ff82… dGVzdA== 16384
    ...

Using the ``graph.mph`` and the ``graph.order`` files, we hash and permute the
source and destination nodes. We also monotonically hash the labels using the
``graph.labels.mph`` function to obtain the arc label identifiers. The
permissions are normalized as one of the 6 possible values in the
``DirEntry.Permission.Type`` enum, and are then stored in the 3 lowest bits of
the label field.

.. code-block:: text

    4421 14773 154
    1877 21441 1134
    1877 14143 1141
    ...

These hashed edges and their compact-form labels are then put in large batches
sorted in an aggressively parallel fashion, which are then stored as
``.bitstream`` files. These batch files are put in a heap structure to perform
a merge sort on the fly on all the batches.

Then, the LabelMapBuilder loads the graph and starts iterating on its edges. It
synchronizes the stream of edges read from the graph with the stream of sorted
edges and labels read from the bitstreams in the heap. At this point, it writes
the labels to the following output files:

- ``graph-labelled.properties``: a property file describing the graph, notably
  containing the basename of the wrapped graph.
- ``graph-labelled.labels``: the compressed labels
- ``graph-labelled.labeloffsets``: the offsets used to access the labels in
  random order.

It then does the same with backward edge batches to get the transposed
equivalent of these files:
``graph-transposed-labelled.{properties,labels,labeloffsets}``.


23. EDGE_LABELS_OBL
-------------------

Cache the label offsets of the forward labelled graph to make loading faster.
The resulting label offset big list is stored in the
``graph-labelled.labelobl`` file.


23. EDGE_LABELS_TRANSPOSE_OBL
-----------------------------

Same as EDGE_LABELS_OBL, but for the transposed labelled graph.
The resulting label offset big list is stored in the
``graph-transposed-labelled.labelobl`` file.


24. CLEAN_TMP
-------------

This step reclaims space by deleting the temporary directory, as well as all
the intermediate outputs that are no longer necessary now that the final graph
has been compressed (shown in gray in the step diagram).
