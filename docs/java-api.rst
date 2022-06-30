.. _swh-graph-java-api:

Using the Java API
==================

.. highlight:: java

While the :ref:`HTTP API <swh-graph-api>` is useful for many common use-cases,
it is often not sufficient to implement more complex algorithms. This section
describes the low-level Java API that ``swh-graph`` provides on top of the
WebGraph framework to manipulate the compressed graph of Software Heritage.

A cursory understanding of the `WebGraph framework
<https://webgraph.di.unimi.it/>`_ and its API is helpful to understand the
notions detailed here.

.. _swh-graph-java-basics:

Basics
------

In the WebGraph framework, graphs are generally subclasses of
`ImmutableGraph
<https://webgraph.di.unimi.it/docs/it/unimi/dsi/webgraph/ImmutableGraph.html>`_,
the abstract class providing the core API to manipulate and iterate on graphs.
Under the hood, compressed graphs are stored as the `BVGraph
<https://webgraph.di.unimi.it/docs/it/unimi/dsi/webgraph/BVGraph.html>`_
class, which contains the actual codec used to compress and decompress
adjacency lists.

Graphs **nodes** are mapped to a contiguous set of integers :math:`[0, n - 1]`
where *n* is the total number of nodes in the graph.
Each node has an associated *adjacency list*, i.e., a list of nodes going from
that source node to a destination node. This list represents the **edges** (or
**arcs**) of the graph.

**Note**: edges are always directed. Undirected graphs are internally stored as
a pair of directed edges (src → dst, dst → src), and are called "symmetric"
graphs.

On disk, a simple BVGraph with the basename ``graph`` would be represented as
the following set of files:

- ``graph.graph``: contains the compressed adjacency lists of each node, which
  can be decompressed by the BVGraph codec.
- ``graph.properties``: contains metadata on the graph, such as the number of
  nodes and arcs, as well as additional loading information needed by the
  BVGraph codec.
- ``graph.offsets``: a list of offsets of where the adjacency list of each node
  is stored in the main graph file.
- ``graph.obl``: optionally, an "offset big-list file" which can be used to
  load graphs faster.

An ImmutableGraph can be loaded using different *load methods*, which have each
different performance implications:

- ``load()``: the entire graph is loaded in RAM and supports random access.
- ``loadMapped()``: the graph is loaded by memory-mapping it from disk (see
  ``mmap(1)``), at the cost of being potentially slower, especially when doing
  random access on slow storage.
- ``loadOffline()``: no data is actually loaded is memory, only sequential
  iteration is possible.

The following code loads a graph stored on disk under the ``compressed/graph``
basename, using the memory-mapped loading mode, and stores it as a generic
ImmutableGraph:

.. code-block:: java

    ImmutableGraph graph = ImmutableGraph.loadMapped("compressed/graph");

Note that most of the time you will want to use the SWH-specific subclass
**SwhUnidirectionalGraph** instead, which has the same API as an ImmutableGraph
except it adds other SWH-specific methods. It is described later in the
:ref:`swh-graph-java-node-mappings` section.


Running the code
----------------

To run a piece of Java code written using the Java API, you need to run it with
all the dependencies in your classpath (the WebGraph libraries and the
swh-graph library). The easiest way to do it is to add the *fat jar*
shipped in the swh-graph library on PyPI, which contains all the required
dependencies.

.. code-block:: console

    $ java -cp venv/share/swh-graph/swh-graph-0.5.2.jar MyAlgo.java


Note that to load bigger graphs, the default heap size of the JVM is likely to
be insufficient to load entire graphs in memory. It is advised to increase this
heap size with the ``-Xmx`` flag:

.. code-block:: console

    $ java -Xmx300G -cp venv/share/swh-graph/swh-graph-0.5.2.jar MyAlgo.java

For more information on performance tuning and memory considerations, you
should also read the :ref:`swh-graph-memory` page, in which we recommend
additional JVM options for loading large graphs.


Simple traversal
----------------

The ImmutableGraph class provides primitives to iterate and traverse graphs. It
contains the following methods:

- ``graph.numNodes()`` returns the number of nodes in the graph (*n*).
- ``graph.numArcs()`` returns the number of arcs in the graph.

And, given a node ID :math:`k \in [0, n - 1]`:

- ``graph.successors(k)`` returns a LazyLongIterator on the nodes that are
  *adjacent* to *k* (i.e., its outgoing *neighbors*).
- ``graph.outdegree(k)`` returns the number of outgoing neighbors of *k*.


Example: Average outdegree
~~~~~~~~~~~~~~~~~~~~~~~~~~

The following code can be used to compute the average
outdegree of a graph, which is a useful measure of its density:

.. code-block:: java

    public static long averageOutdegree(ImmutableGraph graph) {
        return ((long) graph.numArcs()) / graph.numNodes();
    }


Example: Degree distributions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Using the ``outdegree()`` primitive, we can compute the outdegree distribution
of the graph by iterating on all its nodes. The distribution will be returned
as a map that associates to each degree *d* the number of nodes with outdegree
*d*.

.. code-block:: java

    public static Map<Long, Long> outdegreeDistribution(ImmutableGraph graph) {
        HashMap<Long, Long> distribution = new HashMap<Long, Long>();
        for (long k = 0; k < graph.numNodes(); ++k) {
            distribution.merge(graph.outdegree(k), 1L, Long::sum);
        }
        return distribution;
    }


Example: Depth-First Traversal
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``successors`` primitive can be used to write a simple stack-based DFS
traversal on the graph which starts from a given node and prints all the
descendant nodes in its transitive closure:

.. code-block:: java
   :emphasize-lines: 10

    public static void visitNodesDFS(ImmutableGraph graph, long srcNodeId) {
        Stack<Long> stack = new Stack<>();
        HashSet<Long> visited = new HashSet<Long>();
        stack.push(srcNodeId);
        visited.add(srcNodeId);

        while (!stack.isEmpty()) {
            long currentNodeId = stack.pop();
            System.out.println(currentNodeId);

            LazyLongIterator it = graph.successors(currentNodeId);
            for (long neighborNodeId; (neighborNodeId = it.nextLong()) != -1;) {
                if (!visited.contains(neighborNodeId)) {
                    stack.push(neighborNodeId);
                    visited.add(neighborNodeId);
                }
            }
        }
    }

Example: Breadth-First Traversal
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Swapping the stack for a queue changes the traversal order from depth-first
to breadth-first:

.. code-block:: java
   :emphasize-lines: 2

    public static void visitNodesBFS(ImmutableGraph graph, long srcNodeId) {
        Queue<Long> queue = new ArrayDeque<>();
        HashSet<Long> visited = new HashSet<Long>();
        queue.add(srcNodeId);
        visited.add(srcNodeId);

        while (!queue.isEmpty()) {
            long currentNodeId = queue.poll();
            System.out.println(currentNodeId);

            LazyLongIterator it = graph.successors(currentNodeId);
            for (long neighborNodeId; (neighborNodeId = it.nextLong()) != -1;) {
                if (!visited.contains(neighborNodeId)) {
                    queue.add(neighborNodeId);
                    visited.add(neighborNodeId);
                }
            }
        }
    }


.. _swh-graph-java-node-mappings:

Node types and SWHIDs
---------------------

In the Software Heritage archive, nodes are not represented by a simple
integer, but by a :ref:`SWHID <persistent-identifiers>`, which contain both the
*type* of the node (revision, directory, blob...) and its unique identifier. We
use **node mappings** which allow us to translate between SWHIDs and the
compact node IDs used in the compressed graph.

Most notably, we use a MPH (Minimal Perfect Hash) function implemented in the
`GOVMinimalPerfectHashFunction
<http://sux.di.unimi.it/docs/it/unimi/dsi/sux4j/mph/GOVMinimalPerfectHashFunction.html>`_
class of the Sux4J library, which maps N keys to N consecutive integers with no
collisions.

The following files are used to store the mappings between the nodes and their
types:

- ``graph.mph``: contains a serialized minimal perfect hash function computed
  on the list of all the SWHIDs in the graph.
- ``graph.order``: contains the permutation that associates with each output of
  the MPH the node ID to which it corresponds
- ``graph.node2swhid.bin``: contains a compact binary representation of all the
  SWHIDs in the graph, ordered by their rank in the graph file.
- ``graph.node2type.bin``: contains a `LongBigArrayBitVector
  <https://dsiutils.di.unimi.it/docs/it/unimi/dsi/bits/LongBigArrayBitVector.html>`_
  which stores the type of each node.

To use these mappings easily, we provide the class **SwhUnidirectionalGraph**,
an ImmutableGraph which wraps the underlying graph and adds a few
utility methods to obtain SWH-specific information on the graph.

A SwhUnidirectionalGraph can be loaded in a similar way to any ImmutableGraph,
as long as the mapping files listed above are present::

    SwhUnidirectionalGraph graph = SwhUnidirectionalGraph.load(basename);

This class exposes the same graph primitives as an ImmutableGraph, but it
additionally contains the following methods:

- ``SWHID getSWHID(long nodeId)``: returns the SWHID associated with a given
  node ID.  This function does a lookup of the SWHID at offset *i* in the file
  ``graph.node2swhid.bin``.

- ``long getNodeID(SWHID swhid)``: returns the node ID associated with a given
  SWHID. It works by hashing the SWHID with the function stored in
  ``graph.mph``, then permuting it using the permutation stored in
  ``graph.order``. It does additional domain-checking by calling ``getSWHID()``
  on its own result to check that the input SWHID was valid.

- ``SwhType getNodeType(long nodeID)``: returns the type of a given node, as
  an enum of all the different object types in the Software Heritage data
  model. It does so by looking up the value at offset *i* in the bit vector
  stored in ``graph.node2type.bin``.


Example: Find the target directory of a revision
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

As an example, we use the methods mentioned above to perform the
following task: "given a revision, return its target directory". To do so, we
first look up the node ID of the given revision in the compressed graph. We
iterate on the successors of that node, and return the SWHID of the first
destination node that has the "directory" type.


.. code-block:: java
   :emphasize-lines: 2

    public SWHID findDirectoryOfRevision(SwhUnidirectionalGraph graph, SWHID revSwhid) {
        long src = graph.getNodeId(revSwhid);
        assert graph.getNodeType(src) == SwhType.REV;
        LazyLongIterator it = graph.successors(currentNodeId);
        for (long dst; (dst = it.nextLong()) != -1;) {
            if (graph.getNodeType(dst) == SwhType.DIR) {
                return graph.getSWHID(dst);
            }
        }
        throw new RuntimeError("Revision has no target directory");
    }

.. _swh-graph-java-node-properties:

Node properties
---------------

The Software Heritage Graph is a *property graph*, which means it has various
properties associated with its nodes and edges (e.g., commit timestamps,
authors, messages, ...). We compress these properties and store them in files
alongside the compressed graph. This allows you to write traversal algorithms
that depend on these properties.

By default, properties are not assumed to be present are are not loaded when
the graph itself is loaded. If you want to use a property, you need to
explicitly load it first. As an example, this is how you load the "content
length" property to get the length of a given blob::

    SwhUnidirectionalGraph graph = SwhUnidirectionalGraph.load(basename);
    graph.loadContentLength();
    long blobSize = graph.getContentLength(graph.getNodeID(swhid));

The documentation of the SwhGraphProperties class (**TODO: link**) lists all
the different properties, their types, and the methods used to load them and to get
their value for a specific node.

A few things of note:

- A single loading call can load multiple properties at once; this is because
  they are stored in the same file to be more space efficient.

- Persons (authors, committers etc) are exported as a single pseudonymized
  integer ID, that uniquely represents a full name + email.

- Timestamps are stored as a long integer (for the timestamp itself) and a
  short integer (for the UTC offset).


.. _swh-graph-java-edge-properties:

Edge labels
-----------

While looking up graph properties on the *nodes* of the graph is relatively
straightforward, doing so for labels on the *arcs* is comparatively more
difficult. These include the names and permissions of directory entries, as
well as the branch names of snapshots.

The `ArcLabelledImmutableGraph
<https://webgraph.di.unimi.it/docs/it/unimi/dsi/webgraph/labelling/ArcLabelledImmutableGraph.html>`_
class in WebGraph wraps an ImmutableGraph, but augments its iterators by making them
*labelled iterators*, which essentially allow us to look up the label of the
arcs while iterating on them.

This labelled graph is stored in the following files:

- ``graph-labelled.properties``: a property file describing the graph, notably
  containing the basename of the wrapped graph.
- ``graph-labelled.labels``: the compressed labels
- ``graph-labelled.labeloffsets``: the offsets used to access the labels in
  random order.

The SwhUnidirectionalGraph class contains *labelled* loading methods
(``loadLabelled()``, ``loadLabelledMapped()``, ...). When these loading methods
are used instead of the standard non-labelled ones, the graph is loaded as an
ArcLabelledImmutableGraph instead of an ImmutableGraph. The following methods
can then be used:

- ``labelledSuccessors(k)`` returns a `LabelledArcIterator
  <https://webgraph.di.unimi.it/docs/it/unimi/dsi/webgraph/labelling/ArcLabelledNodeIterator.LabelledArcIterator.html>`_
  which is used in the same way as a LazyLongIterator except it also contains a
  ``label()`` method to get the label of the currently traversed arc.
- ``labelledNodeIterator()`` returns an `ArcLabelledNodeIterator
  <https://webgraph.di.unimi.it/docs/it/unimi/dsi/webgraph/labelling/ArcLabelledNodeIterator.html>`_
  of all the nodes in the graph, which replaces the LazyLongIterator of the
  ``successor()`` function by a LabelledArcIterator similar to above.


Label format
~~~~~~~~~~~~

The labels of each arc are returned as a ``DirEntry[]`` array. They encode
both the name of a directory entry and its permissions. For snapshot branches,
only the "name" field is useful.

Arc label names are encoded as an integer ID representing each unique
entry/branch name present in the graph. To retrieve the actual name associated
with a given label ID, one needs to load the reverse mapping similar to how you
would do for a normal property::

    SwhUnidirectionalGraph graph = SwhUnidirectionalGraph.loadLabelled(basename);
    graph.loadLabelNames();

The byte array representing the actual label name can then be loaded with::

    byte[] name = graph.getLabelName(label.filenameId);


Multiedges
~~~~~~~~~~

The Software Heritage is not a *simple graph*, where at most one edge can exist
between two vertices, but a *multigraph*, where multiple edges can be incident
to the same two vertices. Consider for instance the case of a single directory
``test/`` containing twice the same file blob (e.g., the empty file), under two
different names (e.g., ``ISSUES.txt`` and ``TODO.txt``, both completely empty).
The simple graph view of this directory will represent it as a single edge
``test`` → *empty file*, while the multigraph view will represent it as *two*
edges between the same nodes.

Due to the copy-list model of compression, WebGraph only stores simple graphs,
and thus stores multiedges as single edges, to which we cannot associate
a single label name (in our example, we need to associate both names
``ISSUES.txt`` and ``TODO.txt``).
To represent this possibility of having multiple file names for a single arc,
in the case of multiple relationships between two identical nodes, each arc label is
stored as an *array* of DirEntry, each record representing one relationship
between two nodes.


Example: Printing all the entries of a directory
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The following code showcases how one can print all the entries (name,
permission and target SWHID) of a given directory, using the labelled methods
seen above.

.. code-block:: java

    public static void printEntries(ImmutableGraph g, long dirNode) {
        LabelledArcIterator s = g.labelledSuccessors(dirNode);
        for (long dst; (dst = it.nextLong()) >= 0;) {
            DirEntry[] labels = (DirEntry[]) s.label().get();
            for (DirEntry label : labels) {
                System.out.format(
                    "%s %s %d\n",
                    graph.getSWHID(dst);
                    new String(graph.getLabelName(label.filenameId)),
                    label.permission
                );
            }
        }
    }

    // Usage: $PROGRAM <GRAPH_BASENAME> <DIR_SWHID>
    public static void main(String[] args) {
        SwhUnidirectionalGraph g = SwhUnidirectionalGraph.loadLabelledMapped(args[0]);
        g.loadLabelNames();
        long dirNode = g.getNodeID(new SWHID(args[1]));
        printEntries(g, dirNode);
    }


Transposed graph
----------------

Up until now, we have only looked at how to traverse the *forward* graph, i.e.,
the directed graph whose edges are in the same direction as the Merkle DAG of
the Software Heritage archive.
For many purposes, especially that of finding the *provenance* of software
artifacts, it is useful to query the *backward* (or *transposed*) graph
instead, which is the same as the forward graph except all the edges are
reversed.

The transposed graph has its own set of files, counterparts to the files needed
for the forward graph:

- ``graph-transposed.graph``
- ``graph-transposed.properties``
- ``graph-transposed.offsets``
- ``graph-transposed.obl``
- ``graph-transposed-labelled.labels``
- ``graph-transposed-labelled.labeloffsets``
- ``graph-transposed-labelled.properties``

However, because node IDs are the same in the forward and the backward graph,
all the files that pertain to mappings between the node IDs and various
properties (SWHIDs, property data, node permutations etc) remain the same.


Example: Earliest revision containing a given blob
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The following code loads all the committer timestamps of the revisions in the
graph, then walks the *transposed* graph to return the earliest revision
containing a given object.

.. code-block:: java

    public static long findEarliestRevisionContaining(SwhUnidirectionalGraph g, long src) {
        long oldestRev = -1;
        long oldestRevTs = Long.MAX_VALUE;

        Stack<Long> stack = new Stack<>();
        HashSet<Long> visited = new HashSet<Long>();
        stack.push(src);
        visited.add(src);
        while (!stack.isEmpty()) {
            long currentNodeId = stack.pop();
            LazyLongIterator it = graph.successors(currentNodeId);
            for (long neighborNodeId; (neighborNodeId = it.nextLong()) != -1;) {
                if (!visited.contains(neighborNodeId)) {
                    stack.push(neighborNodeId);
                    visited.add(neighborNodeId);
                    if (g.getNodeType(neighborNodeId) == SwhType.REV) {
                        Long ts = g.getCommitterTimestamp(neighborNodeId);
                        if (ts != null && ts < oldestRevTs) {
                            oldestRev = neighborNodeId;
                            oldestRevTs = ts;
                        }
                    }
                }
            }
        }
        return oldestRev;
    }

    // Usage: $PROGRAM <GRAPH_BASENAME> <BLOB_SWHID>
    public static void main(String[] args) {
        // Load the backward (= transposed) graph as a SwhUnidirectionalGraph
        SwhUnidirectionalGraph g = SwhUnidirectionalGraph.loadMapped(args[0] + "-transposed");
        g.loadCommitterTimestamps();
        long node = g.getNodeID(new SWHID(args[1]));
        long oldestRev = findEarliestRevisionContaining(g, node);
        System.out.println(g.getSWHID(oldestRev));
    }




Bidirectional Graph
-------------------


BidirectionalImmutableGraph
~~~~~~~~~~~~~~~~~~~~~~~~~~~

While ``graph-transposed`` can be loaded as a simple SwhUnidirectionalGraph and
then manipulated just like the forward graph, it is often convenient to have
*both* the forward and the backward graph in memory. Some traversal algorithms
require first going down in the forward graph to select some nodes, then going
up to find their provenance.

To achieve that, we use the `BidirectionalImmutableGraph
<https://webgraph.di.unimi.it/docs-big/it/unimi/dsi/big/webgraph/BidirectionalImmutableGraph.html>`_
class from WebGraph, which stores both a graph and its transpose.
This class provides the following methods to iterate on the **backward** graph,
shown here with their counterparts for the forward graph:

.. list-table::
   :header-rows: 1

   * - Forward graph operation
     - Backward graph operation

   * - ``outdegree(k)``
     - ``indegree(k)``

   * - ``successors(k)``
     - ``predecessors(k)``

In addition, the class offers a few convenience methods which are generally
useful when you have both a graph and its transpose:

- ``transpose()`` returns the transpose of the BidirectionalImmutableGraph by
  inverting the references to the forward and the backward graphs. Successors
  become predecessors, and vice-versa.
- ``symmetrize()`` returns the symmetric (= undirected) version of the
  bidirectional graph. It is implemented by a union between the forward and the
  backward graph, which basically boils down to removing the directionality of
  the edges (the successors of a node are also its predecessors).


SwhBidirectionalGraph
~~~~~~~~~~~~~~~~~~~~~

Like for ImmutableGraph, we extend the BidirectionalImmutableGraph with
SWH-specific methods, in the subclass ``SwhBidirectionalGraph``. Notably, it
contains the method ``labelledPredecessors()``, the equivalent of
``labelledSuccessors()`` but on the backward graph.

Because SwhUnidirectionalGraph inherits from ImmutableGraph, and
SwhBidirectionalGraph inherits from BidirectionalImmutableGraph, we put the
common behavior between the two classes in a SwhGraph interface, which can
represent either an unidirectional or a bidirectional graph.

To avoid loading the node properties two times (once for each direction), they
are stored in a separate class called SwhGraphProperties. In a
SwhBidirectionalGraph, the two SwhUnidirectionalGraph share their node
properties in memory by storing references to the same SwhGraphProperty
object.

.. code-block:: text


                     ┌──────────────┐
                     │ImmutableGraph◄────────┐
                     └────▲─────────┘        │extends
                          │                  │
                          │       ┌──────────┴────────────────┐
                   extends│       │BidirectionalImmutableGraph│
                          │       └────────────▲──────────────┘
                          │                    │extends
           ┌──────────────┴───────┐     ┌──────┴──────────────┐
           │SwhUnidirectionalGraph│◄────┤SwhBidirectionalGraph│
           └──┬──────────────┬────┘     └────────┬───────────┬┘
              │              │    contains x2    │           │
              │              │                   │           │
              │    implements│                   │implements │
              │             ┌▼──────────┐        │           │
              │             │SwhGraph(I)◄────────┘           │
      contains│             └───────────┘                    │contains
              │                                              │
              │            ┌──────────────────┐              │
              └────────────►SwhGraphProperties◄──────────────┘
                           └──────────────────┘


Example: Find all the shared-commit forks of a given origin
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

It is possible to define the *forks* of an origin as being the set of origins
which share at least one revision with that origin.

The following code loads the graph in both directions using a
SwhBidirectionalGraph. Given an origin SWHID, it first walks the *forward*
graph to find all its root revisions. It then walks the *backward* graph to
find all the origins containing these root revisions, i.e., its *forks*.

.. code-block:: java

    public static void findSharedCommitForks(SwhUnidirectionalGraph g, long srcOrigin) {
        Stack<Long> forwardStack = new Stack<>();
        HashSet<Long> forwardVisited = new HashSet<Long>();
        Stack<Long> backwardStack = new Stack<>();
        HashSet<Long> backwardVisited = new HashSet<Long>();

        // First traversal (forward graph): find all the root revisions of the
        // origin
        forwardStack.push(srcOrigin);
        forwardVisited.add(srcOrigin);
        while (!forwardStack.isEmpty()) {
            long curr = forwardStack.pop();
            LazyLongIterator it = graph.successors(curr);
            boolean isRootRevision = true;
            for (long succ; (succ = it.nextLong()) != -1;) {
                SwhType nt = g.getNodeType(succ);
                if (!forwardVisited.contains(succ)
                        && nt != SwhType.DIR && nt != SwhType.CNT) {
                    forwardStack.push(succ);
                    forwardVisited.add(succ);
                    isRootRevision = false;
                }
            }
            if (g.getNodeType(curr) == SwhType.REV && isRootRevision) {
                // Found a root revision, add it to the second stack
                backwardStack.push(curr);
                backwardVisited.add(curr);
            }
        }

        // Second traversal (backward graph): find all the origins containing
        // any of these root revisions and print them
        while (!backwardStack.isEmpty()) {
            long curr = backwardStack.pop();
            LazyLongIterator it = graph.predecessors(curr);
            boolean isRootRevision = true;
            for (long succ; (succ = it.nextLong()) != -1;) {
                SwhType nt = g.getNodeType(succ);
                if (!backwardVisited.contains(succ)) {
                    backwardStack.push(succ);
                    backwardVisited.add(succ);
                    if (nt == SwhType.ORI) {
                        // Found an origin, print it.
                        System.out.println(g.getSWHID(succ));
                    }
                }
            }
        }
    }

    // Usage: $PROGRAM <GRAPH_BASENAME> <ORI_SWHID>
    public static void main(String[] args) {
        // Load both forward and backward graphs as a SwhBidirectionalGraph
        SwhBidirectionalGraph g = SwhBidirectionalGraph.loadMapped(args[0]);
        long node = g.getNodeID(new SWHID(args[1]));
        findSharedCommitForks(g, node);
    }


Large-scale processing
----------------------

Multithreading
~~~~~~~~~~~~~~

ImmutableGraph is not thread-safe. When writing multithreaded algorithms,
calling ``successors()`` on the same graph from multiple threads will return
garbage.

Instead, each thread should create its own "lightweight copy" of the graph by
calling ``.copy()``. This will not actually copy the entire graph data, which
will remain shared across threads, but it will create new instances of the
iterators so that each thread can independently iterate on the graph data.


Data structures for large traversals
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When doing very large traversals, such as a BFS on the entire graph, the
usual data structures (HashSet, Stack, ArrayDeque, etc.) will be quite
inefficient. If you know you are going to traverse large parts of the graph,
it's better to use more appropriate data structures, a lot of which can be
found in the dsiutils library. In particular:

- `LongArrayBitVector
  <https://dsiutils.di.unimi.it/docs/it/unimi/dsi/bits/LongArrayBitVector.html>`_
  is an efficient bit-vector implementation, which can be used to store the
  nodes that have already been seen in the visit. Its memory footprint is too
  big to use for small traversals, but it is very efficient to traverse the
  full graph, as every node only takes a single bit.

- `ByteDiskQueue
  <https://dsiutils.di.unimi.it/docs/it/unimi/dsi/io/ByteDiskQueue.html>`_ can
  be used to efficiently store the queue of nodes to visit on disk, when it is
  too large to fit in RAM.

Other types in dsiutils and fastutil can save significant memory:
``LongArrayList`` saves at least 8 bytes per entry over ``ArrayList<Long>``,
and ``Long2LongOpenHashMap`` saves at least 16 bytes for every entry over
``HashMap<Long, Long>``. We strongly recommend reading the documentation of the
unimi libraries and looking at the code for usage examples.


BigArrays
~~~~~~~~~

When working with the Software Heritage graph, is often necessary to store
large arrays of values, with a size exceeding 2^32 items. Unfortunately,
standard Java arrays do not support this.

To circumvent this, WebGraph uses the `BigArrays scheme
<https://fastutil.di.unimi.it/docs/it/unimi/dsi/fastutil/BigArrays.html>`_ from
the fastutil library: "big arrays" are stored as arrays of arrays, supporting
quadrillions of records.

A BigArray ``long[][] a`` can be used with the following methods:

- ``BigArrays.get(a, i)`` to get the value at index *i*
- ``BigArrays.set(a, i, v)`` to set the value at index *i* to *v*.
- ``BigArrays.length(a)`` to get the total length of the bigarray.
