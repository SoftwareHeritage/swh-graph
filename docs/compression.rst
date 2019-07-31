Graph compression
=================

The compression process is based on the `WebGraph framework
<http://webgraph.di.unimi.it/>`_ and ecosystem libraries.

References used:

- Paolo Boldi, Sebastiano Vigna, *The webgraph framework I: compression
  techniques*, Proceedings of the 13th international conference on World Wide
  Web. ACM, 2004. `pdf <http://vigna.di.unimi.it/ftp/papers/WebGraphI.pdf>`_
- Paolo Boldi, Marco Rosa, Massimo Santini, Sebastiano Vigna, *Layered label
  propagation: A multiresolution coordinate-free ordering for compressing social
  networks*. `arxiv <https://arxiv.org/abs/1011.5425>`_
- Alberto Apostolico, Guido Drovandi, *Graph compression by BFS*, Algorithms
  2009, 2(3), 1031-1044. `mdpi <https://www.mdpi.com/1999-4893/2/3/1031/pdf>`_

.. figure:: images/compression_steps.png
    :align: center
    :alt: Compression steps

    Compression steps

1. MPH
------

A node in the `Software Heritage graph
<https://docs.softwareheritage.org/devel/swh-model/data-model.html>`_ is
identified using its string `persistent identifier
<https://docs.softwareheritage.org/devel/swh-model/persistent-identifiers.html#persistent-identifiers>`_.
However, WebGraph internally uses integers to refer to node ids.

Mapping between the strings and longs ids is needed before compressing the
graph. From the `Sux4J <http://sux.di.unimi.it/>`_ utility tool, we use the
`GOVMinimalPerfectHashFunction
<http://sux.di.unimi.it/docs/it/unimi/dsi/sux4j/mph/GOVMinimalPerfectHashFunction.html>`_
class, mapping with no collisions N keys to N consecutive integers.

The step produces a ``.mph`` file (MPH stands for *Minimal Perfect
Hash-function*) storing the hash function taking as input a string and returning
a unique integer.

2. BV compress
--------------

This is the first actual compression step, building a compressed version of the
input graph using WebGraph techniques presented in the framework paper. We use
the `ScatteredArcsASCIIGraph
<http://webgraph.di.unimi.it/docs-big/it/unimi/dsi/big/webgraph/ScatteredArcsASCIIGraph.html>`_
class, from WebGraph.

The resulting BV graph is stored as a set of files:

- ``.graph``: the compressed graph in the BV format
- ``.offsets``: offsets values to read the bit stream graph file
- ``.obl``: offsets cache to load the graph faster
- ``.properties``: entries used to correctly decode graph and offset files

3. BFS
-------

In the LLP paper, authors propose an empirical analysis linking node ordering
and high compression ratio: it is important to use an ordering of nodes ids such
that vertices from the same host are close to one another.

Building on this insight, the previous compression results in the BV compress
step are improved by re-ordering nodes ids using a BFS traversal order. We use
the `BFS
<http://law.di.unimi.it/software/law-docs/it/unimi/dsi/law/big/graph/BFS.html>`_
class from the `LAW <http://law.di.unimi.it/>`_ library.

The resulting ordering is stored in the ``.order`` file, listing nodes ids in
order of traversal.

4. Permute
----------

Once the order is computed (BFS or another ordering technique), the final
compressed graph is created based on the initial BV compress result, and using
the new node order mapping. The permutation uses the `Transform
<http://webgraph.di.unimi.it/docs-big/it/unimi/dsi/big/webgraph/Transform.html>`_
class from WebGraph framework.

The final compressed graph is only stored in the resulting ``.graph``,
``.offsets``, ``.obl``, and ``.properties`` files.

Extra steps
-----------

5. Stats
~~~~~~~~

Compute various statistics on the final compressed graph:

- ``.stats``: entries such as number of nodes, edges, avg/min/max degree,
  average locality, etc.
- ``.indegree``: graph indegree distribution
- ``.outdegree``: graph outdegree distribution

This step uses the `Stats
<http://webgraph.di.unimi.it/docs-big/it/unimi/dsi/big/webgraph/Stats.html>`_
class from WebGraph.

6. Transpose
~~~~~~~~~~~~

Create a transposed graph to allow backward traversal, using the `Transform
<http://webgraph.di.unimi.it/docs-big/it/unimi/dsi/big/webgraph/Transform.html>`_
class from WebGraph.
