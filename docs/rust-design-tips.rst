.. _graph-rust-design-tips:

=========================================
Design tips for Rust code using swh-graph
=========================================

This page documents various designs that have proven to work well with swh-graph in terms of both maintainability and performance.
They should also generalize to similar workloads, ie. when batch-processing immutable data on a single machine and negligible network access.

Use `flamegraph <https://github.com/flamegraph-rs/flamegraph>`_ and `perf top <https://www.brendangregg.com/perf.html>`_ to find bottlenecks.

Parallelism
===========

Use `rayon <https://docs.rs/rayon>`_. It provides an Iterator-like API but parallelizes everything automatically.
Skim the documentation of `ParallelIterator <https://docs.rs/rayon/latest/rayon/iter/trait.ParallelIterator.html>`_
and `IndexedParallelIterator <https://docs.rs/rayon/latest/rayon/iter/trait.IndexedParallelIterator.html>`_
to have an idea of the combinators available to you.
Resist the temptation to use ``IndexedParallelIterator::chunks`` to split work across threads, rayon probably does
a better job of chunking (and work-stealing) internally.
(Skimming `Iterator <https://doc.rust-lang.org/std/iter/trait.Iterator.html>`_'s methods is also a good idea. Many are neat!)

``rayon`` performs best when its output has a known length, because it can divide work evenly in large chunks.
This means that sometimes, it is better to read the whole input in a large ``Vec`` and give it to ``rayon``,
than ``.par_bridge()`` on a stream/iterator.

You probably do not need manual parallelism with `std::thread <https://doc.rust-lang.org/std/thread/>`_.
It is less readable than Rayon (lots of boilerplate), requires lock or unsafe code for combinators Rayon already provides,
and does not provide `work-stealing <https://en.wikipedia.org/wiki/Work_stealing>`_ to balance work across threads.

Do not use locks, you probably don't need them in batch processing.
`DashMap <https://docs.rs/dashmap/latest/dashmap/struct.DashMap.html>`_/`DashSet <https://docs.rs/dashmap/latest/dashmap/struct.DashSet.html>`_
and vectors of `atomic numbers <https://doc.rust-lang.org/std/sync/atomic/>`_
should fit all your needs for concurrent data structures, and `dataset-writer <https://docs.rs/dataset-writer>` for writing output
(it writes to multiple files to avoid locking).

If you still need locks, consider splitting your resources/state across threads using the `thread_local crate <https://docs.rs/thread_local/>`_,
and freeing/collecting them after your main code is done running.

Logging
=======

Use `dsi-progress-logger <https://docs.rs/dsi-progress-logger/>` for progress logging.
``dsi_progress_logger::ConcurrentProgressLog`` works very nicely with Rayon's ``*_with`` methods.

System configuration
====================

Avoid network filesystems at all cost for ``mmap``-ed arrays. Avoid compressed filesystems too, if possible.
We do a lot of tiny random reads of ``mmap``-ed arrays, and this causes a high latency.

If you see ``malloc`` or ``free`` in ``perf top``, it means your memory allocator is a bottleneck.
The default malloc on GNU/Linux (GNU malloc) has high overhead in multithreaded code with many allocations.
Replace it with `jemalloc <https://jemalloc.net/>`_ or `mimalloc <https://microsoft.github.io/mimalloc/>`__,
whichever is faster on your use-case. jemalloc is slow to compile, so default to mimalloc.
You may use either the Rust crates to set a global allocator or ``LD_PRELOAD``. Both seem to work equally.

Avoid heap memory allocation in tight loops at all cost. Avoid small persistent structures on the heap.
This means ``Vec``/``String``, ``HashSet``/``HashMap``, etc.

Associative data structures
===========================

Instead of a large ``HashMap<NodeId, T>``, use ``Vec<T>`` or ``Vec<Option<T>>`` if you know that most
of your nodes are going to have a value.

Instead of ``HashSet<NodeId>``, use ``AdaptiveNodeSet``, which starts as a wrapper for ``HashSet<NodeId>``
but turns it into a `BitVec <https://docs.rs/sux/latest/sux/bits/>`_ when it becomes more CPU- and memory- efficient
than ``HashSet<NodeId>``

Using a temporary ``Vec`` in a loop seems to be fine, but ``HashSet``/``HashMap`` are not.
Numerous small and persistent ``Vec``/``String``, ``HashSet``/``HashMap`` structures perform badly.

If you need many ``Vec``/``String``, for example ``Vec<Vec<T>>`` or ``Vec<String>`` to associate
a String to each NodeId, consider using a single ``Vec<T>`` or ``Vec<String>`` where you concatenate
all values, along with a ``Vec<usize>`` that stores the cutoff points, or insert "sentinel" values
inside to mark them.
`PathStack <https://docs.rs/swh-graph-stdlib/latest/swh_graph_stdlib/collections/struct.PathStack.html>`_
is an example of such a structure.

If you need to store a large set of integers, use `sux::dict::elias_fano <https://docs.rs/sux/latest/sux/dict/elias_fano/>`_.
If you need to store a large map from a range of integers to an increasing list of integers (eg. the cutoff points mentioned before),
use it too.

If you need to store a map from a range of integers to sets of integers, use `webgraph::graphs::bvgraph <https://docs.rs/webgraph/latest/webgraph/graphs/bvgraph>`_.
Despite its name, it's not just for graphs.
Check out `ParSortPairs <https://docs.rs/webgraph/latest/webgraph/utils/par_sort_pairs/>`_ which you will probably need to generate
a BVGraph.

If you need to store a map from integers to anything and the above do not fit the bill,
use `partial_array <https://docs.rs/sux/latest/sux/array/partial_array/>`_.

If you need to store a map from non-integers (eg. strings) to integers, use `VFunc <https://docs.rs/sux/latest/sux/func/struct.VFunc.html>`_.

You can serialize and deserialize all of those with `epserde <https://docs.rs/epserde/>`_

Other data structures
=====================

Prefer column memory layout, ie. ``struct MyStorage { field1: Vec<T>, field2: Vec<U> }`` instead of
``Vec<struct MyRow { field1: T, field2: U }``.
If you need to operate with other languages, use `Apache Arrow <https://arrow.apache.org/>` which
provides a dynamic API to work with them (at the expense of much boilerplate code).
If you use ORC or Parquet, you probably already use Arrow.
Note that linking with arrow-rs can be slow when LTO is enabled.

If you have enough memory, build your structure in memory and serialize it all at once with `epserde <https://docs.rs/epserde/>`_.
Readers can then ``mmap`` it so they don't need any syscall (slow!) to read it.

Otherwise, use `Parquet <https://docs.rs/parquet/>`_.

If you need to read an ORC or Parquet file row-by-row, then the `ar_row <https://docs.rs/ar_row>`_ crate can be handy.
If you need high-performance single-reads in a Parquet table, `parquet_aramid <https://docs.rs/parquet_aramid>`_ may be a good idea,
but is tough to use.
