.. _swh-graph-memory:

Memory & Performance tuning
===========================

This page discusses various considerations related to memory usage and
performance tuning when using the ``swh-graph`` library to load large
compressed graphs.

JVM options
-----------

In production, we tend to use very large servers which have enough RAM to load
the entire graph in RAM. In these setups, the default JVM options are often
suboptimal. We recommend to start the JVM with the following options, which
tend to significantly improve performance::

    java \
        -ea \
        -server \
        -XX:PretenureSizeThreshold=512M \
        -XX:MaxNewSize=4G \
        -XX:+UseLargePages \
        -XX:+UseTransparentHugePages \
        -XX:+UseNUMA \
        -XX:+UseTLAB \
        -XX:+ResizeTLAB \

These options are documented in the manual of ``java(1)`` the Oracle
documentation.


Temporary directory
-------------------

Many of the graph algorithms (either for compression or traversal) tend to
offload some of their run-time memory to disk. For instance, the `BFS
<https://law.di.unimi.it/software/law-docs/it/unimi/dsi/law/big/graph/BFS.html>`_
algorithm in the LAW library uses a temporary directory to write its queue of
nodes to visit.

Because these can be quite large and sometimes overflow the default ``/tmp``
partition, it is advised to systematically specify a path to a local temporary
directory with enough space to accommodate the needs of the Java programs. This
can be done using the ``-Djava.io.tmpdir`` parameter on the Java CLI::

    java -Djava.io.tmpdir=/srv/softwareheritage/ssd/tmp


Memory mapping vs Direct loading
--------------------------------

The main dial you can use to manage your memory usage is to chose between
memory-mapping and direct-loading the graph data. The different loading modes
available when loading the graph are documented in :ref:`swh-graph-java-api`.

Loading in mapped mode will not load any extra data in RAM, but will instead
use the ``mmap(1)`` syscall to put the graph file located on disk in the
virtual address space. The Linux kernel will then be free to arbitrarily cache
the file, either partially or in its entirety, depending on the available
memory space.

In our experiments, memory-mapping a small graph from a SSD only incurs a
relatively small slowdown (about 15-20%). However, when the graph is too big to
fit in RAM, the kernel has to constantly invalidate pages to cache newly
accessed sections, which incurs a very large performance penalty. A full
traversal of a large graph that usually takes about 20 hours when loaded in
main memory could take more than a year when mapped from a hard drive!

When deciding what to direct-load and what to memory-map, here are a few rules
of thumb:

- If you don't need random access to the graph edges, you can consider using
  the "offline" loading mode. The offsets won't be loaded which will save
  dozens of gigabytes of RAM.

- If you only need to query some specific nodes or run trivial traversals,
  memory-mapping the graph from a HDD should be a reasonable solution that
  doesn't take an inordinate amount of time. It might be bad for your disks,
  though.

- If you are constrained in available RAM, memory-mapping the graph from an SSD
  offers reasonable performance for reasonably complex algorithms.

- If you have a heavy workload (i.e. running a full traversal of the entire
  graph) and you can afford the RAM, direct loading will be orders of magnitude
  faster than all the above options.


Sharing mapped data across processes
------------------------------------

Often, multiple processes can be working on the same data (mappings or the
graph itself), for instance when running different experiments on the same
graph. This is problematic in terms of RAM usage when using direct memory
loading, as the same data of potentially hundreds of gigabytes is loaded in
memory twice.
As we have seen, memory-mapping can be used to avoid storing redundant data in
RAM, but comes at the cost of potentially slower I/O as the data is no longer
guaranteed to be loaded in main memory and is reliant on kernel heuristics.

To efficiently share data across two different compressed graph processes,
another option is to copy graph data to a ``tmpfs`` not backed by a disk swap,
which forces the kernel to load it entirely in RAM. Subsequent memory-mappings
of the files stored in the tmpfs will simply map the data stored in RAM to
virtual memory pages, and return a pointer to the in-memory structure.

To do so, we create a directory in ``/dev/shm`` in which we **copy** all the
files that we want to direct-load in RAM, and **symlink** all the others. Then,
we load the graph using the memory-mapped loading mode, which makes it use the
shared memory stored in the tmpfs under the hood.

Here is a systemd service that can be used to perform this task automatically:

.. code-block:: ini

    [Unit]
    Description=swh-graph memory sharing in tmpfs

    [Service]
    Type=oneshot
    RemainAfterExit=yes
    ExecStart=mkdir -p /dev/shm/swh-graph/default
    ExecStart=sh -c "ln -s /.../compressed/* /dev/shm/swh-graph/default"
    ExecStart=cp --remove-destination /.../compressed/graph.graph /dev/shm/swh-graph/default
    ExecStart=cp --remove-destination /.../compressed/graph-transposed.graph /dev/shm/swh-graph/default
    ExecStop=rm -rf /dev/shm/swh-graph/default

    [Install]
    WantedBy=multi-user.target
