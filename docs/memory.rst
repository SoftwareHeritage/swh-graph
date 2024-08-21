.. _swh-graph-memory:

Memory & Performance tuning
===========================

This page discusses various considerations related to memory usage and
performance tuning when using the ``swh-graph`` library to load large
compressed graphs.

Temporary directory
-------------------

Many of the graph algorithms (either for compression or traversal) tend to
offload some of their run-time memory to disk. For instance, the `BFS
<https://law.di.unimi.it/software/law-docs/it/unimi/dsi/law/big/graph/BFS.html>`_
algorithm in the LAW library uses a temporary directory to write its queue of
nodes to visit.

Because these can be quite large and sometimes overflow the default ``/tmp``
partition, it is advised to systematically specify a path to a local temporary
directory with enough space to accommodate the needs of Python or Rust programs,
using this environment variable::

    TMPDIR=/srv/softwareheritage/ssd/tmp



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
