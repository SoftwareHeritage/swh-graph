git2graph
=========

`git2graph` crawls a Git repository and outputs it as a graph, i.e., as a pair
of textual files <nodes, edges>. The nodes file will contain a list of graph
nodes as [Software Heritage](https://www.softwareheritage.org/) (SWH)
[Persistent Identifiers (SWHIDs)](https://docs.softwareheritage.org/devel/swh-model/persistent-identifiers.html);
the edges file a list of graph edges as <from, to> SWHID pairs.


Dependencies
------------

Build time dependencies:

- [libgit2](https://libgit2.org/)

Test dependencies:

- [bats](https://github.com/bats-core/bats-core)


Micro benchmark
---------------

    $ time ./git2graph -n >(zstdmt > nodes.csv.zst) -e >(zstdmt -c > edges.csv.zst) /srv/src/linux
    160,38s user 12,72s system 98% cpu 2:55,02 total

    $ zstdcat nodes.csv.zst | wc -l
    6503403
    $ zstdcat edges.csv.zst | wc -l
    305096029


Parallel use
------------

`git2graph` writes fixed-length lines, long either 51 bytes (nodes) or 102
bytes (edges). When writing to a FIFO less than `PIPE_BUF` bytes (which is 4096
bytes on Linux, and guaranteed to be at least 512 bytes by POSIX), writes are
atomic. Hence it is possible to mass analyze many repositories in parallel with
something like:

    $ mkfifo nodes.fifo edges.fifo
    $ sort -u < nodes.fifo | zstdmt > nodes.csv.zst &
    $ sort -u < edges.fifo | zstdmt > edges.csv.zst &
    $ parallel git2graph -n nodes.fifo -e edges.fifo -- repo_dir_1 repo_dir_2 ...
    $ rm nodes.fifo edges.fifo

Note that you most likely want to tune `sort` in order to be parallel
(`--parallel`), use a large buffer size (`-S`), and use a temporary directory
with enough available space (`-T`).  (The above example uses `parallel`
from [moreutils](https://joeyh.name/code/moreutils/), but it could trivially be
adapted to use [GNU parallel](https://www.gnu.org/software/parallel/) or
similar parallelization tools.)


Limitations
-----------

Snapshot SWHID calculation is not fully compatible with the
[spec](https://docs.softwareheritage.org/devel/apidoc/swh.model.html#swh.model.identifiers.snapshot_identifier),
because currently only HEAD is considered as a symbolic reference. Other
symbolic refs, if present, will be ignored, potentially leading to a different
snapshot SWHID than what Software Heritage will obtain. This is due to a
limitation of libgit2, that at the time of writing doesn't allow to list all
symbolic references.

The graph structure is not affected, but looking up obtained snapshots by SWHID
on the main Software Heritage archive might fail.
