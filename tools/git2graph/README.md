git2graph
=========

`git2graph` crawls a Git repository and outputs it as a graph, i.e., as a pair
of textual files <nodes, edges>. The nodes file will contain a list of graph
nodes as Software Heritage (SWH) Persistent Identifiers (PIDs); the edges file
a list of graph edges as <from, to> PID pairs.


Dependencies
------------

Build time dependencies:

- [libgit2](https://libgit2.org/)

Test dependencies:

- [bats](https://github.com/bats-core/bats-core)


Micro benchmark
---------------

    $ time ./git2graph -n >(pigz -c > nodes.csv.gz) -e >(pigz -c > edges.csv.gz) /srv/src/linux
    ./git2graph /srv/src/linux >(pigz -c > nodes.csv.gz) >(pigz -c > edges.csv.gz  243,30s user 17,28s system 89% cpu 4:51,53 total
    
    $ zcat nodes.csv.gz | wc -l
    6503402
    
    $ zcat edges.csv.gz | wc -l
    305095437


Parallel use
------------

`git2graph` writes fixed-length lines, long either 51 bytes (nodes) or 102
bytes (edges). When writing to a FIFO less than `PIPE_BUF` bytes (which is 4096
bytes on Linux, and guaranteed to be at least 512 bytes by POSIX), writes are
atomic. Hence it is possible to mass analyze many repositories in parallel with
something like:

    $ mkfifo nodes.fifo edges.fifo
    $ sort -u < nodes.fifo | pigz -c > nodes.csv.gz &
    $ sort -u < edges.fifo | pigz -c > edges.csv.gz &
    $ parallel git2graph -n nodes.fifo -e edges.fifo -- repo_dir_1 repo_dir_2 ...
    $ rm nodes.fifo edges.fifo

Note that you most likely want to tune `sort` in order to be parallel
(`--parallel`), use a large buffer size (`-S`), and use a temporary directory
with enough available space (`-T`).  (The above example uses `parallel`
from [moreutils](https://joeyh.name/code/moreutils/), but it could trivially be
adapted to use [GNU parallel](https://www.gnu.org/software/parallel/) or
similar parallelization tools.)
