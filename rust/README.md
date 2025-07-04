# Software Heritage - graph service

## Overview

`swh_graph` is a library and server allowing fast traversal queries through the
[Software Heritage archive](https://archive.softwareheritage.org/), by using an in-memory
compressed representation of [the Software Heritage DAG](https://docs.softwareheritage.org/devel/swh-model/data-model.html)
based on [webgraph](https://docs.rs/webgraph).

The first version of this library and server was written in Java; and this documentation
describes the Rust rewrite, which implements the same concepts and algorithms, but
streamlined and faster.

Every node is identified by an integer from `0` to `n-1` where `n` is the total
number of nodes in a graph

The provided API is:

* given a [SWHID v1.1](https://www.swhid.org/specification/v1.1/) return a node id,
  and vice versa
* likewise for origins, encoded as `swh:1:ori:$hash`, where `$hash` is the SHA1 hash of the origin's URL
* given a node id, list all its successors, such as all the entries in a directory, or all
  parents of a revision/commit (note that a "parent revision" is a successor here when
  seen as a directed arc in the graph). Some arcs may have labels, such as the name of
  a directory entry or a branch.
* given a node id, list all its predecessors, such as all the directories containing a given
  directory entries in a directory, or all children of revision/commit. Some arcs may have
  labels too.
* given a node id, return properties of this node, such as:
  * its type
  * a message or date if the node is a revision or a release, or
  * a length if the node is a content

See the [Tutorial](crate::_tutorial), or the [Crash Course](crate::_crash_course) if you
are already familiar with the Java API.

Additionally, we provide the following tools based on these building blocks:

* A "standard library" of functionalities and collections, in [the swh-graph-stdlib crate](https://docs.rs/swh-graph-stdlib/)
* An implementation of a handful of recursive traversal queries
  as a [gRPC API](https://docs.softwareheritage.org/devel/swh-graph/grpc-api.html)
* An in-memory [small graph builder for tests](crate::graph_builder::GraphBuilder)
* Wrappers for [`SwhGraph`](crate::graph::SwhGraph) that filter or change the nodes
  and arcs it returns.

## Building a distribution

### Dependencies
The code needs stable Rust to be >= 1.79 because we depend webgraph uses
[associated type bounds](https://github.com/rust-lang/rust/issues/52662).

We also need `protoc` to be installed (`apt-get install protobuf-compiler` on Debian)
if you want to build the gRPC server (`swh-graph-grpc-server` crate)

### Distribution
To distribute executables, you can statically compile the code with:
```text
RUSTFLAGS="-C target-cpu=x86-64-v3" cargo build --release --target x86_64-unknown-linux-musl
```
To add the target use:
```text
rustup target add x86_64-unknown-linux-musl
```
The target-cpu will limit the compatible cpus but will enable more optimizations.
Some Interesting architecture are:
- `native` for the compiling CPU architecture, this is the best option for
  performance when you are compiling and running on the same machine.
- `x86-64-v3` for Intel Haswell and newer (2013), oldest architecture that
  supports AVX2 and BMI2 instructions.
- `x86-64-v2` for Intel Core 2 and newer (2006), oldest reasonable architecture
  to compile for.

### Performance consideration
At every random access, we need to query an Elias-Fano data structure to find the bit-offset at
which the codes of the given nodes start. This operation has to find the i-th
one in a given word of memory.

The [implementation in `sux-rs`](https://github.com/vigna/sux-rs/blob/25fbdf42024b6cbe98741bd0d8135f3188293677/src/utils.rs#L26)
can exploit the [pdep instruction](https://www.felixcloutier.com/x86/pdep) to speed up the operation.
So it's important to compile the code with the `pdep` feature enabled, and generally
targeting the intended CPU architecture by using the `RUSTFLAGS` environment variable as:
```text
RUSTFLAGS="-C target-cpu=native" cargo run --release --bin bfs
```
this compiles targeting the compiling CPU architecture.

For this reason, this is enabled by default in the `.cargo/config.toml` file.
Be aware that a file compiled with `pdep` enabled will not run on a CPU that does not support it.
Generally, **running a binary compiled with a given set of CPU features on another one
will result in SIGILL (illegal instruction) error**.

## Minimal setup for tests

Here is the minimal setup you will need to be able to execute python tests in this package.
This is not the recommended build process for production-like deployments!

1. install binary dependencies

```text
swh-graph$ sudo apt install protobuf-compiler
swh-graph$ curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```
The Rust compiler is installed from upstream because Debian stable (bookworm) only provides rust 1.63.


2. compile the binary assets:

```text
swh-graph$ cargo build --all-features
```

This should build binary assets in the local directory `target/debug`. Check
the `swh-graph-index` is present there and can be executed properly:

```text
swh-graph$ ./target/debug/swh-graph-index --help
Commands to (re)generate `.ef` and `.offsets` files, allowing random access to BVGraph

Usage: swh-graph-index <COMMAND>

Commands:
  offsets    Reads a graph file linearly and produce a .offsets file which can be used by the Java backend to randomly access the graph
  ef         Reads either a graph file linearly or .offsets file (generated and used by the Java backend to randomly access the graph), and produces a .ef file suitable to randomly access the graph from the Rust backend
  labels-ef  Reads either a graph file linearly or .offsets file (generated and used by the Java backend to randomly access the graph), and produces a .ef file suitable to randomly access the graph from the Rust backend
  dcf        Reads either a graph file linearly, and produces a degree-cumulative function encoded as an Elias-Fano sequence in a .dcf file, suitable to distribute load while working on the graph
  help       Print this message or the help of the given subcommand(s)

Options:
  -h, --help  Print help
```

## Deployment

### Downloading a compressed graph

See the [Software Heritage Dataset documentation](https://docs.softwareheritage.org/devel/swh-export/graph/dataset.html),
which lists all the exports from the Software Heritage archive; most of them provide
a compressed graph representation that can be loaded with `swh_graph` (though some may
need some conversion, see below).

### Sharing mapped data across processes

Most of the data files are mmapped. You should [pre-load some of them in memory
](https://docs.softwareheritage.org/devel/swh-graph/memory.html#sharing-mapped-data-across-processes)
to ensure they are not evicted from the in-memory cache.

### Loading old graphs

The original Java (and C++) implementation of WebGraph used slightly different
data structures and formats than the Rust implementation.
Therefore, you need to generate new files in order to load old graphs with the Rust
implementation; this takes a few hours for graphs representing full SWH exports.

The `swh graph reindex` command (made available with `cargo install swh-graph && pip3 install swh.graph`)
takes care of running the conversion.

Additionally, the `.ef` format may change from time to time. If you get an error
like this:

```text
Error: Cannot map Elias-Fano pointer list ../swh/graph/example_dataset/compressed/example.ef

Caused by:
    Wrong type hash. Expected: 0x47e8ca1ab8fa94f1 Actual: 0x890ce77a9258940c.
    You are trying to deserialize a file with the wrong type.
    The serialized type is 'sux::dict::elias_fano::EliasFano<sux::rank_sel::select_fixed2::SelectFixed2<sux::bits::bit_vec::CountBitVec, alloc::vec::Vec<u64>, 8>>' and the deserialized type is 'sux::dict::elias_fano::EliasFano<sux::rank_sel::select_adapt_const::SelectAdaptConst<sux::bits::bit_vec::BitVec<alloc::boxed::Box<[usize]>>, alloc::boxed::Box<[usize]>, 12, 4>>'.
```

you need run `swh graph reindex --ef` to re-create them at the current version on your system.
