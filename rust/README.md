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

`swh_graph` also provides an implementation of a handful of recursive traversal queries
on top of these building blocks, as a [gRPC API](https://docs.softwareheritage.org/devel/swh-graph/grpc-api.html)

## Getting started

The central classes of this crate are
[`SwhUnidirectionalGraph`](graph::SwhUnidirectionalGraph) and
[`SwhBidirectionalGraph`](graph::SwhBidirectionalGraph).
They both provide the API above; the difference being that the former does not provide
predecessor access.

In order to avoid loading files unnecessarily, they are instantiated as "naked" graphs
with [`load_unidirectional`](graph::load_unidirectional)
and [`load_bidirectional`](graph::load_bidirectional) which only provide
successor and successor access; and extra data must be loaded with the `load_*` methods.

For example:

```compile_fail
# use std::path::PathBuf;
use swh_graph::java_compat::mph::gov::GOVMPH;

let graph = swh_graph::graph::load_unidirectional(PathBuf::from("./graph"))
    .expect("Could not load graph");
let node_id: usize = graph.node_id("swh:1:snp:486b338078a42de5ece0970638c7270d9c39685f");
```

fails to compile because `node_id` uses one of these properties that are not loaded
by default, so it must be loaded:

```no_run
# use std::path::PathBuf;
# use swh_graph::graph::SwhGraphWithProperties;
use swh_graph::java_compat::mph::gov::GOVMPH;

let graph = swh_graph::graph::load_unidirectional(PathBuf::from("./graph"))
    .expect("Could not load graph")
    .init_properties()
    .load_properties(|properties| properties.load_maps::<GOVMPH>())
    .expect("Could not load SWHID<->node id maps");

let node_id: usize = graph.properties()
    .node_id("swh:1:dir:5e1c24e586ef92dbef0e9cec6b354c6831454340")
    .expect("Unknown SWHID");
```

or alternatively, to load all possible properties at once:

```no_run
# use std::path::PathBuf;
# use swh_graph::graph::SwhGraphWithProperties;
use swh_graph::java_compat::mph::gov::GOVMPH;

let graph = swh_graph::graph::load_unidirectional(PathBuf::from("./graph"))
    .expect("Could not load graph")
    .load_all_properties::<GOVMPH>()
    .expect("Could not load properties");

let node_id: usize = graph.properties()
    .node_id("swh:1:dir:5e1c24e586ef92dbef0e9cec6b354c6831454340")
    .expect("Unknown SWHID");
```

Once you have a node id to start from, you can start using the above API to traverse
the graph. For example, to list all successors of the given object (directory entries
as this example uses a directory):

```no_run
# use std::path::PathBuf;
# use swh_graph::graph::{SwhForwardGraph, SwhGraphWithProperties};
use swh_graph::java_compat::mph::gov::GOVMPH;

let graph = swh_graph::graph::load_unidirectional(PathBuf::from("./graph"))
    .expect("Could not load graph")
    .load_all_properties::<GOVMPH>()
    .expect("Could not load properties");

let node_id: usize = graph.properties()
    .node_id("swh:1:dir:5e1c24e586ef92dbef0e9cec6b354c6831454340")
    .expect("Unknown SWHID");

for succ in graph.successors(node_id) {
    println!("{} -> {}", node_id, succ);
}
```

Or, if you have labelled graphs available locally, you can load them and also print
labels on the arcs from the given object to its successors (file permissions and names
as this example uses a directory):

```no_run
# use std::path::PathBuf;
# use swh_graph::graph::{SwhForwardGraph, SwhLabelledForwardGraph, SwhGraphWithProperties};
use swh_graph::java_compat::mph::gov::GOVMPH;

let graph = swh_graph::graph::load_unidirectional(PathBuf::from("./graph"))
    .expect("Could not load graph")
    .load_all_properties::<GOVMPH>()
    .expect("Could not load properties")
    .load_labels()
    .expect("Could not load labels");

let node_id: usize = graph.properties()
    .node_id("swh:1:dir:5e1c24e586ef92dbef0e9cec6b354c6831454340")
    .expect("Unknown SWHID");

for (succ, labels) in graph.labelled_successors(node_id) {
    for label in labels {
        println!(
            "{} -> {} (permission: {:?}; name: {})",
            node_id,
            succ,
            label.permission(),
            String::from_utf8(
                graph.properties().label_name(label.filename_id()).unwrap()
            ).expect("Could not decode file name as UTF-8")
        );
    }
}
```

## Building an distribution

### Rust version
The code needs stable rust to be >= 1.65 because we require the [GAT feature](https://blog.rust-lang.org/2022/10/28/gats-stabilization.html`).

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

## Deployment

### Downloading a compressed graph

See the [Software Heritage Dataset documentation](https://docs.softwareheritage.org/devel/swh-dataset/graph/dataset.html),
which lists all the exports from the Software Heritage archive; most of them provide
a compressed graph representation that can be loaded with `swh_graph` (though some may
need some conversion, see below).

### Sharing mapped data across processes

Most of the data files are mmapped. You should [pre-load some of them in memory
](https://docs.softwareheritage.org/devel/swh-graph/memory.html#sharing-mapped-data-across-processes)
to ensure they are not evicted from the in-memory cache.

### Loading old graphs

The original Java (and C++) implementation of webgraph used slightly different
data structures in the Rust implementation.
Therefore, you need to generate new files in order to load old graphs with the Rust
implementation; this takes a few hours for graphs representing full SWH exports.

```text
export SOURCE_DIR=~/src/swh-graph
export GRAPH_DIR=~/graph/latest/compressed

# Convert the GOV minimal-perfect-hash function from `.mph` to `.cmph`
java -classpath $SOURCE_DIR/swh/graph/swh-graph.jar $SOURCE_DIR/java/src/main/java/org/softwareheritage/graph/utils/Mph2Cmph.java graph.mph graph.cmph`

# Generate Elias-Fano-encoded offsets of the graph:
cargo run --release --bin build_eliasfano -- $GRAPH_DIR/compressed/graph
cargo run --release --bin compress build-eliasfano -- $GRAPH_DIR/graph-transposed

# Generate Elias-Fano-encoded offsets of the labelled graph:
cargo run --release --bin compress build-labels-eliasfano --  $GRAPH_DIR/graph-labelled $((1+ $(cat /$GRAPH_DIR/graph.nodes.count.txt)))
cargo run --release --bin compress build-labels-eliasfano --  $GRAPH_DIR/graph-transposed-labelled $((1+ $(cat /$GRAPH_DIR/graph.nodes.count.txt)))

# Generate `node2type.bin` (as `node2type.map` is specific to Java):
cargo run --release --bin node2type --  $GRAPH_DIR/graph

# Convert the Java-specific `.property.content.is_skipped.bin` to a plain `.property.content.is_skipped.bits`:
java -classpath $SOURCE_DIR/swh/graph/swh-graph.jar $SOURCE_DIR/java/src/main/java/org/softwareheritage/graph/utils/Bitvec2Bits.java $GRAPH_DIR/graph.property.content.is_skipped.bits $GRAPH_DIR/graph.property.content.is_skipped.bits
```
