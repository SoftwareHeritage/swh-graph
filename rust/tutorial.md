# Documentation: Tutorial

If you are already familiar with the
[Java API](https://docs.softwareheritage.org/devel/swh-graph/java-api.html),
you may skip this guide and read the (shorter) [crash course](crate::_crash_course) instead.

## Generic graph algorithms

The central classes of this crate are
[`SwhUnidirectionalGraph`](crate::graph::SwhUnidirectionalGraph) and
[`SwhBidirectionalGraph`](crate::graph::SwhBidirectionalGraph).
They both allow getting successors of a node ([`SwhForwardGraph`](crate::graph::SwhForwardGraph)
trait); while only the latter allows getting its predecessors
([`SwhBackwardGraph`](crate::graph::SwhBackwardGraph) trait).

You instantiate a graph from the path to the prefix used by a graph on your filesystem.
This graph may be either `swh/graph/example_dataset/compressed/graph` from swh-graph's
source code repository, or a [compressed graph downloaded from Amazon S3](https://docs.softwareheritage.org/devel/swh-export/graph/dataset.html).

```no_run
use std::path::PathBuf;

let graph = swh_graph::graph::load_full::<swh_graph::mph::DynMphf>(PathBuf::from("./graph"))
    .expect("Could not load graph");
```

### Example: Average outdegree

The following code can be used to compute the average
outdegree of a graph, which is a useful measure of its density:

```
 use std::path::PathBuf;
 use swh_graph::graph::*;

let basename = PathBuf::from("../swh/graph/example_dataset/compressed/example");

let graph = swh_graph::graph::load_full::<swh_graph::mph::DynMphf>(basename)
    .expect("Could not load graph");

let average_outdegree = graph.num_arcs() as f64 / graph.num_nodes() as f64;
assert_eq!(average_outdegree, 28. / 24.);
```


### Example: Degree distributions

Using the `outdegree()` primitive, we can compute the outdegree distribution
of the graph by iterating on all its nodes. The distribution will be returned
as a map that associates to each degree *d* the number of nodes with outdegree
*d*.

```
use std::collections::HashMap;
use std::path::PathBuf;

use swh_graph::graph::*;

# let basename = PathBuf::from("../swh/graph/example_dataset/compressed/example");

let graph = swh_graph::graph::load_full::<swh_graph::mph::DynMphf>(basename)
    .expect("Could not load graph");

let mut distribution = HashMap::<usize, u64>::new();
for node in 0..graph.num_nodes() {
    *distribution.entry(graph.outdegree(node)).or_insert(0) += 1;
}

assert_eq!(distribution, HashMap::from([(0, 7), (1, 8), (2, 7), (3, 2)]));
```

### Example: Degree distributions (in parallel)

Or, in parallel using the [rayon](https://docs.rs/rayon/) library:

```
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use rayon::prelude::*;

use std::path::PathBuf;
use swh_graph::graph::*;

# let basename = PathBuf::from("../swh/graph/example_dataset/compressed/example");

let graph = swh_graph::graph::load_full::<swh_graph::mph::DynMphf>(basename)
    .expect("Could not load graph");

let distribution: HashMap<usize, u64> = (0..graph.num_nodes())
    .into_par_iter()
    .fold(
        // In each thread, start with an empty distribution
        || HashMap::new(),
        // In each thread, compute the distribution for the thread's subset of nodes
        |mut thread_distribution, node: usize| {
            *thread_distribution
                .entry(graph.outdegree(node))
                .or_insert(0) += 1;
            thread_distribution
        },
    )
    // Merge distributions computed by each thread
    .reduce(
        // identity
        || HashMap::new(),
        |mut distribution1, distribution2| {
            for (outdegree, count) in distribution2 {
                *distribution1.entry(outdegree).or_insert(0) += count
            }
            distribution1
        }
    );

assert_eq!(distribution, HashMap::from([(0, 7), (1, 8), (2, 7), (3, 2)]));
```


### Example: Depth-First Traversal

The `successors` primitive can be used to write a simple stack-based DFS
traversal on the graph which starts from a given node and prints all the
descendant nodes in its transitive closure:

```
use std::collections::HashSet;
use std::path::PathBuf;

use swh_graph::graph::*;

fn visit_nodes_dfs<G: SwhForwardGraph>(graph: G, root: NodeId) {
    let mut stack = Vec::new();
    let mut visited = HashSet::new();

    stack.push(root);
    visited.insert(root);

    while let Some(node) = stack.pop() {
        println!("{}", node);

        for succ in graph.successors(node) {
            if !visited.contains(&succ) {
                stack.push(succ);
                visited.insert(succ);
            }
        }
    }
}
```


### Example: Breadth-First Traversal

Swapping the stack for a queue changes the traversal order from depth-first
to breadth-first:

```
use std::collections::{HashSet, VecDeque};
use std::path::PathBuf;

use swh_graph::graph::*;

fn visit_nodes_bfs<G: SwhForwardGraph>(graph: G, root: NodeId) {
    let mut queue = VecDeque::new();
    let mut visited = HashSet::new();

    queue.push_back(root);
    visited.insert(root);

    while let Some(node) = queue.pop_front() {
        println!("{}", node);

        for succ in graph.successors(node) {
            if !visited.contains(&succ) {
                queue.push_back(succ);
                visited.insert(succ);
            }
        }
    }
}
```

## Node types and SWHIDs

In the Software Heritage archive, nodes are not represented by a simple
integer, but by a [SWHID](https://docs.softwareheritage.org/devel/swh-model/persistent-identifiers.html), which contain both the
*type* of the node (revision, directory, blob...) and its unique identifier. We
use **node mappings** which allow us to translate between SWHIDs and the
compact node IDs used in the compressed graph.

Most notably, we use a MPH (Minimal Perfect Hash) function,
a [Rust rewrite](crate::mph::DynMphf) of the one implemented in the
[GOVMinimalPerfectHashFunction](
http://sux.di.unimi.it/docs/it/unimi/dsi/sux4j/mph/GOVMinimalPerfectHashFunction.html)
class of the Sux4J library, which maps N keys to N consecutive integers with no
collisions.

The following files are used to store the mappings between the nodes and their
types:

- `graph.cmph`: contains a serialized minimal perfect hash function computed
  on the list of all the SWHIDs in the graph.
- `graph.order`: contains the permutation that associates with each output of
  the MPH the node ID to which it corresponds
- `graph.node2swhid.bin`: contains a compact binary representation of all the
  SWHIDs in the graph, ordered by their rank in the graph file.
- `graph.node2type.bin`: contains an array of three-bits integers, which each
  represents the type of a node.

This struct exposes (through `.properties()`) the following methods:

- [`swhid(NodeId) -> SWHID`](crate::properties::SwhGraphProperties::swhid):
  returns the SWHID associated with a given
  node ID.  This function does a lookup of the SWHID at offset *i* in the file
  `graph.node2swhid.bin`.

- [`node_id(SWHID) -> Result<NodeId, _>`](crate::properties::SwhGraphProperties::node_id):
  returns the node ID associated with a given
  SWHID. It works by hashing the SWHID with the function stored in
  `graph.mph`, then permuting it using the permutation stored in
  `graph.order`. It does additional domain-checking by calling `getSWHID()`
  on its own result to check that the input SWHID was valid.

- [`node_type(NodeId) -> NodeType`](crate::properties::SwhGraphProperties::node_type):
  returns the type of a given node, as
  an enum of all the different object types in the Software Heritage data
  model. It does so by looking up the value at offset *i* in the bit vector
  stored in `graph.node2type.bin`.

### Example: Find the target directory of a revision

As an example, we use the methods mentioned above to perform the
following task: "given a revision, return its target directory". To do so, we
first look up the node ID of the given revision in the compressed graph. We
iterate on the successors of that node, and return the SWHID of the first
destination node that has the "directory" type.


```
use anyhow::{anyhow, ensure, Result};
use swh_graph::graph::{SwhForwardGraph, SwhGraphWithProperties};
use swh_graph::properties;
use swh_graph::{NodeType, SWHID};

fn find_directory_of_revision<G>(graph: &G, rev_swhid: SWHID) -> Result<SWHID>
where
    G: SwhForwardGraph + SwhGraphWithProperties<Maps: properties::Maps>,
{
    let rev = graph.properties().node_id(rev_swhid)?;
    ensure!(graph.properties().node_type(rev) == NodeType::Revision);
    for succ in graph.successors(rev) {
        if graph.properties().node_type(succ) == NodeType::Directory {
            return Ok(graph.properties().swhid(succ));
        }
    }

    Err(anyhow!("Revision has no target directory"))
}
```

## Node properties

The Software Heritage Graph is a *property graph*, which means it has various
properties associated with its nodes and edges (e.g., commit timestamps,
authors, messages, ...). We compress these properties and store them in files
alongside the compressed graph. This allows you to write traversal algorithms
that depend on these properties.

By default, properties are not assumed to be present are not loaded when
the graph itself is loaded. If you want to use a property, you need to
explicitly load it first. As an example, this is how you load the "content
length" property to get the length of a given blob:

```no_run
use std::path::PathBuf;

use anyhow::{Result, ensure};

use swh_graph::graph::SwhGraphWithProperties;
use swh_graph::mph::DynMphf;
use swh_graph::properties;
use swh_graph::{NodeType, SWHID};

fn main() {
    let graph = swh_graph::graph::load_full::<swh_graph::mph::DynMphf>(PathBuf::from("./graph"))
        .unwrap();

    let swhid = SWHID::try_from("swh:1:cnt:94a9ed024d3859793618152ea559a168bbcbb5e2").unwrap();
    println!("Content length: {:?}", get_content_length(&graph, swhid).unwrap())
}

fn get_content_length<G>(graph: &G, cnt_swhid: SWHID) -> Result<Option<u64>>
where
    G: SwhGraphWithProperties<Maps: properties::Maps, Contents: properties::Contents>,
{
    let cnt= graph.properties().node_id(cnt_swhid)?;
    ensure!(graph.properties().node_type(cnt) == NodeType::Content);
    Ok(graph.properties().content_length(cnt)) // Note this may be None if unknown size
}
```

The documentation of the [`SwhGraphProperties`](crate::properties::SwhGraphProperties) class lists
all the different properties, their types, and the methods used to load them and to get
their value for a specific node.

A few things of note:

- A single loading call can load multiple properties at once; this is because
  they are stored in the same file to be more space efficient.

- Persons (authors, committers etc) are exported as a single pseudonymized
  integer ID, that uniquely represents a full name + email.

- Timestamps are stored as an `i64` (for the timestamp itself) and an
  `i16` (for the UTC offset).

## Edge labels

While looking up graph properties on the *nodes* of the graph is relatively
straightforward, doing so for labels on the *arcs* is comparatively more
difficult. These include the names and permissions of directory entries, as
well as the branch names of snapshots.

[`SwhUnidirectionalGraph`](crate::graph::SwhUnidirectionalGraph) and
[`SwhBidirectionalGraph`](crate::graph::SwhBidirectionalGraph) both conditionally implement the
[`SwhLabeledForwardGraph`](crate::graph::SwhLabeledForwardGraph) trait;
and the latter also implements ([`SwhLabeledBackwardGraph`](crate::graph::SwhLabeledBackwardGraph) trait),
which mirror the `successors` and `predecessors` methods we saw before, but augment
them to also look up the label of arcs while iterating on them.

To get them to implement these traits, you need to transform them with the
[`load_labels`](crate::graph::SwhUnidirectionalGraph::load_labels) method:

This labelled graph is stored in the following files:

- `graph-labelled.properties`: a property file describing the graph, notably
  containing the basename of the wrapped graph.
- `graph-labelled.labels`: the compressed labels
- `graph-labelled.labeloffsets`: the offsets used to access the labels in
  random order.

The `SwhUnidirectionalGraph` and `SwhBidirectionalGraph` structs contain a
[`load_labels`](crate::graph::SwhUnidirectionalGraph::load_labels) method. This method
consumes the un-labled struct, and returns a new one with labels available.
The following methods can then be used:

- [`labeled_successors(NodeId)`](crate::graph::SwhLabeledForwardGraph::labeled_successors):
  returns an iterator of (NodeId, iterator of [`EdgeLabel`](crate::labels::EdgeLabel)).
- [`untyped_labeled_successors(NodeId)`](crate::graph::SwhLabeledForwardGraph::untyped_labeled_successors):
  returns an iterator of (NodeId, iterator of [`UntypedEdgeLabel`](crate::labels::UntypedEdgeLabel)),
  which can be more efficient if you know the type of an edge label because of knowledge
  you have of the node.


## Label format

The labels of each arc are returned as an iterator of [`EdgeLabel`](crate::labels::EdgeLabel).
They encode either:

* for directory entries: both the name of a directory entry and its permissions
* for snapshot branches, only the "name" field is useful.
* for origin visits: the timestamp of the visit, and a bit indicating if this is a "full" visit
  (as opposed to a visit that failed halfway)

Arc label names are encoded as an integer ID representing each unique
entry/branch name present in the graph. To retrieve the actual name associated
with a given label ID, one needs to load the reverse mapping with `properties.load_label_names()`,
similar to how you would do for a normal property.


## Multiedges

The Software Heritage is not a *simple graph*, where at most one edge can exist
between two vertices, but a *multigraph*, where multiple edges can be incident
to the same two vertices. Consider for instance the case of a single directory
`test/` containing twice the same file blob (e.g., the empty file), under two
different names (e.g., `ISSUES.txt` and `TODO.txt`, both completely empty).
The simple graph view of this directory will represent it as a single edge
`test` → *empty file*, while the multigraph view will represent it as *two*
edges between the same nodes.

Due to the copy-list model of compression, WebGraph only stores simple graphs,
and thus stores multiedges as single edges, to which we cannot associate
a single label name (in our example, we need to associate both names
`ISSUES.txt` and `TODO.txt`).
To represent this possibility of having multiple file names for a single arc,
in the case of multiple relationships between two identical nodes, each arc label is
stored as an *array* of `EdgeLabel`, each record representing one relationship
between two nodes.


### Example: Printing all the entries of a directory

The following code showcases how one can print all the entries (name,
permission and target SWHID) of a given directory, using the labelled methods
seen above.

```no_run
# use std::path::PathBuf;
# use swh_graph::graph::*;
use swh_graph::mph::DynMphf;
use swh_graph::labels::EdgeLabel;

let graph = swh_graph::graph::load_full::<swh_graph::mph::DynMphf>(PathBuf::from("./graph"))
    .expect("Could not load graph");

let node_id: usize = graph
    .properties()
    .node_id("swh:1:dir:5e1c24e586ef92dbef0e9cec6b354c6831454340")
    .expect("Unknown SWHID");

for (succ, labels) in graph.labeled_successors(node_id) {
    for label in labels {
        if let EdgeLabel::DirEntry(label) = label {
            println!(
                "{} -> {} (permission: {:?}; name: {})",
                node_id,
                succ,
                label.permission(),
                String::from_utf8(
                    graph.properties().label_name(label.filename_id())
                ).expect("Could not decode file name as UTF-8")
            );
        }
    }
}
```


## Transposed graph

Up until now, we have only looked at how to traverse the *forward* graph, i.e.,
the directed graph whose edges are in the same direction as the Merkle DAG of
the Software Heritage archive.
For many purposes, especially that of finding the *provenance* of software
artifacts, it is useful to query the *backward* (or *transposed*) graph
instead, which is the same as the forward graph except all the edges are
reversed.

The transposed graph has its own set of files, counterparts to the files needed
for the forward graph:

- `graph-transposed.graph`
- `graph-transposed.properties`
- `graph-transposed.offsets`
- `graph-transposed.ef`
- `graph-transposed-labelled.labels`
- `graph-transposed-labelled.labeloffsets`
- `graph-transposed-labelled.properties`

However, because node IDs are the same in the forward and the backward graph,
all the files that pertain to mappings between the node IDs and various
properties (SWHIDs, property data, node permutations etc) remain the same.


### Example: Earliest revision containing a given blob

The following code loads all the committer timestamps of the revisions in the
graph, then walks the *transposed* graph to return the earliest revision
containing a given object.

<!-- FIXME: this example actually uses the bidirectional graph, because we don't
     support loading only the transposed graph yet -->

```no_run
use std::collections::HashSet;
use std::path::PathBuf;
use clap::Parser;
use swh_graph::graph::{NodeId, SwhBackwardGraph, SwhLabeledBackwardGraph, SwhGraphWithProperties};
use swh_graph::mph::DynMphf;
use swh_graph::labels::{EdgeLabel};
use swh_graph::properties;
use swh_graph::{NodeType, SWHID};

fn find_earliest_revision_containing<G>(graph: &G, src: NodeId) -> Option<NodeId>
where
    G: SwhBackwardGraph + SwhGraphWithProperties,
    <G as SwhGraphWithProperties>::Maps: properties::Maps,
    <G as SwhGraphWithProperties>::Timestamps: properties::Timestamps,
{

    let mut oldest_rev = None;
    let mut oldest_rev_ts = i64::MAX;

    let mut stack = Vec::new();
    let mut visited = HashSet::new();

    stack.push(src);
    visited.insert(src);

    while let Some(node) = stack.pop() {
        for pred in graph.predecessors(node) {
            if !visited.contains(&pred) {
                stack.push(pred);
                visited.insert(pred);
                if graph.properties().node_type(pred) == NodeType::Revision {
                    if let Some(ts) = graph.properties().committer_timestamp(pred) {
                        if ts < oldest_rev_ts {
                            oldest_rev = Some(pred);
                            oldest_rev_ts = ts;
                        }
                    }
                }
            }
        }
    }

    oldest_rev
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Path to the graph to load
    #[arg(short, long)]
    graph: PathBuf,

    /// SWHID of the content to lookup
    #[arg(short)]
    swhid: String,
}


fn main() {
    let args = Args::parse();

    let graph = swh_graph::graph::SwhBidirectionalGraph::new(args.graph)
        .expect("Could not load graph")
        .init_properties()
        .load_properties(|properties| properties.load_maps::<DynMphf>())
        .expect("Could not load maps")
        .load_properties(|properties| properties.load_timestamps())
        .expect("Could not load timestamps")
        .load_labels()
        .expect("Could not load labels");

    let swhid = SWHID::try_from(args.swhid.as_str()).expect("Invalid SWHID");
    let node = graph.properties().node_id(swhid).expect("Unknown SWHID");
    let oldest_rev = find_earliest_revision_containing(&graph, node);
    if let Some(oldest_rev) = oldest_rev {
        let rev_swhid = graph.properties().swhid(oldest_rev);
        println!("The oldest revision containing {} is {}", swhid, rev_swhid);
    } else {
        println!("No revisions contains {}", swhid);
    }
}
```

<!-- TODO: translate this to Rust
## Example: Find all the shared-commit forks of a given origin

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

-->
