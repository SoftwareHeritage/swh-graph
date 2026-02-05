# Documentation: Crash course

This guide assumes you are already familiar with the
[Java API](https://docs.softwareheritage.org/devel/swh-graph/java-api.html).
If not, read the [tutorial](crate::_tutorial) instead.

## Getting started

The central classes of this crate are
[`SwhUnidirectionalGraph`](crate::graph::SwhUnidirectionalGraph) and
[`SwhBidirectionalGraph`](crate::graph::SwhBidirectionalGraph).
They both provide the API above; the difference being that the former does not provide
predecessor access.

In order to avoid loading files unnecessarily, they are instantiated as "naked" graphs
with [`SwhUnidirectionalGraph::new`](crate::graph::SwhUnidirectionalGraph::new)
and [`SwhBidirectionalGraph::new`](crate::graph::SwhBidirectionalGraph::new) which only provide
successor and predecessor access; and extra data must be loaded with the `load_*` methods.

For example:

```compile_fail
# use std::path::PathBuf;
use swh_graph::mph::DynMphf;

let graph = swh_graph::graph::SwhUnidirectionalGraph::new(PathBuf::from("./graph"))
    .expect("Could not load graph");
let node_id: usize = graph
    .properties()
    .node_id("swh:1:snp:486b338078a42de5ece0970638c7270d9c39685f")
    .unwrap();
```

fails to compile because `node_id` uses one of these properties that are not loaded
by default, so it must be loaded:

```no_run
# use std::path::PathBuf;
# use swh_graph::graph::SwhGraphWithProperties;
use swh_graph::mph::DynMphf;

let graph = swh_graph::graph::SwhUnidirectionalGraph::new(PathBuf::from("./graph"))
    .expect("Could not load graph")
    .init_properties()
    .load_properties(|properties| properties.load_maps::<DynMphf>())
    .expect("Could not load SWHID<->node id maps");

let node_id: usize = graph
    .properties()
    .node_id("swh:1:dir:5e1c24e586ef92dbef0e9cec6b354c6831454340")
    .expect("Unknown SWHID");
```

or alternatively, to load all possible properties at once:

```no_run
# use std::path::PathBuf;
# use swh_graph::graph::SwhGraphWithProperties;
use swh_graph::mph::DynMphf;

let graph = swh_graph::graph::SwhUnidirectionalGraph::new(PathBuf::from("./graph"))
    .expect("Could not load graph")
    .load_all_properties::<DynMphf>()
    .expect("Could not load properties");

let node_id: usize = graph
    .properties()
    .node_id("swh:1:dir:5e1c24e586ef92dbef0e9cec6b354c6831454340")
    .expect("Unknown SWHID");
```

Once you have a node id to start from, you can start using the above API to traverse
the graph. For example, to list all successors of the given object (directory entries
as this example uses a directory):

```no_run
# use std::path::PathBuf;
# use swh_graph::graph::{SwhForwardGraph, SwhGraphWithProperties};
use swh_graph::mph::DynMphf;

let graph = swh_graph::graph::SwhUnidirectionalGraph::new(PathBuf::from("./graph"))
    .expect("Could not load graph")
    .load_all_properties::<DynMphf>()
    .expect("Could not load properties");

let node_id: usize = graph
    .properties()
    .node_id("swh:1:dir:5e1c24e586ef92dbef0e9cec6b354c6831454340")
    .expect("Unknown SWHID");

for succ in graph.successors(node_id) {
    println!("{} -> {}", node_id, succ);
}
```

Or, if you have labeled graphs available locally, you can load them and also print
labels on the arcs from the given object to its successors (file permissions and names
as this example uses a directory):

```no_run
# use std::path::PathBuf;
# use swh_graph::graph::{SwhForwardGraph, SwhLabeledForwardGraph, SwhGraphWithProperties};
use swh_graph::mph::DynMphf;
use swh_graph::labels::{EdgeLabel};

let graph = swh_graph::graph::SwhUnidirectionalGraph::new(PathBuf::from("./graph"))
    .expect("Could not load graph")
    .load_all_properties::<DynMphf>()
    .expect("Could not load properties")
    .load_labels()
    .expect("Could not load labels");

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
                    graph.properties().label_name(label.label_name_id())
                ).expect("Could not decode file name as UTF-8")
            );
        }
    }
}
```

