// Copyright (C) 2025  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::path::PathBuf;

use swh_graph::graph::*;
use swh_graph::views::{Subgraph, WebgraphAdapter};
use swh_graph::webgraph::graphs::vec_graph::VecGraph;
use swh_graph::webgraph::prelude::*;

#[test]
fn test_webgraph_adapter() {
    let graph = SwhUnidirectionalGraph::from_underlying_graph(
        PathBuf::new(),
        VecGraph::from_arcs(vec![(2, 0), (2, 1), (0, 1)]),
    );

    let graph = WebgraphAdapter(graph);

    assert_eq!(graph.num_nodes(), 3);
    assert_eq!(graph.num_arcs_hint(), Some(3));
    assert_eq!(graph.num_arcs(), 3);

    assert_eq!(graph.successors(0).collect::<Vec<_>>(), vec![1]);
    assert_eq!(graph.outdegree(0), 1);
    assert_eq!(graph.successors(1).collect::<Vec<_>>(), Vec::<usize>::new());
    assert_eq!(graph.outdegree(1), 0);
    assert_eq!(graph.successors(2).collect::<Vec<_>>(), vec![0, 1]);
    assert_eq!(graph.outdegree(2), 2);
}

#[test]
fn test_webgraph_adapter_subgraph() {
    let graph = SwhUnidirectionalGraph::from_underlying_graph(
        PathBuf::new(),
        VecGraph::from_arcs(vec![(2, 0), (2, 1), (0, 1)]),
    );
    let graph = Subgraph::with_node_filter(graph, |node| node != 0);

    let graph = WebgraphAdapter(graph);

    assert_eq!(graph.num_nodes(), 3);
    assert_eq!(graph.num_arcs_hint(), Some(3));
    assert_eq!(graph.num_arcs(), 3);

    assert_eq!(graph.successors(0).collect::<Vec<_>>(), Vec::<usize>::new());
    assert_eq!(graph.outdegree(0), 0);
    assert_eq!(graph.successors(1).collect::<Vec<_>>(), Vec::<usize>::new());
    assert_eq!(graph.outdegree(1), 0);
    assert_eq!(graph.successors(2).collect::<Vec<_>>(), vec![1]);
    assert_eq!(graph.outdegree(2), 1);
}
