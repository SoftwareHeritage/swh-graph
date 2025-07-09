// Copyright (C) 2025  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::path::PathBuf;

use swh_graph::graph::*;
use swh_graph::views::{Subgraph, SymmetricWebgraphAdapter, WebgraphAdapter};
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

#[test]
fn test_symmetric_webgraph_adapter() {
    let graph = SwhBidirectionalGraph::from_underlying_graphs(
        PathBuf::new(),
        VecGraph::from_arcs(vec![(2, 0), (2, 1), (0, 1), (2, 3)]),
        VecGraph::from_arcs(vec![(0, 2), (1, 2), (1, 0), (3, 2)]),
    );

    let graph = SymmetricWebgraphAdapter(graph);

    assert_eq!(graph.num_nodes(), 4);
    assert_eq!(graph.num_arcs_hint(), Some(8));
    assert_eq!(graph.num_arcs(), 8);

    assert_eq!(graph.successors(0).collect::<Vec<_>>(), vec![1, 2]);
    assert_eq!(graph.outdegree(0), 2);
    assert_eq!(graph.successors(1).collect::<Vec<_>>(), vec![0, 2]);
    assert_eq!(graph.outdegree(1), 2);
    assert_eq!(graph.successors(2).collect::<Vec<_>>(), vec![0, 1, 3]);
    assert_eq!(graph.outdegree(2), 3);
    assert_eq!(graph.successors(3).collect::<Vec<_>>(), vec![2]);
    assert_eq!(graph.outdegree(3), 1);
}

#[test]
fn test_symmetric_webgraph_adapter_subgraph() {
    let graph = SwhBidirectionalGraph::from_underlying_graphs(
        PathBuf::new(),
        VecGraph::from_arcs(vec![(2, 0), (2, 1), (0, 1), (2, 3)]),
        VecGraph::from_arcs(vec![(0, 2), (1, 2), (1, 0), (3, 2)]),
    );
    let graph = Subgraph::with_node_filter(graph, |node| node != 0);

    let graph = SymmetricWebgraphAdapter(graph);

    assert_eq!(graph.num_nodes(), 4);
    assert_eq!(graph.num_arcs_hint(), Some(8));
    assert_eq!(graph.num_arcs(), 8);

    assert_eq!(graph.successors(0).collect::<Vec<_>>(), Vec::<usize>::new());
    assert_eq!(graph.outdegree(0), 0);
    assert_eq!(graph.successors(1).collect::<Vec<_>>(), vec![2]);
    assert_eq!(graph.outdegree(1), 1);
    assert_eq!(graph.successors(2).collect::<Vec<_>>(), vec![1, 3]);
    assert_eq!(graph.outdegree(2), 2);
    assert_eq!(graph.successors(3).collect::<Vec<_>>(), vec![2]);
    assert_eq!(graph.outdegree(3), 1);
}
