// Copyright (C) 2026  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::path::PathBuf;

use swh_graph::graph::*;
use swh_graph::views::{Subgraph, Symmetric};
use swh_graph::webgraph::graphs::vec_graph::{LabeledVecGraph, VecGraph};

#[test]
fn test_symmetric_bidirectional_graph() {
    // Original: 2 -> 0, 2 -> 1, 0 -> 1
    // Symmetric: 0 <-> 1, 0 <-> 2, 1 <-> 2 (all edges become bidirectional)
    let forward_graph = VecGraph::from_arcs(vec![(2, 0), (2, 1), (0, 1)]);
    let backward_graph = VecGraph::from_arcs(vec![(0, 2), (1, 2), (1, 0)]);
    let graph = SwhBidirectionalGraph::from_underlying_graphs(
        PathBuf::new(),
        forward_graph,
        backward_graph,
    );
    let symmetric = Symmetric(graph);

    assert_eq!(symmetric.num_nodes(), 3);
    assert_eq!(symmetric.num_arcs(), 6);
    assert!(symmetric.has_node(0));
    assert!(symmetric.has_node(1));
    assert!(symmetric.has_node(2));
    assert!(symmetric.has_arc(0, 1));
    assert!(symmetric.has_arc(1, 0));
    assert!(symmetric.has_arc(0, 2));
    assert!(symmetric.has_arc(2, 0));
    assert!(symmetric.has_arc(1, 2));
    assert!(symmetric.has_arc(2, 1));

    assert_eq!(symmetric.successors(0).collect::<Vec<_>>(), vec![1, 2]);
    assert_eq!(symmetric.successors(1).collect::<Vec<_>>(), vec![0, 2]);
    assert_eq!(symmetric.successors(2).collect::<Vec<_>>(), vec![0, 1]);
    assert_eq!(symmetric.predecessors(0).collect::<Vec<_>>(), vec![1, 2]);
    assert_eq!(symmetric.predecessors(1).collect::<Vec<_>>(), vec![0, 2]);
    assert_eq!(symmetric.predecessors(2).collect::<Vec<_>>(), vec![0, 1]);

    assert_eq!(symmetric.outdegree(0), 2);
    assert_eq!(symmetric.outdegree(1), 2);
    assert_eq!(symmetric.outdegree(2), 2);

    assert_eq!(symmetric.indegree(0), 2);
    assert_eq!(symmetric.indegree(1), 2);
    assert_eq!(symmetric.indegree(2), 2);
}

#[test]
fn test_symmetric_labeled_graph() {
    let forward_arcs: Vec<((usize, usize), &[u64])> =
        vec![((0, 1), &[0, 789]), ((2, 0), &[123]), ((2, 1), &[456])];
    let backward_arcs: Vec<((usize, usize), &[u64])> =
        vec![((1, 0), &[0, 789]), ((0, 2), &[123]), ((1, 2), &[456])];
    let graph = SwhBidirectionalGraph::from_underlying_graphs(
        PathBuf::new(),
        LabeledVecGraph::from_arcs(forward_arcs),
        LabeledVecGraph::from_arcs(backward_arcs),
    );
    let symmetric = Symmetric(graph);

    let collect_successors = |node_id| {
        symmetric
            .untyped_labeled_successors(node_id)
            .into_iter()
            .map(|(succ, labels)| (succ, labels.collect::<Vec<_>>()))
            .collect::<Vec<_>>()
    };

    assert_eq!(
        collect_successors(0),
        vec![(1, vec![0.into(), 789.into()]), (2, vec![123.into()])]
    );
    assert_eq!(
        collect_successors(1),
        vec![(0, vec![0.into(), 789.into()]), (2, vec![456.into()])]
    );
    assert_eq!(
        collect_successors(2),
        vec![(0, vec![123.into()]), (1, vec![456.into()])]
    );
}

#[test]
fn test_symmetric_predecessors_equal_successors() {
    let forward_arcs: Vec<((usize, usize), &[u64])> =
        vec![((0, 1), &[0, 789]), ((2, 0), &[123]), ((2, 1), &[456])];
    let backward_arcs: Vec<((usize, usize), &[u64])> =
        vec![((1, 0), &[0, 789]), ((0, 2), &[123]), ((1, 2), &[456])];
    let graph = SwhBidirectionalGraph::from_underlying_graphs(
        PathBuf::new(),
        LabeledVecGraph::from_arcs(forward_arcs),
        LabeledVecGraph::from_arcs(backward_arcs),
    );
    let symmetric = Symmetric(graph);

    assert_eq!(symmetric.predecessors(0).collect::<Vec<_>>(), vec![1, 2]);
    assert_eq!(symmetric.successors(0).collect::<Vec<_>>(), vec![1, 2]);

    assert_eq!(symmetric.predecessors(1).collect::<Vec<_>>(), vec![0, 2]);
    assert_eq!(symmetric.successors(1).collect::<Vec<_>>(), vec![0, 2]);

    assert_eq!(symmetric.predecessors(2).collect::<Vec<_>>(), vec![0, 1]);
    assert_eq!(symmetric.successors(2).collect::<Vec<_>>(), vec![0, 1]);
}

#[test]
fn test_symmetric_subgraph() {
    let forward_graph = VecGraph::from_arcs(vec![(2, 0), (2, 1), (0, 1)]);
    let backward_graph = VecGraph::from_arcs(vec![(0, 2), (1, 2), (1, 0)]);
    let graph = SwhBidirectionalGraph::from_underlying_graphs(
        PathBuf::new(),
        forward_graph,
        backward_graph,
    );

    // Delete all nodes
    let filtered = Subgraph::with_node_filter(graph, |_| false);
    let symmetric = Symmetric(filtered);

    assert!(!symmetric.has_node(0));
    assert!(!symmetric.has_node(1));
    assert!(!symmetric.has_node(2));
}

#[test]
fn test_symmetric_with_filtered_arcs() {
    let forward_graph = VecGraph::from_arcs(vec![(2, 0), (2, 1), (0, 1)]);
    let backward_graph = VecGraph::from_arcs(vec![(0, 2), (1, 2), (1, 0)]);
    let graph = SwhBidirectionalGraph::from_underlying_graphs(
        PathBuf::new(),
        forward_graph,
        backward_graph,
    );

    // Filter to keep only node 0 and node 2
    let filtered = Subgraph::with_node_filter(graph, |node_id| node_id == 0 || node_id == 2);
    let symmetric = Symmetric(filtered);

    assert!(symmetric.has_node(0));
    assert!(!symmetric.has_node(1));
    assert!(symmetric.has_node(2));

    // Original (filtered): 2 -> 0 in forward, 0 -> 2 in backward
    // Symmetric merges both directions
    assert_eq!(symmetric.successors(0).collect::<Vec<_>>(), vec![2]);
    assert_eq!(symmetric.successors(2).collect::<Vec<_>>(), vec![0]);
}

#[test]
fn test_symmetric_empty_graph() {
    let forward_graph = VecGraph::from_arcs(Vec::<(usize, usize)>::new());
    let backward_graph = VecGraph::from_arcs(Vec::<(usize, usize)>::new());
    let graph = SwhBidirectionalGraph::from_underlying_graphs(
        PathBuf::new(),
        forward_graph,
        backward_graph,
    );
    let symmetric = Symmetric(graph);

    assert_eq!(symmetric.num_nodes(), 0);
    assert_eq!(symmetric.num_arcs(), 0);
}

#[test]
#[should_panic(expected = "Called Symmetric::is_transposed()")]
fn test_symmetric_is_transposed_panics() {
    let forward_graph = VecGraph::from_arcs(vec![(0, 1)]);
    let backward_graph = VecGraph::from_arcs(vec![(1, 0)]);
    let graph = SwhBidirectionalGraph::from_underlying_graphs(
        PathBuf::new(),
        forward_graph,
        backward_graph,
    );
    let symmetric = Symmetric(graph);

    let _ = symmetric.is_transposed();
}

#[test]
#[should_panic(expected = "Symmetric's backend has a loop")]
fn test_symmetric_detects_loop_labeled() {
    let forward_arcs: Vec<((usize, usize), &[u64])> = vec![((0, 1), &[1]), ((1, 0), &[2])];
    let backward_arcs: Vec<((usize, usize), &[u64])> = vec![((1, 0), &[1]), ((0, 1), &[2])];
    let graph = SwhBidirectionalGraph::from_underlying_graphs(
        PathBuf::new(),
        LabeledVecGraph::from_arcs(forward_arcs),
        LabeledVecGraph::from_arcs(backward_arcs),
    );
    let symmetric = Symmetric(graph);

    let _ = symmetric
        .untyped_labeled_successors(0)
        .into_iter()
        .collect::<Vec<_>>();
}

#[test]
#[should_panic(expected = "Symmetric's backend has a loop")]
fn test_symmetric_detects_self_loop_labeled() {
    let forward_arcs: Vec<((usize, usize), &[u64])> = vec![((0, 0), &[1])];
    let backward_arcs: Vec<((usize, usize), &[u64])> = vec![((0, 0), &[1])];
    let graph = SwhBidirectionalGraph::from_underlying_graphs(
        PathBuf::new(),
        LabeledVecGraph::from_arcs(forward_arcs),
        LabeledVecGraph::from_arcs(backward_arcs),
    );
    let symmetric = Symmetric(graph);

    let _ = symmetric
        .untyped_labeled_successors(0)
        .into_iter()
        .collect::<Vec<_>>();
}

#[test]
#[should_panic(expected = "Symmetric's backend has a loop")]
fn test_symmetric_detects_inconsistent_backends_labeled() {
    let forward_arcs: Vec<((usize, usize), &[u64])> = vec![((0, 1), &[1])];
    let backward_arcs: Vec<((usize, usize), &[u64])> = vec![((0, 1), &[1])];
    let graph = SwhBidirectionalGraph::from_underlying_graphs(
        PathBuf::new(),
        LabeledVecGraph::from_arcs(forward_arcs),
        LabeledVecGraph::from_arcs(backward_arcs),
    );
    let symmetric = Symmetric(graph);

    let _ = symmetric
        .untyped_labeled_successors(0)
        .into_iter()
        .collect::<Vec<_>>();
}
