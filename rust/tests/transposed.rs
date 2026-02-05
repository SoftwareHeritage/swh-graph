// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::path::PathBuf;

use anyhow::Result;

use swh_graph::graph::*;
use swh_graph::graph_builder::{BuiltGraph, GraphBuilder};
use swh_graph::labels::{Visit, VisitStatus};
use swh_graph::swhid;
use swh_graph::views::{Subgraph, Transposed};
use swh_graph::webgraph::graphs::vec_graph::{LabeledVecGraph, VecGraph};

#[test]
fn test_transpose_forward_graph() {
    let graph = SwhUnidirectionalGraph::from_underlying_graph(
        PathBuf::new(),
        VecGraph::from_arcs(vec![(2, 0), (2, 1), (0, 1)]),
    );
    let transposed = Transposed(graph);

    assert_eq!(transposed.num_nodes(), 3);
    assert_eq!(transposed.num_arcs(), 3);
    assert!(transposed.has_node(0));
    assert!(transposed.has_node(1));
    assert!(transposed.has_node(2));

    // Original: 2 -> 0, 2 -> 1, 0 -> 1
    // Transposed: 0 -> 2, 1 -> 2, 1 -> 0
    // When transposing a forward-only graph, we get a backward-only graph
    // so we use predecessors which give us the transposed successors
    assert!(transposed.has_arc(0, 2));
    assert!(transposed.has_arc(1, 2));
    assert!(transposed.has_arc(1, 0));
    assert!(!transposed.has_arc(2, 0));
    assert!(!transposed.has_arc(2, 1));
    assert!(!transposed.has_arc(0, 1));

    // Predecessors in the transposed graph are the sources of arcs pointing to this node
    // In transposed: 0 -> 2, so 2 has predecessor 0
    assert_eq!(transposed.predecessors(0).collect::<Vec<_>>(), vec![1]);
    assert_eq!(
        transposed.predecessors(1).collect::<Vec<_>>(),
        Vec::<usize>::new()
    );
    assert_eq!(transposed.predecessors(2).collect::<Vec<_>>(), vec![0, 1]);

    assert_eq!(transposed.indegree(0), 1);
    assert_eq!(transposed.indegree(1), 0);
    assert_eq!(transposed.indegree(2), 2);
}

#[test]
fn test_transpose_backward_graph() {
    // Original: 2 -> 0, 2 -> 1, 0 -> 1
    // Transposed: 0 -> 2, 1 -> 2, 1 -> 0
    let forward_graph = VecGraph::from_arcs(vec![(2, 0), (2, 1), (0, 1)]);
    let backward_graph = VecGraph::from_arcs(vec![(0, 2), (1, 2), (1, 0)]);
    let graph = SwhBidirectionalGraph::from_underlying_graphs(
        PathBuf::new(),
        forward_graph,
        backward_graph,
    );
    let transposed = Transposed(graph);

    // Original predecessors become successors
    assert_eq!(transposed.successors(0).collect::<Vec<_>>(), vec![2]);
    assert_eq!(transposed.successors(1).collect::<Vec<_>>(), vec![0, 2]);
    assert_eq!(
        transposed.successors(2).collect::<Vec<_>>(),
        Vec::<usize>::new()
    );

    // Original successors become predecessors
    assert_eq!(transposed.predecessors(0).collect::<Vec<_>>(), vec![1]);
    assert_eq!(
        transposed.predecessors(1).collect::<Vec<_>>(),
        Vec::<usize>::new()
    );
    assert_eq!(transposed.predecessors(2).collect::<Vec<_>>(), vec![0, 1]);

    assert_eq!(transposed.outdegree(0), 1);
    assert_eq!(transposed.outdegree(1), 2);
    assert_eq!(transposed.outdegree(2), 0);

    assert_eq!(transposed.indegree(0), 1);
    assert_eq!(transposed.indegree(1), 0);
    assert_eq!(transposed.indegree(2), 2);
}

#[test]
fn test_transpose_labeled_forward_graph() {
    let forward_arcs: Vec<((usize, usize), &[u64])> =
        vec![((0, 1), &[0, 789]), ((2, 0), &[123]), ((2, 1), &[456])];
    let backward_arcs: Vec<((usize, usize), &[u64])> =
        vec![((1, 0), &[0, 789]), ((0, 2), &[123]), ((1, 2), &[456])];
    let graph = SwhBidirectionalGraph::from_underlying_graphs(
        PathBuf::new(),
        LabeledVecGraph::from_arcs(forward_arcs),
        LabeledVecGraph::from_arcs(backward_arcs),
    );
    let transposed = Transposed(graph);

    let collect_successors = |node_id| {
        transposed
            .untyped_labeled_successors(node_id)
            .map(|(succ, labels)| (succ, labels.collect::<Vec<_>>()))
            .collect::<Vec<_>>()
    };

    // Original: 0 -> 1 [0, 789], 2 -> 0 [123], 2 -> 1 [456]
    // Transposed: 1 -> 0 [0, 789], 0 -> 2 [123], 1 -> 2 [456]
    assert_eq!(collect_successors(0), vec![(2, vec![123.into()])]);
    assert_eq!(
        collect_successors(1),
        vec![(0, vec![0.into(), 789.into()]), (2, vec![456.into()])]
    );
    assert_eq!(collect_successors(2), vec![]);
}

#[test]
fn test_transpose_labeled_backward_graph() {
    let forward_arcs: Vec<((usize, usize), &[u64])> =
        vec![((0, 1), &[0, 789]), ((2, 0), &[123]), ((2, 1), &[456])];
    let backward_arcs: Vec<((usize, usize), &[u64])> =
        vec![((1, 0), &[0, 789]), ((0, 2), &[123]), ((1, 2), &[456])];
    let graph = SwhBidirectionalGraph::from_underlying_graphs(
        PathBuf::new(),
        LabeledVecGraph::from_arcs(forward_arcs),
        LabeledVecGraph::from_arcs(backward_arcs),
    );
    let transposed = Transposed(graph);

    let collect_predecessors = |node_id| {
        transposed
            .untyped_labeled_predecessors(node_id)
            .map(|(pred, labels)| (pred, labels.collect::<Vec<_>>()))
            .collect::<Vec<_>>()
    };

    // Original predecessors become successors, so we check predecessors
    // which are the original successors
    assert_eq!(
        collect_predecessors(0),
        vec![(1, vec![0.into(), 789.into()])]
    );
    assert_eq!(collect_predecessors(1), vec![]);
    assert_eq!(
        collect_predecessors(2),
        vec![(0, vec![123.into()]), (1, vec![456.into()])]
    );
}

#[test]
fn test_transpose_is_transposed() {
    let graph = SwhUnidirectionalGraph::from_underlying_graph(
        PathBuf::new(),
        VecGraph::from_arcs(vec![(2, 0), (2, 1), (0, 1)]),
    );
    assert!(!graph.is_transposed());

    let transposed = Transposed(graph);
    assert!(transposed.is_transposed());

    let double_transposed = Transposed(transposed);
    assert!(!double_transposed.is_transposed());
}

#[test]
fn test_transpose_subgraph() {
    let graph = SwhUnidirectionalGraph::from_underlying_graph(
        PathBuf::new(),
        VecGraph::from_arcs(vec![(2, 0), (2, 1), (0, 1)]),
    );
    // Delete all nodes
    let filtered = Subgraph::with_node_filter(graph, |_| false);
    let transposed = Transposed(filtered);

    assert!(!transposed.has_node(0));
    assert!(!transposed.has_node(1));
    assert!(!transposed.has_node(2));
}

#[test]
fn test_transpose_with_filtered_arcs() {
    let forward_graph = VecGraph::from_arcs(vec![(2, 0), (2, 1), (0, 1)]);
    let backward_graph = VecGraph::from_arcs(vec![(0, 2), (1, 2), (1, 0)]);
    let graph = SwhBidirectionalGraph::from_underlying_graphs(
        PathBuf::new(),
        forward_graph,
        backward_graph,
    );

    // Filter to keep only node 0 and node 2
    let filtered = Subgraph::with_node_filter(graph, |node_id| node_id == 0 || node_id == 2);
    let transposed = Transposed(filtered);

    assert!(transposed.has_node(0));
    assert!(!transposed.has_node(1));
    assert!(transposed.has_node(2));

    // Original (filtered): 2 -> 0
    // Transposed: 0 -> 2
    assert!(transposed.has_arc(0, 2));
    assert!(!transposed.has_arc(2, 0));
    assert_eq!(transposed.successors(0).collect::<Vec<_>>(), vec![2]);
    assert_eq!(
        transposed.successors(2).collect::<Vec<_>>(),
        Vec::<usize>::new()
    );
}

/// Build a simple graph: ori0 -> snp1
fn build_ori_snp_graph() -> Result<BuiltGraph> {
    let mut builder = GraphBuilder::default();
    builder
        .node(swhid!(swh:1:ori:0000000000000000000000000000000000000000))?
        .done();
    builder
        .node(swhid!(swh:1:snp:0000000000000000000000000000000000000001))?
        .done();
    builder.ori_arc(0, 1, VisitStatus::Full, 1770248300);
    builder.ori_arc(0, 1, VisitStatus::Partial, 1770248399);
    builder.done()
}

#[test]
fn test_transpose_labeled_successors() -> Result<()> {
    let graph = build_ori_snp_graph()?;
    let transposed = Transposed(graph);

    // In the original graph: ori0 -> snp1 with Visit labels
    // In the transposed graph: snp1 -> ori0 with Visit labels
    let snp1_successors: Vec<_> = transposed
        .labeled_successors(1)
        .into_iter()
        .map(|(succ, labels)| (succ, labels.collect::<Vec<_>>()))
        .collect();

    assert_eq!(
        snp1_successors,
        vec![(
            0,
            vec![
                Visit::new(VisitStatus::Full, 1770248300).unwrap().into(),
                Visit::new(VisitStatus::Partial, 1770248399).unwrap().into()
            ]
        )],
        "Transposed snapshot -> origin edges should have Visit labels"
    );

    Ok(())
}

#[test]
fn test_transpose_labeled_predecessors() -> Result<()> {
    let graph = build_ori_snp_graph()?;
    let transposed = Transposed(graph);

    // In the original graph: ori0 -> snp1 with Visit labels
    // In the transposed graph: ori0 <- snp1 with Visit labels
    let ori0_predecessors: Vec<_> = transposed
        .labeled_predecessors(0)
        .into_iter()
        .map(|(pred, labels)| (pred, labels.collect::<Vec<_>>()))
        .collect();

    assert_eq!(
        ori0_predecessors,
        vec![(
            1,
            vec![
                Visit::new(VisitStatus::Full, 1770248300).unwrap().into(),
                Visit::new(VisitStatus::Partial, 1770248399).unwrap().into()
            ]
        )],
        "Transposed origin <- snapshot edges should have Visit labels"
    );

    Ok(())
}
