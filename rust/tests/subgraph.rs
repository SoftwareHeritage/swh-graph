// Copyright (C) 2024-2026  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::path::PathBuf;
use std::rc::Rc;

use anyhow::Result;

use swh_graph::graph::*;
use swh_graph::graph_builder::GraphBuilder;
use swh_graph::labels::DirEntry;
use swh_graph::properties;
use swh_graph::views::Subgraph;
use swh_graph::webgraph::graphs::vec_graph::LabeledVecGraph;
use swh_graph::{swhid, NodeConstraint};

#[test]
fn test_node_constraint() -> Result<()> {
    let mut builder = GraphBuilder::default();
    let ori01 = builder
        .node(swhid!(swh:1:ori:0000000000000000000000000000000000000001))?
        .done();
    let snp20 = builder
        .node(swhid!(swh:1:snp:0000000000000000000000000000000000000020))?
        .done();
    let rev09 = builder
        .node(swhid!(swh:1:rev:0000000000000000000000000000000000000009))?
        .done();
    let dir08 = builder
        .node(swhid!(swh:1:dir:0000000000000000000000000000000000000008))?
        .done();
    let cnt01 = builder
        .node(swhid!(swh:1:cnt:0000000000000000000000000000000000000001))?
        .done();
    builder.arc(ori01, snp20);
    builder.arc(snp20, rev09);
    builder.arc(rev09, dir08);
    builder.arc(dir08, cnt01);
    let graph = Rc::new(builder.done()?);
    let props = graph.properties();

    let ori_node = props.node_id(swhid!(swh:1:ori:0000000000000000000000000000000000000001))?;
    let snp_node = props.node_id(swhid!(swh:1:snp:0000000000000000000000000000000000000020))?;
    let rev_node = props.node_id(swhid!(swh:1:rev:0000000000000000000000000000000000000009))?;
    let dir_node = props.node_id(swhid!(swh:1:dir:0000000000000000000000000000000000000008))?;
    let cnt_node = props.node_id(swhid!(swh:1:cnt:0000000000000000000000000000000000000001))?;

    let full_graph =
        Subgraph::with_node_constraint(graph.clone(), "*".parse::<NodeConstraint>().unwrap());
    assert!(full_graph.has_node(snp_node));
    assert!(full_graph.has_node(cnt_node));
    assert!(full_graph.has_arc(ori_node, snp_node));
    assert!(full_graph.has_arc(rev_node, dir_node));
    assert!(full_graph.has_arc(dir_node, cnt_node));

    let fs_graph =
        Subgraph::with_node_constraint(graph.clone(), "dir,cnt".parse::<NodeConstraint>().unwrap());
    assert!(fs_graph.has_node(cnt_node));
    assert!(fs_graph.has_node(dir_node));
    assert!(!fs_graph.has_node(rev_node));
    assert!(fs_graph.has_arc(dir_node, cnt_node));
    assert!(!fs_graph.has_arc(ori_node, snp_node));
    assert!(!fs_graph.has_arc(rev_node, dir_node));

    let history_graph =
        Subgraph::with_node_constraint(graph.clone(), "snp,rel,rev".parse().unwrap());
    assert!(history_graph.has_node(snp_node));
    assert!(history_graph.has_node(rev_node));
    assert!(!history_graph.has_node(ori_node));
    assert!(!history_graph.has_node(cnt_node));
    assert!(!history_graph.has_node(dir_node));
    assert!(history_graph.has_arc(snp_node, rev_node));
    assert!(!history_graph.has_arc(rev_node, dir_node));

    Ok(())
}

#[test]
fn test_iterators_node_constraint() {
    let forward_arcs: Vec<((usize, usize), &[u64])> =
        vec![((1, 0), &[0, 789]), ((0, 2), &[123]), ((1, 2), &[456])];
    let backward_arcs: Vec<((usize, usize), &[u64])> =
        vec![((0, 1), &[0, 789]), ((2, 0), &[123]), ((2, 1), &[456])];
    let graph = SwhBidirectionalGraph::from_underlying_graphs(
        PathBuf::new(),
        LabeledVecGraph::from_arcs(forward_arcs),
        LabeledVecGraph::from_arcs(backward_arcs),
    )
    .init_properties()
    .load_properties(|props| {
        props.with_maps(properties::VecMaps::new(vec![
            swhid!(swh:1:dir:0000000000000000000000000000000000000000),
            swhid!(swh:1:dir:0000000000000000000000000000000000000001),
            swhid!(swh:1:dir:0000000000000000000000000000000000000002),
        ]))
    })
    .expect("Could not load maps");

    // Filter to keep only node 0 and node 2
    let filtered = Subgraph::with_node_filter(graph, |node_id| node_id == 0 || node_id == 2);

    assert!(filtered.has_node(0));
    assert!(!filtered.has_node(1));
    assert!(filtered.has_node(2));

    check_filtered_subgraph(filtered);
}

#[test]
fn test_iterators_arc_constraint() {
    let forward_arcs: Vec<((usize, usize), &[u64])> =
        vec![((1, 0), &[0, 789]), ((0, 2), &[123]), ((1, 2), &[456])];
    let backward_arcs: Vec<((usize, usize), &[u64])> =
        vec![((0, 1), &[0, 789]), ((2, 0), &[123]), ((2, 1), &[456])];
    let graph = SwhBidirectionalGraph::from_underlying_graphs(
        PathBuf::new(),
        LabeledVecGraph::from_arcs(forward_arcs),
        LabeledVecGraph::from_arcs(backward_arcs),
    )
    .init_properties()
    .load_properties(|props| {
        props.with_maps(properties::VecMaps::new(vec![
            swhid!(swh:1:dir:0000000000000000000000000000000000000000),
            swhid!(swh:1:dir:0000000000000000000000000000000000000001),
            swhid!(swh:1:dir:0000000000000000000000000000000000000002),
        ]))
    })
    .expect("Could not load maps");

    // Filter to keep only the arc 2 -> 0
    let filtered = Subgraph::with_arc_filter(graph, |src, dst| src == 0 && dst == 2);

    assert!(filtered.has_node(0));
    assert!(filtered.has_node(1));
    assert!(filtered.has_node(2));

    check_filtered_subgraph(filtered);
}

fn check_filtered_subgraph<G: SwhLabeledForwardGraph + SwhLabeledBackwardGraph>(filtered: G) {
    let collect_successors = |node_id| {
        filtered
            .untyped_labeled_successors(node_id)
            .into_iter()
            .map(|(pred, labels)| (pred, labels.collect::<Vec<_>>()))
            .collect::<Vec<_>>()
    };

    let collect_predecessors = |node_id| {
        filtered
            .untyped_labeled_predecessors(node_id)
            .into_iter()
            .map(|(pred, labels)| (pred, labels.collect::<Vec<_>>()))
            .collect::<Vec<_>>()
    };

    let collect_typed_successors = |node_id| {
        filtered
            .labeled_successors(node_id)
            .into_iter()
            .map(|(pred, labels)| (pred, labels.collect::<Vec<_>>()))
            .collect::<Vec<_>>()
    };

    let collect_typed_predecessors = |node_id| {
        filtered
            .labeled_predecessors(node_id)
            .into_iter()
            .map(|(pred, labels)| (pred, labels.collect::<Vec<_>>()))
            .collect::<Vec<_>>()
    };

    assert!(filtered.has_arc(0, 2));
    assert!(!filtered.has_arc(2, 0));

    assert_eq!(
        filtered.successors(0).into_iter().collect::<Vec<_>>(),
        vec![2]
    );
    assert_eq!(
        filtered.successors(1).into_iter().collect::<Vec<_>>(),
        Vec::<usize>::new()
    );
    assert_eq!(
        filtered.successors(2).into_iter().collect::<Vec<_>>(),
        Vec::<usize>::new()
    );

    assert_eq!(collect_successors(0), vec![(2, vec![123.into()])]);
    assert_eq!(collect_successors(1), vec![]);
    assert_eq!(collect_successors(2), vec![]);

    assert_eq!(
        collect_typed_successors(0),
        vec![(2, vec![DirEntry::from(123).into()])]
    );
    assert_eq!(collect_typed_successors(1), vec![]);
    assert_eq!(collect_typed_successors(2), vec![]);

    assert_eq!(
        filtered.predecessors(0).into_iter().collect::<Vec<_>>(),
        Vec::<usize>::new()
    );
    assert_eq!(
        filtered.predecessors(1).into_iter().collect::<Vec<_>>(),
        Vec::<usize>::new()
    );
    assert_eq!(
        filtered.predecessors(2).into_iter().collect::<Vec<_>>(),
        vec![0]
    );

    assert_eq!(collect_predecessors(0), vec![]);
    assert_eq!(collect_predecessors(1), vec![]);
    assert_eq!(collect_predecessors(2), vec![(0, vec![123.into()])]);

    assert_eq!(collect_typed_predecessors(0), vec![]);
    assert_eq!(collect_typed_predecessors(1), vec![]);
    assert_eq!(
        collect_typed_predecessors(2),
        vec![(0, vec![DirEntry::from(123).into()])]
    );
}
