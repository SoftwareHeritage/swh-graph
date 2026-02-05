// Copyright (C) 2026  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::path::PathBuf;

use anyhow::Result;
use swh_graph::arc_iterators::IntoFlattenedLabeledArcsIterator;
use swh_graph::graph::*;
use swh_graph::graph_builder::{BuiltGraph, GraphBuilder};
use swh_graph::labels::{Permission, Visit, VisitStatus};
use swh_graph::swhid;
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

    assert_eq!(symmetric.outdegree(0), 2);
    assert_eq!(symmetric.outdegree(1), 2);
    assert_eq!(symmetric.outdegree(2), 2);
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

    assert_eq!(symmetric.successors(0).collect::<Vec<_>>(), vec![1, 2]);

    assert_eq!(symmetric.successors(1).collect::<Vec<_>>(), vec![0, 2]);

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

    let _ = symmetric.untyped_labeled_successors(0).collect::<Vec<_>>();
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

    let _ = symmetric.untyped_labeled_successors(0).collect::<Vec<_>>();
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

    let _ = symmetric.untyped_labeled_successors(0).collect::<Vec<_>>();
}

// Tests for labeled_successors with typed edges

/// ```
/// ori0 --> snp1
/// ```
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
fn test_symmetric_ori_snp_labeled_successors() -> Result<()> {
    let graph = build_ori_snp_graph()?;
    let symmetric = Symmetric(graph);

    // Test ori0's successors: should have snp1 with two visit labels (forward direction)
    let ori0_successors = symmetric
        .labeled_successors(0)
        .into_iter()
        .map(|(succ, labels)| (succ, labels.collect::<Vec<_>>()))
        .collect::<Vec<_>>();

    assert_eq!(
        ori0_successors,
        vec![(
            1,
            vec![
                Visit::new(VisitStatus::Full, 1770248300).unwrap().into(),
                Visit::new(VisitStatus::Partial, 1770248399).unwrap().into()
            ]
        )]
    );

    let snp1_successors: Vec<(_, Vec<_>)> = symmetric
        .labeled_successors(1)
        .into_iter()
        .map(|(succ, labels)| (succ, labels.collect()))
        .collect();

    assert_eq!(
        snp1_successors,
        vec![(
            0,
            vec![
                Visit::new(VisitStatus::Full, 1770248300).unwrap().into(),
                Visit::new(VisitStatus::Partial, 1770248399).unwrap().into()
            ]
        )]
    );

    Ok(())
}

/// ```
/// snp0 --> rev1
/// ```
fn build_snp_rev_graph() -> Result<BuiltGraph> {
    let mut builder = GraphBuilder::default();
    builder
        .node(swhid!(swh:1:snp:0000000000000000000000000000000000000000))?
        .done();
    builder
        .node(swhid!(swh:1:rev:0000000000000000000000000000000000000001))?
        .done();
    builder.snp_arc(0, 1, b"refs/heads/main");
    builder.snp_arc(0, 1, b"refs/heads/develop");
    builder.done()
}

#[test]
fn test_symmetric_snp_rev_labeled_successors() -> Result<()> {
    let graph = build_snp_rev_graph()?;
    let symmetric = Symmetric(graph);

    let main_label = symmetric
        .properties()
        .label_name_id(b"refs/heads/main")
        .unwrap();
    let develop_label = symmetric
        .properties()
        .label_name_id(b"refs/heads/develop")
        .unwrap();

    // Test snp0's successors: should have rev1 with branch labels (forward direction)
    let snp0_successors: Vec<(_, Vec<_>)> = symmetric
        .labeled_successors(0)
        .into_iter()
        .map(|(succ, labels)| (succ, labels.collect()))
        .collect();

    assert_eq!(
        snp0_successors,
        vec![(
            1,
            vec![
                swh_graph::labels::Branch::new(main_label).unwrap().into(),
                swh_graph::labels::Branch::new(develop_label)
                    .unwrap()
                    .into()
            ]
        )]
    );

    let rev1_successors: Vec<(_, Vec<_>)> = symmetric
        .labeled_successors(1)
        .into_iter()
        .map(|(succ, labels)| (succ, labels.collect()))
        .collect();

    assert_eq!(
        rev1_successors,
        vec![(
            0,
            vec![
                swh_graph::labels::Branch::new(main_label).unwrap().into(),
                swh_graph::labels::Branch::new(develop_label)
                    .unwrap()
                    .into()
            ]
        )]
    );

    Ok(())
}

/// ```
/// dir0 --> cnt1
///      \-> dir2
/// ```
fn build_fs_graph() -> Result<BuiltGraph> {
    let mut builder = GraphBuilder::default();
    builder
        .node(swhid!(swh:1:dir:0000000000000000000000000000000000000000))?
        .done();
    builder
        .node(swhid!(swh:1:cnt:0000000000000000000000000000000000000001))?
        .done();
    builder
        .node(swhid!(swh:1:dir:0000000000000000000000000000000000000002))?
        .done();
    builder.dir_arc(0, 1, Permission::Content, b"file.txt");
    builder.dir_arc(0, 2, Permission::Directory, b"subdir");
    builder.done()
}

#[test]
fn test_symmetric_fs_labeled_successors() -> Result<()> {
    let graph = build_fs_graph()?;
    let symmetric = Symmetric(graph);

    let file_label = symmetric.properties().label_name_id(b"file.txt").unwrap();
    let subdir_label = symmetric.properties().label_name_id(b"subdir").unwrap();

    // Test dir0's successors: should have cnt1 and dir2 (forward direction)
    let dir0_successors: Vec<(_, Vec<_>)> = symmetric
        .labeled_successors(0)
        .into_iter()
        .map(|(succ, labels)| (succ, labels.collect()))
        .collect();

    assert_eq!(
        dir0_successors,
        vec![
            (
                1,
                vec![
                    swh_graph::labels::DirEntry::new(Permission::Content, file_label)
                        .unwrap()
                        .into()
                ]
            ),
            (
                2,
                vec![
                    swh_graph::labels::DirEntry::new(Permission::Directory, subdir_label)
                        .unwrap()
                        .into()
                ]
            )
        ]
    );

    let cnt1_successors: Vec<(_, Vec<_>)> = symmetric
        .labeled_successors(1)
        .into_iter()
        .map(|(succ, labels)| (succ, labels.collect()))
        .collect();

    assert_eq!(
        cnt1_successors,
        vec![(
            0,
            vec![
                swh_graph::labels::DirEntry::new(Permission::Content, file_label)
                    .unwrap()
                    .into()
            ]
        )]
    );

    let dir2_successors: Vec<(_, Vec<_>)> = symmetric
        .labeled_successors(2)
        .into_iter()
        .map(|(succ, labels)| (succ, labels.collect()))
        .collect();

    assert_eq!(
        dir2_successors,
        vec![(
            0,
            vec![
                swh_graph::labels::DirEntry::new(Permission::Directory, subdir_label)
                    .unwrap()
                    .into()
            ]
        )]
    );

    Ok(())
}

/// ```
/// ori0 --> snp1 --> rev2
///                \-> rel3
/// ```
fn build_histhost_graph() -> Result<BuiltGraph> {
    let mut builder = GraphBuilder::default();
    builder
        .node(swhid!(swh:1:ori:0000000000000000000000000000000000000000))?
        .done();
    builder
        .node(swhid!(swh:1:snp:0000000000000000000000000000000000000001))?
        .done();
    builder
        .node(swhid!(swh:1:rev:0000000000000000000000000000000000000002))?
        .done();
    builder
        .node(swhid!(swh:1:rel:0000000000000000000000000000000000000003))?
        .done();
    builder.ori_arc(0, 1, VisitStatus::Full, 1770248300);
    builder.snp_arc(1, 2, b"refs/heads/main");
    builder.snp_arc(1, 3, b"refs/tags/v1.0");
    builder.done()
}

#[test]
fn test_symmetric_histhost_labeled_successors() -> Result<()> {
    let graph = build_histhost_graph()?;
    let symmetric = Symmetric(graph);

    let main_label = symmetric
        .properties()
        .label_name_id(b"refs/heads/main")
        .unwrap();
    let tag_label = symmetric
        .properties()
        .label_name_id(b"refs/tags/v1.0")
        .unwrap();

    let ori0_successors: Vec<(_, Vec<_>)> = symmetric
        .labeled_successors(0)
        .into_iter()
        .map(|(succ, labels)| (succ, labels.collect()))
        .collect();

    assert_eq!(
        ori0_successors,
        vec![(
            1,
            vec![Visit::new(VisitStatus::Full, 1770248300).unwrap().into()]
        )]
    );

    let snp1_successors: Vec<(_, Vec<_>)> = symmetric
        .labeled_successors(1)
        .into_iter()
        .map(|(succ, labels)| (succ, labels.collect()))
        .collect();

    assert_eq!(
        snp1_successors,
        vec![
            (
                0,
                vec![Visit::new(VisitStatus::Full, 1770248300).unwrap().into()]
            ),
            (
                2,
                vec![swh_graph::labels::Branch::new(main_label).unwrap().into()]
            ),
            (
                3,
                vec![swh_graph::labels::Branch::new(tag_label).unwrap().into()]
            )
        ]
    );

    let rev2_successors: Vec<(_, Vec<_>)> = symmetric
        .labeled_successors(2)
        .into_iter()
        .map(|(succ, labels)| (succ, labels.collect()))
        .collect();

    assert_eq!(
        rev2_successors,
        vec![(
            1,
            vec![swh_graph::labels::Branch::new(main_label).unwrap().into()]
        )]
    );

    let rel3_successors: Vec<(_, Vec<_>)> = symmetric
        .labeled_successors(3)
        .into_iter()
        .map(|(succ, labels)| (succ, labels.collect()))
        .collect();

    assert_eq!(
        rel3_successors,
        vec![(
            1,
            vec![swh_graph::labels::Branch::new(tag_label).unwrap().into()]
        )]
    );

    Ok(())
}

#[test]
fn test_symmetric_flattened_labels() -> Result<()> {
    let graph = build_ori_snp_graph()?;
    let symmetric = Symmetric(graph);

    let ori0_labels = symmetric
        .labeled_successors(0)
        .flatten_labels()
        .into_iter()
        .collect::<Vec<_>>();

    assert_eq!(
        ori0_labels,
        vec![
            (1, Visit::new(VisitStatus::Full, 1770248300).unwrap().into()),
            (
                1,
                Visit::new(VisitStatus::Partial, 1770248399).unwrap().into()
            )
        ]
    );

    Ok(())
}
