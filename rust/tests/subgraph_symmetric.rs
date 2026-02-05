// Copyright (C) 2026  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use anyhow::Result;

use swh_graph::graph::*;
use swh_graph::graph_builder::{BuiltGraph, GraphBuilder};
use swh_graph::labels::{EdgeLabel, Permission, UntypedEdgeLabel};
use swh_graph::swhid;
use swh_graph::views::{Subgraph, Symmetric};

fn untype_labeled_successors(
    labeled_successors: &[(NodeId, Vec<EdgeLabel>)],
) -> Vec<(NodeId, Vec<UntypedEdgeLabel>)> {
    labeled_successors
        .iter()
        .map(|(node_id, labels)| {
            (
                *node_id,
                labels
                    .iter()
                    .map(|label| UntypedEdgeLabel::from(*label))
                    .collect(),
            )
        })
        .collect()
}

/// ```
/// ori0 --> snp3
/// snp3 --> rel4 (main, Release)
/// rel4 --> dir5
/// dir5 --> cnt1  (file.txt, Content)
///      \-> dir2  (subdir, Directory)
/// ```
fn build_graph() -> Result<BuiltGraph> {
    let mut builder = GraphBuilder::default();
    builder
        .node(swhid!(swh:1:ori:0000000000000000000000000000000000000000))?
        .done();
    builder
        .node(swhid!(swh:1:cnt:0000000000000000000000000000000000000001))?
        .done();
    builder
        .node(swhid!(swh:1:dir:0000000000000000000000000000000000000002))?
        .done();
    builder
        .node(swhid!(swh:1:snp:0000000000000000000000000000000000000003))?
        .done();
    builder
        .node(swhid!(swh:1:rel:0000000000000000000000000000000000000004))?
        .done();
    builder
        .node(swhid!(swh:1:dir:0000000000000000000000000000000000000005))?
        .done();
    builder.arc(0, 3);
    builder.snp_arc(3, 4, b"main");
    builder.arc(4, 5);
    builder.dir_arc(5, 1, Permission::Content, b"file.txt");
    builder.dir_arc(5, 2, Permission::Directory, b"subdir");
    builder.done()
}

#[test]
fn test_symmetric_of_subgraph() -> Result<()> {
    let graph = build_graph()?;
    let subgraph = Subgraph::with_node_filter(graph, |node| node != 1);
    let symmetric = Symmetric(subgraph);

    let subdir_id = symmetric.properties().label_name_id(b"subdir").unwrap();
    let entry_subdir: EdgeLabel =
        swh_graph::labels::DirEntry::new(Permission::Directory, subdir_id)
            .unwrap()
            .into();

    let main_id = symmetric.properties().label_name_id(b"main").unwrap();
    let branch_main: EdgeLabel = swh_graph::labels::Branch::new(main_id).unwrap().into();

    let expected = [
        (0, vec![3], vec![(3, vec![])]),
        (1, vec![], vec![]), // filtered out
        (2, vec![5], vec![(5, vec![entry_subdir])]),
        (3, vec![0, 4], vec![(0, vec![]), (4, vec![branch_main])]),
        (4, vec![3, 5], vec![(3, vec![branch_main]), (5, vec![])]),
        (5, vec![2, 4], vec![(2, vec![entry_subdir]), (4, vec![])]),
    ];

    for &(node, ref expected_untyped, ref expected_labeled) in &expected {
        assert_eq!(
            symmetric.successors(node).collect::<Vec<_>>(),
            *expected_untyped,
            "successors({node})",
        );
        let typed: Vec<(_, Vec<_>)> = symmetric
            .labeled_successors(node)
            .into_iter()
            .map(|(succ, labels)| (succ, labels.collect()))
            .collect();
        assert_eq!(
            typed, *expected_labeled,
            "labeled_successors({node})",
        );

        let untyped: Vec<_> = symmetric
            .untyped_labeled_successors(node)
            .map(|(succ, labels)| (succ, labels.collect::<Vec<_>>()))
            .collect();
        assert_eq!(
            untyped,
            untype_labeled_successors(expected_labeled),
            "untyped_labeled_successors({node})",
        );
    }

    Ok(())
}

#[test]
#[should_panic(expected = "Called Symmetric::is_transposed()")]
fn test_subgraph_of_symmetric() {
    let graph = build_graph().unwrap();
    let symmetric = Symmetric(graph);
    let subgraph = Subgraph::with_node_filter(symmetric, |node| node != 1);

    let subdir_id = subgraph.properties().label_name_id(b"subdir").unwrap();
    let entry_subdir: EdgeLabel =
        swh_graph::labels::DirEntry::new(Permission::Directory, subdir_id)
            .unwrap()
            .into();

    let main_id = subgraph.properties().label_name_id(b"main").unwrap();
    let branch_main: EdgeLabel = swh_graph::labels::Branch::new(main_id).unwrap().into();

    let expected = [
        (0, vec![3], vec![(3, vec![])]),
        (1, vec![], vec![]), // filtered out
        (2, vec![5], vec![(5, vec![entry_subdir])]),
        (3, vec![0, 4], vec![(0, vec![]), (4, vec![branch_main])]),
        (4, vec![3, 5], vec![(3, vec![branch_main]), (5, vec![])]),
        (5, vec![2, 4], vec![(2, vec![entry_subdir]), (4, vec![])]),
    ];

    for &(node, ref expected_untyped, ref expected_labeled) in &expected {
        assert_eq!(
            subgraph.successors(node).collect::<Vec<_>>(),
            *expected_untyped,
            "successors() failed for node {}",
            node
        );
        let typed: Vec<(_, Vec<_>)> = subgraph
            .labeled_successors(node)
            .into_iter()
            .map(|(succ, labels)| (succ, labels.collect()))
            .collect();
        assert_eq!(
            typed, *expected_labeled,
            "labeled_successors() failed for node {}",
            node
        );

        let untyped: Vec<_> = subgraph
            .untyped_labeled_successors(node)
            .map(|(succ, labels)| (succ, labels.collect::<Vec<_>>()))
            .collect();
        assert_eq!(
            untyped,
            untype_labeled_successors(expected_labeled),
            "untyped_labeled_successors() failed for node {}",
            node
        );
    }
}
