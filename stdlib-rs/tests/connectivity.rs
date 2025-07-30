// Copyright (C) 2025  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::sync::Arc;

use anyhow::Result;
use epserde::prelude::{Deserialize, Serialize};

use swh_graph::graph_builder::{BuiltGraph, GraphBuilder};
use swh_graph::swhid;

use swh_graph_stdlib::connectivity::{SubgraphWccs, SubgraphWccsState};

/// ```
/// 0 -> 1 -> 2
///      ^
///     /
/// 3 --
/// 4 -> 5 -> 6
/// 7 -> 8
/// ```
fn build_graph() -> BuiltGraph {
    let mut builder = GraphBuilder::default();
    builder
        .node(swhid!(swh:1:rev:0000000000000000000000000000000000000000))
        .unwrap()
        .done();
    builder
        .node(swhid!(swh:1:rev:0000000000000000000000000000000000000001))
        .unwrap()
        .done();
    builder
        .node(swhid!(swh:1:rev:0000000000000000000000000000000000000002))
        .unwrap()
        .done();
    builder
        .node(swhid!(swh:1:rev:0000000000000000000000000000000000000003))
        .unwrap()
        .done();
    builder
        .node(swhid!(swh:1:rev:0000000000000000000000000000000000000004))
        .unwrap()
        .done();
    builder
        .node(swhid!(swh:1:rev:0000000000000000000000000000000000000005))
        .unwrap()
        .done();
    builder
        .node(swhid!(swh:1:rev:0000000000000000000000000000000000000006))
        .unwrap()
        .done();
    builder
        .node(swhid!(swh:1:rev:0000000000000000000000000000000000000007))
        .unwrap()
        .done();
    builder
        .node(swhid!(swh:1:rev:0000000000000000000000000000000000000008))
        .unwrap()
        .done();
    builder.arc(0, 1);
    builder.arc(1, 2);
    builder.arc(3, 1);
    builder.arc(4, 5);
    builder.arc(5, 6);
    builder.arc(7, 8);

    builder.done().unwrap()
}

#[test]
fn test_subgraphwccs_from_closure_two_components() -> Result<()> {
    let graph = Arc::new(build_graph());

    let mut wccs = SubgraphWccs::build_from_closure(graph.clone(), [0, 1, 2, 3, 4, 5, 6])?;
    assert_eq!(
        &*wccs.sort_by_size(),
        &[4, 3],
        "{:?}",
        (0..9).map(|node| wccs.component(node)).collect::<Vec<_>>()
    );
    assert_eq!(
        (0..9).map(|node| wccs.component(node)).collect::<Vec<_>>(),
        vec![
            Some(0),
            Some(0),
            Some(0),
            Some(0),
            Some(1),
            Some(1),
            Some(1),
            None,
            None
        ]
    );

    let mut wccs = SubgraphWccs::build_from_closure(graph.clone(), [0, 1, 3, 6])?;
    assert_eq!(
        &*wccs.sort_by_size(),
        &[4, 3],
        "{:?}",
        (0..9).map(|node| wccs.component(node)).collect::<Vec<_>>()
    );
    assert_eq!(
        (0..9).map(|node| wccs.component(node)).collect::<Vec<_>>(),
        vec![
            Some(0),
            Some(0),
            Some(0),
            Some(0),
            Some(1),
            Some(1),
            Some(1),
            None,
            None
        ]
    );

    let mut wccs = SubgraphWccs::build_from_closure(graph.clone(), [0, 3, 6])?;
    assert_eq!(
        &*wccs.sort_by_size(),
        &[4, 3],
        "{:?}",
        (0..9).map(|node| wccs.component(node)).collect::<Vec<_>>()
    );
    assert_eq!(
        (0..9).map(|node| wccs.component(node)).collect::<Vec<_>>(),
        vec![
            Some(0),
            Some(0),
            Some(0),
            Some(0),
            Some(1),
            Some(1),
            Some(1),
            None,
            None
        ]
    );

    Ok(())
}

#[test]
fn test_subgraphwccs_from_closure_one_components() -> Result<()> {
    let graph = Arc::new(build_graph());

    let mut wccs = SubgraphWccs::build_from_closure(graph.clone(), [0, 1, 2, 3])?;
    assert_eq!(
        &*wccs.sort_by_size(),
        &[4],
        "{:?}",
        (0..9).map(|node| wccs.component(node)).collect::<Vec<_>>()
    );
    assert_eq!(
        (0..9).map(|node| wccs.component(node)).collect::<Vec<_>>(),
        vec![
            Some(0),
            Some(0),
            Some(0),
            Some(0),
            None,
            None,
            None,
            None,
            None
        ]
    );

    let mut wccs = SubgraphWccs::build_from_closure(graph.clone(), [3])?;
    assert_eq!(
        &*wccs.sort_by_size(),
        &[4],
        "{:?}",
        (0..9).map(|node| wccs.component(node)).collect::<Vec<_>>()
    );
    assert_eq!(
        (0..9).map(|node| wccs.component(node)).collect::<Vec<_>>(),
        vec![
            Some(0),
            Some(0),
            Some(0),
            Some(0),
            None,
            None,
            None,
            None,
            None
        ]
    );

    Ok(())
}

#[test]
fn test_subgraphwccs_from_nodes_two_components() -> Result<()> {
    let graph = Arc::new(build_graph());

    let mut wccs = SubgraphWccs::build_from_nodes(graph.clone(), vec![0, 1, 2, 3, 4, 5, 6])?;
    assert_eq!(
        &*wccs.sort_by_size(),
        &[4, 3],
        "{:?}",
        (0..9).map(|node| wccs.component(node)).collect::<Vec<_>>()
    );
    assert_eq!(
        (0..9).map(|node| wccs.component(node)).collect::<Vec<_>>(),
        vec![
            Some(0),
            Some(0),
            Some(0),
            Some(0),
            Some(1),
            Some(1),
            Some(1),
            None,
            None
        ]
    );

    let mut wccs = SubgraphWccs::build_from_nodes(graph.clone(), vec![0, 1, 3, 5])?;
    assert_eq!(
        &*wccs.sort_by_size(),
        &[3, 1],
        "{:?}",
        (0..9).map(|node| wccs.component(node)).collect::<Vec<_>>()
    );
    assert_eq!(
        (0..9).map(|node| wccs.component(node)).collect::<Vec<_>>(),
        vec![
            Some(0),
            Some(0),
            None,
            Some(0),
            None,
            Some(1),
            None,
            None,
            None
        ]
    );

    let mut wccs = SubgraphWccs::build_from_nodes(graph.clone(), vec![0, 1, 2, 5, 6])?;
    assert_eq!(
        &*wccs.sort_by_size(),
        &[3, 2],
        "{:?}",
        (0..9).map(|node| wccs.component(node)).collect::<Vec<_>>()
    );
    assert_eq!(
        (0..9).map(|node| wccs.component(node)).collect::<Vec<_>>(),
        vec![
            Some(0),
            Some(0),
            Some(0),
            None,
            None,
            Some(1),
            Some(1),
            None,
            None
        ]
    );

    let mut wccs = SubgraphWccs::build_from_nodes(graph.clone(), vec![1, 4, 5, 6])?;
    assert_eq!(
        &*wccs.sort_by_size(),
        &[3, 1],
        "{:?}",
        (0..9).map(|node| wccs.component(node)).collect::<Vec<_>>()
    );
    assert_eq!(
        (0..9).map(|node| wccs.component(node)).collect::<Vec<_>>(),
        vec![
            None,
            Some(1),
            None,
            None,
            Some(0),
            Some(0),
            Some(0),
            None,
            None
        ]
    );

    Ok(())
}

#[test]
fn test_subgraphwccs_epserde() -> Result<()> {
    let graph = Arc::new(build_graph());

    let mut original = SubgraphWccs::build_from_closure(graph.clone(), [0, 1, 2, 3, 4, 5, 6])?;

    let mut file = std::io::Cursor::new(vec![]);
    let (_graph, original_state) = original.as_parts();
    original_state.serialize(&mut file)?;
    let data = file.into_inner();
    let deserialized_state = <SubgraphWccsState>::deserialize_eps(&data)?;
    let deserialized = SubgraphWccs::from_parts(graph, deserialized_state)?;

    assert_eq!(original.num_components(), deserialized.num_components());
    assert_eq!(
        (0..9)
            .map(|node| original.component(node))
            .collect::<Vec<_>>(),
        (0..9)
            .map(|node| deserialized.component(node))
            .collect::<Vec<_>>()
    );

    Ok(())
}
