// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use anyhow::Result;
use std::rc::Rc;
use swh_graph::graph::*;
use swh_graph::java_compat::mph::gov::GOVMPH;
use swh_graph::views::Subgraph;
use swh_graph::{swhid, NodeConstraint};

const BASENAME: &str = "../swh/graph/example_dataset/compressed/example";

#[test]
fn test_node_constraint() -> Result<()> {
    let graph = load_unidirectional(BASENAME)?
        .load_all_properties::<GOVMPH>()?
        .load_labels()?;
    let graph = Rc::new(graph);

    let full_graph =
        Subgraph::with_node_constraint(graph.clone(), "*".parse::<NodeConstraint>().unwrap());
    let props = full_graph.properties();
    // assert_eq!(full_graph.num_nodes(), graph.num_nodes());  // num_nodes() is no-op on subgraphs
    let cnt_node = props.node_id(swhid!(swh:1:cnt:0000000000000000000000000000000000000001))?;
    let rel_node = props.node_id(swhid!(swh:1:rel:0000000000000000000000000000000000000010))?;
    let snp_node = props.node_id(swhid!(swh:1:snp:0000000000000000000000000000000000000020))?;
    assert!(full_graph.has_node(cnt_node));
    assert!(full_graph.has_node(rel_node));
    assert!(full_graph.has_node(snp_node));

    let fs_graph =
        Subgraph::with_node_constraint(graph.clone(), "dir,cnt".parse::<NodeConstraint>().unwrap());
    let props = fs_graph.properties();
    let cnt_node = props.node_id(swhid!(swh:1:cnt:0000000000000000000000000000000000000001))?;
    let dir_node = props.node_id(swhid!(swh:1:dir:0000000000000000000000000000000000000002))?;
    // assert_eq!(fs_graph.num_nodes(), 13);  // num_nodes() is no-op on subgraphs
    assert!(fs_graph.has_node(cnt_node));
    assert!(fs_graph.has_node(dir_node));
    assert!(fs_graph.has_arc(dir_node, cnt_node));
    assert!(!fs_graph
        .has_node(props.node_id(swhid!(swh:1:rev:0000000000000000000000000000000000000003))?));

    let history_graph = Subgraph::with_node_constraint(graph.clone(), "rel,rev".parse().unwrap());
    let props = history_graph.properties();
    let rel_node = props.node_id(swhid!(swh:1:rel:0000000000000000000000000000000000000010))?;
    let rev_node = props.node_id(swhid!(swh:1:rev:0000000000000000000000000000000000000009))?;
    // assert_eq!(history_graph.num_nodes(), 7);  // num_nodes() is no-op on subgraphs
    assert!(history_graph.has_node(rel_node));
    assert!(history_graph.has_node(rev_node));
    assert!(history_graph.has_arc(rel_node, rev_node));
    assert!(!history_graph
        .has_node(props.node_id(swhid!(swh:1:cnt:0000000000000000000000000000000000000001))?));

    Ok(())
}
