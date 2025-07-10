// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::rc::Rc;

use anyhow::Result;

use swh_graph::graph::*;
use swh_graph::graph_builder::GraphBuilder;
use swh_graph::views::Subgraph;
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
