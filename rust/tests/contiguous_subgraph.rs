// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

#![cfg(feature = "unstable_contiguous_subgraph")]

use anyhow::Result;
use sux::prelude::EliasFanoBuilder;

use swh_graph::graph::*;
use swh_graph::graph_builder::{BuiltGraph, GraphBuilder};
use swh_graph::views::{ContiguousSubgraph, NodeMap};
use swh_graph::{swhid, NodeType};

fn build_graph() -> Result<BuiltGraph> {
    let mut builder = GraphBuilder::default();
    let ori01 = builder
        .node(swhid!(swh:1:ori:0000000000000000000000000000000000000001))?
        .done();
    let snp20 = builder
        .node(swhid!(swh:1:snp:0000000000000000000000000000000000000020))?
        .done();
    let rev09 = builder
        .node(swhid!(swh:1:rev:0000000000000000000000000000000000000009))?
        .author(b"Rev9 author".into())
        .committer(b"Rev9 committer".into())
        .author_timestamp(10009, 60)
        .committer_timestamp(20009, 120)
        .message(b"revision 9".into())
        .done();
    let rev10 = builder
        .node(swhid!(swh:1:rev:0000000000000000000000000000000000000010))?
        .author(b"Rev10 author".into())
        .committer(b"Rev10 committer".into())
        .author_timestamp(10010, 180)
        .committer_timestamp(20010, 240)
        .message(b"revision 10".into())
        .tag_name(b"tag name (illegal for a revision, but who cares, it's a test)".into())
        .done();
    let dir08 = builder
        .node(swhid!(swh:1:dir:0000000000000000000000000000000000000008))?
        .done();
    let cnt01 = builder
        .node(swhid!(swh:1:cnt:0000000000000000000000000000000000000001))?
        .content_length(1001)
        .is_skipped_content(false)
        .done();
    let cnt02 = builder
        .node(swhid!(swh:1:cnt:0000000000000000000000000000000000000002))?
        .content_length(1002)
        .is_skipped_content(true)
        .done();
    builder.arc(ori01, snp20);
    builder.arc(snp20, rev09);
    builder.arc(rev09, rev10);
    builder.arc(rev10, dir08);
    builder.arc(dir08, cnt01);
    builder.arc(dir08, cnt02);
    builder.done()
}

#[test]
fn test_contiguous_full_graph() -> Result<()> {
    let graph = build_graph()?;
    let props = graph.properties();

    let ori_node = props.node_id(swhid!(swh:1:ori:0000000000000000000000000000000000000001))?;
    let snp_node = props.node_id(swhid!(swh:1:snp:0000000000000000000000000000000000000020))?;
    let rev_node1 = props.node_id(swhid!(swh:1:rev:0000000000000000000000000000000000000009))?;
    let rev_node2 = props.node_id(swhid!(swh:1:rev:0000000000000000000000000000000000000010))?;
    let dir_node = props.node_id(swhid!(swh:1:dir:0000000000000000000000000000000000000008))?;
    let cnt_node1 = props.node_id(swhid!(swh:1:cnt:0000000000000000000000000000000000000001))?;
    let cnt_node2 = props.node_id(swhid!(swh:1:cnt:0000000000000000000000000000000000000002))?;

    let mut nodes_efb = EliasFanoBuilder::new(graph.num_nodes(), graph.num_nodes());
    for node in 0..graph.num_nodes() {
        nodes_efb.push(node);
    }
    let node_map = NodeMap(nodes_efb.build_with_seq_and_dict());
    let full_graph = ContiguousSubgraph::new(&graph, node_map)
        .with_maps()
        .with_contents()
        .with_timestamps()
        .with_persons()
        .with_strings();
    for node in 0..graph.num_nodes() {
        assert!(full_graph.has_node(node));
        assert_eq!(
            full_graph.properties().author_id(node),
            graph.properties().author_id(node)
        );
        assert_eq!(
            full_graph.properties().committer_id(node),
            graph.properties().committer_id(node)
        );
        assert_eq!(
            full_graph.properties().author_timestamp(node),
            graph.properties().author_timestamp(node)
        );
        assert_eq!(
            full_graph.properties().author_timestamp_offset(node),
            graph.properties().author_timestamp_offset(node)
        );
        assert_eq!(
            full_graph.properties().committer_timestamp(node),
            graph.properties().committer_timestamp(node)
        );
        assert_eq!(
            full_graph.properties().committer_timestamp_offset(node),
            graph.properties().committer_timestamp_offset(node)
        );
        assert_eq!(
            full_graph.properties().message(node),
            graph.properties().message(node)
        );
        assert_eq!(
            full_graph.properties().tag_name(node),
            graph.properties().tag_name(node)
        );
        assert_eq!(
            full_graph.properties().is_skipped_content(node),
            graph.properties().is_skipped_content(node)
        );
        assert_eq!(
            full_graph.properties().content_length(node),
            graph.properties().content_length(node)
        );
        let swhid = graph.properties().swhid(node);
        assert_eq!(full_graph.properties().swhid(node), swhid);
        assert_eq!(full_graph.properties().node_id(swhid), Ok(node));
    }
    assert!(full_graph.has_arc(ori_node, snp_node));
    assert!(full_graph.has_arc(rev_node1, rev_node2));
    assert!(full_graph.has_arc(rev_node2, dir_node));
    assert!(full_graph.has_arc(dir_node, cnt_node1));
    assert!(full_graph.has_arc(dir_node, cnt_node2));
    Ok(())
}

#[test]
fn test_contiguous_fs_graph() -> Result<()> {
    let graph = build_graph()?;
    let mut nodes_efb = EliasFanoBuilder::new(3, graph.num_nodes());
    for node in 0..graph.num_nodes() {
        match graph.properties().node_type(node) {
            NodeType::Content | NodeType::Directory => nodes_efb.push(node),
            _ => (),
        }
    }
    let node_map = NodeMap(nodes_efb.build_with_seq_and_dict());
    let fs_graph = ContiguousSubgraph::new(&graph, node_map)
        .with_maps()
        .with_contents()
        .with_timestamps()
        .with_persons()
        .with_strings();
    assert_eq!(
        (0..3)
            .map(|node| fs_graph.properties().swhid(node))
            .collect::<Vec<_>>(),
        vec![
            swhid!(swh:1:dir:0000000000000000000000000000000000000008),
            swhid!(swh:1:cnt:0000000000000000000000000000000000000001),
            swhid!(swh:1:cnt:0000000000000000000000000000000000000002),
        ]
    );
    assert_eq!(
        fs_graph
            .properties()
            .node_id(swhid!(swh:1:dir:0000000000000000000000000000000000000008)),
        Ok(0)
    );
    assert_eq!(
        fs_graph
            .properties()
            .node_id(swhid!(swh:1:cnt:0000000000000000000000000000000000000001)),
        Ok(1)
    );
    assert_eq!(
        fs_graph
            .properties()
            .node_id(swhid!(swh:1:cnt:0000000000000000000000000000000000000002)),
        Ok(2)
    );
    assert!(fs_graph.has_node(0)); // dir_node
    assert!(fs_graph.has_node(1)); // cnt_node1
    assert!(fs_graph.has_node(2)); // cnt_node2

    // in the underlying graph, but not in the subgraph
    let swhid = swhid!(swh:1:rev:0000000000000000000000000000000000000010);
    assert_eq!(
        fs_graph.properties().node_id(swhid),
        Err(swh_graph::properties::NodeIdFromSwhidError::UnknownSwhid(
            swhid
        ))
    );
    assert!(!fs_graph.has_node(3));

    assert_eq!(fs_graph.successors(0).collect::<Vec<_>>(), vec![1, 2]);
    assert_eq!(
        fs_graph.successors(1).collect::<Vec<_>>(),
        Vec::<NodeId>::new()
    );
    assert_eq!(
        fs_graph.successors(2).collect::<Vec<_>>(),
        Vec::<NodeId>::new()
    );
    assert!(fs_graph.has_arc(0, 1));
    assert!(fs_graph.has_arc(0, 2));
    assert!(!fs_graph.has_arc(1, 2));

    // contents
    assert_eq!(
        (0..3)
            .map(|node| fs_graph.properties().is_skipped_content(node))
            .collect::<Vec<_>>(),
        vec![false, false, true]
    );
    assert_eq!(
        (0..3)
            .map(|node| fs_graph.properties().content_length(node))
            .collect::<Vec<_>>(),
        vec![None, Some(1001), Some(1002)]
    );

    // persons
    assert_eq!(
        (0..3)
            .map(|node| fs_graph.properties().author_id(node))
            .collect::<Vec<_>>(),
        vec![None, None, None]
    );
    assert_eq!(
        (0..3)
            .map(|node| fs_graph.properties().committer_id(node))
            .collect::<Vec<_>>(),
        vec![None, None, None]
    );

    // timestamps
    assert_eq!(
        (0..3)
            .map(|node| fs_graph.properties().author_timestamp(node))
            .collect::<Vec<_>>(),
        vec![None, None, None]
    );
    assert_eq!(
        (0..3)
            .map(|node| fs_graph.properties().author_timestamp_offset(node))
            .collect::<Vec<_>>(),
        vec![None, None, None]
    );
    assert_eq!(
        (0..3)
            .map(|node| fs_graph.properties().committer_timestamp(node))
            .collect::<Vec<_>>(),
        vec![None, None, None]
    );
    assert_eq!(
        (0..3)
            .map(|node| fs_graph.properties().committer_timestamp_offset(node))
            .collect::<Vec<_>>(),
        vec![None, None, None]
    );

    // strings
    assert_eq!(
        (0..3)
            .map(|node| fs_graph.properties().message(node))
            .collect::<Vec<_>>(),
        vec![None, None, None]
    );
    assert_eq!(
        (0..3)
            .map(|node| fs_graph.properties().tag_name(node))
            .collect::<Vec<_>>(),
        vec![None, None, None]
    );

    Ok(())
}

#[test]
fn test_contiguous_history_graph() -> Result<()> {
    let graph = build_graph()?;
    let mut nodes_efb = EliasFanoBuilder::new(3, graph.num_nodes());
    for node in 0..graph.num_nodes() {
        match graph.properties().node_type(node) {
            NodeType::Revision | NodeType::Release | NodeType::Snapshot => nodes_efb.push(node),
            _ => (),
        }
    }
    let node_map = NodeMap(nodes_efb.build_with_seq_and_dict());
    let history_graph = ContiguousSubgraph::new(&graph, node_map)
        .with_maps()
        .with_contents()
        .with_timestamps()
        .with_persons()
        .with_strings();

    assert_eq!(
        (0..3)
            .map(|node| history_graph.properties().swhid(node))
            .collect::<Vec<_>>(),
        vec![
            swhid!(swh:1:snp:0000000000000000000000000000000000000020),
            swhid!(swh:1:rev:0000000000000000000000000000000000000009),
            swhid!(swh:1:rev:0000000000000000000000000000000000000010),
        ]
    );
    assert_eq!(
        history_graph
            .properties()
            .node_id(swhid!(swh:1:snp:0000000000000000000000000000000000000020)),
        Ok(0)
    );
    assert_eq!(
        history_graph
            .properties()
            .node_id(swhid!(swh:1:rev:0000000000000000000000000000000000000009)),
        Ok(1)
    );
    assert_eq!(
        history_graph
            .properties()
            .node_id(swhid!(swh:1:rev:0000000000000000000000000000000000000010)),
        Ok(2)
    );
    assert!(history_graph.has_node(0)); // snp_node
    assert!(history_graph.has_node(1)); // rev_node1
    assert!(history_graph.has_node(2)); // rev_node2
                                        //
                                        // in the underlying graph, but not in the subgraph
    let swhid = swhid!(swh:1:dir:0000000000000000000000000000000000000008);
    assert_eq!(
        history_graph.properties().node_id(swhid),
        Err(swh_graph::properties::NodeIdFromSwhidError::UnknownSwhid(
            swhid
        ))
    );
    assert!(!history_graph.has_node(3));

    assert_eq!(history_graph.successors(0).collect::<Vec<_>>(), vec![1]);
    assert_eq!(history_graph.successors(1).collect::<Vec<_>>(), vec![2]);
    assert_eq!(
        history_graph.successors(2).collect::<Vec<_>>(),
        Vec::<NodeId>::new()
    );
    assert!(history_graph.has_arc(0, 1));
    assert!(history_graph.has_arc(1, 2));
    assert!(!history_graph.has_arc(0, 2));

    // contents
    assert_eq!(
        (0..3)
            .map(|node| history_graph.properties().is_skipped_content(node))
            .collect::<Vec<_>>(),
        vec![false, false, false]
    );
    assert_eq!(
        (0..3)
            .map(|node| history_graph.properties().content_length(node))
            .collect::<Vec<_>>(),
        vec![None, None, None]
    );

    // persons
    assert_eq!(
        (0..3)
            .map(|node| history_graph.properties().author_id(node))
            .collect::<Vec<_>>(),
        vec![
            graph.properties().author_id(1), // snp_node
            graph.properties().author_id(2), // rev_node1
            graph.properties().author_id(3), // rev_node2
        ]
    );
    assert_eq!(
        (0..3)
            .map(|node| history_graph.properties().committer_id(node))
            .collect::<Vec<_>>(),
        vec![
            graph.properties().committer_id(1), // snp_node
            graph.properties().committer_id(2), // rev_node1
            graph.properties().committer_id(3), // rev_node2
        ]
    );

    // timestamps
    assert_eq!(
        (0..3)
            .map(|node| history_graph.properties().author_timestamp(node))
            .collect::<Vec<_>>(),
        vec![None, Some(10009), Some(10010)]
    );
    assert_eq!(
        (0..3)
            .map(|node| history_graph.properties().author_timestamp_offset(node))
            .collect::<Vec<_>>(),
        vec![None, Some(60), Some(180)]
    );
    assert_eq!(
        (0..3)
            .map(|node| history_graph.properties().committer_timestamp(node))
            .collect::<Vec<_>>(),
        vec![None, Some(20009), Some(20010)]
    );
    assert_eq!(
        (0..3)
            .map(|node| history_graph.properties().committer_timestamp_offset(node))
            .collect::<Vec<_>>(),
        vec![None, Some(120), Some(240)]
    );

    // strings
    assert_eq!(
        (0..3)
            .map(|node| history_graph.properties().message(node))
            .collect::<Vec<_>>(),
        vec![
            None,
            Some(b"revision 9".into()),
            Some(b"revision 10".into())
        ]
    );
    assert_eq!(
        (0..3)
            .map(|node| history_graph.properties().tag_name(node))
            .collect::<Vec<_>>(),
        vec![
            None,
            None,
            Some(b"tag name (illegal for a revision, but who cares, it's a test)".into())
        ]
    );

    Ok(())
}
