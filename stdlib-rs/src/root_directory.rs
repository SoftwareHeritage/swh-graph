// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use anyhow::{bail, ensure, Result};

use swh_graph::graph::*;
use swh_graph::properties;
use swh_graph::NodeType;

/// Given a node id pointing to a revision or release, returns the node id of
/// the associated topmost ("root") directory.
///
/// If the release points to a revision, this function recurses once through
/// that revision.
pub fn find_root_dir<G>(graph: &G, node: NodeId) -> Result<Option<NodeId>>
where
    G: SwhForwardGraph + SwhGraphWithProperties,
    <G as SwhGraphWithProperties>::Maps: properties::Maps,
{
    match graph.properties().node_type(node) {
        NodeType::Release => find_root_dir_from_rel(graph, node),
        NodeType::Revision => find_root_dir_from_rev(graph, node),
        ty => bail!("Expected node type release or revision, but got {ty} instead."),
    }
}

fn find_root_dir_from_rel<G>(graph: &G, rel_id: NodeId) -> Result<Option<NodeId>>
where
    G: SwhForwardGraph + SwhGraphWithProperties,
    <G as SwhGraphWithProperties>::Maps: properties::Maps,
{
    let props = graph.properties();
    let rel_swhid = props.swhid(rel_id);

    let mut root_dir = None;
    let mut root_rev = None;
    for succ in graph.successors(rel_id) {
        let node_type = props.node_type(succ);
        match node_type {
            NodeType::Directory => {
                ensure!(
                    root_dir.is_none(),
                    "{rel_swhid} has more than one directory successor",
                );
                root_dir = Some(succ);
            }
            NodeType::Revision => {
                ensure!(
                    root_rev.is_none(),
                    "{rel_swhid} has more than one revision successor",
                );
                root_rev = Some(succ);
            }
            _ => (),
        }
    }

    match (root_dir, root_rev) {
        (Some(_), Some(_)) => {
            bail!("{rel_swhid} has both a directory and a revision as successors",)
        }
        (None, Some(root_rev)) => {
            let mut root_dir = None;
            for succ in graph.successors(root_rev) {
                if graph.properties().node_type(succ) == NodeType::Directory {
                    let rev_swhid = graph.properties().swhid(succ);
                    ensure!(
                        root_dir.is_none(),
                        "{rel_swhid} (via {rev_swhid}) has more than one directory successor",
                    );
                    root_dir = Some(succ);
                }
            }
            Ok(root_dir)
        }
        (Some(root_dir), None) => Ok(Some(root_dir)),
        (None, None) => Ok(None),
    }
}

fn find_root_dir_from_rev<G>(graph: &G, rev_id: NodeId) -> Result<Option<NodeId>>
where
    G: SwhForwardGraph + SwhGraphWithProperties,
    <G as SwhGraphWithProperties>::Maps: properties::Maps,
{
    let mut root_dir = None;
    let props = graph.properties();
    for succ in graph.successors(rev_id) {
        let node_type = props.node_type(succ);
        if node_type == NodeType::Directory {
            let rev_swhid = props.swhid(succ);
            ensure!(
                root_dir.is_none(),
                "{rev_swhid} has more than one directory successor",
            );
            root_dir = Some(succ);
        }
    }

    Ok(root_dir)
}
