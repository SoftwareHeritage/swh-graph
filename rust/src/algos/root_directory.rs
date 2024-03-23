// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use anyhow::{bail, ensure, Result};

use crate::graph::*;
use crate::properties;
use crate::SWHType;

/// Given a revision or release id, returns the id of its highest directory.
///
/// If the release points to a revision, this function recurses once through that revision.
pub fn get_root_directory_from_revision_or_release<G>(
    graph: &G,
    node: NodeId,
) -> Result<Option<NodeId>>
where
    G: SwhBackwardGraph + SwhForwardGraph + SwhGraphWithProperties,
    <G as SwhGraphWithProperties>::Maps: properties::Maps,
{
    let node_type = graph.properties().node_type(node);

    match node_type {
        SWHType::Release => get_root_directory_from_release(graph, node),
        SWHType::Revision => get_root_directory_from_revision(graph, node),
        _ => Ok(None), // Ignore this non-rev/rel node
    }
}

fn get_root_directory_from_release<G>(graph: &G, rel_id: NodeId) -> Result<Option<NodeId>>
where
    G: SwhForwardGraph + SwhGraphWithProperties,
    <G as SwhGraphWithProperties>::Maps: properties::Maps,
{
    let rel_swhid = graph.properties().swhid(rel_id);

    let mut root_dir = None;
    let mut root_rev = None;
    for succ in graph.successors(rel_id) {
        let node_type = graph.properties().node_type(succ);
        match node_type {
            SWHType::Directory => {
                ensure!(
                    root_dir.is_none(),
                    "{rel_swhid} has more than one directory successor",
                );
                root_dir = Some(succ);
            }
            SWHType::Revision => {
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
                if graph.properties().node_type(succ) == SWHType::Directory {
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

fn get_root_directory_from_revision<G>(graph: &G, rev_id: NodeId) -> Result<Option<NodeId>>
where
    G: SwhForwardGraph + SwhGraphWithProperties,
    <G as SwhGraphWithProperties>::Maps: properties::Maps,
{
    let mut root_dir = None;
    for succ in graph.successors(rev_id) {
        let node_type = graph.properties().node_type(succ);
        if node_type == SWHType::Directory {
            let rev_swhid = graph.properties().swhid(succ);
            ensure!(
                root_dir.is_none(),
                "{rev_swhid} has more than one directory successor",
            );
            root_dir = Some(succ);
        }
    }

    Ok(root_dir)
}
