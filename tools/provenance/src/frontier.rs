// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use anyhow::{bail, Result};
use sux::prelude::BitVec;

use swh_graph::collections::{AdaptiveNodeSet, NodeSet};
use swh_graph::graph::*;
use swh_graph::labels::FilenameId;
use swh_graph::SWHType;

/// Value in the path_stack between two lists of path parts
const PATH_SEPARATOR: FilenameId = FilenameId(u64::MAX);

/// Yielded by `dfs_with_path` to allow building a path as a `Vec<u8>` only when needed
pub struct PathParts<'a> {
    parts: &'a [FilenameId],
    path_to_directory: bool,
}

impl<'a> PathParts<'a> {
    pub fn build_path<G>(&self, graph: &G) -> Vec<u8>
    where
        G: SwhGraphWithProperties,
        <G as SwhGraphWithProperties>::LabelNames: swh_graph::properties::LabelNames,
    {
        let mut path = Vec::with_capacity(self.parts.len() * 2 + 1);
        for &part in self.parts.iter() {
            path.extend(graph.properties().label_name(part));
            path.push(b'/');
        }
        if !self.path_to_directory {
            path.pop();
        }
        path
    }
}

/// Traverses backward from a directory or content, calling the callbacks on any directory
/// or revision node found.
///
/// `reachable_nodes` is the set of all nodes reachable from a head revision/release
/// (nodes not in the set are ignored).
///
/// If `on_directory` returns `false`, the directory's predecessors are ignored.
///
/// FIXME: `on_directory` is always called on the `root`, even if the `root` is a content
pub fn backward_dfs_with_path<G>(
    graph: &G,
    reachable_nodes: &BitVec,
    mut on_directory: impl FnMut(NodeId, PathParts) -> Result<bool>,
    mut on_revrel: impl FnMut(NodeId, PathParts) -> Result<()>,
    root: NodeId,
) -> Result<()>
where
    G: SwhLabelledBackwardGraph + SwhGraphWithProperties,
    <G as SwhGraphWithProperties>::LabelNames: swh_graph::properties::LabelNames,
    <G as SwhGraphWithProperties>::Maps: swh_graph::properties::Maps,
{
    if !reachable_nodes.get(root) {
        return Ok(());
    }

    let mut visited = AdaptiveNodeSet::new(graph.num_nodes());
    let mut stack = Vec::new();

    // flattened list of paths. Each list is made of parts represented by an id,
    // and lists are separated by PATH_SEPARATOR.
    // Parts are in the order of traversal; ie. backward.
    let mut path_stack = Vec::new();

    let root_is_directory = match graph.properties().node_type(root) {
        SWHType::Content => false,
        SWHType::Directory => true,
        _ => panic!(
            "backward_dfs_with_path called started from {}",
            graph.properties().swhid(root)
        ),
    };

    stack.push(root);
    path_stack.push(PATH_SEPARATOR);
    visited.insert(root);

    while let Some(node) = stack.pop() {
        let mut path_parts = Vec::new();
        while let Some(filename_id) = path_stack.pop() {
            if filename_id == PATH_SEPARATOR {
                break;
            }
            path_parts.push(filename_id);
        }

        let should_recurse = on_directory(
            node,
            PathParts {
                parts: &path_parts,
                path_to_directory: root_is_directory,
            },
        )?;

        if should_recurse {
            // Look for frontiers in subdirectories
            for (pred, labels) in graph.labelled_predecessors(node) {
                if !reachable_nodes.get(pred) || visited.contains(pred) {
                    continue;
                }

                visited.insert(pred);

                match graph.properties().node_type(pred) {
                    SWHType::Directory => {
                        // If the same subdir/file is present in a directory twice under the same name,
                        // pick any name to represent both.
                        let Some(first_label) = labels.into_iter().next() else {
                            bail!(
                                "{} <- {} has no labels",
                                graph.properties().swhid(node),
                                graph.properties().swhid(pred),
                            )
                        };

                        // This is a dir->* arc, so its label is necessarily a DirEntry
                        let first_label: swh_graph::labels::DirEntry = first_label.into();

                        stack.push(pred);
                        path_stack.push(PATH_SEPARATOR);
                        path_stack.extend(path_parts.iter().rev().copied());
                        path_stack.push(first_label.filename_id());
                    }

                    SWHType::Revision | SWHType::Release => {
                        on_revrel(
                            pred,
                            PathParts {
                                parts: &path_parts,
                                path_to_directory: root_is_directory,
                            },
                        )?;

                        if graph.properties().node_type(pred) == SWHType::Revision {
                            for predpred in graph.predecessors(pred) {
                                if graph.properties().node_type(predpred) == SWHType::Release {
                                    on_revrel(
                                        predpred,
                                        PathParts {
                                            parts: &path_parts,
                                            path_to_directory: root_is_directory,
                                        },
                                    )?;
                                }
                            }
                        }
                    }
                    _ => (),
                }
            }
        }
    }

    Ok(())
}
