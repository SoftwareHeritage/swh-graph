// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use anyhow::{bail, Result};

use swh_graph::collections::{AdaptiveNodeSet, NodeSet};
use swh_graph::graph::*;
use swh_graph::labels::FilenameId;
use swh_graph::utils::GetIndex;
use swh_graph::SWHType;

/// Value in the path_stack between two lists of path parts
const PATH_SEPARATOR: FilenameId = FilenameId(u64::MAX);

/// Yielded by `dfs_with_path` to allow building a path as a `Vec<u8>` only when needed
pub struct PathParts<'a> {
    rev_directory_names: &'a [FilenameId],
    filename: Option<FilenameId>,
}

impl<'a> PathParts<'a> {
    pub fn build_path<G>(&self, graph: &G) -> Vec<u8>
    where
        G: SwhGraphWithProperties,
        <G as SwhGraphWithProperties>::LabelNames: swh_graph::properties::LabelNames,
    {
        let mut path = Vec::with_capacity(self.rev_directory_names.len() * 2 + 1);
        for &part in self.rev_directory_names.iter().rev() {
            path.extend(graph.properties().label_name(part));
            path.push(b'/');
        }
        if let Some(filename) = self.filename {
            path.extend(graph.properties().label_name(filename));
        }
        path
    }
}

/// Traverses from a root directory, calling the callbacks on any directory and content
/// node found.
///
/// If `on_directory` returns `false`, the directory's successors are ignored.
pub fn dfs_with_path<G>(
    graph: &G,
    mut on_directory: impl FnMut(NodeId, PathParts) -> Result<bool>,
    mut on_content: impl FnMut(NodeId, PathParts) -> Result<()>,
    root_dir_id: NodeId,
) -> Result<()>
where
    G: SwhLabelledForwardGraph + SwhGraphWithProperties,
    <G as SwhGraphWithProperties>::LabelNames: swh_graph::properties::LabelNames,
    <G as SwhGraphWithProperties>::Maps: swh_graph::properties::Maps,
{
    let mut visited = AdaptiveNodeSet::new(graph.num_nodes());
    let mut stack = Vec::new();

    // flattened list of paths. Each list is made of parts represented by an id,
    // and lists are separated by PATH_SEPARATOR
    let mut path_stack = Vec::new();

    stack.push(root_dir_id);
    path_stack.push(PATH_SEPARATOR);
    visited.insert(root_dir_id);

    while let Some(node) = stack.pop() {
        let mut rev_path_parts = Vec::new();
        while let Some(filename_id) = path_stack.pop() {
            if filename_id == PATH_SEPARATOR {
                break;
            }
            rev_path_parts.push(filename_id);
        }

        let should_recurse = on_directory(
            node,
            PathParts {
                rev_directory_names: &rev_path_parts,
                filename: None,
            },
        )?;

        if should_recurse {
            // Look for frontiers in subdirectories
            for (succ, labels) in graph.labelled_successors(node) {
                if visited.contains(succ) {
                    continue;
                }

                visited.insert(succ);

                match graph.properties().node_type(succ) {
                    SWHType::Directory => {
                        // If the same subdir/file is present in a directory twice under the same name,
                        // pick any name to represent both.
                        let Some(first_label) = labels.into_iter().next() else {
                            bail!(
                                "{} -> {} has no labels",
                                graph.properties().swhid(node),
                                graph.properties().swhid(succ),
                            )
                        };
                        stack.push(succ);
                        path_stack.push(PATH_SEPARATOR);
                        path_stack.extend(rev_path_parts.iter().rev().copied());
                        path_stack.push(first_label.filename_id());
                    }

                    SWHType::Content => {
                        let Some(first_label) = labels.into_iter().next() else {
                            bail!(
                                "{} -> {} has no labels",
                                graph.properties().swhid(node),
                                graph.properties().swhid(succ),
                            )
                        };
                        on_content(
                            succ,
                            PathParts {
                                rev_directory_names: &rev_path_parts,
                                filename: Some(first_label.filename_id()),
                            },
                        )?;
                    }
                    _ => (),
                }
            }
        }
    }

    Ok(())
}
