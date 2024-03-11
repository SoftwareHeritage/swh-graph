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
const PATH_SEPARATOR: FilenameId = FilenameId(u64::MIN);

/// Traverses from a directory, and calls a function on each frontier directory
/// it contains.
///
/// Frontier directories are detected using the `is_frontier` function, and
/// `on_frontier` is called for each of them
pub fn find_frontiers_in_root_directory<G>(
    graph: &G,
    max_timestamps: impl GetIndex<Output = i64>,
    mut is_frontier: impl FnMut(NodeId, i64) -> bool,
    mut on_frontier: impl FnMut(NodeId, i64, Vec<u8>) -> Result<()>,
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

    while let Some(node) = stack.pop() {
        if visited.contains(node) {
            continue;
        }
        visited.insert(node);

        let mut path_parts = Vec::new();
        for &filename_id in path_stack.iter().rev() {
            if filename_id == PATH_SEPARATOR {
                break;
            }
            path_parts.push(filename_id);
        }

        let dir_max_timestamp = max_timestamps.get(node).expect("max_timestamps too small");
        if dir_max_timestamp == i64::MIN {
            // Somehow does not have a max timestamp. Presumably because it does not
            // have any content.
            continue;
        }

        let node_is_frontier = is_frontier(node, dir_max_timestamp);

        if node_is_frontier {
            let mut path = Vec::with_capacity(path_parts.len() * 2 + 1);
            for part in path_parts {
                path.extend(
                    graph
                        .properties()
                        .label_name(part)
                        .expect("Unknown filename id"),
                );
                path.push(b'/');
            }
            on_frontier(node, dir_max_timestamp, path)?;
        } else {
            // Look for frontiers in subdirectories
            for (succ, labels) in graph.labelled_successors(node) {
                if visited.contains(succ) {
                    continue;
                }
                if !visited.contains(succ)
                    && graph
                        .properties()
                        .node_type(succ)
                        .expect("Missing node type")
                        == SWHType::Directory
                {
                    // If the same subdir/file is present in a directory twice under the same name,
                    // pick any name to represent both.
                    let Some(first_label) = labels.into_iter().next() else {
                        bail!(
                            "{} -> {} has no labels",
                            graph.properties().swhid(node).expect("Missing SWHID"),
                            graph.properties().swhid(succ).expect("Missing SWHID"),
                        )
                    };
                    stack.push(succ);
                    path_stack.push(PATH_SEPARATOR);
                    path_stack.extend(path_parts.iter().copied());
                    path_stack.push(first_label.filename_id());
                }
            }
        }
    }

    Ok(())
}
