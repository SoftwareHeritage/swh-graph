// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::collections::HashMap;

use anyhow::Result;

use swh_graph::graph::*;
use swh_graph::labels::FilenameId;
use swh_graph::NodeType;

/// Count the number of occurrences of each name to point to the content
pub fn count_file_names<G>(graph: &G, node: NodeId) -> Result<HashMap<FilenameId, u64>>
where
    G: SwhGraphWithProperties + SwhLabeledBackwardGraph,
    <G as SwhGraphWithProperties>::Maps: swh_graph::properties::Maps,
{
    let mut names = HashMap::new();
    for (dir, labels) in graph.untyped_labeled_predecessors(node) {
        if graph.properties().node_type(dir) != NodeType::Directory {
            continue;
        }
        for label in labels {
            // This is a dir->cnt arc, so its label has to be a DirEntry
            let label: swh_graph::labels::DirEntry = label.into();
            *names.entry(label.filename_id()).or_default() += 1;
        }
    }

    Ok(names)
}
