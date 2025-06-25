// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

//! Version control system (VCS) functions.

use anyhow::{ensure, Result};

use swh_graph::graph::*;
use swh_graph::labels::{EdgeLabel, LabelNameId};
use swh_graph::properties;
use swh_graph::NodeType;

/// Names of references ("branches") that are considered to be pointing to the
/// HEAD revision in a VCS, by [find_head_rev] below. Names are tried in order,
/// when attempting to identify the HEAD revision.
pub const HEAD_REF_NAMES: [&str; 2] = ["refs/heads/main", "refs/heads/master"];

/// Given a graph and a snapshot node in it, return the node id of the revision
/// pointed by the HEAD branch, it it exists.
pub fn find_head_rev<G>(graph: &G, snp: NodeId) -> Result<Option<NodeId>>
where
    G: SwhLabeledForwardGraph + SwhGraphWithProperties,
    <G as SwhGraphWithProperties>::LabelNames: properties::LabelNames,
    <G as SwhGraphWithProperties>::Maps: properties::Maps,
{
    let props = graph.properties();
    let head_ref_name_ids: Vec<LabelNameId> = HEAD_REF_NAMES
        .into_iter()
        .filter_map(|name| props.label_name_id(name.as_bytes()).ok())
        // Note, we ignore errors on purpose here, because some ref names that
        // do exist on the SWH graph might be missing in user-provided graphs,
        // and will fail [label_name_id] call.
        .collect();
    find_head_rev_by_refs(graph, snp, &head_ref_name_ids)
}

/// Same as [find_head_rev], but with the ability to configure which branch
/// names correspond to the HEAD revision.
///
/// Note: this function is also more efficient than [find_head_rev], because
/// branch names are pre-resolved to integers once (before calling this
/// function) and compared as integers.  See the source code of [find_head_rev]
/// for an example of how to translate a given set of branch names (like
/// [HEAD_REF_NAMES]) to label-name IDs for this function.
pub fn find_head_rev_by_refs<G>(
    graph: &G,
    snp: NodeId,
    ref_name_ids: &[LabelNameId],
) -> Result<Option<NodeId>>
where
    G: SwhLabeledForwardGraph + SwhGraphWithProperties,
    <G as SwhGraphWithProperties>::Maps: properties::Maps,
    <G as SwhGraphWithProperties>::LabelNames: properties::LabelNames,
{
    let props = graph.properties();
    let node_type = props.node_type(snp);
    ensure!(
        node_type == NodeType::Snapshot,
        "Type of {snp} should be snp, but is {node_type} instead"
    );
    for (succ, labels) in graph.labeled_successors(snp) {
        let node_type = props.node_type(succ);
        if node_type != NodeType::Revision && node_type != NodeType::Release {
            continue;
        }
        for label in labels {
            if let EdgeLabel::Branch(branch) = label {
                if ref_name_ids.contains(&branch.label_name_id()) {
                    return Ok(Some(succ));
                }
            }
        }
    }
    Ok(None)
}
