// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use swh_graph::collections::{AdaptiveNodeSet, NodeSet};
use swh_graph::graph::*;
use swh_graph::NodeType;

#[derive(Debug, PartialEq, Eq)]
pub struct EarliestRevision {
    pub node: usize,
    pub ts: i64,
    pub rev_occurrences: u64,
}

/// Given a content/directory id, returns the id of the oldest revision that contains it
pub fn find_earliest_revision<G>(graph: &G, src: usize) -> Option<EarliestRevision>
where
    G: SwhBackwardGraph + SwhGraphWithProperties,
    <G as SwhGraphWithProperties>::Maps: swh_graph::properties::Maps,
    <G as SwhGraphWithProperties>::Timestamps: swh_graph::properties::Timestamps,
{
    // Initialize the DFS
    let mut stack = Vec::new();
    let mut visited = AdaptiveNodeSet::new(graph.num_nodes());
    stack.push(src);
    visited.insert(src);

    let mut visited_revisions = 0u64;
    // pair of (earliest_rev, earliest_ts)
    let mut earliest_rev: Option<(usize, i64)> = None;

    while let Some(node) = stack.pop() {
        let node_type = graph.properties().node_type(node);
        if node_type == NodeType::Revision {
            visited_revisions += 1;
            let Some(committer_ts) = graph.properties().committer_timestamp(node) else {
                continue;
            };
            if committer_ts != i64::MIN && committer_ts != 0 {
                // exclude missing and zero (= epoch) as plausible earliest timestamps
                // as they are almost certainly bogus values

                // Update earliest_rev if the current node has a lower timestamp
                match earliest_rev {
                    None => earliest_rev = Some((node, committer_ts)),
                    Some((_, earliest_ts)) if committer_ts < earliest_ts => {
                        earliest_rev = Some((node, committer_ts))
                    }
                    _ => (),
                }
            }
        } else {
            // push ancestors to the stack
            for pred in graph.predecessors(node) {
                use swh_graph::NodeType::*;

                if visited.contains(pred) {
                    continue;
                }

                let pred_type = graph.properties().node_type(pred);

                // Only arcs with type cnt:dir,dir:dir,dir:rev
                match (node_type, pred_type) {
                    (Content, Directory) | (Directory, Directory) | (Directory, Revision) => {
                        stack.push(pred);
                        visited.insert(pred);
                    }
                    _ => {}
                }
            }
        }
    }

    earliest_rev.map(|(earliest_rev_id, earliest_ts)| EarliestRevision {
        node: earliest_rev_id,
        ts: earliest_ts,
        rev_occurrences: visited_revisions,
    })
}
