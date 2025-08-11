// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

//! Standard library to work on Software Heritage compressed graph in Rust

use anyhow::{ensure, Result};

use swh_graph::graph::*;
use swh_graph::labels::{EdgeLabel, VisitStatus};
use swh_graph::properties;
use swh_graph::NodeType;

/// Given a graph and an origin node in it, return the node id and timestamp
/// (as a number of seconds since Epoch) of the most recent snapshot of that
/// origin, if it exists.
///
/// Note: only visit with status `Full` are considered when selecting
/// snapshots.
pub fn find_latest_snp<G>(graph: &G, ori: NodeId) -> Result<Option<(NodeId, u64)>>
where
    G: SwhLabeledForwardGraph + SwhGraphWithProperties,
    <G as SwhGraphWithProperties>::Maps: properties::Maps,
{
    let props = graph.properties();
    let node_type = props.node_type(ori);
    ensure!(
        node_type == NodeType::Origin,
        "Type of {ori} should be ori, but is {node_type} instead"
    );
    // Most recent snapshot thus far, as an optional (node_id, timestamp) pair
    let mut latest_snp: Option<(usize, u64)> = None;
    for (succ, labels) in graph.labeled_successors(ori) {
        let node_type = props.node_type(succ);
        if node_type != NodeType::Snapshot {
            continue;
        }
        for label in labels {
            if let EdgeLabel::Visit(visit) = label {
                if visit.status() != VisitStatus::Full {
                    continue;
                }
                let ts = visit.timestamp();
                if let Some((_cur_snp, cur_ts)) = latest_snp {
                    if ts > cur_ts {
                        latest_snp = Some((succ, ts))
                    }
                } else {
                    latest_snp = Some((succ, ts))
                }
            }
        }
    }
    Ok(latest_snp)
}

pub mod collections;

mod fs;
pub use fs::*;

mod root_directory;
pub use root_directory::find_root_dir;

mod vcs;
pub use vcs::*;

mod visit;
pub use visit::*;

pub mod diff;
