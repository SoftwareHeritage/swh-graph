// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

//! Computers, for any content or directory, the first revision or release that contains it.
//!
//! The algorithm is:
//!
//! 1. Initialize an array of timestamps (as AtomicI64), one for each node, to the maximum
//!    timestamp
//! 2. For each revision/release (in parallel):
//!     a. Get its author date (if none, skip the revision/release)
//!     b. traverse all contents and directories contained by that revision/release.
//!        For each content/directory, atomically set the timestamp to the
//!        current rev/rel's timestamp if it is lower than the existing one
//! 3. Write the array
#![allow(non_snake_case)]
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use clap::Parser;
use dsi_progress_logger::{ProgressLog, ProgressLogger};
use rayon::prelude::*;

use swh_graph::collections::{AdaptiveNodeSet, NodeSet};
use swh_graph::graph::*;
use swh_graph::java_compat::mph::gov::GOVMPH;
use swh_graph::SWHType;

#[derive(Parser, Debug)]
/// Returns a directory of CSV files with header 'author_date,revrel_SWHID,cntdir_SWHID'
/// and a row for each of the contents and directories with the earliest revision/release
/// that contains them.
struct Args {
    graph_path: PathBuf,
    #[arg(short, long, action = clap::ArgAction::Count)]
    verbose: u8,
    #[arg(long)]
    /// Path to write the array of timestamps to
    timestamps_out: PathBuf,
}

pub fn main() -> Result<()> {
    let args = Args::parse();

    stderrlog::new()
        .verbosity(args.verbose as usize)
        .timestamp(stderrlog::Timestamp::Second)
        .init()
        .context("While Initializing the stderrlog")?;

    log::info!("Loading graph");
    let graph = swh_graph::graph::load_bidirectional(args.graph_path)
        .context("Could not load graph")?
        .init_properties()
        .load_properties(|props| props.load_maps::<GOVMPH>())
        .context("Could not load maps")?
        .load_properties(|props| props.load_timestamps())
        .context("Could not load timestamps")?;
    log::info!("Graph loaded.");

    log::info!("Initializing timestamps");
    let mut timestamps = Vec::with_capacity(graph.num_nodes());
    timestamps.resize_with(graph.num_nodes(), || AtomicI64::new(i64::MAX));
    log::info!("Timestamps initialized.");

    let mut timestamps_file = std::fs::File::create(&args.timestamps_out)
        .with_context(|| format!("Could not create {}", &args.timestamps_out.display()))?;

    let mut pl = ProgressLogger::default();
    pl.item_name("node");
    pl.display_memory(true);
    pl.local_speed(true);
    pl.expected_updates(Some(graph.num_nodes()));
    pl.start("[step 1/3] Computing first occurrence date of each content...");
    let pl = Arc::new(Mutex::new(pl));

    swh_graph::utils::shuffle::par_iter_shuffled_range(0..graph.num_nodes()).try_for_each(
        |revrel| -> Result<_> {
            mark_reachable_contents(&graph, &timestamps, revrel)?;

            if revrel % 32768 == 0 {
                pl.lock().unwrap().update_with_count(32768);
            }

            Ok(())
        },
    )?;

    pl.lock().unwrap().done();

    let mut pl = ProgressLogger::default();
    pl.item_name("node");
    pl.display_memory(true);
    pl.local_speed(true);
    pl.expected_updates(Some(graph.num_nodes()));
    pl.start("[step 2/3] Converting timestamps to big-endian");
    let pl = Arc::new(Mutex::new(pl));
    let mut timestamps_be = Vec::with_capacity(graph.num_nodes());
    timestamps
        .into_par_iter()
        .enumerate()
        .map(|(node, timestamp)| {
            if node % 32768 == 0 {
                pl.lock().unwrap().update_with_count(32768);
            }
            // i64::MIN.to_be() indicates the timestamp is unset
            match timestamp.load(Ordering::Relaxed) {
                i64::MAX => i64::MIN,
                timestamp => timestamp,
            }
            .to_be()
        })
        .collect_into_vec(&mut timestamps_be);
    pl.lock().unwrap().done();

    log::info!("[step 3/3] Writing {}", args.timestamps_out.display());
    timestamps_file
        .write_all(bytemuck::cast_slice(&timestamps_be))
        .with_context(|| format!("Could not write to {}", args.timestamps_out.display()))?;

    Ok(())
}

/// Mark any content reachable from the root `revrel` as having a first occurrence
/// older or equal to this revision
fn mark_reachable_contents<G>(graph: &G, timestamps: &[AtomicI64], revrel: NodeId) -> Result<()>
where
    G: SwhForwardGraph + SwhBackwardGraph + SwhGraphWithProperties,
    <G as SwhGraphWithProperties>::Maps: swh_graph::properties::Maps,
    <G as SwhGraphWithProperties>::Timestamps: swh_graph::properties::Timestamps,
{
    if !swh_graph_provenance::filters::is_head(graph, revrel) {
        return Ok(());
    }

    let Some(revrel_timestamp) = graph.properties().author_timestamp(revrel) else {
        // Revision/release has no date, ignore it
        return Ok(());
    };

    let mut stack = vec![revrel];
    let mut visited = AdaptiveNodeSet::new(graph.num_nodes());

    while let Some(node) = stack.pop() {
        match graph.properties().node_type(node) {
            SWHType::Content | SWHType::Directory => {
                timestamps[node].fetch_min(revrel_timestamp, Ordering::Relaxed);
            }
            _ => (),
        }

        for succ in graph.successors(node) {
            match graph.properties().node_type(succ) {
                SWHType::Directory | SWHType::Content => {
                    if !visited.contains(succ) {
                        stack.push(succ);
                        visited.insert(succ);
                    }
                }
                _ => (),
            }
        }
    }

    Ok(())
}
