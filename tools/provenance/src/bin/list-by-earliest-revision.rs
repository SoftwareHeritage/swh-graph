// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

//! Computers, for any content or directory, the first revision or release that contains it.
//!
//! The algorithm is:
//!
//! 1. Initialize an array of [`AtomicOccurrence`], one for each node, to the maximum
//!    timestamp and an undefined node id
//! 2. For each revision/release (in parallel):
//!     a. Get its author date (if none, skip the revision/release)
//!     b. traverse all contents and directories contained by that revision/release.
//!        For each content/directory, atomically set the [`AtomicOccurrence`] to the
//!        current rev/rel and its timestamp if the [`AtomicOccurrence`] has a higher
//!        timestamp than the revision/release
//! 3. For each content/directory (in parallel):
//!     a. Get the `(timestamp, revrel)` pair represented by its  [`AtomicOccurrence`].
//!     b. If the timestamp is not the maximum (ie. if it was set as step 2.b.), then
//!        printthe `(timestamp, revrel, cntdir)` triple
//! 4. Convert the array of [`AtomicOccurrence`] to an array of timestamps and dump it.
#![allow(non_snake_case)]
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use chrono::{DateTime, NaiveDateTime};
use clap::Parser;
use dsi_progress_logger::{ProgressLog, ProgressLogger};
use portable_atomic::AtomicI128;
use rayon::prelude::*;
use serde::Serialize;

use swh_graph::collections::{AdaptiveNodeSet, NodeSet};
use swh_graph::graph::*;
use swh_graph::java_compat::mph::gov::GOVMPH;
use swh_graph::{SWHType, SWHID};

use swh_graph_provenance::csv_dataset::CsvZstDataset;

#[derive(Parser, Debug)]
/// Returns a directory of CSV files with header 'author_date,revrel_SWHID,cntdir_SWHID'
/// and a row for each of the contents and directories with the earliest revision/release
/// that contains them.
struct Args {
    graph_path: PathBuf,
    #[arg(short, long, action = clap::ArgAction::Count)]
    verbose: u8,
    #[arg(long)]
    /// Path to write the content -> rev/rel map to
    revisions_out: PathBuf,
    #[arg(long)]
    /// Path to write the array of timestamps to
    timestamps_out: Option<PathBuf>,
}

#[derive(Debug, Serialize)]
struct OutputRecord {
    author_date: NaiveDateTime,
    revrel_SWHID: SWHID,
    cntdir_SWHID: SWHID,
}

/// A `(timestamp, revision)` pair that can be used with [`AtomicOccurrence`]
#[derive(Copy, Clone)]
struct Occurrence(i128);

impl Occurrence {
    fn new(timestamp: i64, node: NodeId) -> Occurrence {
        let node: u64 = node.try_into().expect("NodeId overflows u64");
        Occurrence((timestamp as i128) << 64 | (node as i128))
    }

    fn timestamp(&self) -> i64 {
        (self.0 >> 64) as i64
    }

    fn node(&self) -> NodeId {
        (self.0 as u64)
            .try_into()
            .expect("node id overflowed usize")
    }
}

/// A `(timestamp, revision)` pair that can be atomically updated based on the timestamp
///
/// This is implemented with an [`AtomicI128`] where the high bits are a i64 timestamp
/// and the low bits are a u64 node id.
/// This allows using x.fetch_min(y, ...) to atomically set x to a new
/// `(timestamp, revision)` pair in `y` only if the new timestamp is older than
/// the one already in `x`.
struct AtomicOccurrence(AtomicI128);

impl AtomicOccurrence {
    fn new(occurrence: Occurrence) -> AtomicOccurrence {
        assert!(AtomicI128::is_lock_free(), "AtomicI128 is not lock-free");
        AtomicOccurrence(AtomicI128::new(occurrence.0))
    }

    fn load(&self, ordering: Ordering) -> Occurrence {
        Occurrence(self.0.load(ordering))
    }

    /// Atomically sets `self` to `other`'s value if `other` has a lower timestamp
    ///
    /// If both have the same timestamp, which node is picked is not guaranteed.
    /// (In practice, the lowest node id is picked for timestamps >= epoch, and the
    /// lowest node id for timestamps < epoch)
    fn fetch_min(&self, other: Occurrence, ordering: Ordering) -> Occurrence {
        Occurrence(self.0.fetch_min(other.0, ordering))
    }
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

    log::info!("Initializing occurrences");
    let mut occurrences = Vec::with_capacity(graph.num_nodes());
    occurrences.resize_with(graph.num_nodes(), || {
        AtomicOccurrence::new(Occurrence::new(i64::MAX, NodeId::MAX))
    });
    log::info!("Occurrences initialized.");

    let timestamps_file = match args.timestamps_out {
        Some(ref timestamps_path) => Some(
            std::fs::File::create(timestamps_path)
                .with_context(|| format!("Could not create {}", timestamps_path.display()))?,
        ),
        None => None,
    };

    let mut pl = ProgressLogger::default();
    pl.item_name("node");
    pl.display_memory(true);
    pl.local_speed(true);
    pl.expected_updates(Some(graph.num_nodes()));
    pl.start("[step 1/3] Computing first occurrence date of each content...");
    let pl = Arc::new(Mutex::new(pl));

    swh_graph::utils::shuffle::par_iter_shuffled_range(0..graph.num_nodes()).try_for_each(
        |revrel| -> Result<_> {
            mark_reachable_contents(&graph, &occurrences, revrel)?;

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
    pl.start("[step 2/3] Writing content -> first-occurrence map...");
    let pl = Arc::new(Mutex::new(pl));

    let output_dataset = CsvZstDataset::new(args.revisions_out)?;

    // Reuse writers across work batches, or we end up with millions of very small files
    let writers = thread_local::ThreadLocal::new();

    // For each content, find a rev/rel that contains it and has the same date as the
    // date of first occurrence as the content (most of the time, there is only one,
    // but we don't care which we pick if there is more than one).
    swh_graph::utils::shuffle::par_iter_shuffled_range(0..graph.num_nodes()).try_for_each_init(
        || {
            writers
                .get_or(|| output_dataset.get_new_writer().unwrap())
                .borrow_mut()
        },
        |writer, cntdir| -> Result<_> {
            let occurrence = occurrences[cntdir].load(Ordering::Relaxed);
            if occurrence.timestamp() != i64::MAX {
                write_earliest_revrel(&graph, writer, cntdir, occurrence)?;
            }

            if cntdir % 32768 == 0 {
                pl.lock().unwrap().update_with_count(32768);
            }

            Ok(())
        },
    )?;

    pl.lock().unwrap().done();

    if let Some(mut timestamps_file) = timestamps_file {
        let mut pl = ProgressLogger::default();
        pl.item_name("node");
        pl.display_memory(true);
        pl.local_speed(true);
        pl.expected_updates(Some(graph.num_nodes()));
        pl.start("[step 3/3] Converting timestamps to big-endian");
        let pl = Arc::new(Mutex::new(pl));
        let mut timestamps = Vec::with_capacity(graph.num_nodes());
        occurrences
            .into_par_iter()
            .enumerate()
            .map(|(node, occurrence)| {
                if node % 32768 == 0 {
                    pl.lock().unwrap().update_with_count(32768);
                }
                // i64::MIN.to_be() indicates the timestamp is unset
                match occurrence.load(Ordering::Relaxed).timestamp() {
                    i64::MAX => i64::MIN,
                    timestamp => timestamp,
                }
                .to_be()
            })
            .collect_into_vec(&mut timestamps);
        pl.lock().unwrap().done();

        log::info!(
            "Writing {}",
            args.timestamps_out.as_ref().unwrap().display()
        );
        timestamps_file
            .write_all(bytemuck::cast_slice(&timestamps))
            .with_context(|| {
                format!(
                    "Could not write to {}",
                    args.timestamps_out.unwrap().display()
                )
            })?;
    }

    Ok(())
}

/// Mark any content reachable from the root `revrel` as having a first occurrence
/// older or equal to this revision
fn mark_reachable_contents<G>(
    graph: &G,
    occurrences: &[AtomicOccurrence],
    revrel: NodeId,
) -> Result<()>
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

    let occurrence = Occurrence::new(revrel_timestamp, revrel);

    let mut stack = vec![revrel];
    let mut visited = AdaptiveNodeSet::new(graph.num_nodes());

    while let Some(node) = stack.pop() {
        match graph.properties().node_type(node) {
            SWHType::Content | SWHType::Directory => {
                occurrences[node].fetch_min(occurrence, Ordering::Relaxed);
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

fn write_earliest_revrel<G, W: Write>(
    graph: &G,
    writer: &mut csv::Writer<W>,
    cntdir: NodeId,
    earliest_occurrence: Occurrence,
) -> Result<()>
where
    G: SwhForwardGraph + SwhBackwardGraph + SwhGraphWithProperties,
    <G as SwhGraphWithProperties>::Maps: swh_graph::properties::Maps,
    <G as SwhGraphWithProperties>::Timestamps: swh_graph::properties::Timestamps,
{
    let revrel_timestamp = earliest_occurrence.timestamp();
    let revrel = earliest_occurrence.node();

    let revrel_date = DateTime::from_timestamp(revrel_timestamp, 0)
        .with_context(|| format!("Could not make {} into a datetime", revrel_timestamp))?
        .naive_utc();
    let revrel_SWHID = graph.properties().swhid(revrel);

    writer
        .serialize(OutputRecord {
            author_date: revrel_date,
            revrel_SWHID,
            cntdir_SWHID: graph.properties().swhid(cntdir),
        })
        .context("Could not write output row")
}
