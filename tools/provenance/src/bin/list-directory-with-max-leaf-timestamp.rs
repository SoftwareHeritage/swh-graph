// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::io;
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use clap::Parser;
use dsi_progress_logger::{ProgressLog, ProgressLogger};
use rayon::prelude::*;
use serde::Deserialize;

use swh_graph::graph::*;
use swh_graph::java_compat::mph::gov::GOVMPH;
use swh_graph::utils::mmap::NumberMmap;
use swh_graph::utils::GetIndex;
use swh_graph::{SWHType, SWHID};

#[derive(Parser, Debug)]
/** Given as argument a binary file containing an array of timestamps which is,
 * for every content, the date of first occurrence of that content in a revision,
 * and as stdin a backward topological order.
 * Produces a binary file in the same format, which contains for each directory,
 * the max of these values for all contents in that directory.
 *
 * If the <path/to/provenance_timestamps.bin> parameter is passed, then this file
 * is written as an array of longs, which can be loaded with LongMappedBigList.
 *
 * This new date is guaranteed to be lower or equal to the one in the input.
 */
struct Args {
    graph_path: PathBuf,
    #[arg(short, long, action = clap::ArgAction::Count)]
    verbose: u8,
    #[arg(long)]
    /// Path to read the array of timestamps from
    timestamps: PathBuf,
    #[arg(long)]
    /// Path to write the array of max timestamps to
    max_timestamps_out: Option<PathBuf>,
}

#[derive(Debug, Deserialize)]
struct InputRecord {
    #[serde(rename = "SWHID")]
    swhid: SWHID,
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
        .context("Could not load maps")?;
    log::info!("Graph loaded.");

    let timestamps = NumberMmap::<byteorder::BE, i64, _>::new(&args.timestamps, graph.num_nodes())
        .with_context(|| format!("Could not mmap {}", args.timestamps.display()))?;

    // This array is NOT in big-endian in order to support AtomicI64::fetch_min
    let mut max_timestamps = Vec::with_capacity(graph.num_nodes());
    max_timestamps.resize_with(graph.num_nodes(), || AtomicI64::new(i64::MAX));

    let max_timestamps_file = match args.max_timestamps_out {
        Some(ref max_timestamps_path) => Some(
            std::fs::File::create(max_timestamps_path)
                .with_context(|| format!("Could not create {}", max_timestamps_path.display()))?,
        ),
        None => None,
    };

    initialize_contents(&graph, &max_timestamps, &timestamps)
        .context("Could not initialize contents")?;

    propagate_through_directories(&graph, &mut max_timestamps)
        .context("Could not propagate through directories")?;

    if let Some(mut max_timestamps_file) = max_timestamps_file {
        let max_timestamps_path = args.max_timestamps_out.unwrap();
        log::info!("Writing binary output to {}", max_timestamps_path.display());
        let max_timestamps_be: Vec<i64> = max_timestamps
            .into_par_iter()
            .map(|max_timestamp| {
                let max_timestamp = max_timestamp.into_inner();
                if max_timestamp == i64::MAX {
                    // Unset, switch it to i64::MIN to be compatible with the format
                    // we used in Java
                    i64::MIN.to_be()
                } else {
                    max_timestamp.to_be()
                }
            })
            .collect();
        max_timestamps_file
            .write_all(bytemuck::cast_slice(max_timestamps_be.as_slice()))
            .with_context(|| format!("Could not write to {}", max_timestamps_path.display()))?;
    }

    Ok(())
}

/// Set each parent directory's max_timestamp to be the maximum of its children's
/// timestamps
fn initialize_contents<G>(
    graph: &G,
    max_timestamps: &[AtomicI64],
    timestamps: impl GetIndex<Output = i64> + Sync,
) -> Result<()>
where
    G: SwhBackwardGraph + SwhGraphWithProperties + Sync,
    <G as SwhGraphWithProperties>::Maps: swh_graph::properties::Maps,
{
    let mut pl = ProgressLogger::default();
    pl.item_name("node");
    pl.display_memory(true);
    pl.expected_updates(Some(graph.num_nodes()));
    pl.start("Initializing from contents...");
    let pl = Arc::new(Mutex::new(pl));
    (0..graph.num_nodes()).into_par_iter().for_each(|node| {
        let node_type = graph
            .properties()
            .node_type(node)
            .expect("missing node type");
        if node_type == SWHType::Content {
            set_max_timestamp_to_predecessors(
                graph,
                max_timestamps,
                node,
                timestamps.get(node).unwrap(),
            )
        }
        if node % 32768 == 0 {
            pl.lock().unwrap().update_with_count(32768);
        }
    });

    pl.lock().unwrap().done();

    Ok(())
}

/// Set each parent directory's max_timestamp to be the maximum of its children's
/// timestamps
fn propagate_through_directories<G>(graph: &G, max_timestamps: &mut [AtomicI64]) -> Result<()>
where
    G: SwhBackwardGraph + SwhGraphWithProperties,
    <G as SwhGraphWithProperties>::Maps: swh_graph::properties::Maps,
{
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_reader(io::stdin());

    let mut pl = ProgressLogger::default();
    pl.item_name("node");
    pl.display_memory(true);
    pl.start("Propagating through directories...");

    for record in reader.deserialize() {
        pl.light_update();
        let InputRecord { swhid } = record.context("Could not deserialize row")?;

        let node = graph
            .properties()
            .node_id(swhid)
            .with_context(|| format!("Unknown SWHID {}", swhid))?;
        let node_type = graph
            .properties()
            .node_type(node)
            .expect("Missing node type");
        if node_type == SWHType::Directory {
            set_max_timestamp_to_predecessors(
                graph,
                max_timestamps,
                node,
                max_timestamps[node].load(Ordering::Relaxed),
            );
        }
    }
    pl.done();
    Ok(())
}

fn set_max_timestamp_to_predecessors<G>(
    graph: &G,
    max_timestamps: &[AtomicI64],
    node: NodeId,
    timestamp: i64,
) where
    G: SwhBackwardGraph + SwhGraphWithProperties,
    <G as SwhGraphWithProperties>::Maps: swh_graph::properties::Maps,
{
    if timestamp == i64::MIN {
        // Unset, ignore it
        return;
    }
    for pred in graph.predecessors(node) {
        let pred_type = graph
            .properties()
            .node_type(pred)
            .expect("Missing node type");
        if pred_type == SWHType::Directory {
            max_timestamps[pred].fetch_min(timestamp, Ordering::Release);
        }
    }
}
