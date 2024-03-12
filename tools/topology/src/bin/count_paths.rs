// Copyright (C) 2023-2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::io;
use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::{Parser, ValueEnum};
use dsi_progress_logger::ProgressLogger;
use itertools::Itertools;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};

use swh_graph::graph::*;
use swh_graph::java_compat::mph::gov::GOVMPH;
use swh_graph::views::Transposed;

#[derive(Debug, Clone, Copy, ValueEnum)]
enum Direction {
    Forward,
    Backward,
}

#[derive(Parser, Debug)]
#[command(about = "Counts the number of (non-singleton) paths reaching each node, from all other nodes.", long_about = Some("
Counts in the output may be large enough to overflow long integers, so they are computed with double-precision floating point number and printed as such.

This requires a topological order as input (as returned by the toposort command).
"))]
struct Args {
    graph_path: PathBuf,
    #[arg(short, long, action = clap::ArgAction::Count)]
    verbose: u8,
    #[arg(short, long)]
    direction: Direction,
    #[arg(long, default_value_t = 1_000_000)]
    /// Number of input records to deserialize at once in parallel
    batch_size: usize,
}

#[derive(Debug, Deserialize)]
struct InputRecord {
    #[serde(rename = "SWHID")]
    swhid: String,
}

#[derive(Debug, Serialize)]
struct OutputRecord {
    #[serde(rename = "SWHID")]
    swhid: String,
    paths_from_roots: f64,
    all_paths: f64,
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

    match args.direction {
        Direction::Forward => count_paths(std::sync::Arc::new(graph), args.batch_size),
        Direction::Backward => count_paths(
            std::sync::Arc::new(Transposed(std::sync::Arc::new(graph))),
            args.batch_size,
        ),
    }
}

use std::ops::Deref;

fn count_paths<G>(graph: G, batch_size: usize) -> Result<()>
where
    G: Deref + Sync + Send + Clone + 'static,
    <G as Deref>::Target: SwhForwardGraph + SwhBackwardGraph + SwhGraphWithProperties,
    <<G as Deref>::Target as SwhGraphWithProperties>::Maps: swh_graph::properties::Maps,
{
    let mut writer = csv::WriterBuilder::new()
        .terminator(csv::Terminator::CRLF)
        .from_writer(io::stdout());

    log::info!("Initializing counts...");
    let mut paths_from_roots_vec = vec![0f64; graph.num_nodes()];
    let mut all_paths_vec = vec![0f64; graph.num_nodes()];

    let mut pl = ProgressLogger::default().display_memory();
    pl.item_name = "node";
    pl.local_speed = true;
    pl.expected_updates = Some(graph.num_nodes());
    pl.start("Listing nodes...");

    // Arbitrary ratio; ensures the deserialization thread is unlikely to block
    // unless the main thread is actually busy
    let (tx, rx) = std::sync::mpsc::sync_channel(4 * batch_size);

    // Delegate SWHID->node_id conversion to its own thread as it is a bottleneck
    let deser_thread = queue_nodes(graph.clone(), tx, batch_size);

    while let Ok((swhid, node)) = rx.recv() {
        pl.light_update();
        let mut paths_from_roots = paths_from_roots_vec[node];
        let mut all_paths = all_paths_vec[node];

        // Print counts for this nodes
        writer
            .serialize(OutputRecord {
                swhid: swhid.clone(),
                paths_from_roots,
                all_paths,
            })
            .with_context(|| format!("Could not write record for {}", swhid))?;

        // Add counts of paths coming from this node to all successors
        all_paths += 1.;
        if paths_from_roots == 0. {
            paths_from_roots += 1.;
        }
        for succ in graph.successors(node) {
            paths_from_roots_vec[succ] += paths_from_roots;
            all_paths_vec[succ] += all_paths;
        }
    }

    log::info!("Cleaning up...");
    deser_thread
        .join()
        .expect("Could not join deserialization thread")
        .context("Error in deserialization thread")?;

    Ok(())
}

/// Reads CSV records from stdin, and queues their SWHIDs and node ids to `tx`,
/// preserving the order.
///
/// This is equivalent to:
///
/// ```
/// std::thread::spawn(move || -> Result<()> {
///     let mut reader = csv::ReaderBuilder::new()
///         .has_headers(true)
///         .from_reader(io::stdin());
///
///     for record in reader.deserialize() {
///         let InputRecord { swhid, .. } =
///             record.with_context(|| format!("Could not deserialize record"))?;
///         let node = graph
///             .properties()
///             .node_id_from_string_swhid(swhid)
///             .with_context(|| format!("Unknown SWHID: {}", swhid))?;
///
///         tx.send((swhid, node))
///     }
/// });
/// ```
///
/// but uses inner parallelism as `node_id()` could otherwise be a bottleneck on systems
/// where accessing `graph.order` has high latency (network and/or compressed filesystem).
/// This reduces the runtime from a couple of weeks to less than a day on the 2023-09-06
/// graph on a ZSTD-compressed ZFS.
fn queue_nodes<G>(
    graph: G,
    tx: std::sync::mpsc::SyncSender<(String, NodeId)>,
    batch_size: usize,
) -> std::thread::JoinHandle<Result<()>>
where
    G: Deref + Sync + Send + Clone + 'static,
    <G as Deref>::Target: SwhGraphWithProperties,
    <<G as Deref>::Target as SwhGraphWithProperties>::Maps: swh_graph::properties::Maps,
{
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_reader(io::stdin());
    std::thread::spawn(move || -> Result<()> {
        reader
            .deserialize()
            .chunks(batch_size)
            .into_iter()
            .try_for_each(|chunk| {
                // Process entries of this chunk in parallel
                let results: Result<Vec<_>> = chunk
                    .collect::<Vec<Result<InputRecord, _>>>()
                    .into_par_iter()
                    .map(|record| {
                        let InputRecord { swhid, .. } =
                            record.with_context(|| format!("Could not deserialize record"))?;

                        let node = graph.properties().node_id_from_string_swhid(&swhid)?;
                        Ok((swhid, node))
                    })
                    .collect();

                // Then collect them **IN ORDER** before pushing to 'tx'.
                for (swhid, node) in results? {
                    tx.send((swhid, node))
                        .expect("Could not send (swhid, node_id)");
                }

                Ok::<_, anyhow::Error>(())
            })
    })
}
