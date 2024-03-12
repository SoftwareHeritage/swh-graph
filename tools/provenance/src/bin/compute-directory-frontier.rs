// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::cell::RefCell;
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use clap::Parser;
use dsi_progress_logger::{ProgressLog, ProgressLogger};
use rand::prelude::*;
use rayon::prelude::*;
use serde::Serialize;

use swh_graph::graph::*;
use swh_graph::java_compat::mph::gov::GOVMPH;
use swh_graph::utils::mmap::NumberMmap;
use swh_graph::utils::GetIndex;
use swh_graph::{SWHType, SWHID};

#[derive(Parser, Debug)]
/** Given as input a binary file with, for each directory, the newest date of first
 * occurrence of any of the content in its subtree (well, DAG), ie.,
 * max_{for all content} (min_{for all occurrence of content} occurrence).
 * Produces the "provenance frontier", as defined in
 * https://gitlab.softwareheritage.org/swh/devel/swh-provenance/-/blob/ae09086a3bd45c7edbc22691945b9d61200ec3c2/swh/provenance/algos/revision.py#L210
 */
struct Args {
    graph_path: PathBuf,
    #[arg(short, long, action = clap::ArgAction::Count)]
    verbose: u8,
    #[arg(long)]
    /// Path to read the array of max timestamps from
    max_timestamps: PathBuf,
    #[arg(long)]
    /// Path to a directory where to write .csv.zst results to
    directories_out: PathBuf,
}

#[allow(non_snake_case)]
#[derive(Debug, Serialize)]
struct OutputRecord {
    max_author_date: i64,
    frontier_dir_SWHID: SWHID,
    rev_author_date: chrono::DateTime<chrono::Utc>,
    rev_SWHID: SWHID,
    #[serde(with = "serde_bytes")] // Serialize a bytestring instead of list of ints
    path: Vec<u8>,
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
        .load_forward_labels()
        .context("Could not load labels")?
        .init_properties()
        .load_properties(|props| props.load_label_names())
        .context("Could not load label names")?
        .load_properties(|props| props.load_maps::<GOVMPH>())
        .context("Could not load maps")?
        .load_properties(|props| props.load_timestamps())
        .context("Could not load timestamps")?;
    log::info!("Graph loaded.");

    let max_timestamps =
        NumberMmap::<byteorder::BE, i64, _>::new(&args.max_timestamps, graph.num_nodes())
            .with_context(|| format!("Could not mmap {}", args.max_timestamps.display()))?;

    std::fs::create_dir_all(&args.directories_out)
        .with_context(|| format!("Could not create {}", args.directories_out.display()))?;

    find_frontiers(&graph, &max_timestamps, args.directories_out)
}

fn find_frontiers<G>(
    graph: &G,
    max_timestamps: impl GetIndex<Output = i64> + Sync + Copy,
    output_dir: PathBuf,
) -> Result<()>
where
    G: SwhBackwardGraph + SwhLabelledForwardGraph + SwhGraphWithProperties + Send + Sync + 'static,
    <G as SwhGraphWithProperties>::LabelNames: swh_graph::properties::LabelNames,
    <G as SwhGraphWithProperties>::Maps: swh_graph::properties::Maps,
    <G as SwhGraphWithProperties>::Timestamps: swh_graph::properties::Timestamps,
{
    let mut pl = ProgressLogger::default();
    pl.item_name("node");
    pl.display_memory(true);
    pl.local_speed(true);
    pl.expected_updates(Some(graph.num_nodes()));
    pl.start("Visiting revisions' directories...");
    let pl = Arc::new(Mutex::new(pl));

    let worker_id = AtomicU64::new(0);

    // Reuse writers across work batches, or we end up with millions of very small files
    let writers = thread_local::ThreadLocal::new();

    let num_chunks = 100_000; // Arbitrary value
    let chunk_size = graph.num_nodes().div_ceil(num_chunks);
    let mut chunks: Vec<usize> = (0..num_chunks).collect();

    // Make workload homogeneous over time; otherwise all threads will process large
    // directory tries at the same time and run out of memory.
    chunks.shuffle(&mut rand::thread_rng());

    chunks
        .into_par_iter()
        // Lazily rebuild the list of nodes from the shuffled chunks
        .flat_map(|chunk_id| {
            (chunk_id * chunk_size)..usize::min((chunk_id + 1) * chunk_size, graph.num_nodes() - 1)
        })
        .try_for_each_init(
            || {
                writers
                    .get_or(|| {
                        let path = output_dir.join(format!(
                            "{}.csv.zst",
                            worker_id.fetch_add(1, Ordering::Relaxed)
                        ));
                        let file = std::fs::File::create(&path)
                            .with_context(|| format!("Could not create {}", path.display()))
                            .unwrap();
                        let compression_level = 3;
                        let zstd_encoder =
                            zstd::stream::write::Encoder::new(file, compression_level)
                                .with_context(|| {
                                    format!("Could not create ZSTD encoder for {}", path.display())
                                })
                                .unwrap()
                                .auto_finish();
                        RefCell::new(
                            csv::WriterBuilder::new()
                                .has_headers(true)
                                .terminator(csv::Terminator::CRLF)
                                .from_writer(zstd_encoder),
                        )
                    })
                    .borrow_mut()
            },
            |writer, node| -> Result<()> {
                let node_type = graph.properties().node_type(node);

                match node_type {
                    SWHType::Revision => {
                        // Allow revisions only if they are a "snapshot head" (ie. one of their
                        // predecessors is a release or a snapshot)
                        if !graph.predecessors(node).into_iter().any(|pred| {
                            let pred_type = graph.properties().node_type(pred);
                            pred_type == SWHType::Snapshot || pred_type == SWHType::Release
                        }) {
                            if node % 32768 == 0 {
                                pl.lock().unwrap().update_with_count(32768);
                            }
                            return Ok(());
                        }
                    }
                    _ => (),
                }

                if let Some(root_dir) =
                    swh_graph::algos::get_root_directory_from_revision_or_release(graph, node)
                        .context("Could not pick root directory")?
                {
                    find_frontiers_in_root_directory(
                        graph,
                        max_timestamps,
                        writer,
                        node,
                        root_dir,
                    )?;
                }

                if node % 32768 == 0 {
                    pl.lock().unwrap().update_with_count(32768);
                }

                Ok(())
            },
        )?;

    pl.lock().unwrap().done();

    log::info!("Visits done, finishing output");

    Ok(())
}

fn find_frontiers_in_root_directory<G, W: Write>(
    graph: &G,
    max_timestamps: impl GetIndex<Output = i64>,
    writer: &mut csv::Writer<W>,
    revrel_id: NodeId,
    root_dir_id: NodeId,
) -> Result<()>
where
    G: SwhLabelledForwardGraph + SwhGraphWithProperties,
    <G as SwhGraphWithProperties>::LabelNames: swh_graph::properties::LabelNames,
    <G as SwhGraphWithProperties>::Maps: swh_graph::properties::Maps,
    <G as SwhGraphWithProperties>::Timestamps: swh_graph::properties::Timestamps,
{
    let Some(revrel_timestamp) = graph.properties().author_timestamp(revrel_id) else {
        return Ok(());
    };
    let revrel_author_date =
        chrono::DateTime::from_timestamp(revrel_timestamp, 0).expect("Could not convert timestamp");

    let revrel_swhid = graph.properties().swhid(revrel_id);

    let is_frontier = |dir: NodeId, dir_max_timestamp: i64| {
        if dir == root_dir_id {
            // The root directory itself cannot be a frontier
            return false;
        }

        // Detect if a node is a frontier according to
        // https://gitlab.softwareheritage.org/swh/devel/swh-provenance/-/blob/ae09086a3bd45c7edbc22691945b9d61200ec3c2/swh/provenance/algos/revision.py#L210
        if dir_max_timestamp < revrel_timestamp {
            // All content is earlier than revision

            // No need to check if it's depth > 1, given that we excluded the root dir above */
            if graph
                .successors(dir)
                .into_iter()
                .any(|succ| graph.properties().node_type(succ) == SWHType::Content)
            {
                // Contains at least one blob
                return true;
            }
        }

        false
    };

    let on_frontier = |dir: NodeId, dir_max_timestamp: i64, path: Vec<u8>| {
        writer
            .serialize(OutputRecord {
                max_author_date: dir_max_timestamp,
                frontier_dir_SWHID: graph.properties().swhid(dir),
                rev_author_date: revrel_author_date,
                rev_SWHID: revrel_swhid,
                path,
            })
            .context("Could not write record")
    };

    swh_graph_provenance::frontier::find_frontiers_in_root_directory(
        graph,
        max_timestamps,
        is_frontier,
        on_frontier,
        false, // Don't recurse through frontiers (frontier-directories-in-revisions will)
        root_dir_id,
    )
}
