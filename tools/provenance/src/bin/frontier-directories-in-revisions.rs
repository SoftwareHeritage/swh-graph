// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

#![allow(non_snake_case)]

use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use clap::Parser;
use dsi_progress_logger::{ProgressLog, ProgressLogger};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use sux::bits::bit_vec::{AtomicBitVec, BitVec};

use swh_graph::graph::*;
use swh_graph::java_compat::mph::gov::GOVMPH;
use swh_graph::utils::mmap::NumberMmap;
use swh_graph::utils::GetIndex;
use swh_graph::SWHID;

use swh_graph_provenance::csv_dataset::CsvZstDataset;

#[derive(Parser, Debug)]
/** Given as input a binary file with, for each directory, the newest date of first
 * occurrence of any of the content in its subtree (well, DAG), ie.,
 * max_{for all content} (min_{for all occurrence of content} occurrence).
 * and as stdin a CSV with header "frontier_dir_SWHID" containing a single column
 * with frontier directories.
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

#[derive(Debug, Deserialize)]
struct InputRecord {
    frontier_dir_SWHID: String,
}

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

    let frontier_directories = read_frontier_directories(&graph)?;

    let output_dataset = CsvZstDataset::new(args.directories_out)?;

    write_frontier_directories_in_revisions(
        &graph,
        &max_timestamps,
        &frontier_directories,
        output_dataset,
    )
}

fn read_frontier_directories<G>(graph: &G) -> Result<BitVec>
where
    G: SwhGraphWithProperties + Send + Sync + 'static,
    <G as SwhGraphWithProperties>::Maps: swh_graph::properties::Maps,
{
    let frontier_directories = AtomicBitVec::new(graph.num_nodes());

    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_reader(std::io::stdin());

    let mut pl = ProgressLogger::default();
    pl.item_name("directory");
    pl.display_memory(true);
    pl.start("Reading frontier directories...");
    let pl = Arc::new(Mutex::new(pl));

    reader
        .deserialize()
        .par_bridge()
        .try_for_each(|record| -> Result<()> {
            let InputRecord { frontier_dir_SWHID } =
                record.context("Could not deserialize input row")?;
            let node_id = graph
                .properties()
                .node_id_from_string_swhid(frontier_dir_SWHID)?;
            if node_id % 32768 == 0 {
                pl.lock().unwrap().update_with_count(32768);
            }

            frontier_directories.set(node_id, true, Ordering::Relaxed);

            Ok(())
        })?;
    pl.lock().unwrap().done();

    Ok(frontier_directories.into())
}

fn write_frontier_directories_in_revisions<G>(
    graph: &G,
    max_timestamps: impl GetIndex<Output = i64> + Sync + Copy,
    frontier_directories: &BitVec,
    output_dataset: CsvZstDataset,
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

    // Reuse writers across work batches, or we end up with millions of very small files
    let writers = thread_local::ThreadLocal::new();

    swh_graph::utils::shuffle::par_iter_shuffled_range(0..graph.num_nodes()).try_for_each_init(
        || {
            writers
                .get_or(|| output_dataset.get_new_writer().unwrap())
                .borrow_mut()
        },
        |writer, node| -> Result<()> {
            if swh_graph_provenance::filters::is_head(graph, node) {
                if let Some(root_dir) =
                    swh_graph::algos::get_root_directory_from_revision_or_release(graph, node)
                        .context("Could not pick root directory")?
                {
                    find_frontiers_in_root_directory(
                        graph,
                        max_timestamps,
                        frontier_directories,
                        writer,
                        node,
                        root_dir,
                    )?;
                }
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
    frontier_directories: &BitVec,
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

    let is_frontier = |dir: NodeId, _dir_max_timestamp: i64| frontier_directories[dir];

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
        true, // Recurse through frontiers
        root_dir_id,
    )
}
