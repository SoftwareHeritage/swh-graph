// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

#![allow(non_snake_case)]

use std::io::Write;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use clap::Parser;
use dsi_progress_logger::{ProgressLog, ProgressLogger};
use rayon::prelude::*;
use serde::Serialize;
use sux::bits::bit_vec::BitVec;

use swh_graph::graph::*;
use swh_graph::java_compat::mph::gov::GOVMPH;
use swh_graph::SWHID;

use swh_graph_provenance::csv_dataset::CsvZstDataset;
use swh_graph_provenance::frontier::PathParts;

#[derive(Parser, Debug)]
/** Given as stdin a CSV with header "frontier_dir_SWHID" containing a single column
 * with frontier directories.
 * Produces the line of contents reachable from each revision, without any going through
 * any directory that is a frontier (relative to any revision).
 */
struct Args {
    graph_path: PathBuf,
    #[arg(short, long, action = clap::ArgAction::Count)]
    verbose: u8,
    #[arg(long)]
    /// Path to a directory where to write .csv.zst results to
    contents_out: PathBuf,
}

#[derive(Debug, Serialize)]
struct OutputRecord {
    cnt_SWHID: SWHID,
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

    let frontier_directories =
        swh_graph_provenance::frontier_set::frontier_directories_from_stdin(&graph)?;

    let output_dataset = CsvZstDataset::new(args.contents_out)?;

    write_contents_in_revisions(&graph, &frontier_directories, output_dataset)
}

fn write_contents_in_revisions<G>(
    graph: &G,
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
                    find_contents_in_root_directory(
                        graph,
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

fn find_contents_in_root_directory<G, W: Write>(
    graph: &G,
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

    let on_directory = |dir: NodeId, _path_parts: PathParts| {
        Ok(!frontier_directories[dir]) // Recurse only if this is not a frontier
    };

    let on_content = |cnt: NodeId, path_parts: PathParts| {
        writer
            .serialize(OutputRecord {
                cnt_SWHID: graph.properties().swhid(cnt),
                rev_author_date: revrel_author_date,
                rev_SWHID: revrel_swhid,
                path: path_parts.build_path(graph),
            })
            .context("Could not write record")
    };

    swh_graph_provenance::frontier::dfs_with_path(graph, on_directory, on_content, root_dir_id)
}
