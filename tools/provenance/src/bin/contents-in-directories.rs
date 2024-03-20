// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

#![allow(non_snake_case)]

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use clap::Parser;
use dsi_progress_logger::{ProgressLog, ProgressLogger};
use rayon::prelude::*;
use serde::Serialize;

use swh_graph::collections::NodeSet;
use swh_graph::graph::*;
use swh_graph::java_compat::mph::gov::GOVMPH;
use swh_graph::SWHID;

use swh_graph::utils::dataset_writer::{ParallelDatasetWriter, ParquetTableWriter};
use swh_graph_provenance::frontier::PathParts;
use swh_graph_provenance::x_in_y_dataset::{
    cnt_in_dir_schema, cnt_in_dir_writer_properties, CntInDirTableBuilder,
};

#[derive(Parser, Debug)]
/** Given as input a binary file with, for each directory, the newest date of first
 * occurrence of any of the content in its subtree (well, DAG), ie.,
 * max_{for all content} (min_{for all occurrence of content} occurrence).
 * and a Parquet table with the node ids of every frontier directory.
 * Produces the list of contents present in each directory.
 */
struct Args {
    graph_path: PathBuf,
    #[arg(short, long, action = clap::ArgAction::Count)]
    verbose: u8,
    #[arg(long)]
    /// Path to the Parquet table with the node ids of frontier directories
    frontier_directories: PathBuf,
    #[arg(long)]
    /// Path to a directory where to write .parquet results to
    contents_out: PathBuf,
}

#[derive(Debug, Serialize)]
struct OutputRecord<'a> {
    cnt_SWHID: SWHID,
    dir_SWHID: &'a SWHID,
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
        .context("Could not load maps")?;
    log::info!("Graph loaded.");

    let mut pl = ProgressLogger::default();
    pl.item_name("node");
    pl.display_memory(true);
    pl.local_speed(true);
    pl.start("Loading frontier directories...");
    let frontier_directories = swh_graph_provenance::frontier_set::from_parquet(
        &graph,
        args.frontier_directories,
        &mut pl,
    )?;
    pl.done();

    let dataset_writer = ParallelDatasetWriter::new_with_schema(
        args.contents_out,
        (
            Arc::new(cnt_in_dir_schema()),
            cnt_in_dir_writer_properties(&graph).build(),
        ),
    )?;

    let mut pl = ProgressLogger::default();
    pl.item_name("directory");
    pl.display_memory(true);
    pl.expected_updates(Some(graph.num_nodes()));
    pl.start("Listing contents in directories...");
    let pl = Arc::new(Mutex::new(pl));

    swh_graph::utils::shuffle::par_iter_shuffled_range(0..graph.num_nodes()).try_for_each_init(
        || dataset_writer.get_thread_writer().unwrap(),
        |writer, node| -> Result<()> {
            if frontier_directories.contains(node) {
                write_contents_in_directory(&graph, writer, node)?;
            }

            if node % 32768 == 0 {
                pl.lock().unwrap().update_with_count(32768);
            }

            Ok(())
        },
    )?;
    pl.lock().unwrap().done();

    Ok(())
}

fn write_contents_in_directory<G>(
    graph: &G,
    writer: &mut ParquetTableWriter<CntInDirTableBuilder>,
    root_dir: NodeId,
) -> Result<()>
where
    G: SwhLabelledForwardGraph + SwhGraphWithProperties,
    <G as SwhGraphWithProperties>::LabelNames: swh_graph::properties::LabelNames,
    <G as SwhGraphWithProperties>::Maps: swh_graph::properties::Maps,
{
    let root_dir_swhid = graph.properties().swhid(root_dir);

    let on_directory = |_dir: NodeId, _path_parts: PathParts| Ok(true); // always recurse

    let on_content = |cnt: NodeId, path_parts: PathParts| {
        let builder = writer.builder()?;
        builder
            .cnt
            .append_value(cnt.try_into().expect("NodeId overflowed u64"));
        builder
            .dir
            .append_value(root_dir.try_into().expect("NodeId overflowed u64"));
        builder.path.append_value(path_parts.build_path(graph));
        Ok(())
    };

    swh_graph_provenance::frontier::dfs_with_path(graph, on_directory, on_content, root_dir)
}
