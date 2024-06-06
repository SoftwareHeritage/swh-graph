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
use sux::prelude::BitVec;

use swh_graph::collections::NodeSet;
use swh_graph::graph::*;
use swh_graph::java_compat::mph::gov::GOVMPH;
use swh_graph::SWHType;

use swh_graph::utils::dataset_writer::{ParallelDatasetWriter, ParquetTableWriter};
use swh_graph_provenance::filters::{load_reachable_nodes, NodeFilter};
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
    #[arg(value_enum)]
    #[arg(long, default_value_t = NodeFilter::Heads)]
    /// Subset of revisions and releases to traverse from
    node_filter: NodeFilter,
    #[arg(long)]
    /// Path to the Parquet table with the node ids of all nodes reachable from
    /// a head revision/release
    reachable_nodes: PathBuf,
    #[arg(long)]
    /// Path to the Parquet table with the node ids of frontier directories
    frontier_directories: PathBuf,
    #[arg(long)]
    /// Path to a directory where to write .parquet results to
    contents_out: PathBuf,
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
        .load_backward_labels()
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

    let reachable_nodes = load_reachable_nodes(&graph, args.node_filter, args.reachable_nodes)?;

    let mut pl = ProgressLogger::default();
    pl.item_name("directory");
    pl.display_memory(true);
    pl.expected_updates(Some(graph.num_nodes()));
    pl.start("Listing contents in directories...");
    let pl = Arc::new(Mutex::new(pl));

    swh_graph::utils::shuffle::par_iter_shuffled_range(0..graph.num_nodes()).try_for_each_init(
        || dataset_writer.get_thread_writer().unwrap(),
        |writer, node| -> Result<()> {
            let is_reachable = match &reachable_nodes {
                None => true, // All nodes are reachable
                Some(reachable_nodes) => reachable_nodes.get(node),
            };
            if is_reachable && graph.properties().node_type(node) == SWHType::Content {
                write_frontier_directories_from_content(
                    &graph,
                    writer,
                    reachable_nodes.as_ref(),
                    &frontier_directories,
                    node,
                )?;
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

fn write_frontier_directories_from_content<G>(
    graph: &G,
    writer: &mut ParquetTableWriter<CntInDirTableBuilder>,
    reachable_nodes: Option<&BitVec>,
    frontier_directories: &BitVec,
    cnt: NodeId,
) -> Result<()>
where
    G: SwhLabelledBackwardGraph + SwhGraphWithProperties,
    <G as SwhGraphWithProperties>::LabelNames: swh_graph::properties::LabelNames,
    <G as SwhGraphWithProperties>::Maps: swh_graph::properties::Maps,
{
    let on_directory = |dir: NodeId, path_parts: PathParts| {
        if frontier_directories.contains(dir) {
            let builder = writer.builder()?;
            builder
                .cnt
                .append_value(cnt.try_into().expect("NodeId overflowed u64"));
            builder
                .dir
                .append_value(dir.try_into().expect("NodeId overflowed u64"));
            builder.path.append_value(path_parts.build_path(graph));
        }
        Ok(true) // always recurse
    };

    let on_revrel = |_cnt: NodeId, _path_parts: PathParts| Ok(());

    swh_graph_provenance::frontier::backward_dfs_with_path(
        graph,
        reachable_nodes,
        on_directory,
        on_revrel,
        cnt,
    )
}
