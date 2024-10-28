// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Parser;
use dsi_progress_logger::{progress_logger, ProgressLog};

use dataset_writer::{ParallelDatasetWriter, ParquetTableWriter};
use swh_graph::graph::*;
use swh_graph::mph::DynMphf;
use swh_graph::utils::mmap::NumberMmap;

use swh_graph_provenance::filters::{load_reachable_nodes, NodeFilter};
use swh_graph_provenance::x_in_y_dataset::{dir_in_revrel_schema, dir_in_revrel_writer_properties};

#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

#[derive(Parser, Debug)]
/** Given as input a binary file with, for each directory, the newest date of first
 * occurrence of any of the content in its subtree (well, DAG), ie.,
 * max_{for all content} (min_{for all occurrence of content} occurrence).
 * and a Parquet table with the node ids of every frontier directory.
 * Produces the line of frontier directories (relative to any revision) present in
 * each revision.
 */
struct Args {
    graph_path: PathBuf,
    #[arg(long)]
    /// Maximum number of bytes in a thread's output Parquet buffer,
    /// before it is flushed to disk
    thread_buffer_size: Option<usize>,
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
    /// Path to read the array of max timestamps from
    max_timestamps: PathBuf,
    #[arg(long)]
    /// Path to a directory where to write .csv.zst results to
    directories_out: PathBuf,
}

pub fn main() -> Result<()> {
    let args = Args::parse();

    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    log::info!("Loading graph");
    let graph = swh_graph::graph::SwhBidirectionalGraph::new(args.graph_path)
        .context("Could not load graph")?
        .load_backward_labels()
        .context("Could not load labels")?
        .init_properties()
        .load_properties(|props| props.load_label_names())
        .context("Could not load label names")?
        .load_properties(|props| props.load_maps::<DynMphf>())
        .context("Could not load maps")?
        .load_properties(|props| props.load_timestamps())
        .context("Could not load timestamps")?;
    log::info!("Graph loaded.");

    let max_timestamps =
        NumberMmap::<byteorder::BE, i64, _>::new(&args.max_timestamps, graph.num_nodes())
            .with_context(|| format!("Could not mmap {}", args.max_timestamps.display()))?;

    let mut pl = progress_logger!(
        item_name = "node",
        display_memory = true,
        local_speed = true,
    );
    pl.start("Loading frontier directories...");
    let frontier_directories = swh_graph_provenance::frontier_set::from_parquet(
        &graph,
        args.frontier_directories,
        &mut pl,
    )?;
    pl.done();

    let reachable_nodes = load_reachable_nodes(&graph, args.node_filter, args.reachable_nodes)?;

    let mut dataset_writer = ParallelDatasetWriter::<ParquetTableWriter<_>>::with_schema(
        args.directories_out,
        (
            Arc::new(dir_in_revrel_schema()),
            dir_in_revrel_writer_properties(&graph).build(),
        ),
    )?;
    dataset_writer.config.autoflush_buffer_size = args.thread_buffer_size;

    swh_graph_provenance::directories_in_revisions::write_revisions_from_frontier_directories(
        &graph,
        &max_timestamps,
        args.node_filter,
        reachable_nodes.as_ref(),
        &frontier_directories,
        dataset_writer,
    )
}
