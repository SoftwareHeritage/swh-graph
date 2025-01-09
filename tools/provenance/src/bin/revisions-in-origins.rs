// Copyright (C) 2024-2025  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Parser;

use dataset_writer::{ParallelDatasetWriter, ParquetTableWriter};
use swh_graph::mph::DynMphf;

use swh_graph_provenance::filters::NodeFilter;
use swh_graph_provenance::x_in_y_dataset::{revrel_in_ori_schema, revrel_in_ori_writer_properties};

#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

#[derive(Parser, Debug)]
/** Given a Parquet table with the node ids of every frontier directory.
 * Produces the list of contents reachable from each revision, without any going through
 * any directory that is a frontier (relative to any revision).
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
    #[arg(long)]
    /// Path to a directory where to write .parquet results to
    revisions_out: PathBuf,
}

pub fn main() -> Result<()> {
    let args = Args::parse();

    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    log::info!("Loading graph");
    let graph = swh_graph::graph::SwhBidirectionalGraph::new(args.graph_path)
        .context("Could not load graph")?
        .init_properties()
        .load_properties(|props| props.load_maps::<DynMphf>())
        .context("Could not load maps")?
        .load_properties(|props| props.load_timestamps())
        .context("Could not load timestamps")?;
    log::info!("Graph loaded.");

    let mut dataset_writer = ParallelDatasetWriter::<ParquetTableWriter<_>>::with_schema(
        args.revisions_out,
        (
            Arc::new(revrel_in_ori_schema()),
            revrel_in_ori_writer_properties(&graph).build(),
        ),
    )?;
    dataset_writer.config.autoflush_buffer_size = args.thread_buffer_size;

    swh_graph_provenance::revisions_in_origins::main(&graph, args.node_filter, dataset_writer)
}
