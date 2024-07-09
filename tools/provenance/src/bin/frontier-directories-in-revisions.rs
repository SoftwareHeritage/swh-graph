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
use sux::bits::bit_vec::BitVec;

use swh_graph::graph::*;
use swh_graph::java_compat::mph::gov::GOVMPH;
use swh_graph::utils::mmap::NumberMmap;
use swh_graph::utils::GetIndex;

use swh_graph::utils::dataset_writer::{ParallelDatasetWriter, ParquetTableWriter};
use swh_graph_provenance::filters::{load_reachable_nodes, NodeFilter};
use swh_graph_provenance::frontier::PathParts;
use swh_graph_provenance::x_in_y_dataset::{
    dir_in_revrel_schema, dir_in_revrel_writer_properties, DirInRevrelTableBuilder,
};

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
    #[arg(short, long, action = clap::ArgAction::Count)]
    verbose: u8,
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
        .context("Could not load maps")?
        .load_properties(|props| props.load_timestamps())
        .context("Could not load timestamps")?;
    log::info!("Graph loaded.");

    let max_timestamps =
        NumberMmap::<byteorder::BE, i64, _>::new(&args.max_timestamps, graph.num_nodes())
            .with_context(|| format!("Could not mmap {}", args.max_timestamps.display()))?;

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

    let reachable_nodes = load_reachable_nodes(&graph, args.node_filter, args.reachable_nodes)?;

    let mut dataset_writer = ParallelDatasetWriter::<ParquetTableWriter<_>>::with_schema(
        args.directories_out,
        (
            Arc::new(dir_in_revrel_schema()),
            dir_in_revrel_writer_properties(&graph).build(),
        ),
    )?;
    dataset_writer.config.autoflush_buffer_size = args.thread_buffer_size;

    write_revisions_from_frontier_directories(
        &graph,
        &max_timestamps,
        args.node_filter,
        reachable_nodes.as_ref(),
        &frontier_directories,
        dataset_writer,
    )
}

fn write_revisions_from_frontier_directories<G>(
    graph: &G,
    max_timestamps: impl GetIndex<Output = i64> + Sync + Copy,
    node_filter: NodeFilter,
    reachable_nodes: Option<&BitVec>,
    frontier_directories: &BitVec,
    dataset_writer: ParallelDatasetWriter<ParquetTableWriter<DirInRevrelTableBuilder>>,
) -> Result<()>
where
    G: SwhLabeledBackwardGraph + SwhGraphWithProperties + Send + Sync + 'static,
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

    swh_graph::utils::shuffle::par_iter_shuffled_range(0..graph.num_nodes()).try_for_each_init(
        || dataset_writer.get_thread_writer().unwrap(),
        |writer, node| -> Result<()> {
            if frontier_directories.get(node) {
                write_revisions_from_frontier_directory(
                    graph,
                    max_timestamps,
                    node_filter,
                    reachable_nodes,
                    frontier_directories,
                    writer,
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

    log::info!("Visits done, finishing output");

    Ok(())
}

fn write_revisions_from_frontier_directory<G>(
    graph: &G,
    max_timestamps: impl GetIndex<Output = i64>,
    node_filter: NodeFilter,
    reachable_nodes: Option<&BitVec>,
    frontier_directories: &BitVec,
    writer: &mut ParquetTableWriter<DirInRevrelTableBuilder>,
    dir: NodeId,
) -> Result<()>
where
    G: SwhLabeledBackwardGraph + SwhGraphWithProperties,
    <G as SwhGraphWithProperties>::LabelNames: swh_graph::properties::LabelNames,
    <G as SwhGraphWithProperties>::Maps: swh_graph::properties::Maps,
    <G as SwhGraphWithProperties>::Timestamps: swh_graph::properties::Timestamps,
{
    if !frontier_directories[dir] {
        return Ok(());
    }
    let dir_max_timestamp = max_timestamps.get(dir).expect("max_timestamps too small");
    if dir_max_timestamp == i64::MIN {
        // Somehow does not have a max timestamp. Presumably because it does not
        // have any content.
        return Ok(());
    }

    let on_directory = |node, _path_parts: PathParts| -> Result<bool> {
        if node == dir {
            // The root is a frontier directory, and we always want to recurse from it
            return Ok(true);
        }
        // Don't recurse if this is a frontier directory
        Ok(!frontier_directories[node])
    };

    let on_revrel = |revrel, path_parts: PathParts| -> Result<()> {
        let Some(revrel_timestamp) = graph.properties().author_timestamp(revrel) else {
            return Ok(());
        };

        if !swh_graph_provenance::filters::is_root_revrel(graph, node_filter, revrel) {
            return Ok(());
        }
        let builder = writer.builder()?;
        builder
            .dir
            .append_value(dir.try_into().expect("NodeId overflowed u64"));
        builder.dir_max_author_date.append_value(dir_max_timestamp);
        builder
            .revrel
            .append_value(revrel.try_into().expect("NodeId overflowed u64"));
        builder.revrel_author_date.append_value(revrel_timestamp);
        builder.path.append_value(path_parts.build_path(graph));

        Ok(())
    };
    swh_graph_provenance::frontier::backward_dfs_with_path(
        graph,
        reachable_nodes,
        on_directory,
        on_revrel,
        dir,
    )
}
