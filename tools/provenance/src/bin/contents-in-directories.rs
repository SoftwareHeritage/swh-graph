// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

#![allow(non_snake_case)]

use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use clap::Parser;
use dsi_progress_logger::{progress_logger, ProgressLog};
use rayon::prelude::*;
use sux::prelude::{AtomicBitVec, BitVec};

use swh_graph::collections::NodeSet;
use swh_graph::graph::*;
use swh_graph::java_compat::mph::gov::GOVMPH;
use swh_graph::NodeType;

use swh_graph::utils::dataset_writer::{ParallelDatasetWriter, ParquetTableWriter};
use swh_graph_provenance::filters::NodeFilter;
use swh_graph_provenance::frontier::PathParts;
use swh_graph_provenance::x_in_y_dataset::{
    cnt_in_dir_schema, cnt_in_dir_writer_properties, CntInDirTableBuilder,
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
 * Produces the list of contents present in each directory.
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
    /// Path to the Parquet table with the node ids of frontier directories
    frontier_directories: PathBuf,
    #[arg(long)]
    /// Path to a directory where to write .parquet results to
    contents_out: PathBuf,
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
        .load_properties(|props| props.load_maps::<GOVMPH>())
        .context("Could not load maps")?;
    log::info!("Graph loaded.");

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

    let mut dataset_writer = ParallelDatasetWriter::<ParquetTableWriter<_>>::with_schema(
        args.contents_out,
        (
            Arc::new(cnt_in_dir_schema()),
            cnt_in_dir_writer_properties(&graph).build(),
        ),
    )?;
    dataset_writer.config.autoflush_buffer_size = args.thread_buffer_size;

    // List all directories (and contents) forward-reachable from a frontier directories.
    // So when walking backward from a content, if the walk ever sees a directory
    // not in this set then it can safely be ignored as walking further backward
    // won't ever reach a frontier directory.
    let mut pl = progress_logger!(
        item_name = "node",
        display_memory = true,
        local_speed = true,
        expected_updates = Some(graph.num_nodes()),
    );
    pl.start("Listing nodes reachable from frontier directories...");
    let pl = Arc::new(Mutex::new(pl));
    let reachable_nodes_from_frontier = AtomicBitVec::new(graph.num_nodes());
    swh_graph::utils::shuffle::par_iter_shuffled_range(0..graph.num_nodes()).for_each(|root| {
        if frontier_directories.contains(root) {
            let mut to_visit = vec![root];
            while let Some(node) = to_visit.pop() {
                if reachable_nodes_from_frontier.get(node, Ordering::Relaxed) {
                    // Node already visisted by another traversal; no need to recurse further
                    continue;
                }
                reachable_nodes_from_frontier.set(node, true, Ordering::Relaxed);
                for succ in graph.successors(node) {
                    match graph.properties().node_type(succ) {
                        NodeType::Directory | NodeType::Content => {
                            to_visit.push(succ);
                        }
                        _ => (),
                    }
                }
            }
        }
        if root % 32768 == 0 {
            pl.lock().unwrap().update_with_count(32768);
        }
    });
    pl.lock().unwrap().done();
    let reachable_nodes_from_frontier: BitVec = reachable_nodes_from_frontier.into();

    let mut pl = progress_logger!(
        item_name = "node",
        display_memory = true,
        local_speed = true,
        expected_updates = Some(graph.num_nodes()),
    );
    pl.start("Listing contents in directories...");
    let pl = Arc::new(Mutex::new(pl));

    swh_graph::utils::shuffle::par_iter_shuffled_range(0..graph.num_nodes()).try_for_each_init(
        || dataset_writer.get_thread_writer().unwrap(),
        |writer, node| -> Result<()> {
            if reachable_nodes_from_frontier.get(node)
                && graph.properties().node_type(node) == NodeType::Content
            {
                write_frontier_directories_from_content(
                    &graph,
                    writer,
                    &reachable_nodes_from_frontier,
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
    reachable_nodes_from_frontier: &BitVec,
    frontier_directories: &BitVec,
    cnt: NodeId,
) -> Result<()>
where
    G: SwhLabeledBackwardGraph + SwhGraphWithProperties,
    <G as SwhGraphWithProperties>::LabelNames: swh_graph::properties::LabelNames,
    <G as SwhGraphWithProperties>::Maps: swh_graph::properties::Maps,
{
    let on_directory = |dir: NodeId, path_parts: PathParts| {
        if !reachable_nodes_from_frontier.contains(dir) {
            // The directory is not reachable from any frontier directory
            return Ok(false);
        }
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
        Some(reachable_nodes_from_frontier),
        on_directory,
        on_revrel,
        cnt,
    )
}
