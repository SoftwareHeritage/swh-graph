// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

#![allow(non_snake_case)]

use std::num::NonZeroU16;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use clap::Parser;
use dsi_progress_logger::{progress_logger, ProgressLog};
use rayon::prelude::*;
use sux::bits::bit_vec::BitVec;

use dataset_writer::{ParallelDatasetWriter, ParquetTableWriter, PartitionedTableWriter};
use swh_graph::graph::*;
use swh_graph::mph::DynMphf;
use swh_graph::utils::progress_logger::{BufferedProgressLogger, MinimalProgressLog};
use swh_graph::NodeType;

use swh_graph_provenance::filters::{load_reachable_nodes, NodeFilter};
use swh_graph_provenance::frontier::PathParts;
use swh_graph_provenance::x_in_y_dataset::{
    cnt_in_revrel_schema, cnt_in_revrel_writer_properties, get_partition, CntInRevrelTableBuilder,
};

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
    /// Number of partitions to split each table into, based on the `cnt` node id.
    /// Disables partitioning if unset or 0
    num_partitions: u16,
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

    let mut dataset_writer =
        ParallelDatasetWriter::<PartitionedTableWriter<ParquetTableWriter<_>>>::with_schema(
            args.contents_out,
            (
                "cnt_partition".into(),
                NonZeroU16::new(args.num_partitions),
                (
                    Arc::new(cnt_in_revrel_schema()),
                    cnt_in_revrel_writer_properties(&graph).build(),
                ),
            ),
        )?;
    dataset_writer.config.autoflush_buffer_size = args.thread_buffer_size;

    write_revisions_from_contents(
        &graph,
        args.node_filter,
        reachable_nodes.as_ref(),
        &frontier_directories,
        dataset_writer,
    )
}

fn write_revisions_from_contents<G>(
    graph: &G,
    node_filter: NodeFilter,
    reachable_nodes: Option<&BitVec>,
    frontier_directories: &BitVec,
    dataset_writer: ParallelDatasetWriter<
        PartitionedTableWriter<ParquetTableWriter<CntInRevrelTableBuilder>>,
    >,
) -> Result<()>
where
    G: SwhLabeledBackwardGraph + SwhGraphWithProperties + Send + Sync + 'static,
    <G as SwhGraphWithProperties>::LabelNames: swh_graph::properties::LabelNames,
    <G as SwhGraphWithProperties>::Maps: swh_graph::properties::Maps,
    <G as SwhGraphWithProperties>::Timestamps: swh_graph::properties::Timestamps,
{
    let mut pl = progress_logger!(
        item_name = "node",
        display_memory = true,
        local_speed = true,
        expected_updates = Some(graph.num_nodes()),
    );
    pl.start("Visiting revisions' directories...");

    let shared_pl = Arc::new(Mutex::new(&mut pl));
    swh_graph::utils::shuffle::par_iter_shuffled_range(0..graph.num_nodes()).try_for_each_init(
        || {
            (
                dataset_writer.get_thread_writer().unwrap(),
                BufferedProgressLogger::new(shared_pl.clone()),
            )
        },
        |(writer, thread_pl), node| -> Result<()> {
            let is_reachable = match reachable_nodes {
                None => true,
                Some(reachable_nodes) => reachable_nodes.get(node),
            };
            if is_reachable && graph.properties().node_type(node) == NodeType::Content {
                let writer = get_partition(writer.partitions(), node, &graph);
                find_revisions_from_content(
                    graph,
                    node_filter,
                    reachable_nodes,
                    frontier_directories,
                    writer,
                    node,
                )?;
            }
            thread_pl.light_update();
            Ok(())
        },
    )?;

    pl.done();

    log::info!("Visits done, finishing output");

    Ok(())
}

fn find_revisions_from_content<G>(
    graph: &G,
    node_filter: NodeFilter,
    reachable_nodes: Option<&BitVec>,
    frontier_directories: &BitVec,
    writer: &mut ParquetTableWriter<CntInRevrelTableBuilder>,
    cnt: NodeId,
) -> Result<()>
where
    G: SwhLabeledBackwardGraph + SwhGraphWithProperties,
    <G as SwhGraphWithProperties>::LabelNames: swh_graph::properties::LabelNames,
    <G as SwhGraphWithProperties>::Maps: swh_graph::properties::Maps,
    <G as SwhGraphWithProperties>::Timestamps: swh_graph::properties::Timestamps,
{
    let on_directory = |dir: NodeId, _path_parts: PathParts| {
        if dir == cnt {
            // FIXME: backward_dfs_with_path always calls this function on the root,
            // even if it is a content.
            return Ok(true);
        }

        Ok(!frontier_directories[dir]) // Recurse only if this is not a frontier
    };

    let on_revrel = |revrel: NodeId, path_parts: PathParts| {
        let Some(revrel_timestamp) = graph.properties().author_timestamp(revrel) else {
            return Ok(());
        };
        if !swh_graph_provenance::filters::is_root_revrel(graph, node_filter, revrel) {
            return Ok(());
        }

        let builder = writer.builder()?;
        builder
            .cnt
            .append_value(cnt.try_into().expect("NodeId overflowed u64"));
        builder.revrel_author_date.append_value(revrel_timestamp);
        builder
            .revrel
            .append_value(revrel.try_into().expect("NodeId overflowed u64"));
        builder.path.append_value(path_parts.build_path(graph));
        Ok(())
    };

    swh_graph_provenance::frontier::backward_dfs_with_path(
        graph,
        reachable_nodes,
        on_directory,
        on_revrel,
        cnt,
    )
}
