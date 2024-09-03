// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::num::NonZeroU16;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

use anyhow::{ensure, Context, Result};
use clap::Parser;
use dsi_progress_logger::{progress_logger, ProgressLog};
use rayon::prelude::*;
use sux::prelude::{AtomicBitVec, BitVec};

use dataset_writer::{ParallelDatasetWriter, ParquetTableWriter, PartitionedTableWriter};
use swh_graph::graph::*;
use swh_graph::java_compat::mph::gov::GOVMPH;
use swh_graph::utils::progress_logger::{BufferedProgressLogger, MinimalProgressLog};
use swh_graph::utils::shuffle::par_iter_shuffled_range;
use swh_graph::NodeType;

use swh_graph_provenance::filters::{is_root_revrel, NodeFilter};
use swh_graph_provenance::node_dataset::{schema, writer_properties, NodeTableBuilder};

#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

#[derive(Parser, Debug)]
/** Writes the list of nodes reachable from a 'head' revision or a release.
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
    #[arg(long, default_value_t = 0)]
    /// Number of subfolders (Hive partitions) to store Parquet files in.
    ///
    /// Rows are partitioned across these folders based on the high bits of the object hash,
    /// to use Parquet statistics for row group filtering.
    ///
    /// RAM usage and number of written files is proportional to this value.
    num_partitions: u16,
    #[arg(long, default_value_t = 1_000_000)] // Parquet's default max_row_group_size
    /// Number of rows written at once, forming a row group on disk.
    ///
    /// Higher values reduce index scans, but lower values reduce time wasted per
    /// false positive in the probabilitistic filters.
    row_group_size: usize,
    #[arg(long, default_value_t = 0.05)] // Parquet's default
    /// false positive probability for 'sha1_git' Bloom filter
    bloom_fpp: f64,
    #[arg(long, default_value_t = 1_000_000)] // Parquet's default
    /// number of distinct values for 'sha1_git' Bloom filter
    bloom_ndv: u64,
    #[arg(long)]
    /// Directory to write the list of nodes to
    nodes_out: PathBuf,
}

pub fn main() -> Result<()> {
    let args = Args::parse();

    ensure!(
        args.num_partitions == 0 || args.num_partitions.is_power_of_two(),
        "--num-partitions must be 0 or a power of 2"
    );
    ensure!(
        args.num_partitions <= 256,
        "--num-partitions cannot be greater than 256"
    );
    let num_partitions: Option<NonZeroU16> = args.num_partitions.try_into().ok();

    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    log::info!("Loading graph");
    let graph = swh_graph::graph::SwhBidirectionalGraph::new(args.graph_path)
        .context("Could not load graph")?
        .init_properties()
        .load_properties(|props| props.load_maps::<GOVMPH>())
        .context("Could not load maps")?;
    log::info!("Graph loaded.");

    let mut dataset_writer =
        ParallelDatasetWriter::<PartitionedTableWriter<ParquetTableWriter<_>>>::with_schema(
            args.nodes_out,
            (
                "partition".to_owned(), // Partition column name
                num_partitions,
                (
                    Arc::new(schema()),
                    writer_properties(&graph)
                        .set_column_bloom_filter_fpp("sha1_git".into(), args.bloom_fpp)
                        .set_column_bloom_filter_ndv("sha1_git".into(), args.bloom_ndv)
                        .build(),
                ),
            ),
        )?;

    // We write at most one row per call to `dataset_writer.builder()`, so every row
    // group will be exactly this size.
    dataset_writer.config.autoflush_row_group_len = Some(args.row_group_size);

    dataset_writer.config.autoflush_buffer_size = args.thread_buffer_size;

    let reachable_nodes = find_reachable_nodes(&graph, args.node_filter)?;

    write_reachable_nodes(
        &graph,
        dataset_writer,
        num_partitions.unwrap_or(NonZeroU16::new(1).unwrap()),
        &reachable_nodes,
    )?;

    Ok(())
}

fn find_reachable_nodes<G>(graph: &G, node_filter: NodeFilter) -> Result<BitVec>
where
    G: SwhForwardGraph + SwhBackwardGraph + SwhGraphWithProperties + Sync,
    <G as SwhGraphWithProperties>::Maps: swh_graph::properties::Maps,
{
    let mut pl = progress_logger!(
        item_name = "node",
        display_memory = true,
        local_speed = true,
        expected_updates = Some(graph.num_nodes()),
    );
    pl.start("Listing reachable contents and directories...");
    let reachable_from_heads = AtomicBitVec::new(graph.num_nodes());

    par_iter_shuffled_range(0..graph.num_nodes()).try_for_each_with(
        BufferedProgressLogger::new(Arc::new(Mutex::new(&mut pl))),
        |thread_pl, root| -> Result<()> {
            if is_root_revrel(graph, node_filter, root) {
                let mut stack = vec![root];

                while let Some(node) = stack.pop() {
                    reachable_from_heads.set(node, true, Ordering::Relaxed);
                    for succ in graph.successors(node) {
                        if reachable_from_heads.get(succ, Ordering::Relaxed) {
                            // Already visited, either by this DFS or an other one
                        } else if let NodeType::Content | NodeType::Directory =
                            graph.properties().node_type(succ)
                        {
                            stack.push(succ);
                        }
                    }
                }
            }
            thread_pl.light_update();
            Ok(())
        },
    )?;

    pl.done();

    Ok(reachable_from_heads.into())
}

fn write_reachable_nodes<G>(
    graph: &G,
    dataset_writer: ParallelDatasetWriter<
        PartitionedTableWriter<ParquetTableWriter<NodeTableBuilder>>,
    >,
    num_partitions: NonZeroU16,
    reachable_nodes: &BitVec,
) -> Result<()>
where
    G: SwhGraphWithProperties + Sync,
    <G as SwhGraphWithProperties>::Maps: swh_graph::properties::Maps,
{
    assert!(num_partitions.is_power_of_two());

    let mut pl = progress_logger!(
        item_name = "node",
        display_memory = true,
        local_speed = true,
        expected_updates = Some(graph.num_nodes()),
    );
    pl.start("Writing list of reachable nodes...");

    // Split into a small number of chunks. This causes the node ids to form long
    // monotonically increasing sequences in the output dataset, which makes them
    // easy to index using Parquet/ORC chunk statistics. And should compress better
    // with delta-encoding.
    // However, we still want this loop (and readers) to parallelize nicely,
    // so the number of chunks cannot be too low either.
    let num_chunks = 96;
    let chunk_size = graph.num_nodes().div_ceil(num_chunks);

    let shared_pl = Arc::new(Mutex::new(&mut pl));
    (0..graph.num_nodes())
        .into_par_iter()
        .by_uniform_blocks(chunk_size)
        .try_for_each_init(
            || {
                (
                    dataset_writer.get_thread_writer().unwrap(),
                    BufferedProgressLogger::new(shared_pl.clone()),
                )
            },
            |(writer, thread_pl), node| -> Result<()> {
                if reachable_nodes.get(node) {
                    let swhid = graph.properties().swhid(node);
                    // Bucket SWHIDs by their high bits, so we can use Parquet's column
                    // statistics to row groups.
                    let partition_id: usize =
                        (swhid.hash[0] as u16 * num_partitions.get() / 256).into();
                    writer.partitions()[partition_id]
                        .builder()?
                        .add_node(graph, node);
                }
                thread_pl.light_update();
                Ok(())
            },
        )?;

    log::info!("Flushing writers...");
    dataset_writer.close()?;

    pl.done();

    Ok(())
}
