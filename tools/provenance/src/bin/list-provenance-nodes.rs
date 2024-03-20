// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use clap::{Parser, ValueEnum};
use dsi_progress_logger::{ProgressLog, ProgressLogger};
use rayon::prelude::*;
use sux::prelude::{AtomicBitVec, BitVec};

use swh_graph::graph::*;
use swh_graph::java_compat::mph::gov::GOVMPH;
use swh_graph::utils::shuffle::par_iter_shuffled_range;
use swh_graph::SWHType;

use swh_graph_provenance::dataset_writer::{ParallelDatasetWriter, ParquetTableWriter};
use swh_graph_provenance::node_dataset::{schema, writer_properties, NodeTableBuilder};

#[derive(ValueEnum, Debug, Clone, Copy)]
enum NodeFilter {
    /// All releases, and only revisions pointed by either a release or a snapshot
    Heads,
    /// All releases and all revisions.
    All,
}

#[derive(Parser, Debug)]
/** Writes the list of nodes reachable from a 'head' revision or a release.
 */
struct Args {
    graph_path: PathBuf,
    #[arg(short, long, action = clap::ArgAction::Count)]
    verbose: u8,
    #[arg(value_enum)]
    #[arg(long, default_value_t = NodeFilter::Heads)]
    /// Subset of revisions and releases to traverse from
    node_filter: NodeFilter,
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

    stderrlog::new()
        .verbosity(args.verbose as usize)
        .timestamp(stderrlog::Timestamp::Second)
        .init()
        .context("While Initializing the stderrlog")?;

    log::info!("Loading graph");
    let graph = swh_graph::graph::load_bidirectional(args.graph_path)
        .context("Could not load graph")?
        .init_properties()
        .load_properties(|props| props.load_maps::<GOVMPH>())
        .context("Could not load maps")?;
    log::info!("Graph loaded.");

    let mut dataset_writer = ParallelDatasetWriter::new_with_schema(
        args.nodes_out,
        (
            Arc::new(schema()),
            writer_properties(&graph)
                .set_column_bloom_filter_fpp("sha1_git".into(), args.bloom_fpp)
                .set_column_bloom_filter_ndv("sha1_git".into(), args.bloom_ndv)
                .build(),
        ),
    )?;

    // We write at most one row per call to `dataset_writer.builder()`, so every row
    // group will be exactly this size.
    dataset_writer.flush_threshold = Some(args.row_group_size);

    let reachable_nodes = find_reachable_nodes(&graph, args.node_filter)?;

    write_reachable_nodes(&graph, dataset_writer, &reachable_nodes)?;

    Ok(())
}

fn find_reachable_nodes<G>(graph: &G, node_filter: NodeFilter) -> Result<BitVec>
where
    G: SwhForwardGraph + SwhBackwardGraph + SwhGraphWithProperties + Sync,
    <G as SwhGraphWithProperties>::Maps: swh_graph::properties::Maps,
{
    let mut pl = ProgressLogger::default();
    pl.item_name("node");
    pl.display_memory(true);
    pl.expected_updates(Some(graph.num_nodes()));
    pl.start("Listing reachable contents and directories...");
    let pl = Arc::new(Mutex::new(pl));

    let reachable_from_heads = AtomicBitVec::new(graph.num_nodes());

    par_iter_shuffled_range(0..graph.num_nodes()).try_for_each(|root| -> Result<()> {
        let search_in_node = match node_filter {
            NodeFilter::All => true,
            NodeFilter::Heads => swh_graph_provenance::filters::is_head(graph, root),
        };

        if search_in_node {
            let mut stack = vec![root];

            while let Some(node) = stack.pop() {
                reachable_from_heads.set(node, true, Ordering::Relaxed);
                for succ in graph.successors(node) {
                    if reachable_from_heads.get(succ, Ordering::Relaxed) {
                        // Already visited, either by this DFS or an other one
                    } else if let SWHType::Content | SWHType::Directory =
                        graph.properties().node_type(succ)
                    {
                        stack.push(succ);
                    }
                }
            }
        }
        if root % 32768 == 0 {
            pl.lock().unwrap().update_with_count(32768);
        }
        Ok(())
    })?;

    pl.lock().unwrap().done();

    Ok(reachable_from_heads.into())
}

fn write_reachable_nodes<G>(
    graph: &G,
    dataset_writer: ParallelDatasetWriter<ParquetTableWriter<NodeTableBuilder>>,
    reachable_nodes: &BitVec,
) -> Result<()>
where
    G: SwhGraphWithProperties + Sync,
    <G as SwhGraphWithProperties>::Maps: swh_graph::properties::Maps,
{
    let mut pl = ProgressLogger::default();
    pl.item_name("node");
    pl.display_memory(true);
    pl.expected_updates(Some(graph.num_nodes()));
    pl.start("Writing list of reachable nodes...");
    let pl = Arc::new(Mutex::new(pl));

    // Split into a small number of chunks. This causes the node ids to form long
    // monotonically increasing sequences in the output dataset, which makes them
    // easy to index using Parquet/ORC chunk statistics. And should compress better
    // with delta-encoding.
    // However, we still want this loop (and readers) to parallelize nicely,
    // so the number of chunks cannot be too low either.
    let num_chunks = 96;
    let chunk_size = graph.num_nodes().div_ceil(num_chunks);

    (0..graph.num_nodes())
        .into_par_iter()
        .by_uniform_blocks(chunk_size)
        .try_for_each_init(
            || dataset_writer.get_thread_writer().unwrap(),
            |writer, node| -> Result<()> {
                if reachable_nodes.get(node) {
                    writer.builder()?.add_node(graph, node);
                }
                if node % 32768 == 0 {
                    pl.lock().unwrap().update_with_count(32768);
                }
                Ok(())
            },
        )?;

    log::info!("Flushing writers...");
    dataset_writer.close()?;

    pl.lock().unwrap().done();

    Ok(())
}
