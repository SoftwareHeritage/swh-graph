// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::{bail, Context, Result};
use clap::Parser;
use dsi_progress_logger::{progress_logger, ProgressLog};
use rayon::prelude::*;
use sux::prelude::BitVec;

use swh_graph::graph::*;
use swh_graph::java_compat::mph::gov::GOVMPH;
use swh_graph::utils::mmap::NumberMmap;
use swh_graph::utils::shuffle::par_iter_shuffled_range;
use swh_graph::utils::GetIndex;
use swh_graph::NodeType;

use swh_graph_provenance::filters::{load_reachable_nodes, NodeFilter};

#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

#[derive(Parser, Debug)]
/** Given as argument a binary file containing an array of timestamps which is,
 * for every content, the date of first occurrence of that content in a revision,
 * produces a binary file in the same format, which contains for each directory,
 * the max of these values for all contents in that directory.
 *
 * If the <path/to/provenance_timestamps.bin> parameter is passed, then this file
 * is written as an array of longs, which can be loaded with LongMappedBigList.
 *
 * This new date is guaranteed to be greater or equal to the one in the input.
 */
struct Args {
    graph_path: PathBuf,
    #[arg(value_enum)]
    #[arg(long, default_value_t = NodeFilter::Heads)]
    /// Subset of revisions and releases to traverse from
    node_filter: NodeFilter,
    #[arg(long)]
    /// Path to the Parquet table with the node ids of all nodes reachable from
    /// a head revision/release
    reachable_nodes: PathBuf,
    #[arg(long)]
    /// Path to read the array of timestamps from
    timestamps: PathBuf,
    #[arg(long)]
    /// Path to write the array of max timestamps to
    max_timestamps_out: Option<PathBuf>,
}

pub fn main() -> Result<()> {
    let args = Args::parse();

    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    log::info!("Loading graph");
    let graph = swh_graph::graph::SwhBidirectionalGraph::new(args.graph_path)
        .context("Could not load graph")?
        .init_properties()
        .load_properties(|props| props.load_maps::<GOVMPH>())
        .context("Could not load maps")?;
    log::info!("Graph loaded.");

    let timestamps = NumberMmap::<byteorder::BE, i64, _>::new(&args.timestamps, graph.num_nodes())
        .with_context(|| format!("Could not mmap {}", args.timestamps.display()))?;
    let timestamps = &timestamps;

    // This array is NOT in big-endian in order to support AtomicI64::fetch_min
    let mut max_timestamps = Vec::with_capacity(graph.num_nodes());
    max_timestamps.resize_with(graph.num_nodes(), || AtomicI64::new(i64::MIN));

    let max_timestamps_file = match args.max_timestamps_out {
        Some(ref max_timestamps_path) => Some(
            std::fs::File::create(max_timestamps_path)
                .with_context(|| format!("Could not create {}", max_timestamps_path.display()))?,
        ),
        None => None,
    };

    let reachable_nodes = load_reachable_nodes(&graph, args.node_filter, args.reachable_nodes)?;
    propagate_through_directories(
        &graph,
        reachable_nodes.as_ref(),
        &timestamps,
        &mut max_timestamps,
    )?;

    if let Some(mut max_timestamps_file) = max_timestamps_file {
        let max_timestamps_path = args.max_timestamps_out.unwrap();
        log::info!("Converting timestamps to big-endian");
        let max_timestamps_be: Vec<i64> = max_timestamps
            .into_par_iter()
            .map(|max_timestamp| max_timestamp.into_inner().to_be())
            .collect();
        log::info!("Writing binary output to {}", max_timestamps_path.display());
        max_timestamps_file
            .write_all(bytemuck::cast_slice(max_timestamps_be.as_slice()))
            .with_context(|| format!("Could not write to {}", max_timestamps_path.display()))?;
    }

    log::info!("Done.");

    Ok(())
}

/// Propagate maximum of timestamps from contents to any directory containing them
fn propagate_through_directories<G>(
    graph: &G,
    reachable_nodes: Option<&BitVec>,
    timestamps: &(impl GetIndex<Output = i64> + Sync),
    max_timestamps: &mut [AtomicI64],
) -> Result<()>
where
    G: SwhBackwardGraph + SwhGraphWithProperties + Sync,
    <G as SwhGraphWithProperties>::Maps: swh_graph::properties::Maps,
{
    let mut pl = progress_logger!(
        item_name = "node",
        display_memory = true,
        local_speed = true,
        expected_updates = Some(graph.num_nodes()),
    );
    pl.start("Propagating through directories...");
    let pl = Arc::new(Mutex::new(pl));

    let reachable = |node| match reachable_nodes {
        Some(reachable_nodes) => reachable_nodes.get(node),
        None => true, // All nodes are reachable
    };

    par_iter_shuffled_range(0..graph.num_nodes()).try_for_each(|cnt| {
        if reachable(cnt) && graph.properties().node_type(cnt) == NodeType::Content {
            let cnt_timestamp = timestamps.get(cnt).unwrap();
            if cnt_timestamp == i64::MIN {
                // Content is not in any timestamped revrel, ignore it.
            } else {
                let mut stack = vec![cnt];

                while let Some(node) = stack.pop() {
                    for pred in graph.predecessors(node) {
                        if !reachable(pred) {
                            continue;
                        }
                        match graph.properties().node_type(pred) {
                            NodeType::Directory => {
                                let previous_max = max_timestamps[pred]
                                    .fetch_max(cnt_timestamp, Ordering::Relaxed);
                                if previous_max >= cnt_timestamp {
                                    // Already traversed from a content with a newer timestamp
                                    // than this one (or already from this one), so every
                                    // directory we would find from now on would too.
                                    // No need to recurse further.
                                } else {
                                    stack.push(pred);
                                }
                            }
                            NodeType::Content => bail!(
                                "{} is predecessor of {}",
                                graph.properties().swhid(pred),
                                graph.properties().swhid(node)
                            ),
                            _ => (),
                        }
                    }
                }
            }
        }
        if cnt % 32768 == 0 {
            pl.lock().unwrap().update_with_count(32768);
        }
        Ok(())
    })?;

    pl.lock().unwrap().done();

    Ok(())
}
