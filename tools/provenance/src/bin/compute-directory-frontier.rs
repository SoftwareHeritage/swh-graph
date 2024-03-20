// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use clap::Parser;
use dsi_progress_logger::{ProgressLog, ProgressLogger};
use rayon::prelude::*;
use sux::prelude::{AtomicBitVec, BitVec};

use swh_graph::collections::{AdaptiveNodeSet, NodeSet};
use swh_graph::graph::*;
use swh_graph::java_compat::mph::gov::GOVMPH;
use swh_graph::utils::mmap::NumberMmap;
use swh_graph::utils::GetIndex;
use swh_graph::SWHType;

use swh_graph_provenance::dataset_writer::ParallelDatasetWriter;
use swh_graph_provenance::frontier_set::{schema, to_parquet, writer_properties};

#[derive(Parser, Debug)]
/** Given as input a binary file with, for each directory, the newest date of first
 * occurrence of any of the content in its subtree (well, DAG), ie.,
 * max_{for all content} (min_{for all occurrence of content} occurrence).
 * Produces a boolean vector, indicating for each directory if it is part of the
 * "provenance frontier", as defined in
 * https://gitlab.softwareheritage.org/swh/devel/swh-provenance/-/blob/ae09086a3bd45c7edbc22691945b9d61200ec3c2/swh/provenance/algos/revision.py#L210
 */
struct Args {
    graph_path: PathBuf,
    #[arg(short, long, action = clap::ArgAction::Count)]
    verbose: u8,
    #[arg(long)]
    /// Path to read the array of max timestamps from
    max_timestamps: PathBuf,
    #[arg(long)]
    /// Path to a directory where to write the bitvec of frontier-ness
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
        .init_properties()
        .load_properties(|props| props.load_maps::<GOVMPH>())
        .context("Could not load maps")?
        .load_properties(|props| props.load_timestamps())
        .context("Could not load timestamps")?;
    log::info!("Graph loaded.");

    let max_timestamps =
        NumberMmap::<byteorder::BE, i64, _>::new(&args.max_timestamps, graph.num_nodes())
            .with_context(|| format!("Could not mmap {}", args.max_timestamps.display()))?;

    let dataset_writer = ParallelDatasetWriter::new_with_schema(
        args.directories_out,
        (Arc::new(schema()), writer_properties(&graph).build()),
    )?;

    let frontiers = find_frontiers(&graph, &max_timestamps)?;

    let mut pl = ProgressLogger::default();
    pl.item_name("node");
    pl.display_memory(true);
    pl.local_speed(true);
    pl.expected_updates(Some(graph.num_nodes()));
    pl.start("[step 2/2] Writing frontiers");

    to_parquet(&graph, frontiers, dataset_writer, &mut pl)?;

    log::info!("Done.");

    Ok(())
}

fn find_frontiers<G>(
    graph: &G,
    max_timestamps: impl GetIndex<Output = i64> + Sync + Copy,
) -> Result<BitVec>
where
    G: SwhBackwardGraph + SwhForwardGraph + SwhGraphWithProperties + Send + Sync + 'static,
    <G as SwhGraphWithProperties>::Maps: swh_graph::properties::Maps,
    <G as SwhGraphWithProperties>::Timestamps: swh_graph::properties::Timestamps,
{
    let frontiers = AtomicBitVec::new(graph.num_nodes());

    let mut pl = ProgressLogger::default();
    pl.item_name("node");
    pl.display_memory(true);
    pl.local_speed(true);
    pl.expected_updates(Some(graph.num_nodes()));
    pl.start("[step 1/2] Visiting revisions' directories...");
    let pl = Arc::new(Mutex::new(pl));

    swh_graph::utils::shuffle::par_iter_shuffled_range(0..graph.num_nodes()).try_for_each(
        |node| -> Result<()> {
            if swh_graph_provenance::filters::is_head(graph, node) {
                if let Some(root_dir) =
                    swh_graph::algos::get_root_directory_from_revision_or_release(graph, node)
                        .context("Could not pick root directory")?
                {
                    find_frontiers_in_root_directory(
                        graph,
                        max_timestamps,
                        &frontiers,
                        node,
                        root_dir,
                    )?;
                }
            }

            if node % 32768 == 0 {
                pl.lock().unwrap().update_with_count(32768);
            }

            Ok(())
        },
    )?;

    pl.lock().unwrap().done();

    log::info!("Visits done, finishing output");

    Ok(frontiers.into())
}

fn find_frontiers_in_root_directory<G>(
    graph: &G,
    max_timestamps: impl GetIndex<Output = i64>,
    frontiers: &AtomicBitVec,
    revrel_id: NodeId,
    root_dir_id: NodeId,
) -> Result<()>
where
    G: SwhForwardGraph + SwhGraphWithProperties,
    <G as SwhGraphWithProperties>::Maps: swh_graph::properties::Maps,
    <G as SwhGraphWithProperties>::Timestamps: swh_graph::properties::Timestamps,
{
    let Some(revrel_timestamp) = graph.properties().author_timestamp(revrel_id) else {
        return Ok(());
    };

    let is_frontier = |dir: NodeId, dir_max_timestamp: i64| {
        // Detect if a node is a frontier according to
        // https://gitlab.softwareheritage.org/swh/devel/swh-provenance/-/blob/ae09086a3bd45c7edbc22691945b9d61200ec3c2/swh/provenance/algos/revision.py#L210
        if dir_max_timestamp < revrel_timestamp {
            // All content is earlier than revision

            // No need to check if it's depth > 1, given that we excluded the root dir above */
            if graph
                .successors(dir)
                .into_iter()
                .any(|succ| graph.properties().node_type(succ) == SWHType::Content)
            {
                // Contains at least one blob
                return true;
            }
        }

        false
    };

    let mut visited = AdaptiveNodeSet::new(graph.num_nodes());
    let mut stack = vec![root_dir_id]; // The root dir itself cannot be a frontier

    while let Some(node) = stack.pop() {
        for succ in graph.successors(node) {
            match graph.properties().node_type(succ) {
                SWHType::Directory => {
                    let dir_max_timestamp =
                        max_timestamps.get(node).expect("max_timestamps too small");
                    if dir_max_timestamp == i64::MIN {
                        // Somehow does not have a max timestamp. Presumably because it does not
                        // have any content.
                        continue;
                    }
                    if is_frontier(succ, dir_max_timestamp) {
                        frontiers.set(succ, true, Ordering::Relaxed);
                    } else if !visited.contains(succ) {
                        stack.push(succ);
                        visited.insert(succ);
                    }
                }
                _ => (),
            }
        }
    }

    Ok(())
}
