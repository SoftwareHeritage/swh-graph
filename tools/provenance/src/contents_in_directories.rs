// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

use anyhow::Result;
use dsi_progress_logger::{progress_logger, ProgressLog};
use rayon::prelude::*;
use sux::prelude::{AtomicBitVec, BitVec};

use dataset_writer::{ParallelDatasetWriter, ParquetTableWriter};
use swh_graph::collections::NodeSet;
use swh_graph::graph::*;
use swh_graph::utils::progress_logger::{BufferedProgressLogger, MinimalProgressLog};
use swh_graph::NodeType;

use crate::frontier::PathParts;
use crate::x_in_y_dataset::CntInDirTableBuilder;

pub fn write_directories_from_contents<G>(
    graph: &G,
    frontier_directories: &BitVec,
    dataset_writer: ParallelDatasetWriter<ParquetTableWriter<CntInDirTableBuilder>>,
) -> Result<()>
where
    G: SwhForwardGraph + SwhLabeledBackwardGraph + SwhGraphWithProperties + Send + Sync + 'static,
    <G as SwhGraphWithProperties>::LabelNames: swh_graph::properties::LabelNames,
    <G as SwhGraphWithProperties>::Maps: swh_graph::properties::Maps,
{
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
    let reachable_nodes_from_frontier = AtomicBitVec::new(graph.num_nodes());
    (0..graph.num_nodes()).into_par_iter().for_each_with(
        BufferedProgressLogger::new(Arc::new(Mutex::new(&mut pl))),
        |thread_pl, root| {
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
            thread_pl.light_update();
        },
    );
    pl.done();
    let reachable_nodes_from_frontier: BitVec = reachable_nodes_from_frontier.into();

    let mut pl = progress_logger!(
        item_name = "node",
        display_memory = true,
        local_speed = true,
        expected_updates = Some(graph.num_nodes()),
    );
    pl.start("Listing contents in directories...");
    let shared_pl = Arc::new(Mutex::new(&mut pl));

    swh_graph::utils::shuffle::par_iter_shuffled_range(0..graph.num_nodes()).try_for_each_init(
        || {
            (
                dataset_writer.get_thread_writer().unwrap(),
                BufferedProgressLogger::new(shared_pl.clone()),
            )
        },
        |(writer, thread_pl), node| -> Result<()> {
            if reachable_nodes_from_frontier.get(node)
                && graph.properties().node_type(node) == NodeType::Content
            {
                write_frontier_directories_from_content(
                    &graph,
                    writer,
                    &reachable_nodes_from_frontier,
                    frontier_directories,
                    node,
                )?;
            }
            thread_pl.light_update();
            Ok(())
        },
    )?;
    pl.done();

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

    crate::frontier::backward_dfs_with_path(
        graph,
        Some(reachable_nodes_from_frontier),
        on_directory,
        on_revrel,
        cnt,
    )
}
