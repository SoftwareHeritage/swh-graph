// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::sync::{Arc, Mutex};

use anyhow::Result;
use dsi_progress_logger::{progress_logger, ProgressLog};
use rayon::prelude::*;
use sux::bits::bit_vec::BitVec;

use dataset_writer::{ParallelDatasetWriter, ParquetTableWriter};
use swh_graph::graph::*;
use swh_graph::utils::progress_logger::{BufferedProgressLogger, MinimalProgressLog};
use swh_graph::utils::GetIndex;

use crate::filters::NodeFilter;
use crate::frontier::PathParts;
use crate::x_in_y_dataset::DirInRevrelTableBuilder;

pub fn write_revisions_from_frontier_directories<G>(
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
    let mut pl = progress_logger!(
        item_name = "node",
        display_memory = true,
        local_speed = true,
        expected_updates = Some(graph.num_nodes()),
    );
    pl.start("Visiting revisions' directories...");
    let shared_pl = Arc::new(Mutex::new(&mut pl));

    (0..graph.num_nodes()).into_par_iter().try_for_each_init(
        || {
            (
                dataset_writer.get_thread_writer().unwrap(),
                BufferedProgressLogger::new(shared_pl.clone()),
            )
        },
        |(writer, thread_pl), node| -> Result<()> {
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
            thread_pl.light_update();
            Ok(())
        },
    )?;

    pl.done();

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

        if !crate::filters::is_root_revrel(graph, node_filter, revrel) {
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
    crate::frontier::backward_dfs_with_path(graph, reachable_nodes, on_directory, on_revrel, dir)
}
