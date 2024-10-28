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
use swh_graph::NodeType;

use crate::filters::NodeFilter;
use crate::frontier::PathParts;
use crate::x_in_y_dataset::CntInRevrelTableBuilder;

pub fn write_revisions_from_contents<G>(
    graph: &G,
    node_filter: NodeFilter,
    reachable_nodes: Option<&BitVec>,
    frontier_directories: &BitVec,
    dataset_writer: ParallelDatasetWriter<ParquetTableWriter<CntInRevrelTableBuilder>>,
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
            let is_reachable = match reachable_nodes {
                None => true,
                Some(reachable_nodes) => reachable_nodes.get(node),
            };
            if is_reachable && graph.properties().node_type(node) == NodeType::Content {
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
        if !crate::filters::is_root_revrel(graph, node_filter, revrel) {
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

    crate::frontier::backward_dfs_with_path(graph, reachable_nodes, on_directory, on_revrel, cnt)
}
