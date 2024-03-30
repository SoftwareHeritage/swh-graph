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
use dsi_progress_logger::{ProgressLog, ProgressLogger};
use rayon::prelude::*;
use sux::bits::bit_vec::BitVec;

use swh_graph::graph::*;
use swh_graph::java_compat::mph::gov::GOVMPH;

use swh_graph::utils::dataset_writer::{
    ParallelDatasetWriter, ParquetTableWriter, PartitionedTableWriter,
};
use swh_graph_provenance::frontier::PathParts;
use swh_graph_provenance::x_in_y_dataset::{
    cnt_in_revrel_schema, cnt_in_revrel_writer_properties, CntInRevrelTableBuilder,
};

#[derive(Parser, Debug)]
/** Given a Parquet table with the node ids of every frontier directory.
 * Produces the list of contents reachable from each revision, without any going through
 * any directory that is a frontier (relative to any revision).
 */
struct Args {
    graph_path: PathBuf,
    #[arg(short, long, action = clap::ArgAction::Count)]
    verbose: u8,
    #[arg(long)]
    /// Number of subfolders (Hive partitions) to store Parquet files in.
    ///
    /// Rows are partitioned across these folders based on the high bits of the object hash,
    /// to use Parquet statistics for row group filtering.
    ///
    /// RAM usage and number of written files is proportional to this value.
    num_partitions: Option<NonZeroU16>,
    #[arg(long)]
    /// Path to the Parquet table with the node ids of frontier directories
    frontier_directories: PathBuf,
    #[arg(long)]
    /// Path to a directory where to write .parquet results to
    contents_out: PathBuf,
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
        .load_forward_labels()
        .context("Could not load labels")?
        .init_properties()
        .load_properties(|props| props.load_label_names())
        .context("Could not load label names")?
        .load_properties(|props| props.load_maps::<GOVMPH>())
        .context("Could not load maps")?
        .load_properties(|props| props.load_timestamps())
        .context("Could not load timestamps")?;
    log::info!("Graph loaded.");

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

    let dataset_writer = ParallelDatasetWriter::new_with_schema(
        args.contents_out,
        (
            "revrel_partition".to_owned(), // Partition column name
            args.num_partitions,
            (
                Arc::new(cnt_in_revrel_schema()),
                cnt_in_revrel_writer_properties(&graph).build(),
            ),
        ),
    )?;

    write_contents_in_revisions(&graph, &frontier_directories, dataset_writer)
}

fn write_contents_in_revisions<G>(
    graph: &G,
    frontier_directories: &BitVec,
    dataset_writer: ParallelDatasetWriter<
        PartitionedTableWriter<ParquetTableWriter<CntInRevrelTableBuilder>>,
    >,
) -> Result<()>
where
    G: SwhBackwardGraph + SwhLabelledForwardGraph + SwhGraphWithProperties + Send + Sync + 'static,
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
            if swh_graph_provenance::filters::is_head(graph, node) {
                if let Some(root_dir) =
                    swh_graph::algos::get_root_directory_from_revision_or_release(graph, node)
                        .context("Could not pick root directory")?
                {
                    find_contents_in_root_directory(
                        graph,
                        frontier_directories,
                        writer,
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

    Ok(())
}

fn find_contents_in_root_directory<G>(
    graph: &G,
    frontier_directories: &BitVec,
    writer: &mut PartitionedTableWriter<ParquetTableWriter<CntInRevrelTableBuilder>>,
    revrel: NodeId,
    root_dir: NodeId,
) -> Result<()>
where
    G: SwhLabelledForwardGraph + SwhGraphWithProperties,
    <G as SwhGraphWithProperties>::LabelNames: swh_graph::properties::LabelNames,
    <G as SwhGraphWithProperties>::Maps: swh_graph::properties::Maps,
    <G as SwhGraphWithProperties>::Timestamps: swh_graph::properties::Timestamps,
{
    let Some(revrel_timestamp) = graph.properties().author_timestamp(revrel) else {
        return Ok(());
    };

    let on_directory = |dir: NodeId, _path_parts: PathParts| {
        Ok(!frontier_directories[dir]) // Recurse only if this is not a frontier
    };

    let on_content = |cnt: NodeId, path_parts: PathParts| {
        // Partition rows by their cnt value, to allow faster cnt->revrel queries
        let partition_id: usize = cnt * writer.partitions().len() / graph.num_nodes();
        let builder = writer.partitions()[partition_id].builder()?;
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

    swh_graph_provenance::frontier::dfs_with_path(graph, on_directory, on_content, root_dir)
}
