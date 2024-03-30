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
use swh_graph::utils::mmap::NumberMmap;
use swh_graph::utils::GetIndex;

use swh_graph::utils::dataset_writer::{
    ParallelDatasetWriter, ParquetTableWriter, PartitionedTableWriter,
};
use swh_graph_provenance::x_in_y_dataset::{
    dir_in_revrel_schema, dir_in_revrel_writer_properties, DirInRevrelTableBuilder,
};

#[derive(Parser, Debug)]
/** Given as input a binary file with, for each directory, the newest date of first
 * occurrence of any of the content in its subtree (well, DAG), ie.,
 * max_{for all content} (min_{for all occurrence of content} occurrence).
 * and a Parquet table with the node ids of every frontier directory.
 * Produces the line of frontier directories (relative to any revision) present in
 * each revision.
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
    /// Path to read the array of max timestamps from
    max_timestamps: PathBuf,
    #[arg(long)]
    /// Path to a directory where to write .csv.zst results to
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

    let max_timestamps =
        NumberMmap::<byteorder::BE, i64, _>::new(&args.max_timestamps, graph.num_nodes())
            .with_context(|| format!("Could not mmap {}", args.max_timestamps.display()))?;

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
        args.directories_out,
        (
            "revrel_partition".to_owned(), // Partition column name
            args.num_partitions,
            (
                Arc::new(dir_in_revrel_schema()),
                dir_in_revrel_writer_properties(&graph).build(),
            ),
        ),
    )?;

    write_frontier_directories_in_revisions(
        &graph,
        &max_timestamps,
        &frontier_directories,
        dataset_writer,
    )
}

fn write_frontier_directories_in_revisions<G>(
    graph: &G,
    max_timestamps: impl GetIndex<Output = i64> + Sync + Copy,
    frontier_directories: &BitVec,
    dataset_writer: ParallelDatasetWriter<
        PartitionedTableWriter<ParquetTableWriter<DirInRevrelTableBuilder>>,
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
                    find_frontiers_in_root_directory(
                        graph,
                        max_timestamps,
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

fn find_frontiers_in_root_directory<G>(
    graph: &G,
    max_timestamps: impl GetIndex<Output = i64>,
    frontier_directories: &BitVec,
    writer: &mut PartitionedTableWriter<ParquetTableWriter<DirInRevrelTableBuilder>>,
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

    let is_frontier = |dir: NodeId, _dir_max_timestamp: i64| frontier_directories[dir];

    let on_frontier = |dir: NodeId, dir_max_timestamp: i64, path: Vec<u8>| {
        // Partition rows by their cnt value, to allow faster dir->revrel queries
        let partition_id: usize = dir * writer.partitions().len() / graph.num_nodes();
        let builder = writer.partitions()[partition_id].builder()?;
        builder
            .dir
            .append_value(dir.try_into().expect("NodeId overflowed u64"));
        builder.dir_max_author_date.append_value(dir_max_timestamp);
        builder
            .revrel
            .append_value(revrel.try_into().expect("NodeId overflowed u64"));
        builder.revrel_author_date.append_value(revrel_timestamp);
        builder.path.append_value(path);

        Ok(())
    };

    swh_graph_provenance::frontier::find_frontiers_from_root_directory(
        graph,
        max_timestamps,
        is_frontier,
        on_frontier,
        /* recurse_through_frontiers: */ true,
        root_dir,
    )
}
