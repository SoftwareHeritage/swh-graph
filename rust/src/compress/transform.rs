// Copyright (C) 2023  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::path::PathBuf;
use std::sync::Mutex;

use anyhow::{Context, Result};
use dsi_bitstream::prelude::BE;
use dsi_progress_logger::{progress_logger, ProgressLog};
use lender::Lender;
use rayon::prelude::*;
use webgraph::graphs::arc_list_graph::ArcListGraph;
use webgraph::prelude::*;

use crate::utils::sort::par_sort_arcs;

/// Writes a new graph on disk, obtained by applying the function to all arcs
/// on the source graph.
pub fn transform<F, G, Iter>(
    input_batch_size: usize,
    sort_batch_size: usize,
    partitions_per_thread: usize,
    graph: G,
    transformation: F,
    target_path: PathBuf,
) -> Result<()>
where
    F: Fn(usize, usize) -> Iter + Send + Sync,
    Iter: IntoIterator<Item = (usize, usize)>,
    G: RandomAccessGraph + Sync,
{
    // Adapted from https://github.com/vigna/webgraph-rs/blob/08969fb1ac4ea59aafdbae976af8e026a99c9ac5/src/bin/perm.rs
    let num_nodes = graph.num_nodes();

    let num_batches = num_nodes.div_ceil(input_batch_size);

    let temp_dir = tempfile::tempdir().context("Could not get temporary_directory")?;

    let num_threads = num_cpus::get();
    let num_partitions = num_threads * partitions_per_thread;
    let nodes_per_partition = num_nodes.div_ceil(num_partitions);

    // Avoid empty partitions at the end when there are very few nodes
    let num_partitions = num_nodes.div_ceil(nodes_per_partition);

    log::info!(
        "Transforming {} nodes with {} threads, {} partitions, {} nodes per partition, {} batches of size {}",
        num_nodes,
        num_threads,
        num_partitions,
        nodes_per_partition,
        num_batches,
        input_batch_size
    );

    let mut pl = progress_logger!(
        display_memory = true,
        item_name = "node",
        expected_updates = Some(num_nodes),
        local_speed = true,
    );
    pl.start("Reading and sorting...");
    let pl = Mutex::new(pl);

    // Merge sorted arc lists into a single sorted arc list
    let sorted_arcs = par_sort_arcs(
        temp_dir.path(),
        sort_batch_size,
        (0..num_batches).into_par_iter(),
        num_partitions,
        (),
        (),
        |buffer, batch_id| -> Result<()> {
            let start = batch_id * input_batch_size;
            let end = (batch_id + 1) * input_batch_size;
            graph // Not using PermutedGraph in order to avoid blanket iter_nodes_from
                .iter_from(start)
                .take_while(|(node_id, _successors)| *node_id < end)
                .try_for_each(|(x, succ)| -> Result<()> {
                    succ.into_iter().try_for_each(|s| -> Result<()> {
                        for (x, s) in transformation(x, s).into_iter() {
                            let partition_id = x / nodes_per_partition;
                            buffer.insert(partition_id, x, s)?;
                        }
                        Ok(())
                    })
                })?;
            pl.lock().unwrap().update_with_count(end - start);
            Ok(())
        },
    )
    .context("Could not sort arcs")?;
    pl.lock().unwrap().done();

    let arc_list_graphs =
        sorted_arcs
            .into_iter()
            .enumerate()
            .map(|(partition_id, sorted_arcs_partition)| {
                webgraph::prelude::Left(ArcListGraph::new_labeled(num_nodes, sorted_arcs_partition))
                    .iter_from(partition_id * nodes_per_partition)
                    .take(nodes_per_partition)
            });

    let compression_flags = CompFlags {
        compression_window: 1,
        min_interval_length: 4,
        max_ref_count: 3,
        ..CompFlags::default()
    };

    let temp_bv_dir = temp_dir.path().join("transform-bv");
    std::fs::create_dir(&temp_bv_dir)
        .with_context(|| format!("Could not create {}", temp_bv_dir.display()))?;
    BvComp::parallel_iter::<BE, _>(
        target_path,
        arc_list_graphs,
        num_nodes,
        compression_flags,
        &rayon::ThreadPoolBuilder::default()
            .build()
            .expect("Could not create BvComp thread pool"),
        &temp_bv_dir,
    )
    .context("Could not build BVGraph from arcs")?;

    drop(temp_dir); // Prevent early deletion

    Ok(())
}
