// Copyright (C) 2023-2025  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::num::NonZeroUsize;
use std::path::PathBuf;

use anyhow::{Context, Result};
use dsi_bitstream::prelude::BE;
use dsi_progress_logger::{concurrent_progress_logger, ProgressLog};
use lender::{IntoIteratorExt, IntoLender, Lender};
use webgraph::prelude::*;
use webgraph::utils::ParSortIters;

/// Writes a new graph on disk, obtained by applying the function to all arcs
/// on the source graph.
pub fn transform<F, G, Iter>(
    input_batch_size: usize,
    partitions_per_thread: usize,
    graph: G,
    transformation: F,
    target_path: PathBuf,
) -> Result<()>
where
    F: Fn(usize, usize) -> Iter + Send + Sync,
    Iter: IntoIterator<Item = (usize, usize), IntoIter: Send + Sync>,
    G: SplitLabeling<Label=usize>,
    for<'a> <<G as SplitLabeling>::IntoIterator<'a> as IntoIterator>::IntoIter: Send + Sync,
    for<'a, 'b> <<<G as SplitLabeling>::SplitLender<'a> as NodeLabelsLender<'b>>::IntoIterator as IntoIterator>::IntoIter: Send + Sync,
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

    let mut pl = concurrent_progress_logger!(
        display_memory = true,
        item_name = "node",
        expected_updates = Some(num_nodes),
        local_speed = true,
    );
    pl.start("Reading and sorting...");

    // Merge sorted arc lists into a single sorted arc list
    let pair_sorter =
        ParSortIters::new(num_nodes)?.num_partitions(NonZeroUsize::new(num_partitions).unwrap());
    let transformation = &transformation;
    let sorted_arcs = {
        let pl = pl.clone();
        pair_sorter
            .sort(
                graph
                    .split_iter(num_partitions)
                    .into_iter()
                    .map(move |partition| {
                        let mut pl = pl.clone();
                        partition
                            .flat_map(move |(src, succ)| {
                                let transformed_succ: Vec<_> = succ
                                    .into_iter()
                                    .flat_map(move |dst| transformation(src, dst))
                                    .collect();
                                pl.light_update();
                                transformed_succ.into_into_lender().into_lender()
                            })
                            .iter()
                    }),
            )
            .context("Could not sort arcs")?
    };
    pl.done();
    let sorted_arcs = Vec::from(sorted_arcs); // Vector of iterators

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
        sorted_arcs,
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
