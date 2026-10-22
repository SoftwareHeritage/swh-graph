// Copyright (C) 2023-2026  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::path::PathBuf;

use anyhow::{Context, Result};
use dsi_bitstream::prelude::BE;
use dsi_progress_logger::{concurrent_progress_logger, ProgressLog};
use lender::{IntoIteratorExt, IntoLender, Lender};
use rayon::prelude::*;
use webgraph::prelude::*;

/// Writes a new graph on disk, obtained by applying the function to all arcs
/// on the source graph.
pub fn transform<F, G, Iter>(
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
    let num_nodes = graph.num_nodes();

    let num_threads = num_cpus::get();
    let num_lenders = num_threads * partitions_per_thread;
    let nodes_per_partition = num_nodes.div_ceil(num_lenders);

    // Avoid empty partitions at the end when there are very few nodes
    let num_lenders = num_nodes.div_ceil(nodes_per_partition);

    log::info!(
        "Transforming {} nodes with {} threads, {} partitions, {} nodes per partition",
        num_nodes,
        num_threads,
        num_lenders,
        nodes_per_partition,
    );

    let mut pl = concurrent_progress_logger!(
        display_memory = true,
        item_name = "node",
        expected_updates = Some(num_nodes),
        local_speed = true,
    );
    pl.start("Reading and sorting...");

    let transformation = &transformation;
    let sorted_graph = {
        let pl = pl.clone();
        ParSortedGraph::config()
            .num_lenders(num_lenders)
            .par_sort_pairs(
                num_nodes,
                graph
                    .split_iter(num_lenders)
                    .into_iter()
                    .collect::<Vec<_>>()
                    .into_par_iter()
                    .flat_map_iter(move |partition| {
                        let mut pl = pl.clone();
                        partition
                            .flat_map(lender::covar_mut!(
                                #![with<'g, G: SplitLabeling<Label=usize>>]
                                for<'lend>
                                move |(src, succ): (usize, <<G as SplitLabeling>::SplitLender<'g> as NodeLabelsLender<'lend>>::IntoIterator)|
                                -> lender::FromIter<std::vec::IntoIter<(usize, usize)>> {
                                    let transformed_succ: Vec<_> = succ
                                        .into_iter()
                                        .flat_map(move |dst: usize| transformation(src, dst).into_iter())
                                        .collect();
                                    pl.light_update();
                                    transformed_succ.into_into_lender().into_lender()
                                }
                            ))
                            .iter()
                    }),
            )
            .context("Could not sort arcs")?
    };
    pl.done();

    BvComp::with_basename(target_path)
        .par_comp::<BE, _>(sorted_graph)
        .context("Could not build BVGraph from arcs")?;

    Ok(())
}
