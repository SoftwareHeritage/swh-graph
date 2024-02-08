// Copyright (C) 2023  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::path::PathBuf;
use std::sync::Mutex;

use anyhow::{Context, Result};
use dsi_bitstream::prelude::BE;
use dsi_progress_logger::ProgressLogger;
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

    let temp_dir = tempfile::tempdir().context("Could not get temporary_directory")?;

    let mut pl = ProgressLogger::default().display_memory();
    pl.item_name = "node";
    pl.expected_updates = Some(num_nodes);
    pl.local_speed = true;
    pl.start("Reading and sorting...");
    let pl = Mutex::new(pl);

    // Merge sorted arc lists into a single sorted arc list
    let sorted_arcs = par_sort_arcs(
        temp_dir.path(),
        sort_batch_size,
        (0usize..=((num_nodes - 1) / input_batch_size)).into_par_iter(),
        |sorter, batch_id| {
            let start = batch_id * input_batch_size;
            let end = (batch_id + 1) * input_batch_size;
            graph // Not using PermutedGraph in order to avoid blanket iter_nodes_from
                .iter_from(start)
                .take_while(|(node_id, _successors)| *node_id < end)
                .for_each(|(x, succ)| {
                    succ.into_iter().for_each(|s| {
                        for (x, s) in transformation(x, s).into_iter() {
                            sorter.push((x, s));
                        }
                    })
                });
            pl.lock().unwrap().update_with_count(end - start);
            Ok(())
        },
    )
    .context("Could not sort arcs")?;
    pl.lock().unwrap().done();

    let arc_list_graph = webgraph::prelude::Left(ArcListGraph::new(num_nodes, sorted_arcs));

    let compression_flags = CompFlags {
        compression_window: 1,
        min_interval_length: 4,
        max_ref_count: 3,
        ..CompFlags::default()
    };
    BVComp::single_thread::<BE, _>(
        target_path,
        &arc_list_graph,
        compression_flags,
        false, // build_offsets (TODO: make it true and remove BUILD_OFFSETS from pipeline)
        Some(num_nodes),
    )
    .context("Could not build BVGraph from arcs")?;

    Ok(())
}
