// Copyright (C) 2023  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::cell::UnsafeCell;
use std::path::PathBuf;
use std::sync::Mutex;

use anyhow::{Context, Result};
use dsi_progress_logger::ProgressLogger;
use itertools::Itertools;
use rayon::prelude::*;
use tempfile;
use webgraph::prelude::COOIterToLabelledGraph;
use webgraph::traits::SequentialGraph;

use super::orc::*;
use crate::utils::sort::par_sort_arcs;

pub fn bv<MPHF>(
    sort_batch_size: usize,
    mph: MPHF,
    num_nodes: usize,
    dataset_dir: PathBuf,
    target_dir: PathBuf,
) -> Result<()>
where
    MPHF: Fn(&[u8; 50]) -> Result<u64> + Send + Sync,
{
    let mut pl = ProgressLogger::default().display_memory();
    pl.item_name = "arc";
    pl.local_speed = true;
    pl.expected_updates = Some(estimate_edge_count(&dataset_dir) as usize);
    pl.start("Reading arcs");

    // Sort in parallel in a bunch of SortPairs instances
    let pl = Mutex::new(pl);
    let counters = thread_local::ThreadLocal::new();
    let temp_dir = tempfile::tempdir().context("Could not get temporary_directory")?;
    let sorted_arcs = par_sort_arcs(
        temp_dir.path(),
        sort_batch_size,
        iter_arcs(&dataset_dir).inspect(|_| {
            // This is safe because only this thread accesses this and only from
            // here.
            let counter = counters.get_or(|| UnsafeCell::new(0));
            let counter: &mut usize = unsafe { &mut *counter.get() };
            *counter += 1;
            if *counter % 32768 == 0 {
                // Update but avoid lock contention at the expense
                // of precision (counts at most 32768 too many at the
                // end of each file)
                pl.lock().unwrap().update_with_count(32768);
                *counter = 0
            }
        }),
        |sorter, (src, dst)| {
            let src = mph(&src)? as usize;
            let dst = mph(&dst)? as usize;
            assert!(src < num_nodes, "src node id is greater than {}", num_nodes);
            assert!(dst < num_nodes, "dst node id is greater than {}", num_nodes);
            sorter.push((src, dst, ()));
            Ok(())
        },
    )?
    .dedup()
    .map(|(src, dst)| (src, dst, ()));
    pl.lock().unwrap().done();

    let mut pl = ProgressLogger::default().display_memory();
    pl.item_name = "node";
    pl.local_speed = true;
    pl.expected_updates = Some(num_nodes);
    pl.start("Building BVGraph");
    let pl = Mutex::new(pl);
    let counters = thread_local::ThreadLocal::new();

    let sequential_graph = COOIterToLabelledGraph::new(num_nodes, sorted_arcs);
    let adjacency_lists = sequential_graph.iter_nodes().inspect(|_| {
        let counter = counters.get_or(|| UnsafeCell::new(0));
        let counter: &mut usize = unsafe { &mut *counter.get() };
        *counter += 1;
        if *counter % 32768 == 0 {
            // Update but avoid lock contention at the expense
            // of precision (counts at most 32768 too many at the
            // end of each file)
            pl.lock().unwrap().update_with_count(32768);
            *counter = 0
        }
    });
    let comp_flags = Default::default();
    let num_threads = num_cpus::get();

    webgraph::graph::bvgraph::parallel_compress_sequential_iter(
        target_dir,
        adjacency_lists,
        comp_flags,
        num_threads,
    )
    .context("Could not build BVGraph from arcs")?;

    pl.lock().unwrap().done();

    drop(temp_dir); // Prevent early deletion

    Ok(())
}
