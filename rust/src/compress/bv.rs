// Copyright (C) 2023  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::cell::UnsafeCell;
use std::path::PathBuf;
use std::sync::Mutex;

use anyhow::{anyhow, Context, Result};
use dsi_bitstream::prelude::BE;
use dsi_progress_logger::ProgressLogger;
use itertools::Itertools;
use lender::Lender;
use rayon::prelude::*;
use tempfile;
use webgraph::graphs::arc_list_graph::ArcListGraph;
use webgraph::prelude::*;

use super::iter_arcs::iter_arcs;
use super::stats::estimate_edge_count;
use crate::mph::SwhidMphf;
use crate::utils::sort::par_sort_arcs;

pub fn bv<MPHF: SwhidMphf + Sync>(
    sort_batch_size: usize,
    partitions_per_thread: usize,
    mph_basepath: PathBuf,
    num_nodes: usize,
    dataset_dir: PathBuf,
    allowed_node_types: &[crate::NodeType],
    target_dir: PathBuf,
) -> Result<()> {
    log::info!("Reading MPH");
    let mph = MPHF::load(mph_basepath).context("Could not load MPHF")?;
    log::info!("MPH loaded, sorting arcs");

    let num_threads = num_cpus::get();
    let num_partitions = num_threads * partitions_per_thread;
    let nodes_per_partition = num_nodes.div_ceil(num_partitions);

    // Avoid empty partitions at the end when there are very few nodes
    let num_partitions = num_nodes.div_ceil(nodes_per_partition);

    let mut pl = ProgressLogger::default().display_memory();
    pl.item_name = "arc";
    pl.local_speed = true;
    pl.expected_updates = Some(
        estimate_edge_count(&dataset_dir, allowed_node_types)
            .context("Could not estimate edge count")? as usize,
    );
    pl.start("Reading arcs");

    // Sort in parallel in a bunch of SortPairs instances
    let pl = Mutex::new(pl);
    let counters = thread_local::ThreadLocal::new();
    let temp_dir = tempfile::tempdir().context("Could not get temporary_directory")?;
    let sorted_arcs_path = temp_dir.path().join("sorted_arcs");
    std::fs::create_dir(&sorted_arcs_path)
        .with_context(|| format!("Could not create {}", sorted_arcs_path.display()))?;
    let sorted_arcs = par_sort_arcs(
        &sorted_arcs_path,
        sort_batch_size,
        iter_arcs(&dataset_dir, allowed_node_types)
            .context("Could not open input files to read arcs")?
            .inspect(|_| {
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
        num_partitions,
        |buffer, (src, dst)| {
            let src = mph
                .hash_str_array(&src)
                .ok_or_else(|| anyhow!("Unknown SWHID {:?}", String::from_utf8_lossy(&src)))?;
            let dst = mph
                .hash_str_array(&dst)
                .ok_or_else(|| anyhow!("Unknown SWHID {:?}", String::from_utf8_lossy(&dst)))?;
            assert!(src < num_nodes, "src node id is greater than {}", num_nodes);
            assert!(dst < num_nodes, "dst node id is greater than {}", num_nodes);
            let partition_id = src / nodes_per_partition;
            buffer.insert(partition_id, src, dst)?;
            Ok(())
        },
    )?;
    pl.lock().unwrap().done();

    let arc_list_graphs =
        sorted_arcs
            .into_iter()
            .enumerate()
            .map(|(partition_id, sorted_arcs_partition)| {
                webgraph::prelude::Left(ArcListGraph::new(num_nodes, sorted_arcs_partition.dedup()))
                    .iter_from(partition_id * nodes_per_partition)
                    .take(nodes_per_partition)
            });
    let comp_flags = Default::default();

    let temp_bv_dir = temp_dir.path().join("bv");
    std::fs::create_dir(&temp_bv_dir)
        .with_context(|| format!("Could not create {}", temp_bv_dir.display()))?;
    BVComp::parallel_iter::<BE, _>(
        target_dir,
        arc_list_graphs,
        num_nodes,
        comp_flags,
        Threads::Default,
        &temp_bv_dir,
    )
    .context("Could not build BVGraph from arcs")?;

    pl.lock().unwrap().done();

    drop(temp_dir); // Prevent early deletion

    Ok(())
}
