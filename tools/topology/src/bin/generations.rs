// Copyright (C) 2023-2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

//! Precomputes the depth of each node's sub-DAG, and a topological order.
//!
//!
//! This runs a regular toposorting DFS, ie. it first counts the indegree of every node,
//! then produces the list of all nodes with indegree 0 and decrements the counter for
//! each of their successors.
//! Then we look at successors with the counter set to zero, we produce that as a new list, etc.
//!
//! Before writing each list, sort it into numbers n_0, n_1, ..., n_m; and store it as
//! n_0, n_1 - n_0, n_2 - n_1, ..., n_m - n_{m-1} using gamma-coding.
//! And we store the beginning of each of these lists in the .offsets file, also gamma-coded
//! into the .bitstream

use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::mpsc::{sync_channel, SyncSender};

use anyhow::{anyhow, bail, Context, Result};
use clap::{Parser, ValueEnum};
use dsi_progress_logger::{concurrent_progress_logger, progress_logger, ProgressLog};
use mmap_rs::MmapFlags;
use rayon::prelude::*;

use swh_graph::graph::*;
use swh_graph::mph::DynMphf;
use swh_graph::views::{Subgraph, Transposed};
use swh_graph::NodeConstraint;

use swh_graph_topology::generations::GenerationsWriter;

#[derive(Debug, Clone, Copy, ValueEnum)]
enum Direction {
    Forward,
    Backward,
}

#[derive(Parser, Debug)]
#[command()]
/// Computes the depth of the subtree of each node, and computes a topological order from it
struct Args {
    graph_path: PathBuf,
    #[arg(short, long)]
    direction: Direction,
    #[arg(short, long, default_value = "*")]
    node_types: NodeConstraint,
    #[arg(long)]
    /// Path where to write the output order, and the accompanying .ef delimiting offsets between
    /// generations
    output_order: PathBuf,
    #[arg(long)]
    /// Pass where to write the generation/depth of each node, in a random-accessible array
    output_depths: PathBuf,
}

pub fn main() -> Result<()> {
    let args = Args::parse();

    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    log::info!("Loading graph");
    let graph = swh_graph::graph::SwhBidirectionalGraph::new(args.graph_path)
        .context("Could not load graph")?
        .init_properties()
        .load_properties(|props| props.load_maps::<DynMphf>())
        .context("Could not load maps")?;

    if let Some(dir) = args.output_order.parent() {
        std::fs::create_dir_all(dir)
            .with_context(|| format!("Could not create {}", dir.display()))?;
    }
    let mut generations_writer = GenerationsWriter::new(&args.output_order)
        .with_context(|| format!("Could not create {}", args.output_order.display()))?;

    let (generations_tx, generations_rx) = sync_channel::<Vec<NodeId>>(10); // arbitrary value

    let node_depths = std::thread::scope(|s| -> Result<_> {
        let writer_thread = std::thread::Builder::new()
            .name("writer".to_owned())
            .spawn_scoped(s, move || -> Result<()> {
                while let Ok(mut generation) = generations_rx.recv() {
                    generations_writer.sort_and_push_generation(&mut generation)?;
                }
                generations_writer.close()?;
                Ok(())
            })
            .expect("Could not spawn writer thread");

        let toposort_thread = std::thread::Builder::new()
            .name("toposort".to_owned())
            .spawn_scoped(s, || -> Result<_> {
                match args.direction {
                    Direction::Forward => toposort(&graph, args.node_types, generations_tx),
                    Direction::Backward => {
                        toposort(Transposed(&graph), args.node_types, generations_tx)
                    }
                }
            })
            .expect("Could not spawn toposort thread");

        let node_depths = toposort_thread
            .join()
            .expect("Could not join toposort thread")?;
        log::info!("Writing end of the topological order...");
        writer_thread
            .join()
            .expect("Could not join writer thread")?;
        Ok(node_depths)
    })?;

    let file_len = (graph.num_nodes() * ((u32::BITS / 8) as usize))
        .try_into()
        .context("File size overflowed u64")?;
    let file = std::fs::File::options()
        .read(true)
        .write(true)
        .create_new(true)
        .open(&args.output_depths)
        .with_context(|| format!("Could not create {}", args.output_depths.display()))?;

    // fallocate the file with zeros so we can fill it without ever resizing it
    file.set_len(file_len).with_context(|| {
        format!(
            "Could not fallocate {} with zeros",
            args.output_depths.display()
        )
    })?;

    let mut data = unsafe {
        mmap_rs::MmapOptions::new(file_len as _)
            .context("Could not initialize mmap")?
            .with_flags(MmapFlags::TRANSPARENT_HUGE_PAGES | MmapFlags::SHARED)
            .with_file(&file, 0)
            .map_mut()
            .with_context(|| format!("Could not mmap {}", args.output_depths.display()))?
    };

    log::info!("Writing node depths...");

    let data_words: &mut [u32] = bytemuck::cast_slice_mut(&mut data);
    data_words
        .par_iter_mut()
        .zip(node_depths.into_par_iter())
        .for_each(|(cell, depth)| *cell = depth.to_be());

    log::info!("Done");

    Ok(())
}

fn toposort<G>(
    graph: G,
    node_constraint: NodeConstraint,
    generations_tx: SyncSender<Vec<NodeId>>,
) -> Result<Vec<u32>>
where
    G: SwhForwardGraph + SwhBackwardGraph + SwhGraphWithProperties + Sync,
    <G as SwhGraphWithProperties>::Maps: swh_graph::properties::Maps,
{
    let graph = Subgraph::with_node_constraint(&graph, node_constraint);

    log::info!("Initializing predecessor counters...");
    let num_unvisited_predecessors: Vec<_> = (0..graph.num_nodes())
        .into_par_iter()
        .map(|_| AtomicU32::new(0))
        .collect();

    let mut pl = concurrent_progress_logger!(
        display_memory = true,
        item_name = "node",
        local_speed = true,
        expected_updates = Some(graph.actual_num_nodes().unwrap_or(graph.num_nodes())),
    );
    pl.start("Listing leaves...");

    let mut total_arcs = 0;
    let mut leaves = Vec::new();

    graph
        .par_iter_nodes(pl.clone())
        .try_fold_with(
            (
                0,          // total_arcs
                Vec::new(), // leaves
            ),
            |(mut thread_total_arcs, mut thread_leaves), node| {
                let outdegree = graph.indegree(node);
                thread_total_arcs += outdegree;
                let outdegree: u32 = outdegree.try_into().with_context(|| {
                    format!(
                        "{} has outdegree {outdegree}, which does not fit in u32",
                        graph.properties().swhid(node)
                    )
                })?;
                num_unvisited_predecessors[node].store(outdegree, Ordering::Relaxed);
                if outdegree == 0 {
                    thread_leaves.push(node);
                }
                Ok((thread_total_arcs, thread_leaves))
            },
        )
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .for_each(|(thread_total_arcs, thread_leaves)| {
            total_arcs += thread_total_arcs;
            leaves.extend(thread_leaves);
        });
    pl.done();

    // Compiled to a no-op
    let mut num_unvisited_predecessors: Vec<_> = num_unvisited_predecessors
        .into_iter()
        .map(|n| n.into_inner())
        .collect();

    log::info!("Leaves listed, starting traversal.");

    let mut pl = progress_logger!(
        display_memory = true,
        item_name = "arc",
        local_speed = true,
        expected_updates = Some(total_arcs),
    );
    pl.start("Sorting...");

    let mut current_generation_number = 0u32;
    let mut node_depths = vec![0u32; graph.num_nodes()];
    let mut next_generation = leaves;

    while !next_generation.is_empty() {
        for &node in &next_generation {
            node_depths[node] = current_generation_number;
        }

        current_generation_number = current_generation_number.checked_add(1).ok_or_else(|| {
            anyhow!(
                "{} has depth {current_generation_number} + 1, which does not fit in u32",
                graph.properties().swhid(*next_generation.first().unwrap())
            )
        })?;

        let current_generation = next_generation;
        next_generation = Vec::with_capacity(current_generation.len());

        for &current_node in &current_generation {
            for successor in graph.successors(current_node) {
                pl.light_update();
                if num_unvisited_predecessors[successor] == 0 {
                    let swhid = graph.properties().swhid(current_node);
                    bail!(
                        "{} has negative number of unvisited ancestors: {}",
                        swhid,
                        num_unvisited_predecessors[successor] as i64 - 1
                    );
                }
                num_unvisited_predecessors[successor] -= 1;
                if num_unvisited_predecessors[successor] == 0 {
                    // If this successor's predecessors are all visited, then this successor
                    // is ready to be visited.
                    next_generation.push(successor)
                }
            }
        }

        // write the generation to disk
        generations_tx
            .send(current_generation)
            .context("writer thread exited before toposort thread")?;
    }

    pl.done();

    Ok(node_depths)
}
