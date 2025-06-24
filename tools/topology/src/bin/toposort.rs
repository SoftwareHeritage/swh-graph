// Copyright (C) 2023  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::collections::VecDeque;
use std::io;
use std::path::PathBuf;

use anyhow::{bail, Context, Result};
use clap::{Parser, ValueEnum};
use dsi_progress_logger::{progress_logger, ProgressLog};
use serde::Serialize;

use swh_graph::graph::*;
use swh_graph::mph::DynMphf;
use swh_graph::views::{Subgraph, Transposed};
use swh_graph::NodeConstraint;

#[derive(Debug, Clone, Copy, ValueEnum)]
enum Algorithm {
    Bfs,
    Dfs,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum Direction {
    Forward,
    Backward,
}

#[derive(Parser, Debug)]
#[command(about = "Computes a topological order of the Software Heritage graph", long_about = None)]
struct Args {
    graph_path: PathBuf,
    #[arg(short, long)]
    algorithm: Algorithm,
    #[arg(short, long)]
    direction: Direction,
    #[arg(short, long, default_value = "*")]
    node_types: NodeConstraint,
}

#[derive(Debug, Serialize)]
struct Record<'a> {
    #[serde(rename = "SWHID")]
    swhid: &'a str,
    ancestors: usize,
    successors: usize,
    sample_ancestor1: &'a str,
    sample_ancestor2: &'a str,
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

    match (args.algorithm, args.direction) {
        (Algorithm::Dfs, Direction::Forward) => toposort::<Stack, _>(graph, args.node_types)?,
        (Algorithm::Bfs, Direction::Forward) => toposort::<Queue, _>(graph, args.node_types)?,
        (Algorithm::Dfs, Direction::Backward) => {
            toposort::<Stack, _>(Transposed(&graph), args.node_types)?
        }
        (Algorithm::Bfs, Direction::Backward) => {
            toposort::<Queue, _>(Transposed(&graph), args.node_types)?
        }
    }

    Ok(())
}

trait ReadySet: Default {
    fn push(&mut self, node: NodeId);
    fn pop(&mut self) -> Option<NodeId>;
}

#[derive(Default)]
struct Stack(Vec<NodeId>);
impl ReadySet for Stack {
    fn push(&mut self, node: NodeId) {
        self.0.push(node);
    }
    fn pop(&mut self) -> Option<NodeId> {
        self.0.pop()
    }
}

#[derive(Default)]
struct Queue(VecDeque<NodeId>);
impl ReadySet for Queue {
    fn push(&mut self, node: NodeId) {
        self.0.push_back(node);
    }
    fn pop(&mut self) -> Option<NodeId> {
        self.0.pop_front()
    }
}

fn toposort<R: ReadySet, G>(graph: G, node_constraint: NodeConstraint) -> Result<()>
where
    G: SwhForwardGraph + SwhBackwardGraph + SwhGraphWithProperties,
    <G as SwhGraphWithProperties>::Maps: swh_graph::properties::Maps,
{
    let graph = Subgraph::with_node_constraint(&graph, node_constraint);

    let mut pl = progress_logger!(
        display_memory = true,
        item_name = "node",
        local_speed = true,
        expected_updates = Some(graph.num_nodes()),
    );
    pl.start("Listing leaves...");

    let mut total_arcs = 0;
    let mut ready = R::default();
    let mut num_unvisited_predecessors = vec![0usize; graph.num_nodes()];
    #[allow(clippy::needless_range_loop)]
    for node in 0..graph.num_nodes() {
        pl.light_update();
        if !graph.has_node(node) {
            continue;
        }
        let outdegree = graph.indegree(node);
        total_arcs += outdegree;
        num_unvisited_predecessors[node] = outdegree;
        if outdegree == 0 {
            ready.push(node);
        }
    }
    pl.done();

    log::info!("Leaves listed, starting traversal.");

    let mut pl = progress_logger!(
        display_memory = true,
        item_name = "arc",
        local_speed = true,
        expected_updates = Some(total_arcs),
    );
    pl.start("Sorting...");

    let mut writer = csv::WriterBuilder::new()
        .terminator(csv::Terminator::CRLF)
        .from_writer(io::stdout());
    while let Some(current_node) = ready.pop() {
        let swhid = graph.properties().swhid(current_node);
        let mut num_successors = 0;
        for successor in graph.successors(current_node) {
            pl.light_update();
            num_successors += 1;
            if num_unvisited_predecessors[successor] == 0 {
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
                ready.push(successor)
            }
        }

        let mut sample_ancestors = Vec::new();
        let mut num_ancestors = 0;
        for predecessor in graph.predecessors(current_node) {
            let predecessor_swhid = graph.properties().swhid(predecessor);
            if sample_ancestors.len() < 2 {
                sample_ancestors.push(predecessor_swhid.to_string())
            }
            num_ancestors += 1;
        }

        #[allow(clippy::get_first)]
        writer
            .serialize(Record {
                swhid: &swhid.to_string(),
                ancestors: num_ancestors,
                successors: num_successors,
                sample_ancestor1: sample_ancestors.get(0).map(String::as_str).unwrap_or(""),
                sample_ancestor2: sample_ancestors.get(1).map(String::as_str).unwrap_or(""),
            })
            .with_context(|| format!("Could not write {swhid}"))?;
    }

    Ok(())
}
