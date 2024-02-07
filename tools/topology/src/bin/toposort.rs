// Copyright (C) 2023  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::collections::VecDeque;
use std::io;
use std::path::PathBuf;

use anyhow::{bail, Context, Result};
use clap::{Parser, ValueEnum};
use dsi_progress_logger::ProgressLogger;
use serde::Serialize;

use swh_graph::graph::*;
use swh_graph::java_compat::mph::gov::GOVMPH;
use swh_graph::utils::parse_allowed_node_types;
use swh_graph::views::Transposed;
use swh_graph::SWHType;

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
    #[arg(short, long, action = clap::ArgAction::Count)]
    verbose: u8,
    #[arg(short, long)]
    algorithm: Algorithm,
    #[arg(short, long)]
    direction: Direction,
    #[arg(short, long, default_value = "*")]
    node_types: String,
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

    let node_types = parse_allowed_node_types(&args.node_types)?;

    stderrlog::new()
        .verbosity(args.verbose as usize)
        .timestamp(stderrlog::Timestamp::Second)
        .init()
        .context("While Initializing the stderrlog")?;

    log::info!("Loading graph");
    let graph = swh_graph::graph::load_bidirectional(args.graph_path)
        .context("Could not load graph")?
        .init_properties()
        .load_properties(|props| props.load_maps::<GOVMPH>())
        .context("Could not load maps")?;

    match (args.algorithm, args.direction) {
        (Algorithm::Dfs, Direction::Forward) => toposort::<Stack, _>(graph, &node_types)?,
        (Algorithm::Bfs, Direction::Forward) => toposort::<Queue, _>(graph, &node_types)?,
        (Algorithm::Dfs, Direction::Backward) => {
            toposort::<Stack, _>(Transposed(&graph), &node_types)?
        }
        (Algorithm::Bfs, Direction::Backward) => {
            toposort::<Queue, _>(Transposed(&graph), &node_types)?
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

fn toposort<R: ReadySet, G>(graph: G, node_types: &[SWHType]) -> Result<()>
where
    G: SwhForwardGraph + SwhBackwardGraph + SwhGraphWithProperties,
    <G as SwhGraphWithProperties>::Maps: swh_graph::properties::MapsTrait,
{
    let graph = graph;

    let mut pl = ProgressLogger::default().display_memory();
    pl.item_name = "node";
    pl.local_speed = true;
    pl.expected_updates = Some(graph.num_nodes());
    pl.start("Listing leaves...");

    let mut total_arcs = 0;
    let mut ready = R::default();
    let mut num_unvisited_predecessors = vec![0usize; graph.num_nodes()];
    for node in 0..graph.num_nodes() {
        pl.light_update();
        if !node_types.contains(
            &graph
                .properties()
                .node_type(node)
                .expect("missing node type"),
        ) {
            continue;
        }
        let mut outdegree = 0;
        for predecessor in graph.predecessors(node) {
            if node_types.contains(
                &graph
                    .properties()
                    .node_type(predecessor)
                    .expect("missing node type"),
            ) {
                outdegree += 1;
            }
        }
        total_arcs += outdegree;
        num_unvisited_predecessors[node] = outdegree;
        if outdegree == 0 {
            ready.push(node);
        }
    }
    pl.done();

    log::info!("Leaves listed, starting traversal.");

    let mut pl = ProgressLogger::default().display_memory();
    pl.item_name = "arc";
    pl.local_speed = true;
    pl.expected_updates = Some(total_arcs);
    pl.start("Sorting...");

    let mut writer = csv::WriterBuilder::new()
        .terminator(csv::Terminator::CRLF)
        .from_writer(io::stdout());
    while let Some(current_node) = ready.pop() {
        let swhid = graph
            .properties()
            .swhid(current_node)
            .expect("Missing SWHID");
        let mut num_successors = 0;
        for successor in graph.successors(current_node) {
            if !node_types.contains(
                &graph
                    .properties()
                    .node_type(successor)
                    .expect("missing node type"),
            ) {
                continue;
            }
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
            let predecessor_swhid = graph
                .properties()
                .swhid(predecessor)
                .expect("Missing predecessor SWHID");
            if !node_types.contains(&predecessor_swhid.node_type) {
                continue;
            }
            if sample_ancestors.len() < 2 {
                sample_ancestors.push(predecessor_swhid.to_string())
            }
            num_ancestors += 1;
        }

        writer
            .serialize(Record {
                swhid: &swhid.to_string(),
                ancestors: num_ancestors,
                successors: num_successors,
                sample_ancestor1: sample_ancestors.get(0).map(String::as_str).unwrap_or(""),
                sample_ancestor2: sample_ancestors.get(1).map(String::as_str).unwrap_or(""),
            })
            .with_context(|| format!("Could not write {}", swhid))?;
    }

    Ok(())
}
