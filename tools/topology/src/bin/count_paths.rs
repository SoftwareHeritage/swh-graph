// Copyright (C) 2023-2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::io;
use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::{Parser, ValueEnum};
use dsi_progress_logger::ProgressLogger;
use serde::{Deserialize, Serialize};

use swh_graph::graph::*;
use swh_graph::java_compat::mph::gov::GOVMPH;
use swh_graph::views::Transposed;
use swh_graph::SWHID;

#[derive(Debug, Clone, Copy, ValueEnum)]
enum Direction {
    Forward,
    Backward,
}

#[derive(Parser, Debug)]
#[command(about = "Counts the number of (non-singleton) paths reaching each node, from all other nodes.", long_about = Some("
Counts in the output may be large enough to overflow long integers, so they are computed with double-precision floating point number and printed as such.

This requires a topological order as input (as returned by the toposort command).
"))]
struct Args {
    graph_path: PathBuf,
    #[arg(short, long, action = clap::ArgAction::Count)]
    verbose: u8,
    #[arg(short, long)]
    direction: Direction,
}

#[derive(Debug, Deserialize)]
struct InputRecord {
    #[serde(rename = "SWHID")]
    swhid: SWHID,
}

#[derive(Debug, Serialize)]
struct OutputRecord {
    #[serde(rename = "SWHID")]
    swhid: SWHID,
    paths_from_roots: f64,
    all_paths: f64,
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
        .init_properties()
        .load_properties(|props| props.load_maps::<GOVMPH>())
        .context("Could not load maps")?;

    match args.direction {
        Direction::Forward => count_paths(graph),
        Direction::Backward => count_paths(Transposed(&graph)),
    }
}

fn count_paths<G>(graph: G) -> Result<()>
where
    G: SwhForwardGraph + SwhBackwardGraph + SwhGraphWithProperties,
    <G as SwhGraphWithProperties>::Maps: swh_graph::properties::Maps,
{
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_reader(io::stdin());
    let mut writer = csv::WriterBuilder::new()
        .terminator(csv::Terminator::CRLF)
        .from_writer(io::stdout());

    log::info!("Initializing counts...");
    let mut paths_from_roots_vec = vec![0f64; graph.num_nodes()];
    let mut all_paths_vec = vec![0f64; graph.num_nodes()];

    let mut pl = ProgressLogger::default().display_memory();
    pl.item_name = "node";
    pl.local_speed = true;
    pl.expected_updates = Some(graph.num_nodes());
    pl.start("Listing nodes...");

    for record in reader.deserialize() {
        let InputRecord { swhid, .. } =
            record.with_context(|| format!("Could not deserialize record"))?;
        pl.light_update();
        let node = graph
            .properties()
            .node_id(swhid)
            .with_context(|| format!("Unknown SWHID: {}", swhid))?;
        let mut paths_from_roots = paths_from_roots_vec[node];
        let mut all_paths = all_paths_vec[node];

        // Print counts for this nodes
        writer
            .serialize(OutputRecord {
                swhid,
                paths_from_roots,
                all_paths,
            })
            .with_context(|| format!("Could not write record for {}", swhid))?;

        // Add counts of paths coming from this node to all successors
        all_paths += 1.;
        if paths_from_roots == 0. {
            paths_from_roots += 1.;
        }
        for succ in graph.successors(node) {
            paths_from_roots_vec[succ] += paths_from_roots;
            all_paths_vec[succ] += all_paths;
        }
    }

    Ok(())
}
