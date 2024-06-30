// Copyright (C) 2023  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::collections::HashSet;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;

use swh_graph::graph::*;
use swh_graph::views::Subgraph;

#[derive(Parser, Debug)]
#[command(about = "gRPC server for the compressed Software Heritage graph", long_about = None)]
struct Args {
    graph_path: PathBuf,
    #[arg(long, default_value = "[::]:5009")]
    bind: std::net::SocketAddr,
    #[arg(long)]
    /// A line-separated list of node SWHIDs to exclude from the graph
    masked_nodes: Option<PathBuf>,
    #[arg(short, long, action = clap::ArgAction::Count)]
    verbose: u8,
}

#[tokio::main]
pub async fn main() -> Result<()> {
    let args = Args::parse();

    stderrlog::new()
        .verbosity(args.verbose as usize)
        .timestamp(stderrlog::Timestamp::Second)
        .init()
        .context("While Initializing the stderrlog")?;

    log::info!("Loading graph");
    let graph = load_bidirectional(args.graph_path).context("Could not load graph")?;

    log::info!("Loading properties");
    let graph = graph
        .load_all_properties::<swh_graph::mph::DynMphf>()
        .context("Could not load graph properties")?;

    log::info!("Loading labels");
    let graph = graph
        .load_labels()
        .context("Could not load labeled graph")?;

    let mut masked_nodes = HashSet::new();
    if let Some(masked_nodes_path) = args.masked_nodes {
        log::info!("Loading masked nodes");
        let f = File::open(&masked_nodes_path)
            .map(BufReader::new)
            .with_context(|| {
                format!(
                    "Could not open masked nodes file {}",
                    masked_nodes_path.display()
                )
            })?;

        for line in f.lines() {
            let line = line.context("Could not deserialize line")?;
            match graph.properties().node_id_from_string_swhid(&line) {
                Ok(node) => {
                    masked_nodes.insert(node);
                }
                Err(e) => log::warn!("Unknown node {line}: {e}"),
            }
        }
    }

    let graph = Subgraph::new_with_node_filter(graph, move |node| !masked_nodes.contains(&node));

    log::info!("Starting server");
    swh_graph::server::serve(graph, args.bind).await?;

    Ok(())
}
