// Copyright (C) 2023  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use anyhow::{Context, Result};
use clap::Parser;
use std::collections::HashSet;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;

use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

use swh_graph::graph::*;
use swh_graph::views::Subgraph;

#[derive(Parser, Debug)]
#[command(about = "gRPC server for the compressed Software Heritage graph", long_about = None)]
struct Args {
    graph_path: PathBuf,
    #[arg(long, default_value = "[::]:50091")]
    bind: std::net::SocketAddr,
    #[arg(long)]
    /// A line-separated list of node SWHIDs to exclude from the graph
    masked_nodes: Option<PathBuf>,
    #[arg(long)]
    /// Defaults to `localhost:8125` (or whatever is configured by the `STATSD_HOST`
    /// and `STATSD_PORT` environment variables).
    statsd_host: Option<String>,
}

pub fn main() -> Result<()> {
    let args = Args::parse();

    let fmt_layer = tracing_subscriber::fmt::layer();
    let filter_layer = tracing_subscriber::EnvFilter::try_from_default_env()
        .or_else(|_| tracing_subscriber::EnvFilter::try_new("info"))
        .unwrap();

    let logger = tracing_subscriber::registry();

    #[cfg(feature = "sentry")]
    let (_guard, sentry_layer) = swh_graph_grpc_server::sentry::setup();

    #[cfg(feature = "sentry")]
    let logger = logger.with(sentry_layer);

    logger
        .with(filter_layer)
        .with(fmt_layer)
        .try_init()
        .context("Could not initialize logging")?;

    let statsd_client = swh_graph_grpc_server::statsd::statsd_client(args.statsd_host)?;

    log::info!("Loading graph");
    let graph = load_full_dyn::<swh_graph::mph::DynMphf>(args.graph_path)?;

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

    let graph = Subgraph::with_node_filter(graph, move |node| !masked_nodes.contains(&node));

    // can't use #[tokio::main] because Sentry must be initialized before we start the tokio runtime
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            log::info!("Starting server");
            swh_graph_grpc_server::serve(graph, args.bind, statsd_client).await
        })?;
    Ok(())
}
