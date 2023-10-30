// Copyright (C) 2023  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;

#[derive(Parser, Debug)]
#[command(about = "gRPC server for the compressed Software Heritage graph", long_about = None)]
struct Args {
    graph_path: PathBuf,
    #[arg(long, default_value = "[::]:5009")]
    bind: std::net::SocketAddr,
}

#[tokio::main]
pub async fn main() -> Result<()> {
    let args = Args::parse();

    stderrlog::new()
        .verbosity(2)
        .timestamp(stderrlog::Timestamp::Second)
        .init()
        .context("While Initializing the stderrlog")?;

    log::info!("Loading graph");
    let graph =
        swh_graph::graph::load_bidirectional(args.graph_path).context("Could not load graph")?;

    log::info!("Loading properties");
    let graph = graph
        .load_all_properties::<swh_graph::mph::DynMphf>()
        .context("Could not load graph properties")?;

    log::info!("Starting server");
    swh_graph::server::serve(graph, args.bind).await?;

    Ok(())
}
