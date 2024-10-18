// Copyright (C) 2023-2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;
use dsi_progress_logger::{progress_logger, ProgressLog};
use serde::Serialize;

use swh_graph::graph::*;
use swh_graph::mph::DynMphf;
use swh_graph::SWHID;

use swh_graph_topology::generations::GenerationsReader;

#[derive(Serialize)]
struct Record {
    #[serde(rename = "SWHID")]
    swhid: SWHID,
    depth: u64,
}

#[derive(Parser, Debug)]
#[command()]
/// Reads a topological order .bitstream file, and streams it as a CSV with header
/// "swhid,generation"
struct Args {
    graph_path: PathBuf,
    #[arg(long)]
    /// Path from where to read the input order, and the accompanying .ef delimiting offsets between
    /// generations
    order: PathBuf,
}

pub fn main() -> Result<()> {
    let args = Args::parse();

    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    log::info!("Loading graph");
    let graph = swh_graph::graph::SwhUnidirectionalGraph::new(args.graph_path)
        .context("Could not load graph")?
        .init_properties()
        .load_properties(|props| props.load_maps::<DynMphf>())
        .context("Could not load maps")?;

    log::info!("Loading order");
    let reader = GenerationsReader::new(args.order).context("Could not load topological order")?;

    let mut pl = progress_logger!(
        display_memory = true,
        item_name = "node",
        local_speed = true,
    );
    pl.start("Streaming topological order");

    let mut writer = csv::WriterBuilder::new()
        .has_headers(true)
        .terminator(csv::Terminator::CRLF)
        .from_writer(std::io::stdout());

    for (depth, node) in reader
        .iter_nodes()
        .context("Could not read topological order")?
    {
        writer.serialize(Record {
            depth,
            swhid: graph.properties().swhid(node),
        })?;
        pl.light_update();
    }

    writer.flush()?;

    pl.done();

    Ok(())
}
