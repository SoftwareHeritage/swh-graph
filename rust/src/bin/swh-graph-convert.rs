// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::io::BufWriter;
use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::{Parser, ValueEnum};

use swh_graph::graph::*;
use swh_graph::mph::DynMphf;

#[derive(ValueEnum, Clone, Debug)]
enum InputFormat {
    Webgraph,
}

#[derive(ValueEnum, Clone, Debug)]
enum OutputFormat {
    Json,
    GraphBuilder,
}

#[derive(Parser, Debug)]
/// Converts a Software Heritage graph to a different format
struct Args {
    #[arg(long, value_enum, default_value_t = InputFormat::Webgraph)]
    input_format: InputFormat,

    #[arg(short, long)]
    /// Path to the graph to read
    input: PathBuf,

    #[arg(long, value_enum, default_value_t = OutputFormat::Json)]
    output_format: OutputFormat,

    #[arg(short, long)]
    /// Path to the graph to read
    output: PathBuf,
}

pub fn main() -> Result<()> {
    let args = Args::parse();

    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let graph = match args.input_format {
        InputFormat::Webgraph => SwhUnidirectionalGraph::new(args.input)
            .context("Could not load input graph")?
            .load_labels()
            .context("Could not load input graph's labels")?,
    };

    match args.output_format {
        OutputFormat::Json => {
            let graph = graph
                .init_properties()
                .load_properties(|props| props.load_maps::<DynMphf>())
                .context("Could not load input graph's maps")?;
            // TODO: make other properties serializable and load them too
            let file = std::fs::File::create(&args.output)
                .with_context(|| format!("Could not create {}", args.output.display()))?;
            let mut serializer = serde_json::Serializer::new(BufWriter::new(file));
            swh_graph::serde::serialize_with_labels_and_maps(&mut serializer, &graph)
                .with_context(|| format!("Could not serialize to {}", args.output.display()))?;
        }
        OutputFormat::GraphBuilder => {
            let graph = graph
                .load_all_properties::<DynMphf>()
                .context("Could not load properties")?;
            let file = std::fs::File::create(&args.output)
                .with_context(|| format!("Could not create {}", args.output.display()))?;
            swh_graph::graph_builder::codegen_from_full_graph(&graph, file)
                .with_context(|| format!("Could not write graph to {}", args.output.display()))?;
        }
    }

    Ok(())
}
