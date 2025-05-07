// Copyright (C) 2023-2025  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use anyhow::{anyhow, ensure, Context, Result};
use clap::{Parser, ValueEnum};
use std::collections::HashSet;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;

use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

use swh_graph::graph::*;
use swh_graph::properties;
use swh_graph::views::Subgraph;

use swh_graph_grpc_server::graph::*;

#[derive(ValueEnum, Clone, Debug, PartialEq, Eq)]
enum Direction {
    Forward,
    Bidirectional,
}

#[derive(ValueEnum, Clone, Debug, PartialEq, Eq)]
enum Labels {
    None,
    Bidirectional,
}

/// On-disk format of the graph. This should always be `Webgraph` unless testing.
#[derive(ValueEnum, Clone, Debug)]
enum GraphFormat {
    Webgraph,
    Json,
}

#[derive(Parser, Debug)]
#[command(about = "gRPC server for the compressed Software Heritage graph", long_about = None)]
struct Args {
    #[arg(long, value_enum, default_value_t = GraphFormat::Webgraph)]
    graph_format: GraphFormat,
    graph_path: PathBuf,
    #[arg(long, default_value = "[::]:50091")]
    bind: std::net::SocketAddr,
    #[arg(long, default_value = "bidirectional")]
    direction: Direction,
    #[arg(long, default_value = "bidirectional")]
    labels: Labels,
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

    log::info!("Loading graph");
    macro_rules! load_properties {
        ($graph:expr) => {
            $graph.init_properties().load_properties(|properties| {
                properties
                    .load_maps::<swh_graph::mph::DynMphf>()
                    .context("Could not load maps")?
                    .opt_load_timestamps()
                    .context("Could not load timestamp properties")?
                    .opt_load_persons()
                    .context("Could not load person properties")?
                    .opt_load_contents()
                    .context("Could not load content properties")?
                    .opt_load_strings()
                    .context("Could not load string properties")
            })
        };
    }
    match args.graph_format {
        GraphFormat::Webgraph => match args.direction {
            Direction::Bidirectional => {
                let graph =
                    SwhBidirectionalGraph::new(&args.graph_path).context("Could not load graph")?;
                let graph = load_properties!(graph)?;
                match args.labels {
                    Labels::Bidirectional => {
                        let graph = graph
                            .load_properties(|properties| {
                                properties
                                    .load_label_names()
                                    .context("Could not load label names")
                            })?
                            .load_labels()
                            .context("Could not load labels")?;
                        main2(graph, args)
                    }
                    Labels::None => {
                        let graph = graph.load_properties(|properties| {
                            properties.with_label_names(StubLabelNames)
                        })?;
                        let graph = StubLabels::new(graph);
                        main2(graph, args)
                    }
                }
            }
            Direction::Forward => {
                let graph = SwhUnidirectionalGraph::new(&args.graph_path)
                    .context("Could not load graph")?;
                let graph = load_properties!(graph)?;
                match args.labels {
                    Labels::Bidirectional => {
                        let graph = graph
                            .load_properties(|properties| {
                                properties
                                    .load_label_names()
                                    .context("Could not load label names")
                            })?
                            .load_labels()
                            .context("Could not load labels")?;
                        let graph = StubBackwardArcs::new(graph);
                        main2(graph, args)
                    }
                    Labels::None => {
                        let graph = graph.load_properties(|properties| {
                            properties.with_label_names(StubLabelNames)
                        })?;
                        let graph = StubBackwardArcs::new(graph);
                        let graph = StubLabels::new(graph);
                        main2(graph, args)
                    }
                }
            }
        },
        GraphFormat::Json => {
            ensure!(args.direction == Direction::Bidirectional, "--graph-format json and --direction are mutually exclusive (graphs deserialized from JSON are always bidirectional)");
            ensure!(args.labels == Labels::Bidirectional, "--graph-format json and --labels are mutually exclusive (graphs' JSON serialization always contains labels)");

            let file = std::fs::File::open(&args.graph_path)
                .with_context(|| format!("Could not open {}", args.graph_path.display()))?;
            let mut deserializer = serde_json::Deserializer::from_reader(BufReader::new(file));
            let graph: SwhBidirectionalGraph<
                properties::SwhGraphProperties<
                    _,
                    properties::VecTimestamps,
                    properties::VecPersons,
                    properties::VecContents,
                    properties::VecStrings,
                    properties::VecLabelNames,
                >,
                _,
                _,
            > = swh_graph::serde::deserialize_with_labels_and_maps(
                &mut deserializer,
                args.graph_path.clone(),
            )
            .map_err(|e| anyhow!("Could not read JSON graph: {e}"))?;
            main2(graph, args)
        }
    }
}

fn main2<G: SwhOptFullGraph + Sync + Send + 'static>(graph: G, args: Args) -> Result<()> {
    let statsd_client = swh_graph_grpc_server::statsd::statsd_client(args.statsd_host)?;

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
