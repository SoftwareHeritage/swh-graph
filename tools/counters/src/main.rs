use anyhow::{Context, Result};
use clap::Parser;
use std::collections::HashMap;
use std::path::PathBuf;

use swh_graph::graph::*;
use swh_graph::mph::DynMphf;
use swh_graph::{NodeType, SWHID};

/// Counts SWH objects in a graph
#[derive(Parser, Debug)]
#[command()]
struct Args {
    graph_path: PathBuf,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    log::info!("Loading graph");
    let graph = swh_graph::graph::SwhUnidirectionalGraph::new(args.graph_path)
        .context("Could not load graph")?
        .init_properties()
        .load_properties(|props| props.load_maps::<DynMphf>())
        .context("Could not load maps")?
        .load_properties(|props| props.load_contents())
        .context("Could not load content properties")?;

    log::info!(
        "Graph loaded: {} nodes / {} edges",
        graph.num_nodes(),
        graph.num_arcs()
    );

    let mut count_by_type: HashMap<NodeType, u64> = HashMap::new();
    let mut max_length: u64 = 0;
    let mut max_length_swhid: Option<SWHID> = None;
    let mut total_len: Option<u64> = Some(0);

    for src in 0..graph.num_nodes() {
        let node_type = graph.properties().node_type(src);
        *count_by_type.entry(node_type).or_insert(0) += 1;
        if node_type == NodeType::Content {
            let len = graph.properties().content_length(src).unwrap_or(0);
            total_len = total_len.map(|total| total.checked_add(len).unwrap());
            if len > max_length {
                max_length = len;
                max_length_swhid = Some(graph.properties().swhid(src));
            }
        }
    }

    for (node_type, count) in count_by_type {
        log::info!("Number of {}={}", node_type, count);
    }

    log::info!("Total length: {}", total_len.unwrap_or(0));
    if max_length_swhid.is_some() {
        log::info!(
            "Max content length: {}, in {}",
            max_length,
            max_length_swhid.unwrap()
        );
    }

    Ok(())
}
