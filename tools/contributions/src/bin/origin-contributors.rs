// Copyright (C) 2023-2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::collections::HashMap;
use std::path::PathBuf;

use anyhow::{bail, ensure, Context, Result};
use clap::{Parser, ValueEnum};
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use swh_graph::graph::*;
use swh_graph::mph::DynMphf;
use swh_graph::properties;
use swh_graph::views::Subgraph;
use swh_graph::NodeType;
use swh_graph::SWHID;

use swh_graph_contributions::ContributorSet;

#[derive(Debug, Clone, Copy, ValueEnum)]
enum Direction {
    Forward,
    Backward,
}

#[derive(Parser, Debug)]
/// Reads a CSV with headers
/// 'swhid,num_predecessors,num_successors,sample_predecessor1,sample_predecessor2' (as returned by
/// the 'toposort' executable) and returns the list of contributor ids contributing to any given
/// origin, as a CSV stdout with header `origin_id,contributor_id,years`
struct Args {
    graph_path: PathBuf,
    #[arg(long)]
    /// Directory where to write CSV files with header `origin_id,origin_url_base64`
    origins_out: PathBuf,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct InputRecord {
    #[serde(rename = "SWHID")]
    swhid: SWHID,
    #[serde(rename = "ancestors")]
    num_predecessors: usize,
    #[serde(rename = "successors")]
    num_successors: usize,
    #[serde(rename = "sample_ancestor1")]
    sample_predecessor1: String,
    #[serde(rename = "sample_ancestor2")]
    sample_predecessor2: String,
}

#[derive(Debug, Serialize)]
struct OriginOutputRecord<'a> {
    origin_id: usize,
    #[serde(with = "serde_bytes")]
    origin_url_base64: &'a [u8],
}

#[derive(Debug, Serialize)]
struct ContribOutputRecord {
    origin_id: usize,
    contributor_id: u32,
    years: String,
}

pub fn main() -> Result<()> {
    let args = Args::parse();

    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    log::info!("Loading graph");
    let graph = swh_graph::graph::SwhUnidirectionalGraph::new(args.graph_path)
        .context("Could not load graph")?
        .init_properties()
        .load_properties(|props| props.load_maps::<DynMphf>())
        .context("Could not load maps")?
        .load_properties(|props| props.load_persons())
        .context("Could not load persons")?
        .load_properties(|props| props.load_strings())
        .context("Could not load strings")?
        .load_properties(|props| props.load_timestamps())
        .context("Could not load timestamps")?;

    let contribs_writer = csv::WriterBuilder::new()
        .has_headers(true)
        .terminator(csv::Terminator::CRLF)
        .from_writer(std::io::stdout());
    let origins_writer = csv::WriterBuilder::new()
        .has_headers(true)
        .terminator(csv::Terminator::CRLF)
        .from_writer(
            std::fs::File::create(&args.origins_out)
                .with_context(|| format!("Could not create {}", args.origins_out.display()))?,
        );

    write_origin_contributors(&graph, contribs_writer, origins_writer)
}

fn write_origin_contributors<G, W1: std::io::Write, W2: std::io::Write>(
    graph: &G,
    mut contribs_writer: csv::Writer<W1>,
    mut origins_writer: csv::Writer<W2>,
) -> Result<()>
where
    G: SwhForwardGraph + SwhGraphWithProperties,
    <G as SwhGraphWithProperties>::Maps: properties::Maps,
    <G as SwhGraphWithProperties>::Persons: properties::Persons,
    <G as SwhGraphWithProperties>::Strings: properties::Strings,
    <G as SwhGraphWithProperties>::Timestamps: properties::Timestamps,
{
    let graph =
        Subgraph::with_node_filter(graph, |node| match graph.properties().node_type(node) {
            NodeType::Origin | NodeType::Snapshot | NodeType::Release | NodeType::Revision => true,
            NodeType::Content | NodeType::Directory => false,
        });

    // Map each node id to its set of contributor person_ids and years
    let mut contributors: HashMap<NodeId, ContributorSet> = HashMap::new();

    // For each node it, counts its number of direct successors that still need to be handled
    let mut pending_successors = HashMap::<NodeId, usize>::new();

    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_reader(std::io::stdin());

    // Makes sure the input at least has a header, even when there is no payload
    ensure!(
        reader
            .headers()
            .context("Invalid header in input")?
            .iter()
            .any(|item| item == "SWHID"),
        "Input has no 'swhid' header"
    );

    for record in reader.deserialize() {
        let InputRecord {
            swhid,
            num_predecessors,
            num_successors,
            sample_predecessor1,
            sample_predecessor2: _,
        } = record.context("Could not parse line")?;
        let node = graph
            .properties()
            .node_id(swhid)
            .with_context(|| format!("Unknown SWHID {}", swhid))?;

        if num_successors > 0 {
            pending_successors.insert(node, num_successors);
        }

        let mut node_contributors: ContributorSet = if num_predecessors == 1 {
            // Reuse the ancestor's set of contributors
            let predecessor = graph
                .properties()
                .node_id(sample_predecessor1.as_str())
                .context("Unknown predecessor SWHID")?;
            if pending_successors.get(&predecessor) == Some(&1) {
                pending_successors.remove(&predecessor);
                contributors
                    .remove(&predecessor)
                    .expect("Predecessor's contributors are not initialized")
            } else {
                // Predecessor is not yet ready to be popped because it has other successors
                // to be visited.  Copy its contributor set
                *pending_successors.get_mut(&predecessor).unwrap() -= 1;
                contributors
                    .get(&predecessor)
                    .expect("Predecessor's contributors are not initialized")
                    .clone()
            }
        } else {
            let mut node_contributors = ContributorSet::default();
            for succ in graph.successors(node) {
                // If 'node' is a revision, then 'succ' is its parent revision
                node_contributors.merge(
                    contributors
                        .get(&succ)
                        .expect("Parent's contributors are not initialized"),
                );
            }
            node_contributors
        };

        match graph.properties().node_type(node) {
            NodeType::Origin => {
                let Some(origin_url_base64) = graph.properties().message_base64(node) else {
                    log::warn!("Missing origin URL for {}", swhid);
                    continue;
                };
                for (contributor, years) in node_contributors {
                    contribs_writer
                        .serialize(ContribOutputRecord {
                            origin_id: node,
                            contributor_id: contributor,
                            years: years.iter().join(" "),
                        })
                        .with_context(|| {
                            format!(
                                "Could not write contributor record for {} ({})",
                                String::from_utf8_lossy(
                                    &graph.properties().message(node).unwrap_or("".into())
                                ),
                                swhid,
                            )
                        })?;
                }

                origins_writer
                    .serialize(OriginOutputRecord {
                        origin_id: node,
                        origin_url_base64,
                    })
                    .with_context(|| {
                        format!(
                            "Could not write origin record for {} ({})",
                            String::from_utf8_lossy(
                                &graph.properties().message(node).unwrap_or(b"".into())
                            ),
                            swhid
                        )
                    })?;
            }
            NodeType::Snapshot => {
                contributors.insert(node, node_contributors);
            }
            NodeType::Release => {
                if let Some(author) = graph.properties().author_id(node) {
                    if let Some(timestamp) = graph.properties().author_timestamp(node) {
                        if timestamp != 0 {
                            node_contributors.insert(author, timestamp);
                        }
                    }
                };
                contributors.insert(node, node_contributors);
            }
            NodeType::Revision => {
                if let Some(author) = graph.properties().author_id(node) {
                    if let Some(timestamp) = graph.properties().author_timestamp(node) {
                        if timestamp != 0 {
                            node_contributors.insert(author, timestamp);
                        }
                    }
                };

                if let Some(committer) = graph.properties().committer_id(node) {
                    if let Some(timestamp) = graph.properties().committer_timestamp(node) {
                        if timestamp != 0 {
                            node_contributors.insert(committer, timestamp);
                        }
                    }
                };
                contributors.insert(node, node_contributors);
            }
            NodeType::Content | NodeType::Directory => {
                bail!("Unexpected node type in input: {}", swhid)
            }
        }
    }

    Ok(())
}
