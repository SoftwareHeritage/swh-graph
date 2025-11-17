/*
 * Copyright (C) 2025  The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

use std::io::BufReader;
use std::path::PathBuf;

use anyhow::{anyhow, ensure, Context, Result};
use clap::{Parser, ValueEnum};

use swh_graph::graph::*;
use swh_graph::labels::{EdgeLabel, VisitStatus};
use swh_graph::properties;
use swh_graph::{NodeType, SWHID};

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
/// Runs any of swh-graph's Minimal Perfect Hash functions
///
/// Lines in stdin are hashed one by one, and a decimal-encoded 64-bits integer is
/// written on the output for each of them.
///
/// If any of the input lines was not in the dataset used to build the MPH, then the result
/// will either be a silent hash collision or cause a non-zero exit.
struct Args {
    #[arg(long, value_enum, default_value_t = GraphFormat::Webgraph)]
    graph_format: GraphFormat,
    #[arg(long, default_value = "bidirectional")]
    direction: Direction,
    #[arg(long, default_value = "bidirectional")]
    labels: Labels,
    graph_path: PathBuf,
    swhid: Vec<String>,
}

pub fn main() -> Result<()> {
    let args = Args::parse();

    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

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
                        for swhid in args.swhid {
                            let swhid = swhid.parse()?;
                            let node = graph.properties().node_id(swhid)?;
                            println!(
                                "{}",
                                serde_json::to_string(&Node {
                                    properties: get_properties(&graph, node)?,
                                    successors: Some(collect_labeled_successors(
                                        &graph,
                                        graph.labeled_successors(node)
                                    )?),
                                    predecessors: Some(collect_labeled_successors(
                                        &graph,
                                        graph.labeled_predecessors(node)
                                    )?),
                                    swhid,
                                })
                                .unwrap()
                            );
                        }
                    }
                    Labels::None => {
                        for swhid in args.swhid {
                            let swhid = swhid.parse()?;
                            let node = graph.properties().node_id(swhid)?;
                            println!(
                                "{}",
                                serde_json::to_string(&Node {
                                    properties: get_properties(&graph, node)?,
                                    successors: Some(collect_successors(
                                        &graph,
                                        graph.successors(node)
                                    )?),
                                    predecessors: Some(collect_successors(
                                        &graph,
                                        graph.predecessors(node)
                                    )?),
                                    swhid,
                                })
                                .unwrap()
                            );
                        }
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
                        for swhid in args.swhid {
                            let swhid = swhid.parse()?;
                            let node = graph.properties().node_id(swhid)?;
                            println!(
                                "{}",
                                serde_json::to_string(&Node {
                                    properties: get_properties(&graph, node)?,
                                    successors: Some(collect_labeled_successors(
                                        &graph,
                                        graph.labeled_successors(node)
                                    )?),
                                    predecessors: None,
                                    swhid,
                                })
                                .unwrap()
                            );
                        }
                    }
                    Labels::None => {
                        for swhid in args.swhid {
                            let swhid = swhid.parse()?;
                            let node = graph.properties().node_id(swhid)?;
                            println!(
                                "{}",
                                serde_json::to_string(&Node {
                                    properties: get_properties(&graph, node)?,
                                    successors: Some(collect_successors(
                                        &graph,
                                        graph.successors(node)
                                    )?),
                                    predecessors: None,
                                    swhid,
                                })
                                .unwrap()
                            );
                        }
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
            for swhid in args.swhid {
                let swhid = swhid.parse()?;
                let node = graph.properties().node_id(swhid)?;
                println!(
                    "{}",
                    serde_json::to_string(&Node {
                        properties: get_properties(&graph, node)?,
                        successors: Some(collect_labeled_successors(
                            &graph,
                            graph.labeled_successors(node)
                        )?),
                        predecessors: Some(collect_labeled_successors(
                            &graph,
                            graph.labeled_predecessors(node)
                        )?),
                        swhid,
                    })
                    .unwrap()
                );
            }
        }
    }

    Ok(())
}

#[derive(serde::Serialize)]
struct Node {
    swhid: SWHID,
    properties: Properties,
    successors: Option<Vec<Succ>>,
    predecessors: Option<Vec<Succ>>,
}

#[derive(serde::Serialize)]
struct Succ {
    swhid: SWHID,
    labels: Option<Vec<Label>>,
}

#[derive(serde::Serialize)]
#[serde(untagged)]
enum Label {
    Branch {
        branch_name: String,
    },
    DirEntry {
        file_name: String,
        permission: Option<u16>,
    },
    Visit {
        status: &'static str,
        timestamp: u64,
    },
}

impl Label {
    fn new<G: SwhGraphWithProperties<LabelNames: properties::LabelNames>>(
        graph: G,
        label: EdgeLabel,
    ) -> Result<Self> {
        Ok(match label {
            EdgeLabel::Branch(branch) => Label::Branch {
                branch_name: String::from_utf8_lossy(
                    &graph.properties().label_name(branch.label_name_id()),
                )
                .into_owned(),
            },
            EdgeLabel::DirEntry(entry) => Label::DirEntry {
                file_name: String::from_utf8_lossy(
                    &graph.properties().label_name(entry.label_name_id()),
                )
                .into_owned(),
                permission: entry.permission().map(|perm| perm.to_git()),
            },
            EdgeLabel::Visit(visit) => Label::Visit {
                status: match visit.status() {
                    VisitStatus::Full => "full",
                    VisitStatus::Partial => "partial",
                },
                timestamp: visit.timestamp(),
            },
        })
    }
}

fn collect_labeled_successors<
    G: SwhGraphWithProperties<Maps: properties::Maps, LabelNames: properties::LabelNames>,
>(
    graph: G,
    successors: impl Iterator<Item = (usize, impl Iterator<Item = EdgeLabel>)>,
) -> Result<Vec<Succ>> {
    successors
        .map(|(succ, labels)| -> Result<_> {
            Ok(Succ {
                swhid: graph.properties().swhid(succ),
                labels: Some(
                    labels
                        .map(|label| Label::new(&graph, label))
                        .collect::<Result<_>>()?,
                ),
            })
        })
        .collect()
}

fn collect_successors<G: SwhGraphWithProperties<Maps: properties::Maps>>(
    graph: G,
    successors: impl Iterator<Item = usize>,
) -> Result<Vec<Succ>> {
    successors
        .map(|succ| -> Result<_> {
            Ok(Succ {
                swhid: graph.properties().swhid(succ),
                labels: None,
            })
        })
        .collect()
}

#[derive(Default, serde::Serialize)]
struct Properties {
    // cnt
    length: Option<u64>,
    is_skipped_content: Option<bool>,

    // rev/rel
    author_timestamp: Option<i64>,
    author_timestamp_offset: Option<i16>,
    committer_timestamp: Option<i64>,
    committer_timestamp_offset: Option<i16>,
    author_id: Option<u32>,
    committer_id: Option<u32>,
    tag_name: Option<String>,
    message: Option<String>,

    // ori
    url: Option<String>,
}

fn get_properties<
    G: SwhGraphWithProperties<
        Maps: properties::Maps,
        Timestamps: properties::OptTimestamps,
        Persons: properties::OptPersons,
        Contents: properties::OptContents,
        Strings: properties::OptStrings,
    >,
>(
    graph: G,
    node: usize,
) -> Result<Properties> {
    let mut properties = Properties::default();
    match graph.properties().node_type(node) {
        NodeType::Content => {
            properties.length = map::<G::Contents, _>(graph.properties().content_length(node));
            properties.is_skipped_content = Some(map::<G::Contents, _>(
                graph.properties().is_skipped_content(node),
            ));
        }
        NodeType::Directory => {}
        NodeType::Revision => {
            properties.author_timestamp =
                map::<G::Timestamps, _>(graph.properties().author_timestamp(node));
            properties.author_timestamp_offset =
                map::<G::Timestamps, _>(graph.properties().author_timestamp_offset(node));
            properties.committer_timestamp =
                map::<G::Timestamps, _>(graph.properties().committer_timestamp(node));
            properties.committer_timestamp_offset =
                map::<G::Timestamps, _>(graph.properties().committer_timestamp_offset(node));
            properties.author_id = map::<G::Persons, _>(graph.properties().author_id(node));
            properties.committer_id = map::<G::Persons, _>(graph.properties().committer_id(node));
            properties.message = map::<G::Strings, _>(graph.properties().message(node))
                .map(|url| String::from_utf8_lossy(&url).into_owned());
        }
        NodeType::Release => {
            properties.author_timestamp =
                map::<G::Timestamps, _>(graph.properties().author_timestamp(node));
            properties.author_timestamp_offset =
                map::<G::Timestamps, _>(graph.properties().author_timestamp_offset(node));
            properties.author_id = map::<G::Persons, _>(graph.properties().author_id(node));
            properties.tag_name = map::<G::Strings, _>(graph.properties().tag_name(node))
                .map(|tag_name| String::from_utf8_lossy(&tag_name).into_owned());
            properties.message = map::<G::Strings, _>(graph.properties().message(node))
                .map(|message| String::from_utf8_lossy(&message).into_owned());
        }
        NodeType::Snapshot => {}
        NodeType::Origin => {
            properties.url = map::<G::Strings, _>(graph.properties().message(node))
                .map(|url| String::from_utf8_lossy(&url).into_owned());
        }
    }

    Ok(properties)
}

fn map<PB: properties::PropertiesBackend, T: Default>(
    value: <PB::DataFilesAvailability as properties::DataFilesAvailability>::Result<'_, T>,
) -> T {
    use swh_graph::properties::DataFilesAvailability;

    PB::DataFilesAvailability::make_result(value).unwrap_or_default()
}
