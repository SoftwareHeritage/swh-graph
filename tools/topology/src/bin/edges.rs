// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Parser;
use dsi_progress_logger::{concurrent_progress_logger, ProgressLog};
use rayon::prelude::*;
use serde::Serialize;

use dataset_writer::{CsvZstTableWriter, ParallelDatasetWriter, TableWriter};
use swh_graph::graph::*;
use swh_graph::labels::{Branch, DirEntry, Visit, VisitStatus};
use swh_graph::mph::DynMphf;
use swh_graph::views::Subgraph;
use swh_graph::{NodeConstraint, NodeType, SWHID};

#[derive(Parser, Debug)]
#[command(about = "Reads a graph and re-creates the list of edges used to create the graph", long_about = None)]
struct Args {
    graph_path: PathBuf,
    #[arg(short, long, default_value = "*")]
    node_types: NodeConstraint,
    #[arg(short, long, default_value_t = 3)]
    compression_level: i32,
    #[arg(short, long)]
    output_dir: PathBuf,
}

#[derive(Debug, Serialize)]
struct Node {
    node: SWHID,
}

#[derive(Debug, Serialize)]
struct UnlabeledEdge {
    src: SWHID,
    dst: SWHID,
}

#[derive(Debug, Serialize)]
struct DirectoryEdge<'a> {
    src: SWHID,
    dst: SWHID,
    name: &'a str,
    permission: u16,
}

#[derive(Debug, Serialize)]
struct SnapshotEdge<'a> {
    src: SWHID,
    dst: SWHID,
    name: &'a str,
}

#[derive(Debug, Serialize)]
struct OriginEdge<'a> {
    src: SWHID,
    dst: SWHID,
    timestamp: u64,
    status: &'a str,
}

pub struct NodeAndEdgeWriter<'a> {
    node_writer: CsvZstTableWriter<'a>,
    edge_writer: CsvZstTableWriter<'a>,
}
impl TableWriter for NodeAndEdgeWriter<'_> {
    type Schema = ();
    type CloseResult = ();
    type Config = ();

    fn new(path: PathBuf, schema: Self::Schema, config: Self::Config) -> Result<Self> {
        std::fs::create_dir_all(&path)
            .with_context(|| format!("Could not create {}", path.display()))?;
        Ok(NodeAndEdgeWriter {
            node_writer: CsvZstTableWriter::new(path.join("nodes"), schema, config)
                .context("Could not create node writer")?,
            edge_writer: CsvZstTableWriter::new(path.join("edges"), schema, config)
                .context("Could not create edge writer")?,
        })
    }

    fn flush(&mut self) -> Result<()> {
        self.node_writer
            .flush()
            .context("Could not flush node writer")?;
        self.edge_writer
            .flush()
            .context("Could not flush edge writer")?;
        Ok(())
    }

    fn close(self) -> Result<()> {
        self.node_writer
            .close()
            .context("Could not close node writer")?;
        self.edge_writer
            .close()
            .context("Could not close edge writer")?;
        Ok(())
    }
}

pub fn main() -> Result<()> {
    let args = Args::parse();

    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    log::info!("Loading graph");
    let graph = swh_graph::graph::SwhUnidirectionalGraph::new(args.graph_path)
        .context("Could not load graph")?
        .load_labels()
        .context("Could not load labels")?
        .init_properties()
        .load_properties(|props| props.load_maps::<DynMphf>())
        .context("Could not load maps")?
        .load_properties(|props| props.load_label_names())
        .context("Could not load maps")?;

    let graph = Arc::new(Subgraph::with_node_constraint(&graph, args.node_types));

    let output_dir = args.output_dir;
    std::fs::create_dir(&output_dir)
        .with_context(|| format!("Could not create {}", output_dir.display()))?;

    let content_writers = ParallelDatasetWriter::new(output_dir.join("content"))
        .context("Could not create content writer")?;
    let directory_writers = ParallelDatasetWriter::new(output_dir.join("directory"))
        .context("Could not create directory writer")?;
    let origin_writers = ParallelDatasetWriter::new(output_dir.join("origin"))
        .context("Could not create origin writer")?;
    let release_writers = ParallelDatasetWriter::new(output_dir.join("release"))
        .context("Could not create release writer")?;
    let revision_writers = ParallelDatasetWriter::new(output_dir.join("revision"))
        .context("Could not create revision writer")?;
    let snapshot_writers = ParallelDatasetWriter::new(output_dir.join("snapshot"))
        .context("Could not create snapshot writer")?;

    let mut pl = concurrent_progress_logger!(
        display_memory = true,
        local_speed = true,
        item_name = "node",
        expected_updates = Some(graph.actual_num_nodes().unwrap_or(graph.num_nodes())),
    );
    pl.start("Listing nodes and edges...");

    graph
        .par_iter_nodes(pl.clone())
        .try_for_each(|node| -> Result<()> {
            let node_type = graph.properties().node_type(node);

            match node_type {
                NodeType::Content => write_unlabeled_edges(
                    &*graph,
                    node,
                    &mut content_writers.get_thread_writer().unwrap(),
                )?,
                NodeType::Directory => write_directory_edges(
                    &*graph,
                    node,
                    &mut directory_writers.get_thread_writer().unwrap(),
                )?,
                NodeType::Origin => write_origin_edges(
                    &*graph,
                    node,
                    &mut origin_writers.get_thread_writer().unwrap(),
                )?,
                NodeType::Release => write_unlabeled_edges(
                    &*graph,
                    node,
                    &mut release_writers.get_thread_writer().unwrap(),
                )?,
                NodeType::Revision => write_unlabeled_edges(
                    &*graph,
                    node,
                    &mut revision_writers.get_thread_writer().unwrap(),
                )?,
                NodeType::Snapshot => write_snapshot_edges(
                    &*graph,
                    node,
                    &mut snapshot_writers.get_thread_writer().unwrap(),
                )?,
            }
            Ok::<_, anyhow::Error>(())
        })?;

    content_writers.close()?;
    directory_writers.close()?;
    origin_writers.close()?;
    release_writers.close()?;
    revision_writers.close()?;
    snapshot_writers.close()?;

    pl.done();

    Ok(())
}

fn write_unlabeled_edges<G: SwhGraphWithProperties + SwhForwardGraph>(
    graph: &G,
    node: NodeId,
    writers: &mut NodeAndEdgeWriter,
) -> Result<()>
where
    <G as SwhGraphWithProperties>::Maps: swh_graph::properties::Maps,
{
    let NodeAndEdgeWriter {
        node_writer,
        edge_writer,
    } = writers;

    // Write node
    let swhid = graph.properties().swhid(node);
    node_writer
        .serialize(Node { node: swhid })
        .context("Could not serialize node")?;

    // Write outgoing edges
    for succ in graph.successors(node) {
        let succ_swhid = graph.properties().swhid(succ);
        edge_writer
            .serialize(UnlabeledEdge {
                src: swhid,
                dst: succ_swhid,
            })
            .context("Could not serialize unlabeled edge")?;
    }
    Ok(())
}

fn write_directory_edges<G: SwhGraphWithProperties + SwhLabeledForwardGraph>(
    graph: &G,
    node: NodeId,
    writers: &mut NodeAndEdgeWriter,
) -> Result<()>
where
    <G as SwhGraphWithProperties>::Maps: swh_graph::properties::Maps,
    <G as SwhGraphWithProperties>::LabelNames: swh_graph::properties::LabelNames,
{
    let NodeAndEdgeWriter {
        node_writer,
        edge_writer,
    } = writers;

    // Write node
    let swhid = graph.properties().swhid(node);
    node_writer
        .serialize(Node { node: swhid })
        .context("Could not serialize node")?;

    // Write outgoing edges
    for (succ, dir_entries) in graph.untyped_labeled_successors(node) {
        let succ_swhid = graph.properties().swhid(succ);
        let mut got_entry = false;
        for dir_entry in dir_entries {
            let dir_entry = DirEntry::from(dir_entry);
            got_entry = true;
            let label = graph
                .properties()
                .label_name_base64(dir_entry.label_name_id());
            edge_writer
                .serialize(DirectoryEdge {
                    src: swhid,
                    dst: succ_swhid,
                    name: &String::from_utf8(label.clone()).with_context(|| {
                        format!("Could not decode {}", String::from_utf8_lossy(&label))
                    })?,
                    permission: dir_entry
                        .permission()
                        .expect("Missing permission for dir entry")
                        .to_git(),
                })
                .context("Could not serialize labeled directory edge")?;
        }
        if !got_entry {
            edge_writer
                .serialize(UnlabeledEdge {
                    src: swhid,
                    dst: succ_swhid,
                })
                .context("Could not serialize unlabeled edge for directory")?;
        }
    }
    Ok(())
}

fn write_snapshot_edges<G: SwhGraphWithProperties + SwhLabeledForwardGraph>(
    graph: &G,
    node: NodeId,
    writers: &mut NodeAndEdgeWriter,
) -> Result<()>
where
    <G as SwhGraphWithProperties>::Maps: swh_graph::properties::Maps,
    <G as SwhGraphWithProperties>::LabelNames: swh_graph::properties::LabelNames,
{
    let NodeAndEdgeWriter {
        node_writer,
        edge_writer,
    } = writers;

    // Write node
    let swhid = graph.properties().swhid(node);
    node_writer
        .serialize(Node { node: swhid })
        .context("Could not serialize node")?;

    // Write outgoing edges
    for (succ, branches) in graph.untyped_labeled_successors(node) {
        let succ_swhid = graph.properties().swhid(succ);
        let mut got_entry = false;
        for branch in branches {
            got_entry = true;
            let branch = Branch::from(branch);
            let label = graph.properties().label_name_base64(branch.label_name_id());
            edge_writer
                .serialize(SnapshotEdge {
                    src: swhid,
                    dst: succ_swhid,
                    name: &String::from_utf8(label.clone()).with_context(|| {
                        format!("Could not decode {}", String::from_utf8_lossy(&label))
                    })?,
                })
                .context("Could not serialize labeled snapshot edge")?;
        }
        if !got_entry {
            edge_writer
                .serialize(UnlabeledEdge {
                    src: swhid,
                    dst: succ_swhid,
                })
                .context("Could not serialize unlabeled edge for snapshot")?;
        }
    }
    Ok(())
}

fn write_origin_edges<G: SwhGraphWithProperties + SwhLabeledForwardGraph>(
    graph: &G,
    node: NodeId,
    writers: &mut NodeAndEdgeWriter,
) -> Result<()>
where
    <G as SwhGraphWithProperties>::Maps: swh_graph::properties::Maps,
{
    let NodeAndEdgeWriter {
        node_writer,
        edge_writer,
    } = writers;

    // Write node
    let swhid = graph.properties().swhid(node);
    node_writer
        .serialize(Node { node: swhid })
        .context("Could not serialize node")?;

    // Write outgoing edges
    for (succ, visits) in graph.untyped_labeled_successors(node) {
        let succ_swhid = graph.properties().swhid(succ);
        let mut got_entry = false;
        for visit in visits {
            let visit = Visit::from(visit);
            got_entry = true;
            edge_writer
                .serialize(OriginEdge {
                    src: swhid,
                    dst: succ_swhid,
                    timestamp: visit.timestamp(),
                    status: match visit.status() {
                        VisitStatus::Full => "full",
                        VisitStatus::Partial => "partial",
                    },
                })
                .context("Could not serialize labeled origin edge")?;
        }
        if !got_entry {
            edge_writer
                .serialize(UnlabeledEdge {
                    src: swhid,
                    dst: succ_swhid,
                })
                .context("Could not serialize origin edge for directory")?;
        }
    }
    Ok(())
}
