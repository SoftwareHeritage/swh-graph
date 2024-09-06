// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use clap::Parser;
use dsi_progress_logger::{progress_logger, ProgressLog};
use rayon::prelude::*;
use serde::Serialize;

use swh_graph::graph::*;
use swh_graph::labels::FilenameId;
use swh_graph::mph::DynMphf;
use swh_graph::properties;
use swh_graph::NodeType;
use swh_graph::SWHID;

use dataset_writer::{CsvZstTableWriter, ParallelDatasetWriter};
use swh_graph::utils::progress_logger::{BufferedProgressLogger, MinimalProgressLog};

#[derive(Parser, Debug)]
/** Computes, for every content object, the list of names it directories refer to it as.
 */
struct Args {
    graph_path: PathBuf,
    filename: Vec<String>,
    #[arg(long)]
    /// Path to a directory where to write CSV files to.
    out: PathBuf,
}

#[allow(non_snake_case)]
#[derive(Debug, Serialize)]
struct OutputRecord<'a> {
    snp_SWHID: SWHID,
    #[serde(with = "serde_bytes")] // Serialize a bytestring instead of list of ints
    branch_name: &'a [u8],
    dir_SWHID: SWHID,
    #[serde(with = "serde_bytes")] // Serialize a bytestring instead of list of ints
    file_name: Vec<u8>,
    cnt_SWHID: SWHID,
}

/// A pair orderable by the second item.
#[derive(Debug, PartialEq, Eq)]
pub struct NameWithOccurences<N: PartialEq, O: PartialOrd>(N, O);

impl<N: PartialEq, O: PartialOrd> PartialOrd for NameWithOccurences<N, O> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.1.partial_cmp(&other.1)
    }
}

impl<N: Eq, O: Ord> Ord for NameWithOccurences<N, O> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.1.cmp(&other.1)
    }
}

pub fn main() -> Result<()> {
    let args = Args::parse();

    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    log::info!("Loading graph...");
    let graph = swh_graph::graph::SwhUnidirectionalGraph::new(args.graph_path)
        .context("Could not load graph")?
        .load_labels()
        .context("Could not load labels")?
        .init_properties()
        .load_properties(|props| props.load_contents())
        .context("Could not load content properties")?
        .load_properties(|props| props.load_maps::<DynMphf>())
        .context("Could not load maps")?
        .load_properties(|props| props.load_label_names())
        .context("Could not load label names")?;

    log::info!("Hashing branch and file names...");
    let filename_ids = args
        .filename
        .into_iter()
        .map(|filename| graph.properties().label_name_id(filename))
        .collect::<Result<_, _>>()
        .context("Could not find filename id")?;
    let branchname_ids: HashSet<_> = ["refs/heads/master", "refs/heads/main", "HEAD"]
        .into_iter()
        .flat_map(|branchname| graph.properties().label_name_id(branchname.as_bytes()))
        .collect();
    assert!(!branchname_ids.is_empty(), "Missing branch names");

    let dataset_writer = ParallelDatasetWriter::with_schema(args.out, ())?;

    let mut pl = progress_logger!(
        item_name = "node",
        display_memory = true,
        local_speed = true,
        expected_updates = Some(graph.num_nodes()),
    );
    pl.start("Writing file names");

    let shared_pl = Arc::new(Mutex::new(&mut pl));
    swh_graph::utils::shuffle::par_iter_shuffled_range(0..graph.num_nodes()).try_for_each_init(
        || {
            (
                dataset_writer.get_thread_writer().unwrap(),
                BufferedProgressLogger::new(shared_pl.clone()),
            )
        },
        |(writer, thread_pl), node| -> Result<()> {
            if graph.properties().node_type(node) == NodeType::Snapshot {
                write_files_by_name_in_snapshot(
                    &graph,
                    writer,
                    &filename_ids,
                    &branchname_ids,
                    node,
                )?;
            }
            thread_pl.light_update();
            Ok(())
        },
    )?;

    dataset_writer.close()?;

    pl.done();

    Ok(())
}

fn write_files_by_name_in_snapshot<G>(
    graph: &G,
    writer: &mut CsvZstTableWriter,
    filename_ids: &HashSet<FilenameId>,
    branchname_ids: &HashSet<FilenameId>,
    snp: NodeId,
) -> Result<()>
where
    G: SwhLabeledForwardGraph + SwhGraphWithProperties,
    <G as SwhGraphWithProperties>::Maps: properties::Maps,
    <G as SwhGraphWithProperties>::LabelNames: properties::LabelNames,
{
    let mut snp_swhid = None; // Computed lazily when needed
    for (branch_target, labels) in graph.untyped_labeled_successors(snp) {
        for label in labels {
            // This is snp->*, so we know the label has to be a Branch
            let label: swh_graph::labels::Branch = label.into();
            if !branchname_ids.contains(&label.filename_id()) {
                continue;
            }
            let branch_name = graph.properties().label_name(label.filename_id());

            let snp_swhid = *snp_swhid.get_or_insert_with(|| graph.properties().swhid(snp));

            match graph.properties().node_type(branch_target) {
                NodeType::Directory => write_files_by_name_in_directory(
                    graph,
                    writer,
                    filename_ids,
                    snp_swhid,
                    &branch_name,
                    branch_target,
                )?,
                NodeType::Revision | NodeType::Release => write_files_by_name_in_revrel(
                    graph,
                    writer,
                    filename_ids,
                    snp_swhid,
                    &branch_name,
                    branch_target,
                )?,
                NodeType::Content | NodeType::Snapshot | NodeType::Origin => (),
            }
        }
    }

    Ok(())
}

fn write_files_by_name_in_revrel<G>(
    graph: &G,
    writer: &mut CsvZstTableWriter,
    filename_ids: &HashSet<FilenameId>,
    snp_swhid: SWHID,
    branch_name: &[u8],
    revrel: NodeId,
) -> Result<()>
where
    G: SwhLabeledForwardGraph + SwhGraphWithProperties,
    <G as SwhGraphWithProperties>::Maps: properties::Maps,
    <G as SwhGraphWithProperties>::LabelNames: properties::LabelNames,
{
    for succ in graph.successors(revrel) {
        match graph.properties().node_type(succ) {
            NodeType::Directory => {
                write_files_by_name_in_directory(
                    graph,
                    writer,
                    filename_ids,
                    snp_swhid,
                    branch_name,
                    succ,
                )?;
            }
            NodeType::Revision => {
                if graph.properties().node_type(revrel) == NodeType::Release {
                    // snp->rel->rev
                    write_files_by_name_in_revrel(
                        graph,
                        writer,
                        filename_ids,
                        snp_swhid,
                        branch_name,
                        succ,
                    )?
                }
            }
            _ => (),
        }
    }

    Ok(())
}

fn write_files_by_name_in_directory<G>(
    graph: &G,
    writer: &mut CsvZstTableWriter,
    filename_ids: &HashSet<FilenameId>,
    snp_swhid: SWHID,
    branch_name: &[u8],
    root_dir: NodeId,
) -> Result<()>
where
    G: SwhLabeledForwardGraph + SwhGraphWithProperties,
    <G as SwhGraphWithProperties>::Maps: properties::Maps,
    <G as SwhGraphWithProperties>::LabelNames: properties::LabelNames,
{
    let mut to_visit = vec![root_dir];
    let mut visited = HashSet::new();

    while let Some(dir) = to_visit.pop() {
        if visited.contains(&dir) {
            continue;
        }
        visited.insert(dir);
        for (succ, labels) in graph.untyped_labeled_successors(dir) {
            match graph.properties().node_type(succ) {
                NodeType::Directory => to_visit.push(succ),
                NodeType::Content => {
                    for label in labels {
                        // This is cnt->dir, so we know the label has to be a DirEntry
                        let label: swh_graph::labels::DirEntry = label.into();

                        if filename_ids.contains(&label.filename_id()) {
                            writer
                                .serialize(OutputRecord {
                                    snp_SWHID: snp_swhid,
                                    branch_name,
                                    dir_SWHID: graph.properties().swhid(dir),
                                    file_name: graph.properties().label_name(label.filename_id()),
                                    cnt_SWHID: graph.properties().swhid(succ),
                                })
                                .context("Could not write output record")?;
                        }
                    }
                }
                NodeType::Revision | NodeType::Release | NodeType::Snapshot | NodeType::Origin => {}
            }
        }
    }

    Ok(())
}
