// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use clap::Parser;
use dsi_progress_logger::{ProgressLog, ProgressLogger};
use rayon::prelude::*;
use serde::Serialize;

use swh_graph::graph::*;
use swh_graph::java_compat::mph::gov::GOVMPH;
use swh_graph::labels::FilenameId;
use swh_graph::properties;
use swh_graph::SWHType;
use swh_graph::SWHID;

use swh_graph::utils::dataset_writer::{CsvZstTableWriter, ParallelDatasetWriter};

#[derive(Parser, Debug)]
/** Computes, for every content object, the list of names it directories refer to it as.
 */
struct Args {
    graph_path: PathBuf,
    #[arg(short, long, action = clap::ArgAction::Count)]
    verbose: u8,
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

    stderrlog::new()
        .verbosity(args.verbose as usize)
        .timestamp(stderrlog::Timestamp::Second)
        .init()
        .context("While Initializing the stderrlog")?;

    log::info!("Loading graph...");
    let graph = swh_graph::graph::load_unidirectional(args.graph_path)
        .context("Could not load graph")?
        .load_labels()
        .context("Could not load labels")?
        .init_properties()
        .load_properties(|props| props.load_contents())
        .context("Could not load content properties")?
        .load_properties(|props| props.load_maps::<GOVMPH>())
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

    let dataset_writer = ParallelDatasetWriter::new_with_schema(args.out, ())?;

    let mut pl = ProgressLogger::default();
    pl.item_name("node");
    pl.display_memory(true);
    pl.local_speed(true);
    pl.expected_updates(Some(graph.num_nodes()));
    pl.start("Writing file names");
    let pl = Arc::new(Mutex::new(pl));

    swh_graph::utils::shuffle::par_iter_shuffled_range(0..graph.num_nodes()).try_for_each_init(
        || dataset_writer.get_thread_writer().unwrap(),
        |writer, node| -> Result<()> {
            if graph.properties().node_type(node) == SWHType::Snapshot {
                write_files_by_name_in_snapshot(
                    &graph,
                    writer,
                    &filename_ids,
                    &branchname_ids,
                    node,
                )?;
            }
            if node % 32768 == 0 {
                pl.lock().unwrap().update_with_count(32768);
            }
            Ok(())
        },
    )?;

    dataset_writer.close()?;

    pl.lock().unwrap().done();

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
    G: SwhLabelledForwardGraph + SwhGraphWithProperties,
    <G as SwhGraphWithProperties>::Maps: properties::Maps,
    <G as SwhGraphWithProperties>::LabelNames: properties::LabelNames,
{
    let mut snp_swhid = None; // Computed lazily when needed
    for (branch_target, labels) in graph.labelled_successors(snp) {
        for label in labels {
            // This is snp->*, so we know the label has to be a Branch
            let label: swh_graph::labels::Branch = label.into();
            if !branchname_ids.contains(&label.filename_id()) {
                continue;
            }
            let branch_name = graph.properties().label_name(label.filename_id());

            let snp_swhid = snp_swhid
                .get_or_insert_with(|| graph.properties().swhid(snp))
                .clone();

            match graph.properties().node_type(branch_target) {
                SWHType::Directory => write_files_by_name_in_directory(
                    graph,
                    writer,
                    filename_ids,
                    snp_swhid,
                    &branch_name,
                    branch_target,
                )?,
                SWHType::Revision | SWHType::Release => write_files_by_name_in_revrel(
                    graph,
                    writer,
                    filename_ids,
                    snp_swhid,
                    &branch_name,
                    branch_target,
                )?,
                SWHType::Content | SWHType::Snapshot | SWHType::Origin => (),
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
    G: SwhLabelledForwardGraph + SwhGraphWithProperties,
    <G as SwhGraphWithProperties>::Maps: properties::Maps,
    <G as SwhGraphWithProperties>::LabelNames: properties::LabelNames,
{
    for succ in graph.successors(revrel) {
        match graph.properties().node_type(succ) {
            SWHType::Directory => {
                write_files_by_name_in_directory(
                    graph,
                    writer,
                    filename_ids,
                    snp_swhid,
                    branch_name,
                    succ,
                )?;
            }
            SWHType::Revision => {
                if graph.properties().node_type(revrel) == SWHType::Release {
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
    G: SwhLabelledForwardGraph + SwhGraphWithProperties,
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
        for (succ, labels) in graph.labelled_successors(dir) {
            match graph.properties().node_type(succ) {
                SWHType::Directory => to_visit.push(succ),
                SWHType::Content => {
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
                SWHType::Revision | SWHType::Release | SWHType::Snapshot | SWHType::Origin => (),
            }
        }
    }

    Ok(())
}
