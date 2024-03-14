// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

#![allow(non_snake_case)]

use std::io::Write;
use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;
use dsi_progress_logger::{ProgressLog, ProgressLogger};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};

use swh_graph::graph::*;
use swh_graph::java_compat::mph::gov::GOVMPH;
use swh_graph::SWHID;

use swh_graph_provenance::csv_dataset::CsvZstDataset;
use swh_graph_provenance::frontier::PathParts;

#[derive(Parser, Debug)]
/** Given as input a binary file with, for each directory, the newest date of first
 * occurrence of any of the content in its subtree (well, DAG), ie.,
 * max_{for all content} (min_{for all occurrence of content} occurrence).
 * and as stdin a CSV with header "frontier_dir_SWHID" containing a single column
 * with frontier directories.
 * Produces the line of frontier directories (relative to any revision) present in
 * each revision.
 */
struct Args {
    graph_path: PathBuf,
    #[arg(short, long, action = clap::ArgAction::Count)]
    verbose: u8,
    #[arg(long)]
    /// Path to a directory where to write .csv.zst results to
    contents_out: PathBuf,
}

#[derive(Debug, Deserialize)]
struct InputRecord {
    dir_SWHID: String,
}

#[derive(Debug, Serialize)]
struct OutputRecord<'a> {
    cnt_SWHID: SWHID,
    dir_SWHID: &'a str,
    #[serde(with = "serde_bytes")] // Serialize a bytestring instead of list of ints
    path: Vec<u8>,
}

pub fn main() -> Result<()> {
    let args = Args::parse();

    stderrlog::new()
        .verbosity(args.verbose as usize)
        .timestamp(stderrlog::Timestamp::Second)
        .init()
        .context("While Initializing the stderrlog")?;

    log::info!("Loading graph");
    let graph = swh_graph::graph::load_bidirectional(args.graph_path)
        .context("Could not load graph")?
        .load_forward_labels()
        .context("Could not load labels")?
        .init_properties()
        .load_properties(|props| props.load_label_names())
        .context("Could not load label names")?
        .load_properties(|props| props.load_maps::<GOVMPH>())
        .context("Could not load maps")?;
    log::info!("Graph loaded.");

    let output_dataset = CsvZstDataset::new(args.contents_out)?;

    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_reader(std::io::stdin());

    let mut pl = ProgressLogger::default();
    pl.item_name("directory");
    pl.display_memory(true);
    pl.start("Listing contents in directories...");

    // Reuse writers across work batches, or we end up with millions of very small files
    let writers = thread_local::ThreadLocal::new();

    reader
        .deserialize()
        .inspect(|_| pl.light_update())
        .par_bridge()
        .try_for_each_init(
            || {
                writers
                    .get_or(|| output_dataset.get_new_writer().unwrap())
                    .borrow_mut()
            },
            |writer, record| -> Result<()> {
                let InputRecord { dir_SWHID } =
                    record.context("Could not deserialize input row")?;

                write_contents_in_directory(&graph, writer, &dir_SWHID)?;

                Ok(())
            },
        )?;
    pl.done();

    Ok(())
}

fn write_contents_in_directory<G, W: Write>(
    graph: &G,
    writer: &mut csv::Writer<W>,
    root_dir_swhid: &str,
) -> Result<()>
where
    G: SwhLabelledForwardGraph + SwhGraphWithProperties,
    <G as SwhGraphWithProperties>::LabelNames: swh_graph::properties::LabelNames,
    <G as SwhGraphWithProperties>::Maps: swh_graph::properties::Maps,
{
    let root_dir = graph
        .properties()
        .node_id_from_string_swhid(root_dir_swhid)?;

    let on_directory = |_dir: NodeId, _path_parts: PathParts| Ok(true); // always recurse

    let on_content = |cnt: NodeId, path_parts: PathParts| {
        writer
            .serialize(OutputRecord {
                cnt_SWHID: graph.properties().swhid(cnt),
                dir_SWHID: root_dir_swhid,
                path: path_parts.build_path(graph),
            })
            .context("Could not write record")
    };

    swh_graph_provenance::frontier::dfs_with_path(graph, on_directory, on_content, root_dir)
}
