// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

#![allow(non_snake_case)]

use std::io;
use std::io::Write;
use std::path::PathBuf;

use anyhow::{bail, Context, Result};
use chrono::NaiveDateTime;
use clap::Parser;
use dsi_progress_logger::{ProgressLog, ProgressLogger};
use serde::{Deserialize, Serialize};

use swh_graph::graph::*;
use swh_graph::java_compat::mph::gov::GOVMPH;
use swh_graph::{SWHType, SWHID};

#[derive(Parser, Debug)]
/// Given a list of release/revisions (with header 'author_date,SWHID') on stdin,
/// returns a CSV with header 'author_date,revrel_SWHID,cntdir_SWHID' and a row for
/// each of the contents and directories they contain
struct Args {
    graph_path: PathBuf,
    #[arg(short, long, action = clap::ArgAction::Count)]
    verbose: u8,
    #[arg(long)]
    /// Path to write the array of timestamps to
    timestamps_out: Option<PathBuf>,
}

#[derive(Debug, Deserialize)]
struct InputRecord {
    author_date: String,
    #[serde(rename = "SWHID")]
    swhid: String,
}

#[derive(Debug, Serialize)]
struct OutputRecord<'a> {
    author_date: &'a str,
    revrel_SWHID: &'a str,
    cntdir_SWHID: SWHID,
}

/// Marker in array of timestamps indicating the given cell is empty;
const NO_TIMESTAMP: i64 = i64::MIN.to_be();

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
        .init_properties()
        .load_properties(|props| props.load_maps::<GOVMPH>())
        .context("Could not load maps")?;
    log::info!("Graph loaded.");

    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_reader(io::stdin());
    let mut writer = csv::WriterBuilder::new()
        .has_headers(true)
        .terminator(csv::Terminator::CRLF)
        .from_writer(io::stdout());

    // Encoded as big-endian i64
    let mut timestamps = vec![NO_TIMESTAMP; graph.num_nodes()];

    let timestamps_file = match args.timestamps_out {
        Some(ref timestamps_path) => Some(
            std::fs::File::create(timestamps_path)
                .with_context(|| format!("Could not create {}", timestamps_path.display()))?,
        ),
        None => None,
    };

    let mut pl = ProgressLogger::default();
    pl.item_name("revrel");
    pl.display_memory(true);
    pl.start("Listing contents and directories...");

    let mut previous_dt = NaiveDateTime::MIN;
    for record in reader.deserialize() {
        pl.light_update();
        let InputRecord { author_date, swhid } = record.context("Could not deserialize row")?;

        let dt = NaiveDateTime::parse_from_str(&author_date, "%Y-%m-%dT%H:%M:%S")
            .with_context(|| format!("Could not parse date {}", author_date))?;
        if dt < previous_dt {
            bail!(
                "Dates are not correctly ordered ({swhid} has {dt}, which follows {previous_dt})"
            );
        }
        previous_dt = dt;
        let timestamp = dt.and_utc().timestamp();

        visit_from_node(
            &graph,
            &mut writer,
            &mut timestamps,
            &author_date,
            timestamp,
            swhid,
        )?;
    }

    pl.done();

    if let Some(mut timestamps_file) = timestamps_file {
        timestamps_file
            .write_all(bytemuck::cast_slice(&timestamps))
            .with_context(|| {
                format!(
                    "Could not write to {}",
                    args.timestamps_out.unwrap().display()
                )
            })?;
    }

    Ok(())
}

/// For every child of `swhid` that was not seen before, write a list to stdout and
/// add its timestamp to `timestamps` (a big-endian-encoded array of i64)
fn visit_from_node<'a, G>(
    graph: &G,
    writer: &mut csv::Writer<io::Stdout>,
    timestamps: &'a mut [i64],
    author_date: &str,
    timestamp: i64,
    revrel_SWHID: String,
) -> Result<()>
where
    G: SwhForwardGraph + SwhBackwardGraph + SwhGraphWithProperties,
    <G as SwhGraphWithProperties>::Maps: swh_graph::properties::Maps,
{
    let node = graph
        .properties()
        .node_id_from_string_swhid(&revrel_SWHID)
        .with_context(|| format!("unknown SWHID {}", revrel_SWHID))?;
    let node_type = graph
        .properties()
        .node_type(node)
        .expect("missing node type");
    match node_type {
        SWHType::Release => (), // Allow releases
        SWHType::Revision => {
            // Allow revisions only if they are a "snapshot head" (ie. one of their
            // predecessors is a release or a snapshot)
            if !graph.predecessors(node).into_iter().any(|pred| {
                let pred_type = graph
                    .properties()
                    .node_type(pred)
                    .expect("missing node type");
                pred_type == SWHType::Snapshot || pred_type == SWHType::Release
            }) {
                return Ok(());
            }
        }
        _ => bail!("{revrel_SWHID} has unexpected type {node_type}"),
    }

    let mut stack = Vec::new();
    stack.push(node);

    while let Some(node) = stack.pop() {
        let node_type = graph
            .properties()
            .node_type(node)
            .expect("missing node type");

        // Set its timestamp to the array if it's not a revision or a release
        // (we already have timestamps for those in the graph properties)
        if node_type == SWHType::Directory || node_type == SWHType::Content {
            timestamps[node] = timestamp.to_be();
        }

        for succ in graph.successors(node) {
            if timestamps[succ] != NO_TIMESTAMP {
                // Already visited
                continue;
            }

            // Ignore the successor if it's not a content or directory
            let succ_type = graph
                .properties()
                .node_type(succ)
                .expect("missing node type");
            if succ_type != SWHType::Directory && succ_type != SWHType::Content {
                continue;
            }

            stack.push(succ);
            writer.serialize(OutputRecord {
                author_date,
                revrel_SWHID: revrel_SWHID.as_ref(),
                cntdir_SWHID: graph.properties().swhid(succ).expect("missing SWHID"),
            })?
        }
    }

    Ok(())
}
