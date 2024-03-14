// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

#![allow(non_snake_case)]

use std::io;
use std::io::Write;
use std::path::PathBuf;
use std::sync::mpsc::{sync_channel, SyncSender};

use anyhow::{bail, Context, Result};
use chrono::NaiveDateTime;
use clap::Parser;
use dsi_progress_logger::{ProgressLog, ProgressLogger};
use itertools::Itertools;
use rayon::prelude::*;
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

struct ParsedInputRecord {
    author_date: String,
    swhid: String,
    node: NodeId,
}

struct OutputRecord {
    author_date: String,
    revrel_SWHID: String,
    cntdir: NodeId,
}

#[derive(Debug, Serialize)]
struct UnparsedOutputRecord {
    author_date: String,
    revrel_SWHID: String,
    cntdir_SWHID: SWHID,
}

/// Marker in array of timestamps indicating the given cell is empty;
const NO_TIMESTAMP: i64 = i64::MIN.to_be();

const BATCH_SIZE: usize = 50_000; // Arbitrary

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

    let mut writer = csv::WriterBuilder::new()
        .has_headers(true)
        .terminator(csv::Terminator::CRLF)
        .from_writer(io::stdout());

    // Encoded as big-endian i64
    log::info!("Initializing timestamps");
    let mut timestamps = vec![NO_TIMESTAMP; graph.num_nodes()];
    log::info!("Timestamps initialized");

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

    let (deser_tx, deser_rx) = sync_channel(BATCH_SIZE * 2);
    let (ser_tx, ser_rx) = sync_channel(BATCH_SIZE * 2);

    std::thread::scope(|scope| -> Result<()> {
        let graph = &graph;
        let pl = &mut pl;
        let timestamps = &mut timestamps;

        let deser_thread = scope.spawn(|| -> Result<()> { deserialize_input(graph, deser_tx) });

        let work_thread = scope.spawn(move || {
            let mut previous_dt = NaiveDateTime::MIN;
            while let Ok(ParsedInputRecord { author_date, swhid, node }) = deser_rx.recv() {
                let dt = NaiveDateTime::parse_from_str(&author_date, "%Y-%m-%dT%H:%M:%S%.f")
                    .with_context(|| format!("Could not parse date {}", author_date))?;
                if dt < previous_dt {
                    bail!(
                        "Dates are not correctly ordered ({swhid} has {dt}, which follows {previous_dt})"
                    );
                }
                previous_dt = dt;
                let timestamp = dt.and_utc().timestamp();

                visit_from_node(
                    graph,
                    timestamps,
                    &author_date,
                    timestamp,
                    node,
                    swhid,
                    &ser_tx,
                )?;
            }
            drop(ser_tx); // Close the channel so ser_thread can end
            log::info!("Done processing remaining revisions, finishing serialization");
            Ok(())
        });

        let ser_thread = scope.spawn(move || -> Result<()> {
            while let Ok(record) = ser_rx.recv() {
                let OutputRecord {
                    author_date,
                    revrel_SWHID,
                    cntdir,
                } = record;
                writer.serialize(UnparsedOutputRecord {
                    author_date,
                    revrel_SWHID,
                    cntdir_SWHID: graph.properties().swhid(cntdir),
                })?;
                pl.light_update();
            }
            log::info!("Done serializing results");
            Ok(())
        });

        deser_thread
            .join()
            .expect("Could not join deserialization thread")?;
        work_thread.join().expect("Could not join work thread")?;
        ser_thread
            .join()
            .expect("Could not join serialization thread")?;

        Ok(())
    })?;

    pl.done();

    if let Some(mut timestamps_file) = timestamps_file {
        log::info!("Writing timestamps");
        timestamps_file
            .write_all(bytemuck::cast_slice(&timestamps))
            .with_context(|| {
                format!(
                    "Could not write to {}",
                    args.timestamps_out.unwrap().display()
                )
            })?;
    }
    log::info!("Done");

    Ok(())
}

/// Reads stdin and writes to `tx`
///
/// This is equivalent to:
///
/// ```
/// for record in reader.deserialize().chunks(batch_size) {
///     let InputRecord { author_date, swhid } =
///         record.context("Could not deserialize row")?;
///     tx.send(ParsedInputRecord {
///         author_date,
///         node: graph
///             .properties()
///             .node_id_from_string_swhid(&swhid)
///             .with_context(|| format!("unknown SWHID {}", swhid))?,
///         swhid,
///     }).unwrap();
/// }
/// ```
///
/// but runs in parallel while preserving order
fn deserialize_input<G>(graph: &G, deser_tx: SyncSender<ParsedInputRecord>) -> Result<()>
where
    G: SwhGraphWithProperties + Sync,
    <G as SwhGraphWithProperties>::Maps: swh_graph::properties::Maps,
{
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_reader(io::stdin());
    for records in &reader.deserialize().chunks(BATCH_SIZE) {
        let mut records_to_send = Vec::with_capacity(BATCH_SIZE);
        let records: Vec<_> = records.collect();
        records
            .into_par_iter()
            .map(|record| -> Result<_> {
                let InputRecord { author_date, swhid } =
                    record.context("Could not deserialize row")?;
                Ok(ParsedInputRecord {
                    author_date,
                    node: graph
                        .properties()
                        .node_id_from_string_swhid(&swhid)
                        .with_context(|| format!("unknown SWHID {}", swhid))?,
                    swhid,
                })
            })
            .collect_into_vec(&mut records_to_send);
        for record in records_to_send {
            deser_tx
                .send(record?)
                .expect("deserialization channel closed by receiver");
        }
    }
    log::info!("Done parsing input, processing remaining revisions");
    drop(deser_tx); // Close the channel so work_thread can end
    Ok(())
}

/// For every child of `swhid` that was not seen before, write a list to stdout and
/// add its timestamp to `timestamps` (a big-endian-encoded array of i64)
fn visit_from_node<'a, G>(
    graph: &G,
    timestamps: &'a mut [i64],
    author_date: &str,
    timestamp: i64,
    revrel: NodeId,
    revrel_SWHID: String,
    tx: &SyncSender<OutputRecord>,
) -> Result<()>
where
    G: SwhForwardGraph + SwhBackwardGraph + SwhGraphWithProperties,
    <G as SwhGraphWithProperties>::Maps: swh_graph::properties::Maps,
{
    let node_type = graph.properties().node_type(revrel);
    match node_type {
        SWHType::Release => (), // Allow releases
        SWHType::Revision => {
            // Allow revisions only if they are a "snapshot head" (ie. one of their
            // predecessors is a release or a snapshot)
            if !graph.predecessors(revrel).into_iter().any(|pred| {
                let pred_type = graph.properties().node_type(pred);
                pred_type == SWHType::Snapshot || pred_type == SWHType::Release
            }) {
                return Ok(());
            }
        }
        _ => bail!("{revrel_SWHID} has unexpected type {node_type}"),
    }

    let mut stack = Vec::new();
    stack.push(revrel);

    while let Some(node) = stack.pop() {
        let node_type = graph.properties().node_type(node);

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
            let succ_type = graph.properties().node_type(succ);
            if succ_type != SWHType::Directory && succ_type != SWHType::Content {
                continue;
            }

            stack.push(succ);
            tx.send(OutputRecord {
                author_date: author_date.to_string(),
                revrel_SWHID: revrel_SWHID.clone(),
                cntdir: succ,
            })
            .expect("serialization channel closed by receiver")
        }
    }

    Ok(())
}
