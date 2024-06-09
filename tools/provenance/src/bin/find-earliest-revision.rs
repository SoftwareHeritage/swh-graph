// Copyright (C) 2023  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::io;
use std::path::PathBuf;
use std::sync::Mutex;

use anyhow::{Context, Result};
use clap::Parser;
use dsi_progress_logger::{ProgressLog, ProgressLogger};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};

use swh_graph::graph::*;
use swh_graph::java_compat::mph::gov::GOVMPH;
use swh_graph::SWHID;

use swh_graph_provenance::earliest_revision::{find_earliest_revision, EarliestRevision};

#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

#[derive(Parser, Debug)]
#[command(about = "Given a list of directory/content SWHID on stdin, returns a CSV with header 'swhid,earliest_swhid,earliest_ts,rev_occurrences'", long_about = None)]
struct Args {
    graph_path: PathBuf,
    #[arg(short, long, action = clap::ArgAction::Count)]
    verbose: u8,
}

#[derive(Debug, Deserialize)]
struct InputRecord {
    swhid: String,
}

#[derive(Debug, Serialize)]
struct OutputRecord {
    swhid: String,
    earliest_swhid: SWHID,
    earliest_ts: i64,
    rev_occurrences: u64,
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
        .init_properties()
        .load_properties(|props| props.load_maps::<GOVMPH>())
        .context("Could not load maps")?
        .load_properties(|props| props.load_timestamps())
        .context("Could not load timestamps")?;

    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_reader(io::stdin());
    let writer = csv::WriterBuilder::new()
        .has_headers(true)
        .from_writer(io::stdout());
    let writer = Mutex::new(writer);

    let mut pl = ProgressLogger::default();
    pl.item_name("SWHID");
    pl.display_memory(true);
    pl.start("Looking up SWHID provenance...");
    let pl = Mutex::new(pl);

    reader.deserialize().par_bridge().try_for_each(|record| {
        let InputRecord { swhid } = record.context("Could not deserialize input")?;

        let node = graph.properties().node_id_from_string_swhid(&swhid)?;
        match find_earliest_revision(&graph, node) {
            Some(EarliestRevision {
                node: earliest_rev_id,
                ts: earliest_ts,
                rev_occurrences,
            }) => {
                let earliest_swhid = graph.properties().swhid(earliest_rev_id);
                let record = OutputRecord {
                    swhid,
                    earliest_swhid,
                    earliest_ts,
                    rev_occurrences,
                };
                writer
                    .lock()
                    .unwrap()
                    .serialize(record)
                    .context("Could not write record")?
            }
            None => log::debug!("no revision found containing {swhid}"),
        }
        pl.lock().unwrap().light_update();
        Ok::<(), anyhow::Error>(())
    })?;
    pl.lock().unwrap().done();

    Ok(())
}
