// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use dsi_progress_logger::{ProgressLog, ProgressLogger};
use rayon::prelude::*;
use serde::Deserialize;
use sux::bits::bit_vec::{AtomicBitVec, BitVec};

use swh_graph::graph::*;

#[allow(non_snake_case)]
#[derive(Debug, Deserialize)]
struct InputRecord {
    frontier_dir_SWHID: String,
}

/// Reads a CSV from stdin, returns the set of SWHIDs it contains
pub fn frontier_directories_from_stdin<G>(graph: &G) -> Result<BitVec>
where
    G: SwhGraphWithProperties + Send + Sync + 'static,
    <G as SwhGraphWithProperties>::Maps: swh_graph::properties::Maps,
{
    let frontier_directories = AtomicBitVec::new(graph.num_nodes());

    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_reader(std::io::stdin());

    let mut pl = ProgressLogger::default();
    pl.item_name("directory");
    pl.display_memory(true);
    pl.start("Reading frontier directories...");
    let pl = Arc::new(Mutex::new(pl));

    reader
        .deserialize()
        .par_bridge()
        .try_for_each(|record| -> Result<()> {
            let InputRecord { frontier_dir_SWHID } =
                record.context("Could not deserialize input row")?;
            let node_id = graph
                .properties()
                .node_id_from_string_swhid(frontier_dir_SWHID)?;
            if node_id % 32768 == 0 {
                pl.lock().unwrap().update_with_count(32768);
            }

            frontier_directories.set(node_id, true, Ordering::Relaxed);

            Ok(())
        })?;
    pl.lock().unwrap().done();

    Ok(frontier_directories.into())
}
