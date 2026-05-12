// Copyright (C) 2023-2026  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::path::PathBuf;

use anyhow::{ensure, Result};
use dsi_progress_logger::{concurrent_progress_logger, ProgressLog};
use rayon::prelude::*;

use crate::compress::zst_dir::*;
use crate::mph::SwhidFmphgo;

/// Reads textual SWHIDs from the path and return a MPH function for them.
pub fn build_swhids_mphf(swhids_dir: PathBuf, num_nodes: usize) -> Result<SwhidFmphgo> {
    let mut pl = concurrent_progress_logger!(
        display_memory = true,
        item_name = "SWHID",
        local_speed = true,
        expected_updates = Some(num_nodes),
    );
    pl.start("Reading SWHIDs...");
    let swhids: Vec<Box<[u8]>> = par_iter_lines_from_dir(&swhids_dir, pl).collect();
    ensure!(
        swhids.len() == num_nodes,
        "Expected {num_nodes} nodes, read {}",
        swhids.len()
    );

    let mphf = ph::fmph::GOFunction::new(swhids);
    let len = mphf.len();
    ensure!(
        len == num_nodes,
        "Built MPHF from {num_nodes}, but its range is {len}"
    );
    Ok(SwhidFmphgo(mphf))
}
