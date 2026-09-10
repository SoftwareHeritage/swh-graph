// Copyright (C) 2023-2026  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};

use anyhow::{ensure, Result};
use dsi_progress_logger::{concurrent_progress_logger, ProgressLog};
use ph::fmph::GOFunction;
use ph::fmph::{GOBuildConf, GOConf};

use crate::compress::zst_dir::*;
use crate::compress::SWHID_TXT_SIZE;
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
    /*
    let swhids: Vec<[u8; SWHID_TXT_SIZE]> = par_iter_lines_from_dir(&swhids_dir, pl).collect();
    ensure!(
        swhids.len() == num_nodes,
        "Expected {num_nodes} nodes, read {}",
        swhids.len()
    );*/
    let pass_counter = AtomicU32::new(1);
    let iter_swhids = || {
        let pass_counter = pass_counter.fetch_add(1, Ordering::Relaxed);
        let mut pl = concurrent_progress_logger!(
            display_memory = true,
            item_name = "SWHID",
            local_speed = true,
            expected_updates = Some(num_nodes),
        );
        pl.start(format!(
            "Reading SWHIDs (pass #{pass_counter} sequentially)"
        ));
        iter_lines_from_dir(&swhids_dir, pl)
    };
    let par_iter_swhids = || {
        let pass_counter = pass_counter.fetch_add(1, Ordering::Relaxed);
        let mut pl = concurrent_progress_logger!(
            display_memory = true,
            item_name = "SWHID",
            local_speed = true,
            expected_updates = Some(num_nodes),
        );
        pl.start(format!("Reading SWHIDs (pass #{pass_counter} in parallel)"));
        par_iter_lines_from_dir::<[u8; SWHID_TXT_SIZE]>(&swhids_dir, pl)
    };
    let get_iter = (iter_swhids, par_iter_swhids);
    let clone_threshold = 1_000_000; // arbitrary
    let key_set =
        ph::fmph::keyset::CachedKeySet::dynamic_with_len(get_iter, num_nodes, clone_threshold);

    let conf = GOBuildConf::new(GOConf::default_bigger());
    let mphf = GOFunction::with_conf(key_set, conf);
    let len = mphf.len();
    ensure!(
        len == num_nodes,
        "Built MPHF from {num_nodes}, but its range is {len}"
    );
    Ok(SwhidFmphgo(mphf))
}
