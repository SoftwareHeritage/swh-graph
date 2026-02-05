// Copyright (C) 2023  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::path::PathBuf;

use anyhow::{Context, Result};
use dsi_progress_logger::{concurrent_progress_logger, ProgressLog};
use pthash::{BuildConfiguration, PartitionedPhf, Phf};
use rayon::prelude::*;

use crate::compress::zst_dir::*;
use crate::mph::{HashableSWHID, SwhidPthash};

/// Reads textual SWHIDs from the path and return a MPH function for them.
pub fn build_swhids_mphf(swhids_dir: PathBuf, num_nodes: usize) -> Result<SwhidPthash> {
    let mut pass_counter = 0;
    let iter_swhids = || {
        pass_counter += 1;
        let mut pl = concurrent_progress_logger!(
            display_memory = true,
            item_name = "SWHID",
            local_speed = true,
            expected_updates = Some(num_nodes),
        );
        pl.start(format!("Reading SWHIDs (pass #{pass_counter})"));
        par_iter_lines_from_dir(&swhids_dir, pl).map(HashableSWHID::<Vec<u8>>)
    };
    let temp_dir = tempfile::tempdir().unwrap();

    // Tuned by zack on the 2023-09-06 graph on a machine with two Intel Xeon Gold 6342 CPUs
    let mut config = BuildConfiguration::new(temp_dir.path().to_owned());
    config.c = 5.;
    config.alpha = 0.94;
    config.num_partitions = num_nodes.div_ceil(10000000) as u64;
    config.num_threads = num_cpus::get() as u64;

    log::info!("Building MPH with parameters: {:?}", config);

    let mut f = PartitionedPhf::new();
    f.par_build_in_internal_memory_from_bytes(iter_swhids, &config)
        .context("Failed to build MPH")?;
    Ok(SwhidPthash(f))
}
