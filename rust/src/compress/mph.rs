// Copyright (C) 2023  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use dsi_progress_logger::{progress_logger, ProgressLog};
use ph::fmph;
use pthash::{BuildConfiguration, PartitionedPhf, Phf};
use rayon::prelude::*;

use crate::compress::zst_dir::*;
use crate::mph::{HashableSWHID, SwhidPthash};

fn par_iter_swhids(
    swhids_dir: &Path,
    num_nodes: usize,
) -> impl ParallelIterator<Item = Vec<u8>> + '_ {
    let mut pl = progress_logger!(
        display_memory = true,
        item_name = "SWHID",
        local_speed = true,
        expected_updates = Some(num_nodes),
    );
    pl.start("Reading SWHIDs");
    let pl = Arc::new(Mutex::new(pl));
    par_iter_lines_from_dir(swhids_dir, pl)
}

/// Reads textual SWHIDs from the path and return a MPH function for them.
pub fn build_swhids_mphf(swhids_dir: PathBuf, num_nodes: usize) -> Result<SwhidPthash> {
    let swhids: Vec<_> = par_iter_swhids(&swhids_dir, num_nodes)
        .map(HashableSWHID)
        .collect();
    let temp_dir = tempfile::tempdir().unwrap();

    // Tuned by zack on the 2023-09-06 graph on a machine with two Intel Xeon Gold 6342 CPUs
    let mut config = BuildConfiguration::new(temp_dir.path().to_owned());
    config.c = 5.;
    config.alpha = 0.94;
    config.num_partitions = num_nodes.div_ceil(10000000) as u64;
    config.num_threads = num_cpus::get() as u64;

    log::info!("Building MPH with parameters: {:?}", config);

    let mut f = PartitionedPhf::new();
    f.build_in_internal_memory_from_bytes(&swhids, &config)
        .context("Failed to build MPH")?;
    Ok(SwhidPthash(f))
}

pub fn build_mph<Item>(in_dir: PathBuf, out_mph: PathBuf, item_name: &str) -> Result<()>
where
    Item: TryFrom<Vec<u8>> + Clone + std::hash::Hash + Send + Sync,
    <Item as TryFrom<Vec<u8>>>::Error: std::fmt::Debug,
{
    let clone_threshold = 10240; // TODO: tune this
    let conf = fmph::BuildConf::default();
    let call_counts = Mutex::new(0);
    let len = Mutex::new(None);

    let get_pl = |parallel| {
        let mut call_counts = call_counts.lock().unwrap();
        *call_counts += 1;
        let mut pl = progress_logger!(
            display_memory = true,
            item_name = item_name,
            local_speed = true,
            expected_updates = *len.lock().unwrap(),
        );
        pl.start(&format!(
            "{} reading {} (pass {})",
            if parallel {
                "parallelly"
            } else {
                "sequentially"
            },
            item_name,
            call_counts
        ));
        Arc::new(Mutex::new(pl))
    };

    let get_key_iter = || iter_lines_from_dir(&in_dir, get_pl(false));
    let get_par_key_iter = || par_iter_lines_from_dir(&in_dir, get_pl(true));

    *len.lock().unwrap() = Some(get_par_key_iter().count());

    let keys = fmph::keyset::CachedKeySet::<Item, _>::dynamic(
        GetParallelLineIterator {
            len: len.lock().unwrap().unwrap(),
            get_key_iter: &get_key_iter,
            get_par_key_iter: &get_par_key_iter,
        },
        clone_threshold,
    );
    //let keys = fmph::keyset::CachedKeySet::dynamic(&get_key_iter, clone_threshold);
    let mph = fmph::Function::with_conf(keys, conf);

    let mut file =
        File::create(&out_mph).with_context(|| format!("Cannot create {}", out_mph.display()))?;
    mph.write(&mut file).context("Could not write MPH file")?;

    Ok(())
}
