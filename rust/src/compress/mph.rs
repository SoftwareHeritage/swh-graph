// Copyright (C) 2023  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::fs::File;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use dsi_progress_logger::ProgressLogger;
use ph::fmph;
use rayon::prelude::*;

use crate::compress::zst_dir::*;

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
        let mut pl = ProgressLogger::default().display_memory();
        pl.item_name = item_name;
        pl.local_speed = true;
        pl.expected_updates = *len.lock().unwrap();
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
