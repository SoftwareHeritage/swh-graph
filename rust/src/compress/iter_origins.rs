// Copyright (C) 2023-2026  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

//! Iterator on the set of all origin URLs in an ORC dataset
use std::path::PathBuf;

use anyhow::Result;
use ar_row_derive::ArRowDeserialize;
use rayon::prelude::*;

use super::ExportTableReader;

pub fn iter_origins<R: ExportTableReader>(
    dataset_dir: &PathBuf,
) -> Result<impl ParallelIterator<Item = (String, String)>> {
    #[derive(ArRowDeserialize, Default, Clone)]
    struct Origin {
        id: String,
        url: String,
    }

    Ok(R::new(dataset_dir, "origin")?
        .into_par_iter()
        .flat_map(|reader| {
            reader.par_iter(|ori: Origin| [(ori.url, format!("swh:1:ori:{}", ori.id))])
        }))
}
