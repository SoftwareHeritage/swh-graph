// Copyright (C) 2023-2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

//! Iterator on the set of all origin URLs in an ORC dataset
use std::path::PathBuf;

use anyhow::Result;
use ar_row_derive::ArRowDeserialize;
use rayon::prelude::*;

use super::orc::{get_dataset_readers, par_iter_arrow};

pub fn iter_origins(
    dataset_dir: &PathBuf,
) -> Result<impl ParallelIterator<Item = (String, String)>> {
    #[derive(ArRowDeserialize, Default, Clone)]
    struct Origin {
        id: String,
        url: String,
    }

    Ok(get_dataset_readers(dataset_dir, "origin")?
        .into_par_iter()
        .flat_map(|reader_builder| {
            par_iter_arrow(reader_builder, |ori: Origin| {
                [(ori.url, format!("swh:1:ori:{}", ori.id))]
            })
        }))
}
