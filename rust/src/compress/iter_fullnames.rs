// Copyright (C) 2025-2026  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

//! Iterator on the set of all person ids in an ORC dataset
use std::path::PathBuf;

use anyhow::Result;
use ar_row::deserialize::{ArRowDeserialize, ArRowStruct};
use ar_row_derive::ArRowDeserialize;

use rayon::prelude::*;

use super::ExportTableReader;

type ExportedFullname = (Box<[u8]>, Box<[u8]>);

pub fn iter_fullnames<R: ExportTableReader>(
    dataset_dir: &PathBuf,
    subdirectory: &str,
) -> Result<impl ParallelIterator<Item = ExportedFullname>> {
    let map_get_dataset_readers = |dataset_dir, subdirectory| R::new(dataset_dir, subdirectory);

    Ok([].into_par_iter().chain(
        map_get_dataset_readers(dataset_dir, subdirectory)?
            .into_par_iter()
            .flat_map(iter_fullnames_from_file),
    ))
}

fn map_fullnames<T, F, R: ExportTableReader>(
    reader: R,
    f: F,
) -> impl ParallelIterator<Item = ExportedFullname>
where
    F: Fn(T) -> Vec<ExportedFullname> + Send + Sync,
    T: ArRowDeserialize + ArRowStruct + Send,
{
    reader.par_iter(f)
}

fn iter_fullnames_from_file<R: ExportTableReader>(
    reader: R,
) -> impl ParallelIterator<Item = ExportedFullname> {
    #[derive(ArRowDeserialize, Default, Clone)]
    struct Row {
        fullname: Box<[u8]>,
        sha256_fullname: Box<[u8]>,
    }

    map_fullnames(reader, |row: Row| vec![(row.fullname, row.sha256_fullname)])
}
