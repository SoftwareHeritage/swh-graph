// Copyright (C) 2025 The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

//! Iterator on the set of all person ids in an ORC dataset
use std::path::PathBuf;

use anyhow::Result;
use ar_row::deserialize::{ArRowDeserialize, ArRowStruct};
use ar_row_derive::ArRowDeserialize;
use orc_rust::arrow_reader::ArrowReaderBuilder;
use orc_rust::reader::ChunkReader;
use rayon::prelude::*;

use super::orc::{get_dataset_readers, par_iter_arrow};

type ExportedFullname = (Box<[u8]>, Box<[u8]>);

pub fn iter_fullnames(
    dataset_dir: &PathBuf,
    subdirectory: &str,
) -> Result<impl ParallelIterator<Item = ExportedFullname>> {
    let map_get_dataset_readers =
        |dataset_dir, subdirectory| get_dataset_readers(dataset_dir, subdirectory);

    Ok([].into_par_iter().chain(
        map_get_dataset_readers(dataset_dir, subdirectory)?
            .into_par_iter()
            .flat_map(iter_fullnames_from_file),
    ))
}

fn map_fullnames<T, F, R: ChunkReader + Send>(
    reader_builder: ArrowReaderBuilder<R>,
    f: F,
) -> impl ParallelIterator<Item = ExportedFullname>
where
    F: Fn(T) -> Vec<ExportedFullname> + Send + Sync,
    T: ArRowDeserialize + ArRowStruct + Send,
{
    par_iter_arrow(reader_builder, f)
}

fn iter_fullnames_from_file<R: ChunkReader + Send>(
    reader_builder: ArrowReaderBuilder<R>,
) -> impl ParallelIterator<Item = ExportedFullname> {
    #[derive(ArRowDeserialize, Default, Clone)]
    struct Row {
        fullname: Box<[u8]>,
        sha256_fullname: Box<[u8]>,
    }

    map_fullnames(reader_builder, |row: Row| {
        vec![(row.fullname, row.sha256_fullname)]
    })
}
