// Copyright (C) 2023-2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

//! Readers for the ORC dataset.
use std::path::Path;

use anyhow::{Context, Result};
use ar_row::deserialize::{ArRowDeserialize, ArRowStruct};
use orc_rust::arrow_reader::ArrowReaderBuilder;
use orc_rust::projection::ProjectionMask;
use orc_rust::reader::ChunkReader;
use rayon::prelude::*;

pub(crate) const ORC_BATCH_SIZE: usize = 1024;
/// The value was computed experimentally to minimize both run time and memory,
/// by running `swh-graph-extract extract-nodes` on the 2023-09-06 dataset,
/// on Software Heritage's Maxxi computer (Xeon Gold 6342 CPU @ 2.80GHz,
/// 96 threads, 4TB RAM)

pub(crate) fn get_dataset_readers<P: AsRef<Path>>(
    dataset_dir: P,
    subdirectory: &str,
) -> Result<Vec<ArrowReaderBuilder<std::fs::File>>> {
    let mut dataset_dir = dataset_dir.as_ref().to_owned();
    dataset_dir.push(subdirectory);
    std::fs::read_dir(&dataset_dir)
        .with_context(|| format!("Could not list {}", dataset_dir.display()))?
        .map(|file_path| {
            let file_path = file_path
                .with_context(|| format!("Failed to list {}", dataset_dir.display()))?
                .path();
            let file = std::fs::File::open(&file_path)
                .with_context(|| format!("Could not open {}", file_path.display()))?;
            let builder = ArrowReaderBuilder::try_new(file)
                .with_context(|| format!("Could not read {}", file_path.display()))?;
            Ok(builder)
        })
        .collect()
}

pub(crate) fn iter_arrow<R: ChunkReader, T, IntoIterU, U, F>(
    reader_builder: ArrowReaderBuilder<R>,
    mut f: F,
) -> impl Iterator<Item = U>
where
    F: FnMut(T) -> IntoIterU,
    IntoIterU: IntoIterator<Item = U>,
    T: ArRowDeserialize + ArRowStruct,
{
    let field_names = <T>::columns();
    let projection = ProjectionMask::named_roots(
        reader_builder.file_metadata().root_data_type(),
        field_names.as_slice(),
    );
    let reader = reader_builder
        .with_projection(projection)
        .with_batch_size(ORC_BATCH_SIZE)
        .build();

    reader.flat_map(move |chunk| {
        let chunk: arrow_array::RecordBatch =
            chunk.unwrap_or_else(|e| panic!("Could not read chunk: {}", e));
        let items: Vec<T> = T::from_record_batch(chunk).expect("Could not deserialize from arrow");
        items.into_iter().flat_map(&mut f).collect::<Vec<_>>()
    })
}

pub(crate) fn par_iter_arrow<R: ChunkReader + Send, T, IntoIterU, U: Send, F>(
    reader_builder: ArrowReaderBuilder<R>,
    f: F,
) -> impl ParallelIterator<Item = U>
where
    F: Fn(T) -> IntoIterU + Send + Sync,
    IntoIterU: IntoIterator<Item = U> + Send + Sync,
    T: ArRowDeserialize + ArRowStruct + Send,
{
    let field_names = <T>::columns();
    let projection = ProjectionMask::named_roots(
        reader_builder.file_metadata().root_data_type(),
        field_names.as_slice(),
    );
    let reader = reader_builder
        .with_projection(projection)
        .with_batch_size(ORC_BATCH_SIZE)
        .build();

    reader.par_bridge().flat_map_iter(move |chunk| {
        let chunk: arrow_array::RecordBatch =
            chunk.unwrap_or_else(|e| panic!("Could not read chunk: {}", e));
        let items: Vec<T> = T::from_record_batch(chunk).expect("Could not deserialize from arrow");
        items.into_iter().flat_map(&f).collect::<Vec<_>>()
    })
}
