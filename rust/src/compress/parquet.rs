// Copyright (C) 2023-2026  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

//! Readers for the Parquet dataset.
use std::path::Path;

use anyhow::{Context, Result};
use ar_row::deserialize::{ArRowDeserialize, ArRowStruct};
use arrow::array::RecordBatchReader;
use arrow::datatypes::DataType;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ProjectionMask;
use parquet::schema::types::SchemaDescriptor;
use rayon::prelude::*;

use super::ExportTableReader;

/// Based on [`super::orc::ORC_BATCH_SIZE`]
pub(crate) const PARQUET_BATCH_SIZE: usize = 1024;

fn projection_mask<T: ArRowStruct>(schema_descriptor: &SchemaDescriptor) -> Result<ProjectionMask> {
    let field_names = <T>::columns();

    Ok(ProjectionMask::columns(
        schema_descriptor,
        field_names.iter().map(AsRef::as_ref),
    ))
}

pub struct ParquetTableReader(ParquetRecordBatchReaderBuilder<std::fs::File>);

impl ExportTableReader for ParquetTableReader {
    fn new<P: AsRef<Path>>(dataset_dir: P, subdirectory: &str) -> Result<Vec<Self>> {
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
                let builder = ParquetRecordBatchReaderBuilder::try_new(file)
                    .with_context(|| format!("Could not read {}", file_path.display()))?;
                Ok(Self(builder))
            })
            .collect()
    }

    fn iter<T, IntoIterU, U, F>(self, mut f: F) -> impl Iterator<Item = U>
    where
        F: FnMut(T) -> IntoIterU,
        IntoIterU: IntoIterator<Item = U>,
        T: ArRowDeserialize + ArRowStruct,
    {
        let Self(reader_builder) = self;

        let projection = projection_mask::<T>(reader_builder.parquet_schema())
            .expect("Could not build ProjectionMask");

        let reader_builder = reader_builder
            .with_projection(projection)
            .with_batch_size(PARQUET_BATCH_SIZE);

        let reader = reader_builder
            .build()
            .expect("Could not build Parquet reader");

        T::check_datatype(&DataType::Struct(reader.schema().fields().clone()))
            .expect("Invalid data type in Parquet file");

        reader.flat_map(move |chunk| {
            let chunk: arrow_array::RecordBatch =
                chunk.unwrap_or_else(|e| panic!("Could not read chunk: {e}"));
            let items: Vec<T> =
                T::from_record_batch(chunk).expect("Could not deserialize from arrow");
            items.into_iter().flat_map(&mut f).collect::<Vec<_>>()
        })
    }

    fn par_iter<T, IntoIterU, U: Send, F>(self, f: F) -> impl ParallelIterator<Item = U>
    where
        F: Fn(T) -> IntoIterU + Send + Sync,
        IntoIterU: IntoIterator<Item = U> + Send + Sync,
        T: ArRowDeserialize + ArRowStruct + Send,
    {
        let Self(reader_builder) = self;

        let projection = projection_mask::<T>(reader_builder.parquet_schema())
            .expect("Could not build ProjectionMask");

        let reader_builder = reader_builder
            .with_projection(projection)
            .with_batch_size(PARQUET_BATCH_SIZE);

        let reader = reader_builder
            .build()
            .expect("Could not build Parquet reader");

        T::check_datatype(&DataType::Struct(reader.schema().fields().clone()))
            .expect("Invalid data type in Parquet file");

        reader.par_bridge().flat_map_iter(move |chunk| {
            let chunk: arrow_array::RecordBatch =
                chunk.unwrap_or_else(|e| panic!("Could not read chunk: {e}"));
            let items: Vec<T> =
                T::from_record_batch(chunk).expect("Could not deserialize from arrow");
            items.into_iter().flat_map(&f).collect::<Vec<_>>()
        })
    }

    fn count_rows(self) -> u64 {
        let Self(reader_builder) = self;
        reader_builder
            .metadata()
            .row_groups()
            .iter()
            .map(|row_group| u64::try_from(row_group.num_rows()).expect("Negative number of rows"))
            .sum()
    }
}
