// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};

use arrow::datatypes::Schema;
use parquet::arrow::ArrowWriter as ParquetWriter;
use parquet::file::properties::WriterProperties;
use parquet::format::FileMetaData;

use super::{StructArrayBuilder, TableWriter};

/// Writer to a .parquet file, usable with [`ParallelDatasetWriter`]
///
/// `Builder` should follow the pattern documented by
/// [`arrow::builder`](https://docs.rs/arrow/latest/arrow/array/builder/index.html)
pub struct ParquetTableWriter<Builder: Default + StructArrayBuilder> {
    path: PathBuf,
    /// Automatically flushes the builder to disk when it length reaches the value.
    /// To avoid uneven row group sizes, this value plus the number of values added
    /// to the builder between calls to [`Self::builder`] should be either equal or
    /// equal to [`max_row_group_size`](WriterProperties::max_row_group_size)
    /// (or a multiple of it).
    pub flush_threshold: usize,
    file_writer: Option<ParquetWriter<File>>, // None only between .close() call and Drop
    builder: Builder,
}

impl<Builder: Default + StructArrayBuilder> TableWriter for ParquetTableWriter<Builder> {
    const EXTENSION: &'static str = ".parquet";
    type Schema = (Arc<Schema>, WriterProperties);
    type CloseResult = FileMetaData;

    fn new(
        path: PathBuf,
        (schema, properties): Self::Schema,
        flush_threshold: Option<usize>,
    ) -> Result<Self> {
        let file =
            File::create(&path).with_context(|| format!("Could not create {}", path.display()))?;
        let file_writer = ParquetWriter::try_new(file, schema.clone(), Some(properties.clone()))
            .with_context(|| {
                format!(
                    "Could not create writer for {} with schema {} and properties {:?}",
                    path.display(),
                    schema,
                    properties.clone()
                )
            })?;

        Ok(ParquetTableWriter {
            path,
            // See above, we need to make sure the user does not write more than
            // `properties.max_row_group_size()` minus `flush_threshold` rows between
            // two calls to self.builder() to avoid uneven group sizes. This seems
            // like a safe ratio.
            flush_threshold: flush_threshold.unwrap_or(properties.max_row_group_size() * 9 / 10),
            file_writer: Some(file_writer),
            builder: Builder::default(),
        })
    }

    fn flush(&mut self) -> Result<()> {
        // Get built array
        let mut tmp = Builder::default();
        std::mem::swap(&mut tmp, &mut self.builder);
        let struct_array = tmp.finish()?;

        let file_writer = self
            .file_writer
            .as_mut()
            .expect("File writer is unexpectedly None");

        // Write it
        file_writer
            .write(&struct_array.into())
            .with_context(|| format!("Could not write to {}", self.path.display()))?;
        file_writer
            .flush()
            .with_context(|| format!("Could not flush to {}", self.path.display()))
    }

    fn close(mut self) -> Result<FileMetaData> {
        self.flush()?;
        self.file_writer
            .take()
            .expect("File writer is unexpectedly None")
            .close()
            .with_context(|| format!("Could not close {}", self.path.display()))
    }
}

impl<Builder: Default + StructArrayBuilder> ParquetTableWriter<Builder> {
    /// Flushes the internal buffer is too large, then returns the array builder.
    pub fn builder(&mut self) -> Result<&mut Builder> {
        if self.builder.len() >= self.flush_threshold {
            self.flush()?;
        }

        Ok(&mut self.builder)
    }
}

impl<Builder: Default + StructArrayBuilder> Drop for ParquetTableWriter<Builder> {
    fn drop(&mut self) {
        if self.file_writer.is_some() {
            self.flush().unwrap();
            self.file_writer
                .take()
                .unwrap()
                .close()
                .with_context(|| format!("Could not close {}", self.path.display()))
                .unwrap();
        }
    }
}
