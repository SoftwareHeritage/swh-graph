// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::fs::File;
use std::path::PathBuf;

use anyhow::{Context, Result};

use arrow::datatypes::Schema;
use arrow::ipc::writer::FileWriter;

use super::{StructArrayBuilder, TableWriter};

/// Writer to a .arrow file, usable with [`ParallelDatasetWriter`]
///
/// `Builder` should follow the pattern documented by
/// [`arrow::builder`](https://docs.rs/arrow/latest/arrow/array/builder/index.html)
pub struct ArrowTableWriter<Builder: Default + StructArrayBuilder> {
    path: PathBuf,
    file_writer: FileWriter<File>,
    builder: Builder,
    pub flush_threshold: usize,
}

impl<Builder: Default + StructArrayBuilder> TableWriter for ArrowTableWriter<Builder> {
    const EXTENSION: &'static str = ".arrow";
    type Schema = Schema;
    type CloseResult = ();

    fn new(path: PathBuf, schema: Self::Schema, flush_threshold: Option<usize>) -> Result<Self> {
        let file =
            File::create(&path).with_context(|| format!("Could not create {}", path.display()))?;
        let file_writer = FileWriter::try_new(file, &schema).with_context(|| {
            format!(
                "Could not create writer for {} with schema {}",
                path.display(),
                schema
            )
        })?;

        Ok(ArrowTableWriter {
            path,
            file_writer,
            flush_threshold: flush_threshold.unwrap_or(1024 * 1024), // Arbitrary
            builder: Builder::default(),
        })
    }

    fn flush(&mut self) -> Result<()> {
        let mut tmp = Builder::default();
        std::mem::swap(&mut tmp, &mut self.builder);
        let struct_array = tmp.finish()?;
        self.file_writer
            .write(&struct_array.into())
            .with_context(|| format!("Could not write to {}", self.path.display()))
    }

    fn close(mut self) -> Result<()> {
        self.flush()?;
        self.file_writer
            .finish()
            .with_context(|| format!("Could not close {}", self.path.display()))
    }
}

impl<Builder: Default + StructArrayBuilder> ArrowTableWriter<Builder> {
    /// Flushes the internal buffer is too large, then returns the array builder.
    pub fn builder(&mut self) -> Result<&mut Builder> {
        if self.builder.len() >= self.flush_threshold {
            self.flush()?;
        }

        Ok(&mut self.builder)
    }
}

impl<Builder: Default + StructArrayBuilder> Drop for ArrowTableWriter<Builder> {
    fn drop(&mut self) {
        self.flush().unwrap();
        self.file_writer
            .finish()
            .with_context(|| format!("Could not close {}", self.path.display()))
            .unwrap();
    }
}
