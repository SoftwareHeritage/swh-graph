// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::cell::{RefCell, RefMut};
use std::fs::File;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, Result};
use arrow::array::StructArray;
use rayon::prelude::*;
use thread_local::ThreadLocal;

#[cfg(feature = "ipc")]
mod ipc;
#[cfg(feature = "ipc")]
pub use ipc::*;

mod parquet;
pub use parquet::*;

pub trait StructArrayBuilder {
    fn len(&self) -> usize;
    fn finish(self) -> Result<StructArray>;
}

/// Writes a set of files (called tables here) to a directory.
pub struct ParallelDatasetWriter<W: TableWriter + Send> {
    num_files: AtomicU64,
    schema: W::Schema,
    path: PathBuf,
    writers: ThreadLocal<RefCell<W>>,
    /// See [`ParquetTableWriter::flush_threshold`]
    pub flush_threshold: Option<usize>,
}

impl<W: TableWriter<Schema = ()> + Send> ParallelDatasetWriter<W> {
    pub fn new(path: PathBuf) -> Result<Self> {
        std::fs::create_dir_all(&path)
            .with_context(|| format!("Could not create {}", path.display()))?;
        Ok(ParallelDatasetWriter {
            num_files: AtomicU64::new(0),
            schema: (),
            path,
            writers: ThreadLocal::new(),
            flush_threshold: None,
        })
    }
}

impl<W: TableWriter + Send> ParallelDatasetWriter<W> {
    pub fn new_with_schema(path: PathBuf, schema: W::Schema) -> Result<Self> {
        std::fs::create_dir_all(&path)
            .with_context(|| format!("Could not create {}", path.display()))?;
        Ok(ParallelDatasetWriter {
            num_files: AtomicU64::new(0),
            schema,
            path,
            writers: ThreadLocal::new(),
            flush_threshold: None,
        })
    }

    fn get_new_seq_writer(&self) -> Result<RefCell<W>> {
        let path = self.path.join(format!(
            "{}{}",
            self.num_files.fetch_add(1, Ordering::Relaxed),
            W::EXTENSION,
        ));
        Ok(RefCell::new(W::new(
            path,
            self.schema.clone(),
            self.flush_threshold,
        )?))
    }

    /// Returns a new sequential writer.
    ///
    /// # Panics
    ///
    /// When called from a thread holding another reference to a sequential writer
    /// of this dataset.
    pub fn get_thread_writer(&self) -> Result<RefMut<W>> {
        self.writers
            .get_or_try(|| self.get_new_seq_writer())
            .map(|writer| writer.borrow_mut())
    }

    /// Flushes all underlying writers
    pub fn flush(&mut self) -> Result<()> {
        self.writers
            .iter_mut()
            .collect::<Vec<_>>()
            .into_par_iter()
            .map(|writer| writer.get_mut().flush())
            .collect::<Result<Vec<()>>>()
            .map(|_: Vec<()>| ())
    }

    /// Closes all underlying writers
    pub fn close(mut self) -> Result<Vec<W::CloseResult>> {
        let mut tmp = ThreadLocal::new();
        std::mem::swap(&mut tmp, &mut self.writers);
        tmp.into_iter()
            .collect::<Vec<_>>()
            .into_par_iter()
            .map(|writer| writer.into_inner().close())
            .collect()
    }
}

impl<W: TableWriter + Send> Drop for ParallelDatasetWriter<W> {
    fn drop(&mut self) {
        let mut tmp = ThreadLocal::new();
        std::mem::swap(&mut tmp, &mut self.writers);
        tmp.into_iter()
            .collect::<Vec<_>>()
            .into_par_iter()
            .try_for_each(|writer| writer.into_inner().close().map(|_| ()))
            .expect("Could not close ParallelDatasetWriter");
    }
}

pub trait TableWriter {
    const EXTENSION: &'static str;
    type Schema: Clone;
    type CloseResult: Send;

    fn new(path: PathBuf, schema: Self::Schema, flush_threshold: Option<usize>) -> Result<Self>
    where
        Self: Sized;

    /// Calls `.into()` on the internal builder, and writes its result to disk.
    fn flush(&mut self) -> Result<()>;

    fn close(self) -> Result<Self::CloseResult>;
}

pub type CsvZstTableWriter<'a> = csv::Writer<zstd::stream::AutoFinishEncoder<'a, File>>;

impl<'a> TableWriter for CsvZstTableWriter<'a> {
    const EXTENSION: &'static str = ".csv.zst";
    type Schema = ();
    type CloseResult = ();

    fn new(path: PathBuf, _schema: Self::Schema, _flush_threshold: Option<usize>) -> Result<Self> {
        let file =
            File::create(&path).with_context(|| format!("Could not create {}", path.display()))?;
        let compression_level = 3;
        let zstd_encoder = zstd::stream::write::Encoder::new(file, compression_level)
            .with_context(|| format!("Could not create ZSTD encoder for {}", path.display()))?
            .auto_finish();
        Ok(csv::WriterBuilder::new()
            .has_headers(true)
            .terminator(csv::Terminator::CRLF)
            .from_writer(zstd_encoder))
    }

    fn flush(&mut self) -> Result<()> {
        self.flush().context("Could not flush CsvZst writer")
    }

    fn close(mut self) -> Result<()> {
        self.flush().context("Could not close CsvZst writer")
    }
}
