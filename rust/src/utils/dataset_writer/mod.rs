// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::cell::{RefCell, RefMut};
use std::fs::File;
use std::num::NonZeroU16;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{ensure, Context, Result};
#[cfg(feature = "arrow")]
use arrow::array::StructArray;
use rayon::prelude::*;
use thread_local::ThreadLocal;

#[cfg(feature = "arrow-ipc")]
mod ipc;
#[cfg(feature = "arrow-ipc")]
pub use ipc::*;

#[cfg(feature = "parquet")]
mod parquet;
#[cfg(feature = "parquet")]
pub use parquet::*;

#[cfg(feature = "arrow")]
#[allow(clippy::len_without_is_empty)]
pub trait StructArrayBuilder {
    /// Number of rows currently in the buffer (not capacity)
    fn len(&self) -> usize;
    /// Number of bytes currently in the buffer (not capacity)
    fn buffer_size(&self) -> usize;
    fn finish(self) -> Result<StructArray>;
}

/// Writes a set of files (called tables here) to a directory.
pub struct ParallelDatasetWriter<W: TableWriter + Send> {
    num_files: AtomicU64,
    schema: W::Schema,
    path: PathBuf,
    writers: ThreadLocal<RefCell<W>>,
    pub config: W::Config,
}

impl<W: TableWriter<Schema = (), Config = ()> + Send> ParallelDatasetWriter<W> {
    pub fn new(path: PathBuf) -> Result<Self> {
        std::fs::create_dir_all(&path)
            .with_context(|| format!("Could not create {}", path.display()))?;
        Ok(ParallelDatasetWriter {
            num_files: AtomicU64::new(0),
            schema: (),
            path,
            writers: ThreadLocal::new(),
            config: (),
        })
    }
}

impl<W: TableWriter + Send> ParallelDatasetWriter<W>
where
    W::Config: Default,
{
    pub fn with_schema(path: PathBuf, schema: W::Schema) -> Result<Self> {
        std::fs::create_dir_all(&path)
            .with_context(|| format!("Could not create {}", path.display()))?;
        Ok(ParallelDatasetWriter {
            num_files: AtomicU64::new(0),
            schema,
            path,
            writers: ThreadLocal::new(),
            config: W::Config::default(),
        })
    }

    fn get_new_seq_writer(&self) -> Result<RefCell<W>> {
        let path = self
            .path
            .join(self.num_files.fetch_add(1, Ordering::Relaxed).to_string());
        Ok(RefCell::new(W::new(
            path,
            self.schema.clone(),
            self.config.clone(),
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
    type Schema: Clone;
    type CloseResult: Send;
    type Config: Clone;

    fn new(path: PathBuf, schema: Self::Schema, config: Self::Config) -> Result<Self>
    where
        Self: Sized;

    /// Calls `.into()` on the internal builder, and writes its result to disk.
    fn flush(&mut self) -> Result<()>;

    fn close(self) -> Result<Self::CloseResult>;
}

/// Wraps `N` [`TableWriter`] in such a way that they each write to `base/0/x.parquet`,
/// ..., `base/N-1/x.parquet` instead of `base/x.parquet`.
///
/// This allows Hive partitioning while writing with multiple threads (`x` is the
/// thread id in the example above).
///
/// If `num_partitions` is `None`, disables partitioning.
pub struct PartitionedTableWriter<PartitionWriter: TableWriter + Send> {
    partition_writers: Vec<PartitionWriter>,
}

impl<PartitionWriter: TableWriter + Send> TableWriter for PartitionedTableWriter<PartitionWriter> {
    /// `(partition_column, num_partitions, underlying_schema)`
    type Schema = (String, Option<NonZeroU16>, PartitionWriter::Schema);
    type CloseResult = Vec<PartitionWriter::CloseResult>;
    type Config = PartitionWriter::Config;

    fn new(
        mut path: PathBuf,
        (partition_column, num_partitions, schema): Self::Schema,
        config: Self::Config,
    ) -> Result<Self> {
        // Remove the last part of the path (the thread id), so we can insert the
        // partition number between the base path and the thread id.
        let thread_id = path.file_name().map(|p| p.to_owned());
        ensure!(
            path.pop(),
            "Unexpected root path for partitioned writer: {}",
            path.display()
        );
        let thread_id = thread_id.unwrap();
        Ok(PartitionedTableWriter {
            partition_writers: (0..num_partitions.map(NonZeroU16::get).unwrap_or(1))
                .map(|partition_id| {
                    let partition_path = if num_partitions.is_some() {
                        path.join(format!("{}={}", partition_column, partition_id))
                    } else {
                        // Partitioning disabled
                        path.to_owned()
                    };
                    std::fs::create_dir_all(&partition_path).with_context(|| {
                        format!("Could not create {}", partition_path.display())
                    })?;
                    PartitionWriter::new(
                        partition_path.join(&thread_id),
                        schema.clone(),
                        config.clone(),
                    )
                })
                .collect::<Result<_>>()?,
        })
    }

    fn flush(&mut self) -> Result<()> {
        self.partition_writers
            .par_iter_mut()
            .try_for_each(|writer| writer.flush())
    }

    fn close(self) -> Result<Self::CloseResult> {
        self.partition_writers
            .into_par_iter()
            .map(|writer| writer.close())
            .collect()
    }
}

impl<PartitionWriter: TableWriter + Send> PartitionedTableWriter<PartitionWriter> {
    pub fn partitions(&mut self) -> &mut [PartitionWriter] {
        &mut self.partition_writers
    }
}

pub type CsvZstTableWriter<'a> = csv::Writer<zstd::stream::AutoFinishEncoder<'a, File>>;

impl<'a> TableWriter for CsvZstTableWriter<'a> {
    type Schema = ();
    type CloseResult = ();
    type Config = ();

    fn new(mut path: PathBuf, _schema: Self::Schema, _config: ()) -> Result<Self> {
        path.set_extension("csv.zst");
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
