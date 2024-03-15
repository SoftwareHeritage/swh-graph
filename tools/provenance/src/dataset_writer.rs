// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::cell::{RefCell, RefMut};
use std::fs::File;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, Result};
use thread_local::ThreadLocal;

pub struct ParallelDatasetWriter<W: SequentialDatasetWriter + Send> {
    num_files: AtomicU64,
    path: PathBuf,
    writers: ThreadLocal<RefCell<W>>,
}

impl<W: SequentialDatasetWriter + Send> ParallelDatasetWriter<W> {
    pub fn new(path: PathBuf) -> Result<Self> {
        std::fs::create_dir_all(&path)
            .with_context(|| format!("Could not create {}", path.display()))?;
        Ok(ParallelDatasetWriter {
            num_files: AtomicU64::new(0),
            path,
            writers: ThreadLocal::new(),
        })
    }

    fn get_new_seq_writer(&self) -> Result<RefCell<W>> {
        let path = self.path.join(format!(
            "{}{}",
            self.num_files.fetch_add(1, Ordering::Relaxed),
            W::EXTENSION,
        ));
        Ok(RefCell::new(W::new(path)?))
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
}

pub trait SequentialDatasetWriter {
    const EXTENSION: &'static str;

    fn new(path: PathBuf) -> Result<Self>
    where
        Self: Sized;
}

pub type SequentialCsvZstDatasetWriter<'a> = csv::Writer<zstd::stream::AutoFinishEncoder<'a, File>>;

impl<'a> SequentialDatasetWriter for SequentialCsvZstDatasetWriter<'a> {
    const EXTENSION: &'static str = ".csv.zst";

    fn new(path: PathBuf) -> Result<Self> {
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
}
