// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::cell::RefCell;
use std::fs::File;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, Result};

pub struct CsvZstDataset {
    num_files: AtomicU64,
    path: PathBuf,
}

impl CsvZstDataset {
    pub fn new(path: PathBuf) -> Result<Self> {
        std::fs::create_dir_all(&path)
            .with_context(|| format!("Could not create {}", path.display()))?;
        Ok(CsvZstDataset {
            num_files: AtomicU64::new(0),
            path,
        })
    }

    pub fn get_new_writer(
        &self,
    ) -> Result<RefCell<csv::Writer<zstd::stream::AutoFinishEncoder<File>>>> {
        let path = self.path.join(format!(
            "{}.csv.zst",
            self.num_files.fetch_add(1, Ordering::Relaxed)
        ));
        let file =
            File::create(&path).with_context(|| format!("Could not create {}", path.display()))?;
        let compression_level = 3;
        let zstd_encoder = zstd::stream::write::Encoder::new(file, compression_level)
            .with_context(|| format!("Could not create ZSTD encoder for {}", path.display()))?
            .auto_finish();
        Ok(RefCell::new(
            csv::WriterBuilder::new()
                .has_headers(true)
                .terminator(csv::Terminator::CRLF)
                .from_writer(zstd_encoder),
        ))
    }
}
