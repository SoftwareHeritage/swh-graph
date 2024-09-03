/*
 * Copyright (C) 2024  The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

//! Parallel sorting and deduplication for lists of SWHIDS that don't fit in RAM

use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;

use anyhow::{ensure, Context, Result};
use dsi_progress_logger::ProgressLog;
use mmap_rs::MmapFlags;
use rayon::prelude::*;
use rdst::{RadixKey, RadixSort};

use super::ParallelDeduplicatingExternalSorter;
use crate::SWHID;

#[derive(Copy, Clone)]
struct SwhidExternalSorter {
    buffer_size: usize,
}

impl ParallelDeduplicatingExternalSorter<SWHID> for SwhidExternalSorter {
    fn buffer_capacity(&self) -> usize {
        self.buffer_size.div_ceil(SWHID::LEVELS).next_power_of_two()
    }

    fn sort_vec(&self, vec: &mut Vec<SWHID>) -> Result<()> {
        vec.radix_sort_unstable();
        Ok(())
    }

    fn serialize(path: PathBuf, swhids: impl Iterator<Item = SWHID>) -> Result<()> {
        let file = File::create_new(path)
            .context("Could not create sorted file in temporary directory")?;
        let mut writer = BufWriter::new(file);
        for swhid in swhids {
            writer
                .write_all(&<[u8; SWHID::BYTES_SIZE]>::from(swhid))
                .context("Could not write SWHID")?;
        }
        writer.flush().context("Could not flush sorted file")?;
        Ok(())
    }

    fn deserialize(path: PathBuf) -> Result<impl Iterator<Item = SWHID>> {
        let file_len = path
            .metadata()
            .with_context(|| format!("Could not stat {}", path.display()))?
            .len();
        ensure!(
            file_len % (SWHID::BYTES_SIZE as u64) == 0,
            "File size is not a multiple of a SWHID's binary size"
        );
        log::debug!("Reading {} bytes from {}", file_len, path.display());
        let num_swhids = (file_len / (SWHID::BYTES_SIZE as u64)) as usize;
        let file = std::fs::File::open(&path)
            .with_context(|| format!("Could not open {}", path.display()))?;
        let data = unsafe {
            mmap_rs::MmapOptions::new(file_len as _)
                .context("Could not initialize mmap")?
                .with_flags(MmapFlags::TRANSPARENT_HUGE_PAGES | MmapFlags::SEQUENTIAL)
                .with_file(&file, 0)
                .map()
                .with_context(|| format!("Could not mmap {}", path.display()))?
        };
        Ok((0..num_swhids).map(move |i| {
            let buf = &data[i * SWHID::BYTES_SIZE..(i + 1) * SWHID::BYTES_SIZE];
            let buf = <[u8; SWHID::BYTES_SIZE]>::try_from(buf).unwrap();
            SWHID::try_from(buf).unwrap()
        }))
    }
}

pub fn par_sort_swhids<Iter: ParallelIterator<Item = SWHID>>(
    iter: Iter,
    pl: impl ProgressLog + Send,
    buffer_size: usize,
) -> Result<impl Iterator<Item = SWHID>> {
    SwhidExternalSorter { buffer_size }.par_sort_dedup(iter, pl)
}
