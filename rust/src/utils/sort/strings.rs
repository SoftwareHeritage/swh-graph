/*
 * Copyright (C) 2024  The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

//! Parallel sorting and deduplication for lists of SWHIDS that don't fit in RAM
// Adapted from https://archive.softwareheritage.org/swh:1:cnt:d5129fef934309da995a8895ba9509a6faae0bba;origin=https://github.com/vigna/webgraph-rs;visit=swh:1:snp:76b76a6b68240ad1ec27aed81f7cc30441b69d7c;anchor=swh:1:rel:ef30092122d472899fdfa361e784fc1e04495dab;path=/src/utils/sort_pairs.rs;lines=410-512

use std::io::{Read, Write};
use std::path::PathBuf;

use anyhow::{ensure, Context, Result};
use dsi_progress_logger::ProgressLog;
use rayon::prelude::*;

use super::ParallelDeduplicatingExternalSorter;

/// arbitrary value to pick a buffer capacity
const AVERAGE_STRING_LENGTH: usize = 64;

type Bytestring = Box<[u8]>;

#[derive(Copy, Clone)]
struct BytestringExternalSorter {
    buffer_size: usize,
}

impl ParallelDeduplicatingExternalSorter<Bytestring> for BytestringExternalSorter {
    fn buffer_capacity(&self) -> usize {
        self.buffer_size
            .div_ceil(AVERAGE_STRING_LENGTH)
            .next_power_of_two()
    }

    #[allow(clippy::get_first)]
    fn sort_vec(&self, vec: &mut Vec<Bytestring>) -> Result<()> {
        // Perform a one-level radix sort before handing off to a generic sort.

        // Note: bucket distribution is uniform when called from ExtractPersons because
        // we manipulate sha256 digests; but is very heterogeneous when called from
        // ExtractLabels because labels are mostly ASCII text.

        let mut partitions: Vec<_> = (0..65536)
            .map(|_| Vec::with_capacity(vec.len().div_ceil(65536)))
            .collect();

        // Split into partitions
        for string in vec.drain(0..) {
            let partition_id = ((string.get(0).copied().unwrap_or(0u8) as usize) << 8)
                | string.get(1).copied().unwrap_or(0u8) as usize;
            partitions[partition_id].push(string);
        }

        // Sort each partition. We use a single-threaded sort for each partition
        // because we are already within a thread, and it would needlessly add churn to
        // Rayon's scheduler.
        partitions
            .par_iter_mut()
            .for_each(|partition| partition.sort_unstable());

        for partition in partitions {
            vec.extend(partition);
        }
        Ok(())
    }

    fn serialize(path: PathBuf, strings: impl Iterator<Item = Bytestring>) -> Result<()> {
        let file = std::fs::File::create_new(&path)
            .with_context(|| format!("Could not create {}", path.display()))?;
        let compression_level = 3;
        let mut encoder = zstd::stream::write::Encoder::new(file, compression_level)
            .with_context(|| format!("Could not create ZSTD encoder for {}", path.display()))?;
        for string in strings {
            let len: u32 = string
                .len()
                .try_into()
                .context("String is 2^32 bytes or longer")?;
            ensure!(len != u32::MAX, "String is 2^32 -1 bytes long");
            encoder
                .write_all(&len.to_ne_bytes())
                .with_context(|| format!("Could not write string to {}", path.display()))?;
            encoder
                .write_all(&string)
                .with_context(|| format!("Could not write string to {}", path.display()))?;
        }
        // mark end of file
        encoder
            .write_all(&u32::MAX.to_ne_bytes())
            .with_context(|| format!("Could not write string to {}", path.display()))?;

        encoder
            .finish()
            .with_context(|| format!("Could not flush to {}", path.display()))?;
        Ok(())
    }

    fn deserialize(path: PathBuf) -> Result<impl Iterator<Item = Bytestring>> {
        let file = std::fs::File::open(&path)
            .with_context(|| format!("Could not open {}", path.display()))?;
        let mut decoder =
            zstd::stream::read::Decoder::new(file).context("Could not decompress sorted file")?;
        Ok(std::iter::repeat(()).map_while(move |()| {
            let mut buf = [0u8; 4];
            decoder
                .read_exact(&mut buf)
                .expect("Could not read string size");
            let size = u32::from_ne_bytes(buf);
            if size == u32::MAX {
                // end of file marker
                return None;
            }
            let mut line = vec![0; size.try_into().unwrap()].into_boxed_slice();
            decoder
                .read_exact(&mut line)
                .expect("Could not read string");
            Some(line)
        }))
    }
}

pub fn par_sort_strings<Iter: ParallelIterator<Item = Bytestring>>(
    iter: Iter,
    pl: impl ProgressLog + Send,
    buffer_size: usize,
) -> Result<impl Iterator<Item = Bytestring>> {
    BytestringExternalSorter { buffer_size }.par_sort_dedup(iter, pl)
}
