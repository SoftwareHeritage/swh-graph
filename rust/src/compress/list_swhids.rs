/*
 * Copyright (C) 2023  The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

//! Iterators on newline-separated ZSTD-compressed file containing textual SWHIDs.

use dsi_progress_logger::ProgressLogger;
use rayon::prelude::*;
use std::io::BufRead;
use std::path::Path;
use std::sync::Arc;
use std::sync::Mutex;

/// Yields textual swhids from a newline-separated ZSTD-compressed file
pub fn iter_swhids_from_file<'a>(
    path: &Path,
    pl: Arc<Mutex<ProgressLogger<'a>>>,
) -> impl Iterator<Item = [u8; 50]> + 'a {
    std::io::BufReader::new(
        zstd::stream::read::Decoder::new(
            std::fs::File::open(&path).unwrap_or_else(|e| {
                panic!("Could not open {} for reading: {:?}", path.display(), e)
            }),
        )
        .unwrap_or_else(|e| panic!("{} is not a ZSTD file: {:?}", path.display(), e)),
    )
    .lines()
    .enumerate()
    .map(move |(i, line)| {
        if i % 32768 == 0 {
            // Update but avoid lock contention at the expense
            // of precision (counts at most 32768 too many at the
            // end of each file)
            pl.lock().unwrap().update_with_count(32768);
        }
        let line: [u8; 50] = line
            .as_ref()
            .unwrap_or_else(|_| panic!("Could not parse swhid {:?}", &line))
            .as_bytes()
            .try_into()
            .unwrap_or_else(|_| panic!("Could not parse swhid {:?}", &line));
        line
    })
}

/// Yields textual swhids from a directory of newline-separated ZSTD-compressed files
pub fn iter_swhids_from_dir<'a>(
    path: &'a Path,
    pl: Arc<Mutex<ProgressLogger<'a>>>,
) -> impl Iterator<Item = [u8; 50]> + 'a {
    std::fs::read_dir(path)
        .unwrap_or_else(|e| panic!("Could not list {}: {:?}", path.display(), e))
        .flat_map(move |swhids_path| {
            iter_swhids_from_file(
                swhids_path
                    .as_ref()
                    .unwrap_or_else(|e| panic!("Could not read {} entry: {:?}", path.display(), e))
                    .path()
                    .as_path(),
                pl.clone(),
            )
        })
}

/// Yields textual swhids from a directory of newline-separated ZSTD-compressed files
pub fn par_iter_swhids_from_dir<'a>(
    path: &'a Path,
    pl: Arc<Mutex<ProgressLogger<'a>>>,
) -> impl ParallelIterator<Item = [u8; 50]> + 'a {
    std::fs::read_dir(path)
        .unwrap_or_else(|e| panic!("Could not list {}: {:?}", path.display(), e))
        .par_bridge()
        .flat_map_iter(move |swhids_path| {
            iter_swhids_from_file(
                swhids_path
                    .as_ref()
                    .unwrap_or_else(|e| panic!("Could not read {} entry: {:?}", path.display(), e))
                    .path()
                    .as_path(),
                pl.clone(),
            )
        })
}

pub struct GetParallelSwhidIterator<
    'a,
    I: Iterator<Item = [u8; 50]>,
    PI: ParallelIterator<Item = [u8; 50]>,
    GI: Fn() -> I,
    GPI: Fn() -> PI,
> {
    pub len: usize,
    pub get_key_iter: &'a GI,
    pub get_par_key_iter: &'a GPI,
}

impl<
        'a,
        I: Iterator<Item = [u8; 50]>,
        PI: ParallelIterator<Item = [u8; 50]>,
        GI: Fn() -> I,
        GPI: Fn() -> PI,
    > ph::fmph::keyset::GetIterator for GetParallelSwhidIterator<'a, I, PI, GI, GPI>
{
    type Item = [u8; 50];
    type Iterator = I;
    type ParallelIterator = PI;

    fn iter(&self) -> Self::Iterator {
        (self.get_key_iter)()
    }
    fn par_iter(&self) -> Option<Self::ParallelIterator> {
        Some((self.get_par_key_iter)())
    }
    fn has_par_iter(&self) -> bool {
        true
    }
    fn len(&self) -> usize {
        self.len
    }
}
