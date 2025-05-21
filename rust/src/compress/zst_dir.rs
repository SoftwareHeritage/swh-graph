/*
 * Copyright (C) 2023-2025  The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

//! Iterators on newline-separated ZSTD-compressed files.

use std::io::BufRead;
use std::path::Path;

use dsi_progress_logger::{ConcurrentProgressLog, ProgressLog};
use rayon::prelude::*;

// Inspired from https://archive.softwareheritage.org/swh:1:cnt:5c1d2d8f46cd47edf2adb15f5b7642098e03883f;origin=https://github.com/rust-lang/rust;visit=swh:1:snp:e93a6ff91a26c85dfe1d515afa437ab63e290357;anchor=swh:1:rev:c67cb3e577bdd4de640eb11d96cd5ef5afe0eb0b;path=/library/std/src/io/mod.rs;lines=2847-2871
pub struct ByteLines<B: std::io::BufRead> {
    buf: B,
}

impl<B: BufRead> Iterator for ByteLines<B> {
    type Item = std::io::Result<Vec<u8>>;

    fn next(&mut self) -> Option<std::io::Result<Vec<u8>>> {
        let mut buf = Vec::new();
        match self.buf.read_until(b'\n', &mut buf) {
            Ok(0) => None,
            Ok(_n) => {
                if buf.last() == Some(&b'\n') {
                    buf.pop();
                    if buf.last() == Some(&b'\r') {
                        buf.pop();
                    }
                }
                Some(Ok(buf))
            }
            Err(e) => Some(Err(e)),
        }
    }
}

pub trait ToByteLines: std::io::BufRead + Sized {
    fn byte_lines(self) -> ByteLines<Self> {
        ByteLines { buf: self }
    }
}

impl<B: std::io::BufRead> ToByteLines for B {}

/// Yields textual lines from a newline-separated ZSTD-compressed file
pub fn iter_lines_from_file<'a, Line>(
    path: &Path,
    mut pl: impl ProgressLog + 'a,
) -> impl Iterator<Item = Line> + 'a
where
    Line: TryFrom<Vec<u8>>,
    <Line as TryFrom<Vec<u8>>>::Error: std::fmt::Debug,
{
    std::io::BufReader::new(
        zstd::stream::read::Decoder::new(
            std::fs::File::open(path).unwrap_or_else(|e| {
                panic!("Could not open {} for reading: {:?}", path.display(), e)
            }),
        )
        .unwrap_or_else(|e| panic!("{} is not a ZSTD file: {:?}", path.display(), e)),
    )
    .byte_lines()
    .map(move |line| {
        pl.light_update();
        line.unwrap_or_else(|line| panic!("Could not parse swhid {:?}", &line))
            .try_into()
            .unwrap_or_else(|line| panic!("Could not parse swhid {:?}", &line))
    })
}

/// Yields textual swhids from a directory of newline-separated ZSTD-compressed files.
///
/// Files are read in alphabetical order of their name.
pub fn iter_lines_from_dir<'a, Line>(
    path: &'a Path,
    pl: impl ConcurrentProgressLog + 'a,
) -> impl Iterator<Item = Line> + 'a
where
    Line: TryFrom<Vec<u8>>,
    <Line as TryFrom<Vec<u8>>>::Error: std::fmt::Debug,
{
    let mut file_paths: Vec<_> = std::fs::read_dir(path)
        .unwrap_or_else(|e| panic!("Could not list {}: {:?}", path.display(), e))
        .map(|entry| {
            entry
                .as_ref()
                .unwrap_or_else(|e| panic!("Could not read {} entry: {:?}", path.display(), e))
                .path()
        })
        .collect();
    file_paths.sort();
    file_paths
        .into_iter()
        .flat_map(move |file_path| iter_lines_from_file(&file_path, pl.clone()))
}

/// Yields textual swhids from a directory of newline-separated ZSTD-compressed files
///
/// Files are read in alphabetical order of their name.
pub fn par_iter_lines_from_dir<'a, Line>(
    path: &'a Path,
    pl: impl ConcurrentProgressLog + 'a,
) -> impl ParallelIterator<Item = Line> + 'a
where
    Line: TryFrom<Vec<u8>> + Send,
    <Line as TryFrom<Vec<u8>>>::Error: std::fmt::Debug,
{
    let mut file_paths: Vec<_> = std::fs::read_dir(path)
        .unwrap_or_else(|e| panic!("Could not list {}: {:?}", path.display(), e))
        .map(|entry| {
            entry
                .as_ref()
                .unwrap_or_else(|e| panic!("Could not read {} entry: {:?}", path.display(), e))
                .path()
        })
        .collect();
    file_paths.sort();
    file_paths
        .into_par_iter()
        .flat_map_iter(move |file_path| iter_lines_from_file(&file_path, pl.clone()))
}
