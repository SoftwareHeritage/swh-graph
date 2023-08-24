/*
 * Copyright (C) 2023  The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

/// Parallel string sorting and deduplication for data that doesn't fit in RAM
use std::cell::UnsafeCell;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Mutex;

use anyhow::{Context, Result};
use dsi_progress_logger::ProgressLogger;
use rayon::prelude::*;
use thread_local::ThreadLocal;
use webgraph::prelude::SortPairs;

/// Provides a `unique_sort_to_dir` method to deduplicate, sort, and write to disk
///
/// # Panics
///
/// If files cannot be created in `temp_dir`.
pub trait Sortable<Line: IntoIterator<Item = u8>>: ParallelIterator<Item = Line> {
    /// Drains a [`ParallelIterator`], sorts its values, deduplicates them, and write them
    /// to multiple newline-separated ZSTD-compressed files in the given directory.
    fn unique_sort_to_dir(
        self,
        target_dir: PathBuf,
        target_prefix: &str,
        temp_dir: &Path,
        pl: ProgressLogger,
        args: &[&str],
    ) -> Result<()> {
        let pl = Mutex::new(pl);
        let sorted_files = Mutex::new(Vec::new());
        let mut sorter_buffers = ThreadLocal::new();
        let mut sorters = ThreadLocal::new();
        let counters = ThreadLocal::new();
        let buffer_size = 5_000_000;

        let flush_buffer = |buffer: &mut Vec<u8>| {
            // Flush to the sorter
            let sort = sorters
                .get_or(|| {
                    let file = tempfile::NamedTempFile::new_in(temp_dir)
                        .expect("Could not open temporary sorted file");
                    let path: PathBuf = file.path().into();
                    sorted_files.lock().unwrap().push(file);

                    let sort = std::process::Command::new("sort")
                        .arg("--buffer-size=100M")
                        .arg("--compress-program=zstd")
                        .arg("--unique") // Removes duplicates early to save space
                        .arg("--parallel=1") // Slightly faster as we already max out the CPU
                        .args(args)
                        .env("TMPDIR", temp_dir)
                        .env("LC_ALL", "C")
                        .stdout(std::fs::File::create(path).unwrap())
                        .stdin(std::process::Stdio::piped())
                        .spawn()
                        .expect("Could not start 'sort' process");
                    UnsafeCell::new(sort)
                })
                .get();
            // This is safe because the main thread won't access this until this
            // one ends, and other threads don't access it.
            let sort: &mut std::process::Child = unsafe { &mut *sort };

            let stdin = sort.stdin.as_mut().unwrap();
            stdin
                .write_all(&buffer)
                .expect("Could not write to sort's stdin");

            // Ditto
            let counter = counters.get_or(|| UnsafeCell::new(0));
            let counter: &mut usize = unsafe { &mut *counter.get() };

            pl.lock().unwrap().update_with_count(*counter);

            *counter = 0;

            buffer.clear();
        };

        self.for_each(|item| {
            let counter: &UnsafeCell<usize> = counters.get_or(|| UnsafeCell::new(0));
            let buffer: &UnsafeCell<Vec<_>> =
                sorter_buffers.get_or(|| UnsafeCell::new(Vec::<u8>::with_capacity(buffer_size)));
            // This is safe because the main thread won't access this until this
            // one ends, and other threads don't access it.
            let counter: &mut usize = unsafe { &mut *counter.get() };
            let buffer: &mut Vec<u8> = unsafe { &mut *buffer.get() };

            *counter += 1;

            buffer.extend(item);
            buffer.push(b'\n');

            if buffer.len() >= buffer_size {
                flush_buffer(buffer);
            }
        });

        // Write remaining buffers
        for buffer in sorter_buffers.iter_mut() {
            // This is safe because other threads ended
            let buffer = unsafe { &mut *buffer.get() };
            flush_buffer(buffer)
        }

        // Notify sorters they reached the end of their inputs
        for sorter in &mut sorters {
            // This is safe because other threads ended
            let sorter = unsafe { &mut *sorter.get() };
            drop(sorter.stdin.take().unwrap());
        }

        // Wait for sorters to finish
        for sorter in sorters {
            // This is safe because other threads ended
            let sorter = unsafe { &mut *sorter.get() };
            sorter.wait().with_context(|| "Sorter crashed")?;
        }

        pl.lock().unwrap().done();

        let sorted_files = sorted_files.lock().unwrap();

        assert!(sorted_files.len() > 0, "Sorters did not run");

        let mut target_path_prefix = target_dir.clone();
        target_path_prefix.push(format!("{}.", target_prefix));

        if target_dir.exists() {
            std::fs::remove_dir(&target_dir)
                .with_context(|| format!("Could not delete directory {}", target_dir.display()))?;
        }
        std::fs::create_dir(&target_dir)
            .with_context(|| format!("Could not create directory {}", target_dir.display()))?;

        // TODO: this is the longest step, we need to log progress here.
        // also, it would be nice to start merging without waiting for all sorters
        // to be done.
        // -> rewrite the merger in-process?
        let mut merge = std::process::Command::new("sort")
            .arg("--buffer-size=100M")
            .arg("--compress-program=zstdmt")
            .env("TMPDIR", temp_dir)
            .env("LC_ALL", "C")
            .arg("--merge")
            .arg("--unique")
            .args(sorted_files.iter().map(|file| file.path()))
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .spawn()
            .with_context(|| "Could not start merging 'sort' process")?;
        let merge_out = merge.stdout.take().unwrap();

        let mut split = std::process::Command::new("split")
            .arg("--lines=100000000") // 100M
            .arg("--suffix-length=6")
            .arg("--numeric-suffixes")
            .arg("--filter=zstdmt > $FILE")
            .arg("--additional-suffix=.zst")
            .arg("-")
            .arg(&target_path_prefix)
            .stdin(Stdio::from(merge_out))
            .spawn()
            .with_context(|| "Could not start zstdmt")?;

        merge.wait().with_context(|| "merger crashed")?;
        split.wait().with_context(|| "split/zstdmt crashed")?;

        Ok(())
    }
}

impl<Line: IntoIterator<Item = u8>, T: ParallelIterator<Item = Line>> Sortable<Line> for T {}

/// An iterator on sorted pairs, constructed from another iterator (not necessarily of
/// pairs) and a function to insert its items to [`SortPairs`].
///
/// # Panics
///
/// When the temp_dir is not writeable
pub struct SortedArcsIterator {
    sorters: ThreadLocal<UnsafeCell<SortPairs<()>>>,
    used_sorters: Vec<UnsafeCell<SortPairs<()>>>,
}

// This needs to be implemented as a structure with two methods instead of a single
// function, because we need the lifetime of the SortPairs to outlive the output iterator
impl SortedArcsIterator {
    pub fn new<Item, Iter, F>(temp_dir: &Path, iter: Iter, f: F) -> Result<SortedArcsIterator>
    where
        F: Fn(&mut SortPairs<()>, Item) -> Result<()> + Send + Sync,
        Iter: ParallelIterator<Item = Item>,
    {
        let sorters = ThreadLocal::new();
        let batch_size = 5_000_000; // SortPairs creates one file per batch
        iter.try_for_each(|item| {
            let sorter = sorters
                .get_or(|| {
                    use rand::Rng;
                    let sorter_id = rand::thread_rng().gen::<u64>();
                    let mut sorter_temp_dir = temp_dir.to_owned();
                    sorter_temp_dir.push(format!("sort-arcs-permute-{:#x}", sorter_id));
                    std::fs::create_dir(&sorter_temp_dir)
                        .with_context(|| {
                            format!(
                                "Could not create temporary directory {}",
                                sorter_temp_dir.display()
                            )
                        })
                        .unwrap();

                    UnsafeCell::new(
                        SortPairs::new(batch_size, &sorter_temp_dir)
                            .context("Could not create SortPairs")
                            .unwrap(),
                    )
                })
                .get();

            // This is safe because the main thread won't access this until this
            // one ends, and other threads don't access it.
            let sorter: &mut SortPairs<()> = unsafe { &mut *sorter };
            f(sorter, item)?;
            Ok::<(), anyhow::Error>(())
        })?;

        Ok(SortedArcsIterator {
            sorters,
            used_sorters: Vec::new(),
        })
    }

    pub fn iter(&mut self) -> Result<impl Iterator<Item = (usize, usize)> + Clone> {
        let mut sorted_arc_lists = Vec::new();
        let mut sorters = ThreadLocal::new();
        std::mem::swap(&mut sorters, &mut self.sorters);
        for sorter in sorters {
            // This is safe because other threads ended
            let sorter_ref = unsafe { &mut *sorter.get() };

            sorted_arc_lists.push(
                sorter_ref
                    .iter()
                    .context("Could not get sorted arc lists")?,
            );
            self.used_sorters.push(sorter); // put it there to avoid being dropped
        }

        // Merge sorted arc lists into a single sorted arc list
        Ok(itertools::kmerge(sorted_arc_lists).map(|(src, dst, ())| (src, dst)))
    }
}
