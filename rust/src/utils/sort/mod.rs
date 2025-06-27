/*
 * Copyright (C) 2024  The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

//! Parallel sorting and deduplication for data that doesn't fit in RAM
// Adapted from https://archive.softwareheritage.org/swh:1:cnt:d5129fef934309da995a8895ba9509a6faae0bba;origin=https://github.com/vigna/webgraph-rs;visit=swh:1:snp:76b76a6b68240ad1ec27aed81f7cc30441b69d7c;anchor=swh:1:rel:ef30092122d472899fdfa361e784fc1e04495dab;path=/src/utils/sort_pairs.rs;lines=410-512

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use dary_heap::{PeekMut, QuaternaryHeap};
use dsi_progress_logger::{concurrent_progress_logger, ConcurrentProgressLog, ProgressLog};
use itertools::Itertools;
use rayon::prelude::*;
use tempfile::TempDir;

mod arcs;
pub use arcs::{par_sort_arcs, PartitionedBuffer};
mod strings;
pub use strings::par_sort_strings;
mod swhids;
pub use swhids::par_sort_swhids;

/// A pair of (Item, Iterator<Item=Item>) where comparison is the **reverse** of the head
#[derive(Clone, Debug)]
struct HeadTail<I: Iterator> {
    head: I::Item,
    tail: I,
}

impl<I: Iterator> PartialEq for HeadTail<I>
where
    I::Item: PartialEq,
{
    #[inline(always)]
    fn eq(&self, other: &Self) -> bool {
        self.head.eq(&other.head)
    }
}

impl<I: Iterator> Eq for HeadTail<I> where I::Item: Eq {}

impl<I: Iterator> PartialOrd for HeadTail<I>
where
    I::Item: PartialOrd,
{
    #[inline(always)]
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.head
            .partial_cmp(&other.head)
            .map(std::cmp::Ordering::reverse)
    }
}

impl<I: Iterator> Ord for HeadTail<I>
where
    I::Item: Ord,
{
    #[inline(always)]
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.head.cmp(&other.head).reverse()
    }
}

/// A structure using a [quaternary heap](dary_heap::QuaternaryHeap) to merge sorted iterators,
/// and yields in increasing order.
struct KMergeIters<I: Iterator>
where
    I::Item: Eq + Ord,
{
    heap: QuaternaryHeap<HeadTail<I>>,
}

impl<I: Iterator> KMergeIters<I>
where
    I::Item: Eq + Ord,
{
    pub fn new(iters: impl IntoIterator<Item = I>) -> Self {
        let iters = iters.into_iter();
        let mut heap = QuaternaryHeap::with_capacity(iters.size_hint().1.unwrap_or(10));
        for mut iter in iters {
            if let Some(new_head) = iter.next() {
                heap.push(HeadTail {
                    head: new_head,
                    tail: iter,
                });
            }
        }
        KMergeIters { heap }
    }
}

impl<I: Iterator> Iterator for KMergeIters<I>
where
    I::Item: Eq + Ord,
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        let mut head_tail = self.heap.peek_mut()?;

        match head_tail.tail.next() {
            None => Some(PeekMut::pop(head_tail).head),
            Some(item) => Some(std::mem::replace(&mut head_tail.head, item)),
        }
    }
}

trait ParallelDeduplicatingExternalSorter<Item: Eq + Ord + Send>: Sync + Sized {
    fn buffer_capacity(&self) -> usize;
    fn sort_vec(&self, vec: &mut Vec<Item>) -> Result<()>;
    fn serialize(path: PathBuf, items: impl Iterator<Item = Item>) -> Result<()>;
    fn deserialize(path: PathBuf) -> Result<impl Iterator<Item = Item>>;

    /// Takes an iterator if items, and returns an iterator of the same items,
    /// sorted and deduplicated
    fn par_sort_dedup<Iter: ParallelIterator<Item = Item>>(
        self,
        iter: Iter,
        mut pl: impl ProgressLog + Send,
    ) -> Result<impl Iterator<Item = Item>> {
        let unmerged_tmpdir =
            tempfile::tempdir().context("Could not create temporary directory for sorting")?;
        let (num_items_estimate, unmerged_paths) = self
            .par_sort_unmerged(iter, &unmerged_tmpdir, &mut pl)
            .context("Sorting items failed before merging")?;
        pl.done();

        let mut pl = concurrent_progress_logger!(
            display_memory = true,
            item_name = "item",
            local_speed = true,
            expected_updates = Some(num_items_estimate),
        );
        pl.start("Pre-merging");
        let pre_merged_tmpdir =
            tempfile::tempdir().context("Could not create temporary directory for sorting")?;
        let (num_items_estimate, pre_merged_paths) = self
            .pre_merge_sorted(unmerged_paths, &pre_merged_tmpdir, &mut pl)
            .context("Could not pre-merge")?;
        pl.done();
        log::info!("Removing sorted but unmerged files...");
        drop(unmerged_tmpdir); // Free disk space
        log::info!("Done");

        let mut pl = concurrent_progress_logger!(
            display_memory = true,
            item_name = "item",
            local_speed = true,
            expected_updates = Some(num_items_estimate),
        );
        pl.start("Merging");
        Self::merge_sorted(pre_merged_paths, pre_merged_tmpdir, pl).context("Could not merge")
    }

    #[doc(hidden)]
    /// Given an iterator of items, returns a list of paths to files, each containing
    /// a sorted list of items.
    ///
    /// The files are not sorted which respect to each other, so they need to be merged
    /// before use.
    fn par_sort_unmerged<Iter: ParallelIterator<Item = Item>>(
        &self,
        iter: Iter,
        tmpdir: &TempDir,
        mut pl: &mut (impl ProgressLog + Send),
    ) -> Result<(usize, Vec<PathBuf>)> {
        let num_flushed_buffers = AtomicU64::new(0);
        let mut buffer_paths = Vec::new();
        let num_items_estimate = AtomicUsize::new(0);
        {
            let buffer_paths = Arc::new(Mutex::new(&mut buffer_paths));
            let pl = Arc::new(Mutex::new(&mut pl));
            let flush = |buf: &mut Vec<Item>| -> Result<()> {
                if buf.is_empty() {
                    // Nothing to write; and mmaping a 0 bytes file would error
                    return Ok(());
                }

                self.sort_vec(buf).context("Could not sort buffer")?;

                let buffer_id = num_flushed_buffers.fetch_add(1, Ordering::Relaxed);
                let buf_path = tmpdir.path().join(format!("step1_{buffer_id}"));

                let buf_len = buf.len();

                // early deduplication to save some space
                Self::serialize(buf_path.clone(), buf.drain(0..).dedup())
                    .context("Could not serialize sorted list")?;
                log::debug!("Wrote {} items to {}", buf.len(), buf_path.display());

                pl.lock().unwrap().update_with_count(buf_len);
                num_items_estimate.fetch_add(buf_len, Ordering::Relaxed);
                buf.clear();
                buffer_paths.lock().unwrap().push(buf_path);
                Ok(())
            };

            // Sort in parallel
            iter.try_fold(
                || Vec::with_capacity(self.buffer_capacity()),
                |mut buf, item| -> Result<_> {
                    if let Some(previous_item) = buf.last() {
                        if *previous_item == item {
                            // early deduplication to save some sorting time
                            return Ok(buf);
                        }
                    }
                    if buf.len() >= buf.capacity() {
                        flush(&mut buf)?;
                    }
                    buf.push(item);
                    Ok(buf)
                },
            )
            .try_for_each(|buf| flush(&mut buf?))?;
        }
        let num_items_estimate = num_items_estimate.into_inner();

        Ok((num_items_estimate, buffer_paths))
    }

    /// Turns a long list of sorted lists into a shorter list (one per thread)
    fn pre_merge_sorted(
        &self,
        unmerged_paths: Vec<PathBuf>,
        tmpdir: &TempDir,
        pl: &mut impl ConcurrentProgressLog,
    ) -> Result<(usize, Vec<PathBuf>)> {
        let num_items_estimate = AtomicUsize::new(0);
        let pre_merged_paths = std::thread::scope(|s| {
            let tmpdir = &tmpdir;
            let num_items_estimate = &num_items_estimate;
            let chunks_size = unmerged_paths.len().div_ceil(num_cpus::get());
            unmerged_paths
                .into_iter()
                .chunks(chunks_size)
                .into_iter()
                .map(|buffer_paths_chunk| buffer_paths_chunk.into_iter().collect::<Vec<_>>())
                .enumerate()
                .map(|(i, buffer_paths_chunk)| {
                    let mut thread_pl = pl.clone();
                    s.spawn(move || -> Result<PathBuf> {
                        let mut num_items_in_thread = 0;
                        let merged_items = KMergeIters::new(
                            buffer_paths_chunk
                                .into_iter()
                                .map(|path| {
                                    Self::deserialize(path).context("Could not read sorted list")
                                })
                                .collect::<Result<Vec<_>>>()?
                                .into_iter(),
                        );
                        let merged_path = tmpdir.path().join(format!("step2_{i}"));
                        Self::serialize(
                            merged_path.clone(),
                            merged_items
                                .inspect(|_| thread_pl.light_update())
                                .dedup()
                                .inspect(|_| num_items_in_thread += 1),
                        )?;
                        log::debug!(
                            "Wrote {} items to {}",
                            num_items_in_thread,
                            merged_path.display()
                        );
                        num_items_estimate.fetch_add(num_items_in_thread, Ordering::Relaxed);
                        Ok(merged_path)
                    })
                })
                .collect::<Vec<_>>()
                .into_iter()
                .map(|handle| handle.join().expect("Pre-merge thread failed"))
                .collect::<Result<Vec<_>>>()
        })?;
        let num_items_estimate = num_items_estimate.into_inner();

        Ok((num_items_estimate, pre_merged_paths))
    }

    /// Merge a list of sorted lists into a single sorted list
    fn merge_sorted(
        unmerged_paths: Vec<PathBuf>,
        input_dir: TempDir,
        mut pl: impl ConcurrentProgressLog,
    ) -> Result<impl Iterator<Item = Item>> {
        let buffers = unmerged_paths
            .into_iter()
            .map(|path| Self::deserialize(path).context("Could not read pre-merged buffer"))
            .collect::<Result<Vec<_>>>()?;
        drop(input_dir); // Prevent deletion before we opened the input files
        Ok(KMergeIters::new(buffers)
            .inspect(move |_| pl.light_update())
            .dedup())
    }
}
