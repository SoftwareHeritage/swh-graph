/*
 * Copyright (C) 2023-2024  The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

//! Parallel string sorting and deduplication for data that doesn't fit in RAM
use std::cell::RefCell;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use dsi_bitstream::prelude::NE;
use rayon::prelude::*;
use webgraph::prelude::{BitDeserializer, BitSerializer};
use webgraph::utils::sort_pairs::{BatchIterator, BitReader, BitWriter, KMergeIters, Triple};

pub struct PartitionedBuffer<
    L: Ord + Copy + Send + Sync,
    S: BitSerializer<NE, BitWriter, SerType = L>,
    D: BitDeserializer<NE, BitReader>,
> {
    partitions: Vec<Vec<Triple<L>>>,
    capacity: usize,
    sorted_iterators: Arc<Mutex<Vec<Vec<BatchIterator<D>>>>>,
    temp_dir: PathBuf,
    label_serializer: S,
    label_deserializer: D,
}

impl<
        L: Ord + Copy + Send + Sync,
        S: BitSerializer<NE, BitWriter, SerType = L> + Copy,
        D: BitDeserializer<NE, BitReader, DeserType = L> + Copy,
    > PartitionedBuffer<L, S, D>
{
    fn new(
        sorted_iterators: Arc<Mutex<Vec<Vec<BatchIterator<D>>>>>,
        temp_dir: &Path,
        batch_size: usize,
        num_partitions: usize,
        label_serializer: S,
        label_deserializer: D,
    ) -> Self {
        let capacity = batch_size / num_partitions;
        PartitionedBuffer {
            partitions: vec![Vec::with_capacity(capacity); num_partitions],
            sorted_iterators,
            temp_dir: temp_dir.to_owned(),
            capacity,
            label_serializer,
            label_deserializer,
        }
    }

    pub fn insert_labeled(
        &mut self,
        partition_id: usize,
        src: usize,
        dst: usize,
        label: L,
    ) -> Result<()> {
        let partition_buffer = self
            .partitions
            .get_mut(partition_id)
            .expect("Partition sorter out of bound");
        partition_buffer.push(Triple {
            pair: [src, dst],
            label,
        });
        if partition_buffer.len() + 1 >= self.capacity {
            self.flush(partition_id)?;
        }
        Ok(())
    }

    fn flush_all(&mut self) -> Result<()> {
        for partition_id in 0..self.partitions.len() {
            self.flush(partition_id)?;
        }
        Ok(())
    }

    fn flush(&mut self, partition_id: usize) -> Result<()> {
        let partition_buffer = self
            .partitions
            .get_mut(partition_id)
            .expect("Partition buffer out of bound");
        let batch = flush(
            &self.temp_dir,
            &mut partition_buffer[..],
            self.label_serializer,
            self.label_deserializer,
        )?;
        self.sorted_iterators
            .lock()
            .unwrap()
            .get_mut(partition_id)
            .expect("Partition sorters out of bound")
            .push(batch);
        partition_buffer.clear();
        Ok(())
    }
}

impl PartitionedBuffer<(), (), ()> {
    pub fn insert(&mut self, partition_id: usize, src: usize, dst: usize) -> Result<()> {
        self.insert_labeled(partition_id, src, dst, ())
    }
}

/// Given an iterator and a function to insert its items to [`BatchIterator`]s, returns an
/// iterator of pairs.
///
/// `f` gets as parameters `num_partitions` `BatchIterator`s; and should place its pair
/// in such a way that all `(src, dst, labels)` in partition `n` should be lexicographically
/// lower than those in partition `n+1`.
///
/// In orther words, `f` writes in arbitrary order in each partition, but partitions
/// should be sorted with respect to each other. This allows merging partitions in
/// parallel after they are sorted.
pub fn par_sort_arcs<Item, Iter, F, L, S, D>(
    temp_dir: &Path,
    batch_size: usize,
    iter: Iter,
    num_partitions: usize,
    label_serializer: S,
    label_deserializer: D,
    f: F,
) -> Result<Vec<KMergeIters<impl Iterator<Item = (usize, usize, L)> + Clone + Send + Sync, L>>>
where
    F: Fn(&mut PartitionedBuffer<L, S, D>, Item) -> Result<()> + Send + Sync,
    Iter: ParallelIterator<Item = Item>,
    L: Ord + Copy + Send + Sync,
    S: BitSerializer<NE, BitWriter, SerType = L> + Send + Sync + Copy,
    D: BitDeserializer<NE, BitReader, DeserType = L> + Send + Sync + Copy,
{
    // For each thread, stores a vector of `num_shards` BatchIterator. The n-th BatchIterator
    // of each thread stores arcs for nodes [n*shard_size; (n+1)*shard_size)
    let buffers = thread_local::ThreadLocal::new();

    // Read the input to buffers, and flush buffer to disk (through BatchIterator)
    // from time to time
    let sorted_iterators = Arc::new(Mutex::new(vec![Vec::new(); num_partitions]));

    iter.try_for_each_init(
        || -> std::cell::RefMut<PartitionedBuffer<L, S, D>> {
            buffers
                .get_or(|| {
                    RefCell::new(PartitionedBuffer::new(
                        sorted_iterators.clone(),
                        temp_dir,
                        batch_size,
                        num_partitions,
                        label_serializer,
                        label_deserializer,
                    ))
                })
                .borrow_mut()
        },
        |thread_buffers, item| -> Result<()> {
            let thread_buffers = &mut *thread_buffers;
            f(thread_buffers, item)
        },
    )?;

    log::info!("Flushing remaining buffers to BatchIterator...");

    // Flush all buffers even if not full
    buffers.into_iter().par_bridge().try_for_each(
        |thread_buffer: RefCell<PartitionedBuffer<L, S, D>>| -> Result<()> {
            thread_buffer.into_inner().flush_all()
        },
    )?;
    log::info!("Done sorting all buffers.");

    Ok(Arc::into_inner(sorted_iterators)
        .expect("Dangling references to sorted_iterators Arc")
        .into_inner()
        .unwrap()
        .into_iter()
        // Concatenate partitions
        .map(|partition_sorted_iterators| {
            // Sort within each partition
            KMergeIters::new(partition_sorted_iterators)
        })
        .collect())
}

fn flush<
    L: Ord + Copy + Send + Sync,
    S: BitSerializer<NE, BitWriter, SerType = L>,
    D: BitDeserializer<NE, BitReader, DeserType = L>,
>(
    temp_dir: &Path,
    buffer: &mut [Triple<L>],
    label_serializer: S,
    label_deserializer: D,
) -> Result<BatchIterator<D>> {
    use rand::Rng;
    let sorter_id = rand::thread_rng().gen::<u64>();
    let mut sorter_temp_file = temp_dir.to_owned();
    sorter_temp_file.push(format!("sort-arcs-permute-{:#x}", sorter_id));

    // This is equivalent to BatchIterator::new_from_vec(&sorter_temp_file, buffer),
    // but without parallelism, which would cause Rayon to re-enter
    // par_sort_arcs and cause deadlocks: https://github.com/rayon-rs/rayon/issues/1083
    buffer.sort_unstable_by_key(
        |Triple {
             pair: [src, dst],
             label: _,
         }| (*src, *dst), // not sorting by label, KMergeIters loses the order anyway
    );
    BatchIterator::new_from_vec_sorted_labeled(
        &sorter_temp_file,
        buffer,
        &label_serializer,
        label_deserializer,
    )
    .with_context(|| {
        format!(
            "Could not create BatchIterator in {}",
            sorter_temp_file.display()
        )
    })
}
