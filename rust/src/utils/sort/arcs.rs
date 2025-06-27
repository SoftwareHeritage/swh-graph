/*
 * Copyright (C) 2023-2025  The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

//! Parallel string sorting and deduplication for data that doesn't fit in RAM
use std::cell::RefCell;
use std::fs::File;
use std::io::BufWriter;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use dsi_bitstream::prelude::*;
use dsi_progress_logger::{concurrent_progress_logger, ProgressLog};
use mmap_rs::MmapFlags;
use rayon::prelude::*;
use webgraph::prelude::{ArcMmapHelper, BitDeserializer, BitSerializer, MmapHelper};
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
    // total number of items flushed this the buffer was created
    total_flushed: Arc<AtomicUsize>,
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
        total_flushed: Arc<AtomicUsize>,
    ) -> Self {
        let capacity = batch_size / num_partitions;
        PartitionedBuffer {
            partitions: vec![Vec::with_capacity(capacity); num_partitions],
            sorted_iterators,
            temp_dir: temp_dir.to_owned(),
            capacity,
            label_serializer,
            label_deserializer,
            total_flushed,
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
        self.total_flushed
            .fetch_add(partition_buffer.len(), Ordering::Relaxed);
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
) -> Result<Vec<impl Iterator<Item = (usize, usize, L)> + Clone + Send + Sync>>
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

    let unmerged_sorted_dir = temp_dir.join("unmerged");
    std::fs::create_dir(&unmerged_sorted_dir)
        .with_context(|| format!("Could not create {}", unmerged_sorted_dir.display()))?;

    let num_arcs = Arc::new(AtomicUsize::new(0));

    iter.try_for_each_init(
        || -> std::cell::RefMut<PartitionedBuffer<L, S, D>> {
            buffers
                .get_or(|| {
                    RefCell::new(PartitionedBuffer::new(
                        sorted_iterators.clone(),
                        &unmerged_sorted_dir,
                        batch_size,
                        num_partitions,
                        label_serializer,
                        label_deserializer,
                        num_arcs.clone(),
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

    let sorted_iterators = Arc::into_inner(sorted_iterators)
        .expect("Dangling references to sorted_iterators Arc")
        .into_inner()
        .unwrap();

    let num_arcs = Arc::into_inner(num_arcs)
        .expect("Could not take ownership of num_arcs")
        .into_inner();

    let merged_sorted_dir = temp_dir.join("merged");
    std::fs::create_dir(&merged_sorted_dir)
        .with_context(|| format!("Could not create {}", merged_sorted_dir.display()))?;

    let mut pl = concurrent_progress_logger!(
        display_memory = true,
        item_name = "arc",
        local_speed = true,
        expected_updates = Some(num_arcs),
    );
    pl.start("Merging sorted arcs");

    let merged_sorted_iterators = sorted_iterators
        .into_par_iter()
        .enumerate()
        // Concatenate partitions
        .map_with(
            pl.clone(),
            |thread_pl, (partition_id, partition_sorted_iterators)| {
                // In the previous step, each of the N threads generated M partitions,
                // so N×M lists. (Unless Rayon did something funny, N=M.)
                // We now transpose, by taking for each partition what each thread produced,
                // and merge them together, to get only M lists.
                // This is done *in parallel*, and saves work when *sequentially* consuming
                // the final iterator
                let path = merged_sorted_dir.join(format!("part_{partition_id}"));
                let num_arcs_in_partition = serialize(
                    &path,
                    thread_pl,
                    label_serializer,
                    KMergeIters::new(partition_sorted_iterators),
                )?;

                deserialize(&path, label_deserializer, num_arcs_in_partition)
            },
        )
        .collect::<Result<Vec<_>>>()?;

    pl.done();

    log::info!("Deleted unmerged sorted files");
    std::fs::remove_dir_all(&unmerged_sorted_dir)
        .with_context(|| format!("Could not remove {}", unmerged_sorted_dir.display()))?;
    log::info!("Done");

    Ok(merged_sorted_iterators)
}

fn serialize<L, S>(
    path: &Path,
    pl: &mut impl ProgressLog,
    label_serializer: S,
    arcs: impl Iterator<Item = (usize, usize, L)>,
) -> Result<usize>
where
    S: BitSerializer<NE, BitWriter, SerType = L> + Send + Sync + Copy,
{
    let file =
        File::create_new(path).with_context(|| format!("Could not create {}", path.display()))?;
    let mut write_stream =
        <BufBitWriter<NE, _>>::new(<WordAdapter<usize, _>>::new(BufWriter::new(file)));
    let mut prev_src = 0;
    let mut prev_dst = 0;
    let mut num_arcs_in_partition: usize = 0;
    for (src, dst, label) in arcs {
        write_stream
            .write_gamma((src - prev_src).try_into().expect("usize overflowed u64"))
            .context("Could not write src gamma")?;
        if src != prev_src {
            prev_dst = 0;
        }
        write_stream
            .write_gamma((dst - prev_dst).try_into().expect("usize overflowed u64"))
            .context("Could not write dst gamma")?;
        label_serializer
            .serialize(&label, &mut write_stream)
            .context("Could not serialize label")?;
        prev_src = src;
        prev_dst = dst;
        pl.light_update();
        num_arcs_in_partition += 1;
    }
    write_stream.flush().context("Could not flush stream")?;
    Ok(num_arcs_in_partition)
}

fn deserialize<L, D>(
    path: &Path,
    label_deserializer: D,
    num_arcs: usize,
) -> Result<impl Iterator<Item = (usize, usize, L)> + Clone + Send + Sync>
where
    D: BitDeserializer<NE, BitReader, DeserType = L> + Send + Sync + Copy,
{
    let mut read_stream = <BufBitReader<NE, _>>::new(MemWordReader::new(ArcMmapHelper(Arc::new(
        MmapHelper::mmap(
            path,
            MmapFlags::TRANSPARENT_HUGE_PAGES | MmapFlags::SEQUENTIAL,
        )
        .with_context(|| format!("Could not mmap {}", path.display()))?,
    ))));

    let mut prev_src = 0;
    let mut prev_dst = 0;
    let arcs = (0..num_arcs).map(move |_| {
        let src = prev_src + read_stream.read_gamma().expect("Could not read src gamma");
        if src != prev_src {
            prev_dst = 0;
        }
        let dst = prev_dst + read_stream.read_gamma().expect("Could not read dst gamma");
        let label = label_deserializer
            .deserialize(&mut read_stream)
            .expect("Could not deserialize label");
        prev_src = src;
        prev_dst = dst;
        let src = usize::try_from(src).expect("deserialized usize overflows usize");
        let dst = usize::try_from(dst).expect("deserialized usize overflows usize");
        (src, dst, label)
    });
    Ok(arcs)
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
    let sorter_id = rand::thread_rng().r#gen::<u64>();
    let mut sorter_temp_file = temp_dir.to_owned();
    sorter_temp_file.push(format!("sort-arcs-permute-{sorter_id:#x}"));

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
