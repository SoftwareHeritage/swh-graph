/*
 * Copyright (C) 2023-2024  The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

/// Parallel string sorting and deduplication for data that doesn't fit in RAM
use std::cell::{RefCell, UnsafeCell};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use dsi_bitstream::prelude::NE;
use dsi_progress_logger::ProgressLog;
use rayon::prelude::*;
use webgraph::prelude::{BitDeserializer, BitSerializer};
use webgraph::utils::sort_pairs::{BatchIterator, BitReader, BitWriter, KMergeIters, Triple};

/// Provides a `unique_sort_to_dir` method to deduplicate, sort, and write to disk
///
/// # Panics
///
/// If files cannot be created in `temp_dir`.
pub trait Sortable<Line: IntoIterator<Item = u8>>: ParallelIterator<Item = Line>
where
    <Line as IntoIterator>::IntoIter: ExactSizeIterator,
{
    /// Drains a [`ParallelIterator`], sorts its values, deduplicates them, and write them
    /// to multiple newline-separated ZSTD-compressed files in the given directory.
    ///
    /// `buffer_size` is the RAM used by each of this process' threads before flushing
    /// to `sort`.
    #[allow(clippy::too_many_arguments)]
    fn unique_sort_to_dir(
        self,
        target_dir: PathBuf,
        target_prefix: &str,
        temp_dir: &Path,
        pl: impl ProgressLog + Send,
        args: &[&str],
        buffer_size: usize,
        expected_lines: usize,
    ) -> Result<()> {
        let pl = Mutex::new(pl);
        let sorted_files = Mutex::new(Vec::new());

        struct ThreadState {
            buffer: Vec<u8>,
            sort: std::process::Child,
            counter: usize,
        }

        let new_thread_state = || {
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
            UnsafeCell::new(ThreadState {
                buffer: Vec::<u8>::with_capacity(buffer_size),
                sort,
                counter: 0,
            })
        };
        let mut thread_states = thread_local::ThreadLocal::new();

        let flush_buffer = |state: &mut ThreadState| {
            let stdin = state.sort.stdin.as_mut().unwrap();
            stdin
                .write_all(&state.buffer)
                .expect("Could not write to sort's stdin");

            pl.lock().unwrap().update_with_count(state.counter);

            state.counter = 0;

            state.buffer.clear();
        };

        let num_rows = self
            .map(|item| {
                let state = thread_states.get_or(&new_thread_state).get();
                let item = item.into_iter();

                // This is safe because the main thread won't access this until this
                // one ends, and other threads don't access it.
                let state = unsafe { &mut *state };

                if state.buffer.len() + item.len() + 1 >= buffer_size {
                    flush_buffer(state);
                }

                state.counter += 1;

                state.buffer.extend(item);
                state.buffer.push(b'\n');
            })
            .count();

        let is_empty = num_rows == 0;

        // Write remaining buffers
        for state in thread_states.iter_mut() {
            // This is safe because other threads ended
            let state = unsafe { &mut *state.get() };
            flush_buffer(state)
        }

        // Notify sorters they reached the end of their inputs
        for state in thread_states.iter_mut() {
            // This is safe because other threads ended
            let state = unsafe { &mut *state.get() };
            drop(state.sort.stdin.take().unwrap());
        }

        // Wait for sorters to finish
        for state in thread_states.iter_mut() {
            // This is safe because other threads ended
            let state = unsafe { &mut *state.get() };
            state.sort.wait().with_context(|| "Sorter crashed")?;
        }

        pl.lock().unwrap().done();

        let sorted_files = sorted_files.lock().unwrap();

        let mut target_path_prefix = target_dir.clone();
        target_path_prefix.push(format!("{}.", target_prefix));

        if target_dir.exists() {
            std::fs::remove_dir(&target_dir)
                .with_context(|| format!("Could not delete directory {}", target_dir.display()))?;
        }
        std::fs::create_dir_all(&target_dir)
            .with_context(|| format!("Could not create directory {}", target_dir.display()))?;

        if is_empty {
            // No persons; write an empty file so the rest of the pipeline does not
            // need special-casing for the absence of files.
            let path = target_dir.join("0.csv.zst");
            let file = std::fs::File::create(&path)
                .with_context(|| format!("Could not create {}", path.display()))?;
            let compression_level = 3;
            let writer = zstd::stream::write::Encoder::new(file, compression_level)
                .with_context(|| format!("Could not create ZSTD encoder for {}", path.display()))?;
            let mut file = writer
                .finish()
                .with_context(|| format!("Could not finishZSTD encoder for {}", path.display()))?;
            file.flush()
                .with_context(|| format!("Could not flush {}", path.display()))?;
            return Ok(());
        }

        assert!(sorted_files.len() > 0, "Sorters did not run");

        // Spawn sort * | pv | split

        // TODO: it would be nice to start merging without waiting for all sorters
        // to be done. -> rewrite the merger in-process?
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

        let mut pv = std::process::Command::new("pv")
            .arg("--line-mode")
            .arg(&format!("--size={}", expected_lines))
            .stdin(Stdio::from(merge_out))
            .stdout(Stdio::piped())
            .spawn()
            .with_context(|| "Could not start pv")?;
        let pv_out = pv.stdout.take().unwrap();

        let mut split = std::process::Command::new("split")
            .arg("--lines=100000000") // 100M
            .arg("--suffix-length=6")
            .arg("--numeric-suffixes")
            .arg("--filter=zstdmt > $FILE")
            .arg("--additional-suffix=.zst")
            .arg("-")
            .arg(&target_path_prefix)
            .stdin(Stdio::from(pv_out))
            .spawn()
            .with_context(|| "Could not start zstdmt")?;

        merge.wait().with_context(|| "merger crashed")?;
        merge.wait().with_context(|| "pv crashed")?;
        split.wait().with_context(|| "split/zstdmt crashed")?;

        Ok(())
    }
}

impl<Line: IntoIterator<Item = u8>, T: ParallelIterator<Item = Line>> Sortable<Line> for T where
    <Line as IntoIterator>::IntoIter: ExactSizeIterator
{
}

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
