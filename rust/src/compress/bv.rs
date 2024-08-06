// Copyright (C) 2023-2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::cell::UnsafeCell;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Context, Result};
use dsi_bitstream::codes::{GammaRead, GammaWrite};
use dsi_bitstream::prelude::{BitRead, BitWrite, BufBitReader, BufBitWriter, WordAdapter, BE, NE};
use dsi_progress_logger::ProgressLogger;
use itertools::Itertools;
use lender::{for_, Lender};
use nonmax::NonMaxU64;
use pthash::Phf;
use rayon::prelude::*;
use tempfile;
use webgraph::graphs::arc_list_graph::ArcListGraph;
use webgraph::prelude::sort_pairs::{BitReader, BitWriter};
use webgraph::prelude::*;

use super::iter_arcs::iter_arcs;
use super::iter_labeled_arcs::iter_labeled_arcs;
use super::label_names::{LabelNameHasher, LabelNameMphf};
use super::stats::estimate_edge_count;
use crate::map::{MappedPermutation, Permutation};
use crate::mph::SwhidMphf;
use crate::utils::sort::par_sort_arcs;

pub fn bv<MPHF: SwhidMphf + Sync>(
    sort_batch_size: usize,
    partitions_per_thread: usize,
    mph_basepath: PathBuf,
    num_nodes: usize,
    dataset_dir: PathBuf,
    allowed_node_types: &[crate::NodeType],
    target_dir: PathBuf,
) -> Result<()> {
    log::info!("Reading MPH");
    let mph = MPHF::load(mph_basepath).context("Could not load MPHF")?;
    log::info!("MPH loaded, sorting arcs");

    let num_threads = num_cpus::get();
    let num_partitions = num_threads * partitions_per_thread;
    let nodes_per_partition = num_nodes.div_ceil(num_partitions);

    // Avoid empty partitions at the end when there are very few nodes
    let num_partitions = num_nodes.div_ceil(nodes_per_partition);

    let mut pl = ProgressLogger::default().display_memory();
    pl.item_name = "arc";
    pl.local_speed = true;
    pl.expected_updates = Some(
        estimate_edge_count(&dataset_dir, allowed_node_types)
            .context("Could not estimate edge count")? as usize,
    );
    pl.start("Reading arcs");

    // Sort in parallel in a bunch of SortPairs instances
    let pl = Mutex::new(pl);
    let counters = thread_local::ThreadLocal::new();
    let temp_dir = tempfile::tempdir().context("Could not get temporary_directory")?;
    let sorted_arcs_path = temp_dir.path().join("sorted_arcs");
    std::fs::create_dir(&sorted_arcs_path)
        .with_context(|| format!("Could not create {}", sorted_arcs_path.display()))?;
    let sorted_arcs = par_sort_arcs(
        &sorted_arcs_path,
        sort_batch_size,
        iter_arcs(&dataset_dir, allowed_node_types)
            .context("Could not open input files to read arcs")?
            .inspect(|_| {
                // This is safe because only this thread accesses this and only from
                // here.
                let counter = counters.get_or(|| UnsafeCell::new(0));
                let counter: &mut usize = unsafe { &mut *counter.get() };
                *counter += 1;
                if *counter % 32768 == 0 {
                    // Update but avoid lock contention at the expense
                    // of precision (counts at most 32768 too many at the
                    // end of each file)
                    pl.lock().unwrap().update_with_count(32768);
                    *counter = 0
                }
            }),
        num_partitions,
        (),
        (),
        |buffer, (src, dst)| {
            let src = mph
                .hash_str_array(&src)
                .ok_or_else(|| anyhow!("Unknown SWHID {:?}", String::from_utf8_lossy(&src)))?;
            let dst = mph
                .hash_str_array(&dst)
                .ok_or_else(|| anyhow!("Unknown SWHID {:?}", String::from_utf8_lossy(&dst)))?;
            assert!(src < num_nodes, "src node id is greater than {}", num_nodes);
            assert!(dst < num_nodes, "dst node id is greater than {}", num_nodes);
            let partition_id = src / nodes_per_partition;
            buffer.insert(partition_id, src, dst)?;
            Ok(())
        },
    )?;
    pl.lock().unwrap().done();

    let arc_list_graphs =
        sorted_arcs
            .into_iter()
            .enumerate()
            .map(|(partition_id, sorted_arcs_partition)| {
                webgraph::prelude::Left(ArcListGraph::new_labeled(
                    num_nodes,
                    sorted_arcs_partition.dedup(),
                ))
                .iter_from(partition_id * nodes_per_partition)
                .take(nodes_per_partition)
            });
    let comp_flags = Default::default();

    let temp_bv_dir = temp_dir.path().join("bv");
    std::fs::create_dir(&temp_bv_dir)
        .with_context(|| format!("Could not create {}", temp_bv_dir.display()))?;
    BVComp::parallel_iter::<BE, _>(
        target_dir,
        arc_list_graphs,
        num_nodes,
        comp_flags,
        rayon::ThreadPoolBuilder::default()
            .build()
            .expect("Could not create BVComp thread pool"),
        &temp_bv_dir,
    )
    .context("Could not build BVGraph from arcs")?;

    pl.lock().unwrap().done();

    drop(temp_dir); // Prevent early deletion

    Ok(())
}

/// Writes `-labelled.labels`,  `-labelled.labeloffsets`, and returns the label width
pub fn edge_labels<MPHF: SwhidMphf + Sync>(
    sort_batch_size: usize,
    partitions_per_thread: usize,
    mph_basepath: PathBuf,
    order: MappedPermutation,
    label_name_hasher: LabelNameHasher,
    num_nodes: usize,
    dataset_dir: PathBuf,
    allowed_node_types: &[crate::NodeType],
    transposed: bool,
    target_dir: &Path,
) -> Result<usize> {
    log::info!("Reading MPH");
    let mph = MPHF::load(mph_basepath).context("Could not load MPHF")?;
    log::info!("MPH loaded, sorting arcs");

    let num_threads = num_cpus::get();
    let num_partitions = num_threads * partitions_per_thread;
    let nodes_per_partition = num_nodes.div_ceil(num_partitions);
    let label_width = label_width(label_name_hasher.mphf());

    // Avoid empty partitions at the end when there are very few nodes
    let num_partitions = num_nodes.div_ceil(nodes_per_partition);

    let mut pl = ProgressLogger::default().display_memory();
    pl.item_name = "arc";
    pl.local_speed = true;
    pl.expected_updates = Some(
        estimate_edge_count(&dataset_dir, allowed_node_types)
            .context("Could not estimate edge count")? as usize,
    );
    pl.start("Reading and sorting arcs");

    // Sort in parallel in a bunch of SortPairs instances
    let pl = Mutex::new(pl);
    let counters = thread_local::ThreadLocal::new();
    let total_labeled_arcs = AtomicUsize::new(0);
    let temp_dir = tempfile::tempdir().context("Could not get temporary_directory")?;
    let sorted_arcs_path = temp_dir.path().join("sorted_arcs");
    std::fs::create_dir(&sorted_arcs_path)
        .with_context(|| format!("Could not create {}", sorted_arcs_path.display()))?;
    let sorted_arcs = par_sort_arcs(
        &sorted_arcs_path,
        sort_batch_size,
        iter_labeled_arcs(&dataset_dir, allowed_node_types, label_name_hasher)
            .context("Could not open input files to read arcs")?
            .inspect(|_| {
                // This is safe because only this thread accesses this and only from
                // here.
                let counter = counters.get_or(|| UnsafeCell::new(0));
                let counter: &mut usize = unsafe { &mut *counter.get() };
                *counter += 1;
                if *counter % 32768 == 0 {
                    // Update but avoid lock contention at the expense
                    // of precision (counts at most 32768 too many at the
                    // end of each file)
                    pl.lock().unwrap().update_with_count(32768);
                    total_labeled_arcs.fetch_add(32768, Ordering::Relaxed);
                    *counter = 0
                }
            }),
        num_partitions,
        LabelSerializer { label_width },
        LabelDeserializer { label_width },
        |buffer, (src, dst, label)| {
            let mut src = mph
                .hash_str_array(&src)
                .ok_or_else(|| anyhow!("Unknown SWHID {:?}", String::from_utf8_lossy(&src)))?;
            let mut dst = mph
                .hash_str_array(&dst)
                .ok_or_else(|| anyhow!("Unknown SWHID {:?}", String::from_utf8_lossy(&dst)))?;
            if transposed {
                (src, dst) = (dst, src);
            }
            assert!(src < num_nodes, "src node id is greater than {}", num_nodes);
            assert!(dst < num_nodes, "dst node id is greater than {}", num_nodes);
            let src = order.get(src).expect("Could not permute src");
            let dst = order.get(dst).expect("Could not permute dst");
            let partition_id = src / nodes_per_partition;
            buffer.insert_labeled(partition_id, src, dst, label)?;
            Ok(())
        },
    )?;
    pl.lock().unwrap().done();

    let arc_list_graphs =
        sorted_arcs
            .into_iter()
            .enumerate()
            .map(|(partition_id, sorted_arcs_partition)| {
                // no sorted_arcs_partition.dedup() on labels
                ArcListGraph::new_labeled(num_nodes, sorted_arcs_partition)
                    .iter_from(partition_id * nodes_per_partition)
                    .take(nodes_per_partition)
            });

    let mut pl = ProgressLogger::default().display_memory();
    pl.item_name = "arc";
    pl.local_speed = true;
    pl.expected_updates = Some(total_labeled_arcs.load(Ordering::Relaxed));
    pl.start("Writing arc labels");
    let pl = Arc::new(Mutex::new(pl));

    // Build chunks of .labels and .labeloffsets for each partition
    let num_partitions = arc_list_graphs.len();
    let labels_files: Vec<_> = (0..num_partitions).map(|_| OnceLock::new()).collect();
    let offsets_files: Vec<_> = (0..num_partitions).map(|_| OnceLock::new()).collect();
    let nodes_per_thread: Vec<_> = (0..num_partitions).map(|_| OnceLock::new()).collect();
    arc_list_graphs.enumerate().par_bridge().try_for_each(
        |(partition_id, partition_graph)| -> Result<()> {
            let labels_file = tempfile::tempfile().context("Could not create temp file")?;
            let offsets_file = tempfile::tempfile().context("Could not create temp file")?;
            let mut labels_writer = BufBitWriter::<BE, _, _>::new(WordAdapter::<u8, _>::new(
                BufWriter::new(labels_file),
            ));
            let mut offsets_writer = BufBitWriter::<BE, _, _>::new(WordAdapter::<u8, _>::new(
                BufWriter::new(offsets_file),
            ));
            let mut thread_nodes = 0u64;

            let mut written_labels = 0;

            for_!( (src, successors) in partition_graph {
                thread_nodes += 1;
                let mut offset_bits = 0u64;
                for (_dst, labels) in &successors.group_by(|(dst, _label)| *dst) {
                    let mut labels: Vec<u64> = labels
                        .flat_map(|(_dst, label)| label)
                        .map(|label: NonMaxU64| u64::from(label))
                        .collect();
                    written_labels += labels.len();
                    labels.par_sort_unstable();

                    // Write length-prefixed list of labels
                    offset_bits = offset_bits
                        .checked_add(
                            labels_writer
                                .write_gamma(labels.len() as u64)
                                .context("Could not write number of labels")?
                                as u64,
                        )
                        .context("offset overflowed u64")?;
                    for label in labels {
                        offset_bits = offset_bits
                            .checked_add(
                                labels_writer
                                    .write_bits(label, label_width)
                                    .context("Could not write label")?
                                    as u64,
                            )
                            .context("offset overflowed u64")?;
                    }
                }

                if src % 32768 == 0 {
                    pl.lock().unwrap().update_with_count(written_labels);
                    written_labels = 0;
                }

                // Write offset of the end of this edge's label list (and start of the next one)
                offsets_writer
                    .write_gamma(offset_bits)
                    .context("Could not write thread offset")?;
            });

            labels_files[partition_id]
                .set(
                    labels_writer
                        .into_inner()
                        .context("Could not flush thread's labels writer")?
                        .into_inner()
                        .into_inner()
                        .context("Could not flush thread's labels bufwriter")?,
                )
                .map_err(|_| anyhow!("labels_files[{}] was set twice", partition_id))?;
            offsets_files[partition_id]
                .set(
                    offsets_writer
                        .into_inner()
                        .context("Could not close thread's label offsets writer")?
                        .into_inner()
                        .into_inner()
                        .context("Could not flush thread's label offsets bufwriter")?,
                )
                .map_err(|_| anyhow!("offsets_files[{}] was set twice", partition_id))?;
            nodes_per_thread[partition_id]
                .set(thread_nodes)
                .map_err(|_| anyhow!("offsets_files[{}] was set twice", partition_id))?;

            Ok(())
        },
    )?;
    pl.lock().unwrap().done();

    // Collect thread results (can't use .map() and .collect() above, because .par_bridge does not
    // preserve order)
    let labels_files = labels_files
        .into_iter()
        .map(|file| {
            file.into_inner()
                .ok_or(anyhow!("Missing thread's labels file"))
        })
        .collect::<Result<Vec<_>>>()?;
    let offsets_files = offsets_files
        .into_iter()
        .map(|file| {
            file.into_inner()
                .ok_or(anyhow!("Missing thread's offsets file"))
        })
        .collect::<Result<Vec<_>>>()?;
    let nodes_per_thread = nodes_per_thread
        .into_iter()
        .map(|file| {
            file.into_inner()
                .ok_or(anyhow!("Missing thread's nodes_per_thread"))
        })
        .collect::<Result<Vec<_>>>()?;

    // Concatenate arc labels written by each thread
    let mut pl = ProgressLogger::default().display_memory();
    pl.item_name = "bytes";
    pl.local_speed = true;
    pl.expected_updates = Some(
        labels_files
            .iter()
            .map(|file| {
                file.metadata()
                    .expect("Could not get thread's labels file metadata")
                    .len()
            })
            .sum::<u64>() as usize,
    );
    pl.start("Concatenating arc labels");

    let mut labels_path = target_dir.to_owned();
    labels_path.as_mut_os_string().push("-labelled.labels");
    let mut labels_file = File::create(&labels_path)
        .with_context(|| format!("Could not create {}", labels_path.display()))?;
    let mut buf = vec![0; 65536]; // arbitrary constant
    let mut chunk_offsets = vec![0];
    for mut thread_labels_file in labels_files {
        let mut chunk_size = 0;
        thread_labels_file
            .seek(SeekFrom::Start(0))
            .context("Could not seek in thread labels file")?;
        loop {
            let buf_size = thread_labels_file
                .read(&mut buf)
                .context("Could not read thread's labels")?;
            if buf_size == 0 {
                break;
            }
            chunk_size += buf_size;
            labels_file
                .write(&buf[0..buf_size])
                .context("Could not write labels")?;
            pl.update_with_count(buf_size);
        }
        chunk_offsets.push(u64::try_from(chunk_size).expect("Chunk size overflowed u64"));
        drop(thread_labels_file); // Removes the temporary file
    }

    labels_file.flush().context("Could not flush labels")?;
    drop(labels_file);

    pl.done();

    // Collect offsets of each thread (relative to the start of the chunk of .labels of that thread),
    // shift them (so they are relative to the start of the .labels), then write them in
    // .labeloffsets
    let mut pl = ProgressLogger::default().display_memory();
    pl.item_name = "node";
    pl.local_speed = true;
    pl.expected_updates = Some(num_nodes);
    pl.start("Concatenating arc label offsets");

    let mut offsets_path = target_dir.to_owned();
    offsets_path
        .as_mut_os_string()
        .push("-labelled.labeloffsets");

    let mut offsets_writer =
        BufBitWriter::<BE, _, _>::new(WordAdapter::<u8, _>::new(BufWriter::new(
            File::create(&offsets_path)
                .with_context(|| format!("Could not create {}", offsets_path.display()))?,
        )));

    for ((chunk_offset, thread_offsets_file), thread_nodes) in chunk_offsets
        .into_iter()
        .zip(offsets_files.into_iter())
        .zip(nodes_per_thread.into_iter())
    {
        let mut thread_offsets_reader = BufBitReader::<BE, _, _>::new(WordAdapter::<u8, _>::new(
            BufReader::new(thread_offsets_file),
        ));

        for _ in 0..thread_nodes {
            pl.light_update();
            let offset = thread_offsets_reader
                .read_gamma()
                .context("Could not read thread offset")?;
            offsets_writer
                .write_gamma(offset + chunk_offset)
                .context("Could not write offset")?;
        }

        drop(thread_offsets_reader); // Removes the temporary file
    }

    drop(
        offsets_writer
            .into_inner()
            .context("Could not close label offsets writer")?
            .into_inner()
            .into_inner()
            .context("Could not flush label offsets bufwriter"),
    );

    pl.done();

    drop(temp_dir); // Prevent early deletion

    Ok(label_width)
}

fn label_width(mphf: &LabelNameMphf) -> usize {
    use crate::labels::{
        Branch, DirEntry, EdgeLabel, FilenameId, Permission, UntypedEdgeLabel, Visit, VisitStatus,
    };
    let num_label_names = mphf.num_keys();

    // Visit timestamps cannot be larger than the current timestamp
    let max_visit_timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Could not get current time")
        .as_secs();

    [
        EdgeLabel::Branch(Branch::new(FilenameId(num_label_names)).unwrap()),
        EdgeLabel::DirEntry(DirEntry::new(Permission::None, FilenameId(num_label_names)).unwrap()),
        EdgeLabel::Visit(Visit::new(VisitStatus::Full, max_visit_timestamp).unwrap()),
    ]
    .into_iter()
    .map(|label| UntypedEdgeLabel::from(label).0) // Convert to on-disk representation
    .map(|label| label.checked_ilog2().unwrap() + 1) // Number of bits needed to represent it
    .max()
    .unwrap() as usize
}

#[derive(Clone, Copy)]
struct LabelDeserializer {
    label_width: usize,
}
#[derive(Clone, Copy)]
struct LabelSerializer {
    label_width: usize,
}

impl BitDeserializer<NE, BitReader> for LabelDeserializer {
    type DeserType = Option<NonMaxU64>;
    fn deserialize(
        &self,
        bitstream: &mut BitReader,
    ) -> Result<Self::DeserType, <BitReader as BitRead<NE>>::Error> {
        assert_ne!(self.label_width, 64, "label_width = 64 is not implemented");
        let max = (1u64 << self.label_width) - 1; // Largest value that fits in the given width
        let value = bitstream.read_bits(self.label_width)?;
        assert!(value <= max, "Read unexpectedly large value");
        if value == max {
            Ok(None)
        } else {
            Ok(Some(NonMaxU64::try_from(value).unwrap()))
        }
    }
}

impl BitSerializer<NE, BitWriter> for LabelSerializer {
    type SerType = Option<NonMaxU64>;
    fn serialize(
        &self,
        value: &Self::SerType,
        bitstream: &mut BitWriter,
    ) -> Result<usize, <BitWriter as BitWrite<NE>>::Error> {
        assert_ne!(self.label_width, 64, "label_width = 64 is not implemented");
        let max = (1u64 << self.label_width) - 1;
        match *value {
            Some(value) => {
                assert!(u64::from(value) < max, "value does not fit in label width");
                bitstream.write_bits(u64::from(value), self.label_width)
            }
            None => bitstream.write_bits(max, self.label_width),
        }
    }
}
