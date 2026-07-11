// Copyright (C) 2023-2026  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::fs::File;
use std::io::{BufReader, BufWriter, Seek};
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Context, Result};
use dsi_bitstream::codes::{GammaRead, GammaWrite};
use dsi_bitstream::prelude::{BitRead, BitWrite, BufBitReader, BufBitWriter, WordAdapter, BE, NE};
use dsi_progress_logger::{concurrent_progress_logger, progress_logger, ProgressLog};
use itertools::Itertools;
use lender::{for_, Lender};
use nonmax::NonMaxU64;
use rayon::prelude::*;
use tempfile;
use webgraph::graphs::arc_list_graph::ArcListGraph;
use webgraph::prelude::*;
use webgraph::prelude::{BitReader, BitWriter};
use webgraph::utils::grouped_gaps::GroupedGapsCodec;
use webgraph::utils::ParSortPairs;

use super::iter_arcs::iter_arcs;
use super::iter_labeled_arcs::iter_labeled_arcs;
use super::label_names::LabelNameHasher;
use super::stats::estimate_edge_count;
use crate::map::{MappedPermutation, Permutation};
use crate::mph::LoadableSwhidMphf;

#[allow(clippy::too_many_arguments)]
pub fn bv<MPHF: LoadableSwhidMphf + Sync>(
    partitions_per_thread: usize,
    mph_basepath: PathBuf,
    num_nodes: usize,
    order: Option<PathBuf>,
    dataset_dir: PathBuf,
    allowed_node_types: &[crate::NodeType],
    target_dir: PathBuf,
) -> Result<()> {
    log::info!("Reading MPH");
    let mph = MPHF::load(mph_basepath).context("Could not load MPHF")?;
    let order = order
        .map(|order_path| {
            log::info!("Mmapping order");
            MappedPermutation::load(num_nodes, &order_path)
                .with_context(|| format!("Could not mmap order from {}", order_path.display()))
        })
        .transpose()?;

    log::info!("MPH loaded, sorting arcs");

    let num_threads = num_cpus::get();
    let num_partitions = num_threads * partitions_per_thread;
    let nodes_per_partition = num_nodes.div_ceil(num_partitions);

    // Avoid empty partitions at the end when there are very few nodes
    let num_partitions = num_nodes.div_ceil(nodes_per_partition);

    let mut pl = concurrent_progress_logger!(
        display_memory = true,
        item_name = "arc",
        local_speed = true,
        expected_updates = Some(
            estimate_edge_count(&dataset_dir, allowed_node_types)
                .context("Could not estimate edge count")? as usize,
        ),
    );
    pl.start("Reading arcs");

    // Sort in parallel in a bunch of SortPairs instances
    let temp_dir = tempfile::tempdir().context("Could not get temporary_directory")?;
    let sorted_arcs_path = temp_dir.path().join("sorted_arcs");
    std::fs::create_dir(&sorted_arcs_path)
        .with_context(|| format!("Could not create {}", sorted_arcs_path.display()))?;
    let pair_sorter = ParSortPairs::new(num_nodes)?
        .num_partitions(NonZeroUsize::new(num_partitions).unwrap())
        .expected_num_pairs(
            estimate_edge_count(&dataset_dir, allowed_node_types)
                .context("Could not estimate edge count")? as usize,
        );
    let sorted_arcs = pair_sorter
        .try_sort(
            iter_arcs(&dataset_dir, allowed_node_types)
                .context("Could not open input files to read arcs")?
                .map_with(pl.clone(), |thread_pl, (src, dst)| -> Result<_> {
                    let mut src = mph.hash_str_array(&src).ok_or_else(|| {
                        anyhow!("Unknown SWHID {:?}", String::from_utf8_lossy(&src))
                    })?;
                    let mut dst = mph.hash_str_array(&dst).ok_or_else(|| {
                        anyhow!("Unknown SWHID {:?}", String::from_utf8_lossy(&dst))
                    })?;
                    if let Some(order) = &order {
                        src = order.get(src).expect("src is greater than num_nodes");
                        dst = order.get(dst).expect("dst is greater than num_nodes");
                    }
                    assert!(src < num_nodes, "permuted src is greater than {num_nodes}");
                    assert!(dst < num_nodes, "permuted dst is greater than {num_nodes}");
                    thread_pl.light_update();
                    Ok((src, dst))
                }),
        )
        .context("Could not sort pairs")?;
    pl.done();

    let arc_list_graphs = Vec::from(sorted_arcs.iters).into_iter().enumerate().map(
        |(partition_id, sorted_arcs_partition)| {
            ArcListGraph::new(num_nodes, sorted_arcs_partition.into_iter().dedup())
                .iter_from(sorted_arcs.boundaries[partition_id])
                .take(
                    sorted_arcs.boundaries[partition_id + 1]
                        .checked_sub(sorted_arcs.boundaries[partition_id])
                        .expect("sorted_arcs.boundaries is not sorted"),
                )
        },
    );

    BvComp::with_basename(target_dir)
        .par_comp_lenders::<BE, _>(arc_list_graphs, num_nodes)
        .context("Could not build BVGraph from arcs")?;

    drop(temp_dir); // Prevent early deletion

    Ok(())
}

/// Writes `-labelled.labels`,  `-labelled.labeloffsets`, and returns the label width
#[allow(clippy::too_many_arguments)]
pub fn edge_labels<MPHF: LoadableSwhidMphf + Sync>(
    partitions_per_thread: usize,
    mph_basepath: PathBuf,
    order: MappedPermutation,
    label_name_hasher: &LabelNameHasher,
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
    let label_width = label_width(label_name_hasher);

    // Avoid empty partitions at the end when there are very few nodes
    let num_partitions = num_nodes.div_ceil(nodes_per_partition);

    let labeled_arcs_counters = thread_local::ThreadLocal::new();

    // Sort in parallel in a bunch of SortPairs instances
    let temp_dir = tempfile::tempdir().context("Could not get temporary_directory")?;
    let sorted_arcs_path = temp_dir.path().join("sorted_arcs");
    std::fs::create_dir(&sorted_arcs_path)
        .with_context(|| format!("Could not create {}", sorted_arcs_path.display()))?;
    let pair_sorter = ParSortPairs::new(num_nodes)?
        .num_partitions(NonZeroUsize::new(num_partitions).unwrap())
        // allows running other tasks at the same time, at the expense of making merges slower:
        .memory_usage(MemoryUsage::from_perc(25.0));
    let codec: GroupedGapsCodec<NE, _, _> = GroupedGapsCodec::new(
        LabelSerializer { label_width },
        LabelDeserializer { label_width },
    );
    let sorted_arcs = pair_sorter
        .try_sort_labeled(
            &codec,
            iter_labeled_arcs(&dataset_dir, allowed_node_types, label_name_hasher)
                .context("Could not open input files to read arcs")?
                .map_init(
                    || labeled_arcs_counters.get_or(AtomicUsize::default),
                    |labeled_arcs_counter, (src, dst, label)| -> Result<_> {
                        labeled_arcs_counter.fetch_add(1, Ordering::Relaxed);
                        let mut src = mph.hash_str_array(&src).ok_or_else(|| {
                            anyhow!("Unknown SWHID {:?}", String::from_utf8_lossy(&src))
                        })?;
                        let mut dst = mph.hash_str_array(&dst).ok_or_else(|| {
                            anyhow!("Unknown SWHID {:?}", String::from_utf8_lossy(&dst))
                        })?;
                        if transposed {
                            (src, dst) = (dst, src);
                        }
                        assert!(src < num_nodes, "src node id is greater than {num_nodes}");
                        assert!(dst < num_nodes, "dst node id is greater than {num_nodes}");
                        let src = order.get(src).expect("Could not permute src");
                        let dst = order.get(dst).expect("Could not permute dst");
                        Ok(((src, dst), label))
                    },
                ),
        )
        .context("Could not sort pairs")?;

    // Somewhat incorrect, we would need Ordering::Release here (and Ordering::Acquire in the
    // worker threads). But it's only an approximation so we don't care (plus the worker threads
    // should be shut down now even if the compiler doesn't know it).
    //
    // TODO: use total_labeled_arcs.into_inner() after webgraph 0.6.1, as it will remove
    // the constraint that the closure that borrowed total_labeled_arcs must outlive sorted_arcs.
    let total_labeled_arcs = labeled_arcs_counters
        .iter()
        .map(|counter| counter.load(Ordering::Relaxed))
        .sum();

    let mut pl = concurrent_progress_logger!(
        log_target = "swh_graph::compress::bv::edge_labels::merge",
        display_memory = true,
        item_name = "arc",
        local_speed = true,
        expected_updates = Some(total_labeled_arcs),
    );
    pl.start("Merging arc labels");

    // Compress each partition of labels independently
    struct MergedPartition {
        num_offsets: u64,
        length: u64,
        labels_reader: BufBitReader<BE, WordAdapter<u32, BufReader<File>>>,
        offsets_reader: BufBitReader<BE, WordAdapter<u32, BufReader<File>>>,
    }
    let merged_arcs_path = temp_dir.path().join("merged_arcs");
    std::fs::create_dir(&merged_arcs_path)
        .with_context(|| format!("Could not create {}", merged_arcs_path.display()))?;
    let partitions = Vec::from(sorted_arcs.iters)
        .into_par_iter()
        .enumerate()
        .map_with(pl.clone(), |pl, (partition_id, sorted_arcs_partition)| {
            let open_options = File::options()
                .create_new(true)
                .read(true)
                .append(true)
                .clone();
            let labels_path = merged_arcs_path.join(format!("{partition_id}.labels"));
            let mut labels_writer =
                BufBitWriter::<BE, _, _>::new(WordAdapter::<u32, _>::new(BufWriter::new(
                    open_options
                        .open(&labels_path)
                        .with_context(|| format!("Could not create {}", labels_path.display()))?,
                )));

            let offsets_path = merged_arcs_path.join(format!("{partition_id}.offsets"));
            let mut offsets_writer =
                BufBitWriter::<BE, _, _>::new(WordAdapter::<u32, _>::new(BufWriter::new(
                    open_options
                        .open(&offsets_path)
                        .with_context(|| format!("Could not create {}", offsets_path.display()))?,
                )));
            let mut total_length = 0u64;
            let mut num_offsets = 0u64;

            // no sorted_arcs_partition.dedup() on labels
            let graph = ArcListGraph::new_labeled(num_nodes, sorted_arcs_partition.into_iter())
                .iter_from(sorted_arcs.boundaries[partition_id])
                .take(
                    sorted_arcs.boundaries[partition_id + 1]
                        .checked_sub(sorted_arcs.boundaries[partition_id])
                        .expect("sorted_arcs.boundaries is not sorted"),
                );

            for_!( (_src, successors) in graph {
                let mut length = 0u64;
                for (_dst, labels) in &successors.group_by(|(dst, _label)| *dst) {
                    let mut labels: Vec<u64> = labels
                        .flat_map(|(_dst, label)| label)
                        .map(|label: NonMaxU64| u64::from(label))
                        .collect();
                    labels.par_sort_unstable();

                    // Write length-prefixed list of labels
                    length = length
                        .checked_add(
                            labels_writer
                                .write_gamma(labels.len() as u64)
                                .context("Could not write number of labels")?
                                as u64,
                        )
                        .context("length overflowed u64")?;
                    for label in labels {
                        length = length
                            .checked_add(
                                labels_writer
                                    .write_bits(label, label_width)
                                    .context("Could not write label")?
                                    as u64,
                            )
                            .context("length overflowed u64")?;
                        pl.light_update();
                    }
                }

                // Write length of this node's label list
                offsets_writer
                    .write_gamma(length)
                    .context("Could not write length")?;
                num_offsets += 1;
                total_length += length;
            });

            let mut labels_file = labels_writer
                .into_inner()
                .context("Could not flush labels bit writer")?
                .into_inner()
                .into_inner()
                .map_err(|e| e.into_error())
                .context("Could not flush labels byte writer")?;
            labels_file
                .rewind()
                .context("Could not rewind labels file")?;
            let labels_reader =
                BufBitReader::<BE, _>::new(WordAdapter::<u32, _>::new(BufReader::new(labels_file)));
            let mut offsets_file = offsets_writer
                .into_inner()
                .context("Could not flush offsets bit writer")?
                .into_inner()
                .into_inner()
                .map_err(|e| e.into_error())
                .context("Could not flush offsets byte writer")?;
            offsets_file
                .rewind()
                .context("Could not rewind labels offsets file")?;
            let offsets_reader = BufBitReader::<BE, _>::new(WordAdapter::<u32, _>::new(
                BufReader::new(offsets_file),
            ));
            Ok(MergedPartition {
                num_offsets,
                length: total_length,
                offsets_reader,
                labels_reader,
            })
        })
        .collect::<Result<Vec<_>>>()
        .context("Could not merge labels")?;
    pl.done();
    drop(pl);

    let mut labels_path = target_dir.to_owned();
    labels_path.as_mut_os_string().push("-labelled.labels");
    let mut labels_writer = BufBitWriter::<BE, _>::new(WordAdapter::<u8, _>::new(BufWriter::new(
        File::create(&labels_path)
            .with_context(|| format!("Could not create {}", labels_path.display()))?,
    )));

    let mut offsets_path = target_dir.to_owned();
    offsets_path
        .as_mut_os_string()
        .push("-labelled.labeloffsets");
    let mut offsets_writer =
        BufBitWriter::<BE, _, _>::new(WordAdapter::<u8, _>::new(BufWriter::new(
            File::create(&offsets_path)
                .with_context(|| format!("Could not create {}", offsets_path.display()))?,
        )));

    // Write offset (in *bits*) of the adjacency list of the first node
    offsets_writer
        .write_gamma(0)
        .context("Could not write initial offset")?;

    let mut total_num_offsets = 0u64;
    let mut total_length = 0u64;
    let mut labels_readers = Vec::new();
    let mut offsets_readers = Vec::new();
    for MergedPartition {
        num_offsets,
        length,
        labels_reader,
        offsets_reader,
    } in partitions
    {
        total_length = total_length
            .checked_add(length)
            .context("total_length overflowed u64")?;
        total_num_offsets = total_num_offsets
            .checked_add(num_offsets)
            .context("num_offsets overflowed u64")?;
        labels_readers.push((length, labels_reader));
        offsets_readers.push((num_offsets, offsets_reader));
    }
    let (r1, r2) = rayon::join(
        || -> Result<()> {
            let mut pl = progress_logger!(
                log_target = "swh_graph::compress::bv::edge_labels::write_offsets",
                display_memory = true,
                item_name = "offset",
                local_speed = true,
                expected_updates = total_num_offsets.try_into().ok(),
            );
            pl.start("Writing offsets");
            for (num_offsets, mut offsets_reader) in offsets_readers {
                for _ in 0..num_offsets {
                    let offset = offsets_reader
                        .read_gamma()
                        .context("Could not read length")?;
                    offsets_writer
                        .write_gamma(offset)
                        .context("Could not write offset")?;
                }
            }
            drop(
                offsets_writer
                    .into_inner()
                    .context("Could not flush label offsets bit writer")?
                    .into_inner()
                    .into_inner()
                    .context("Could not flush label offsets bufwriter")?,
            );
            pl.done();
            Ok(())
        },
        || -> Result<()> {
            let mut pl = progress_logger!(
                log_target = "swh_graph::compress::bv::edge_labels::write_labels",
                display_memory = true,
                item_name = "bit",
                local_speed = true,
                expected_updates = total_length.try_into().ok(),
            );
            pl.start("Writing arc labels");
            for (mut remaining_bits, mut labels_reader) in labels_readers {
                while remaining_bits > 0 {
                    let chunk_bits = remaining_bits.min(4 * 1024 * 1024 * 8); // 4MiB, arbitrary
                    labels_writer
                        .copy_from(&mut labels_reader, chunk_bits)
                        .context("Could not copy chunk of labels")?;
                    pl.update_with_count(chunk_bits as usize);
                    remaining_bits = remaining_bits
                        .checked_sub(chunk_bits)
                        .expect("inconsistent arithmetic");
                }
            }
            drop(
                labels_writer
                    .into_inner()
                    .context("Could not close labels bit writer")?
                    .into_inner()
                    .into_inner()
                    .context("Could not flush labels bufwriter")?,
            );
            pl.done();
            Ok(())
        },
    );
    r1.context("Could not write offsets")?;
    r2.context("Could not write labels")?;

    drop(temp_dir); // Prevent early deletion

    Ok(label_width)
}

fn label_width(hasher: &LabelNameHasher) -> usize {
    use crate::labels::{
        Branch, DirEntry, EdgeLabel, LabelNameId, Permission, UntypedEdgeLabel, Visit, VisitStatus,
        VisitType,
    };
    let num_label_names = u64::try_from(hasher.len()).expect("number of labels overflows u64");

    // Visit timestamps cannot be larger than the current timestamp
    let max_visit_timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Could not get current time")
        .as_secs();

    let max_label = [
        EdgeLabel::Branch(Branch::new(LabelNameId(num_label_names)).unwrap()),
        EdgeLabel::DirEntry(DirEntry::new(Permission::None, LabelNameId(num_label_names)).unwrap()),
        EdgeLabel::Visit(
            Visit::new(VisitStatus::Full, max_visit_timestamp, VisitType::Unknown).unwrap(),
        ),
    ]
    .into_iter()
    .map(|label| UntypedEdgeLabel::from(label).0) // Convert to on-disk representation
    .max()
    .unwrap();
    width_for_max_label_value(max_label)
}

/// Given the maximum label, returns the number of bits needed to represent labels
fn width_for_max_label_value(max_label: u64) -> usize {
    let num_label_values = max_label + 1; // because we want to represent all values from 0 to max_label inclusive
    let num_values = num_label_values + 1; // because the max value is used to represent the lack of value (ie. None)
    num_values
        .next_power_of_two() // because checked_ilog2() rounds down
        .checked_ilog2()
        .unwrap() as usize
}

#[test]
fn test_width_for_max_label_value() {
    assert_eq!(width_for_max_label_value(0), 1); // values are 0 and None
    assert_eq!(width_for_max_label_value(1), 2); // values are 0, 1, and None
    assert_eq!(width_for_max_label_value(2), 2); // values are 0, 1, 2, and None
    for i in 3..=6 {
        assert_eq!(width_for_max_label_value(i), 3);
    }
    for i in 7..=14 {
        assert_eq!(width_for_max_label_value(i), 4);
    }
    assert_eq!(width_for_max_label_value(15), 5);
}

#[derive(Clone, Copy)]
struct LabelDeserializer {
    label_width: usize,
}
#[derive(Clone, Copy)]
struct LabelSerializer {
    label_width: usize,
}

impl BitDeserializer<NE, BitReader<NE>> for LabelDeserializer {
    type DeserType = Option<NonMaxU64>;
    fn deserialize(
        &self,
        bitstream: &mut BitReader<NE>,
    ) -> Result<Self::DeserType, <BitReader<NE> as BitRead<NE>>::Error> {
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

impl BitSerializer<NE, BitWriter<NE>> for LabelSerializer {
    type SerType = Option<NonMaxU64>;
    fn serialize(
        &self,
        value: &Self::SerType,
        bitstream: &mut BitWriter<NE>,
    ) -> Result<usize, <BitWriter<NE> as BitWrite<NE>>::Error> {
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
