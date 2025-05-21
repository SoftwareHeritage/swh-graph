// Copyright (C) 2023-2025  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::fs::File;
use std::io::BufWriter;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Context, Result};
use dsi_bitstream::codes::GammaWrite;
use dsi_bitstream::prelude::{BitRead, BitWrite, BufBitWriter, WordAdapter, BE, NE};
use dsi_progress_logger::{concurrent_progress_logger, progress_logger, ProgressLog};
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
use crate::mph::LoadableSwhidMphf;
use crate::utils::sort::par_sort_arcs;

pub fn bv<MPHF: LoadableSwhidMphf + Sync>(
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
    let sorted_arcs = par_sort_arcs(
        &sorted_arcs_path,
        sort_batch_size,
        iter_arcs(&dataset_dir, allowed_node_types)
            .context("Could not open input files to read arcs")?
            .map_with(pl.clone(), |thread_pl, (src, dst)| {
                thread_pl.light_update();
                (src, dst)
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
            assert!(src < num_nodes, "src node id is greater than {num_nodes}");
            assert!(dst < num_nodes, "dst node id is greater than {num_nodes}");
            let partition_id = src / nodes_per_partition;
            buffer.insert(partition_id, src, dst)?;
            Ok(())
        },
    )?;
    pl.done();

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
    BvComp::parallel_iter::<BE, _>(
        target_dir,
        arc_list_graphs,
        num_nodes,
        comp_flags,
        &rayon::ThreadPoolBuilder::default()
            .build()
            .expect("Could not create BvComp thread pool"),
        &temp_bv_dir,
    )
    .context("Could not build BVGraph from arcs")?;

    drop(temp_dir); // Prevent early deletion

    Ok(())
}

/// Writes `-labelled.labels`,  `-labelled.labeloffsets`, and returns the label width
#[allow(clippy::too_many_arguments)]
pub fn edge_labels<MPHF: LoadableSwhidMphf + Sync>(
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

    let mut pl = concurrent_progress_logger!(
        display_memory = true,
        item_name = "arc",
        local_speed = true,
        expected_updates = Some(
            estimate_edge_count(&dataset_dir, allowed_node_types)
                .context("Could not estimate edge count")? as usize,
        ),
    );
    pl.start("Reading and sorting arcs");

    // Sort in parallel in a bunch of SortPairs instances
    let temp_dir = tempfile::tempdir().context("Could not get temporary_directory")?;
    let sorted_arcs_path = temp_dir.path().join("sorted_arcs");
    std::fs::create_dir(&sorted_arcs_path)
        .with_context(|| format!("Could not create {}", sorted_arcs_path.display()))?;
    let sorted_arcs = par_sort_arcs(
        &sorted_arcs_path,
        sort_batch_size,
        iter_labeled_arcs(&dataset_dir, allowed_node_types, label_name_hasher)
            .context("Could not open input files to read arcs")?
            .map_with(pl.clone(), |thread_pl, (src, dst, label)| {
                thread_pl.light_update();
                (src, dst, label)
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
            assert!(src < num_nodes, "src node id is greater than {num_nodes}");
            assert!(dst < num_nodes, "dst node id is greater than {num_nodes}");
            let src = order.get(src).expect("Could not permute src");
            let dst = order.get(dst).expect("Could not permute dst");
            let partition_id = src / nodes_per_partition;
            buffer.insert_labeled(partition_id, src, dst, label)?;
            Ok(())
        },
    )?;
    let total_labeled_arcs = pl.count();
    pl.done();

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

    let mut labels_path = target_dir.to_owned();
    labels_path.as_mut_os_string().push("-labelled.labels");
    let mut labels_writer =
        BufBitWriter::<BE, _, _>::new(WordAdapter::<u8, _>::new(BufWriter::new(
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

    let mut pl = progress_logger!(
        display_memory = true,
        item_name = "arc",
        local_speed = true,
        expected_updates = Some(total_labeled_arcs),
    );
    pl.start("Writing arc labels");

    // Write offset (in *bits*) of the adjacency list of the first node
    offsets_writer
        .write_gamma(0)
        .context("Could not write initial offset")?;

    for partition_graph in arc_list_graphs {
        for_!( (_src, successors) in partition_graph {
            let mut offset_bits = 0u64;
            for (_dst, labels) in &successors.group_by(|(dst, _label)| *dst) {
                let mut labels: Vec<u64> = labels
                    .flat_map(|(_dst, label)| label)
                    .map(|label: NonMaxU64| u64::from(label))
                    .collect();
                labels.par_sort_unstable();
                pl.update_with_count(labels.len());

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

            // Write offset of the end of this edge's label list (and start of the next one)
            offsets_writer
                .write_gamma(offset_bits)
                .context("Could not write offset")?;
        });
    }

    drop(
        labels_writer
            .into_inner()
            .context("Could not flush labels writer")?
            .into_inner()
            .into_inner()
            .context("Could not flush labels bufwriter")?,
    );
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
        Branch, DirEntry, EdgeLabel, LabelNameId, Permission, UntypedEdgeLabel, Visit, VisitStatus,
    };
    let num_label_names = mphf.num_keys();

    // Visit timestamps cannot be larger than the current timestamp
    let max_visit_timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Could not get current time")
        .as_secs();

    let max_label = [
        EdgeLabel::Branch(Branch::new(LabelNameId(num_label_names)).unwrap()),
        EdgeLabel::DirEntry(DirEntry::new(Permission::None, LabelNameId(num_label_names)).unwrap()),
        EdgeLabel::Visit(Visit::new(VisitStatus::Full, max_visit_timestamp).unwrap()),
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
