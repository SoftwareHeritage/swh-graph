// Copyright (C) 2023-2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

//! Iterator on the set of all arcs in an ORC dataset

use std::path::PathBuf;

use anyhow::Result;
use ar_row::deserialize::{ArRowDeserialize, ArRowStruct};
use ar_row_derive::ArRowDeserialize;
use orc_rust::arrow_reader::ArrowReaderBuilder;
use orc_rust::projection::ProjectionMask;
use orc_rust::reader::ChunkReader;
use rayon::prelude::*;

use super::orc::{get_dataset_readers, iter_arrow};
use crate::NodeType;

fn count_arrow_rows<R: ChunkReader>(reader_builder: ArrowReaderBuilder<R>) -> u64 {
    let empty_mask = ProjectionMask::roots(reader_builder.file_metadata().root_data_type(), []); // Don't need to read any column
    let reader = reader_builder.with_projection(empty_mask).build();
    reader.total_row_count()
}

pub fn estimate_node_count(dataset_dir: &PathBuf, allowed_node_types: &[NodeType]) -> Result<u64> {
    let mut readers = Vec::new();
    if allowed_node_types.contains(&NodeType::Directory) {
        readers.extend(get_dataset_readers(dataset_dir, "directory")?);
    }
    if allowed_node_types.contains(&NodeType::Content) {
        readers.extend(get_dataset_readers(dataset_dir, "content")?);
    }
    if allowed_node_types.contains(&NodeType::Origin) {
        readers.extend(get_dataset_readers(dataset_dir, "origin")?);
    }
    if allowed_node_types.contains(&NodeType::Release) {
        readers.extend(get_dataset_readers(dataset_dir, "release")?);
    }
    if allowed_node_types.contains(&NodeType::Revision) {
        readers.extend(get_dataset_readers(dataset_dir, "revision")?);
    }
    if allowed_node_types.contains(&NodeType::Snapshot) {
        readers.extend(get_dataset_readers(dataset_dir, "snapshot")?);
    }
    Ok(readers.into_par_iter().map(count_arrow_rows).sum())
}

pub fn estimate_edge_count(dataset_dir: &PathBuf, allowed_node_types: &[NodeType]) -> Result<u64> {
    let mut readers = Vec::new();
    if allowed_node_types.contains(&NodeType::Directory) {
        readers.extend(get_dataset_readers(dataset_dir, "directory_entry")?)
    }
    if allowed_node_types.contains(&NodeType::Origin) {
        readers.extend(get_dataset_readers(
            // Count the source...
            dataset_dir.clone(),
            "origin_visit_status",
        )?);
        readers.extend(get_dataset_readers(
            // ... and destination of each arc
            dataset_dir.clone(),
            "origin_visit_status",
        )?);
    }
    if allowed_node_types.contains(&NodeType::Release) {
        readers.extend(get_dataset_readers(dataset_dir, "release")?);
    }
    if allowed_node_types.contains(&NodeType::Revision) {
        readers.extend(get_dataset_readers(dataset_dir, "revision")?);
        readers.extend(get_dataset_readers(dataset_dir, "revision_history")?);
    }
    if allowed_node_types.contains(&NodeType::Snapshot) {
        readers.extend(get_dataset_readers(dataset_dir, "snapshot_branch")?);
    }
    Ok(readers.into_par_iter().map(count_arrow_rows).sum())
}

type EdgeStats = [[usize; NodeType::NUMBER_OF_TYPES]; NodeType::NUMBER_OF_TYPES];

pub fn count_edge_types(
    dataset_dir: &PathBuf,
    allowed_node_types: &[NodeType],
) -> Result<impl ParallelIterator<Item = EdgeStats>> {
    let maybe_get_dataset_readers = |dataset_dir, subdirectory, node_type| {
        if allowed_node_types.contains(&node_type) {
            get_dataset_readers(dataset_dir, subdirectory)
        } else {
            Ok(Vec::new())
        }
    };

    Ok([]
        .into_par_iter()
        .chain(
            maybe_get_dataset_readers(dataset_dir, "directory_entry", NodeType::Directory)?
                .into_par_iter()
                .map(count_edge_types_from_dir),
        )
        .chain(
            maybe_get_dataset_readers(dataset_dir, "origin_visit_status", NodeType::Origin)?
                .into_par_iter()
                .map(count_edge_types_from_ovs),
        )
        .chain(
            maybe_get_dataset_readers(dataset_dir, "release", NodeType::Release)?
                .into_par_iter()
                .map(count_edge_types_from_rel),
        )
        .chain(
            maybe_get_dataset_readers(dataset_dir, "revision", NodeType::Revision)?
                .into_par_iter()
                .map(count_dir_edge_types_from_rev),
        )
        .chain(
            maybe_get_dataset_readers(dataset_dir, "revision_history", NodeType::Revision)?
                .into_par_iter()
                .map(count_parent_edge_types_from_rev),
        )
        .chain(
            maybe_get_dataset_readers(dataset_dir, "snapshot_branch", NodeType::Snapshot)?
                .into_par_iter()
                .map(count_edge_types_from_snp),
        ))
}

fn for_each_edge<T, F, R: ChunkReader + Send>(reader_builder: ArrowReaderBuilder<R>, mut f: F)
where
    F: FnMut(T) + Send + Sync,
    T: ArRowDeserialize + ArRowStruct + Send,
{
    iter_arrow(reader_builder, move |record: T| -> [(); 0] {
        f(record);
        []
    })
    .count();
}

fn inc(stats: &mut EdgeStats, src_type: NodeType, dst_type: NodeType) {
    stats[src_type as usize][dst_type as usize] += 1;
}

fn count_edge_types_from_dir<R: ChunkReader + Send>(
    reader_builder: ArrowReaderBuilder<R>,
) -> EdgeStats {
    let mut stats = EdgeStats::default();

    #[derive(ArRowDeserialize, Default, Clone)]
    struct DirectoryEntry {
        r#type: String,
    }

    for_each_edge(reader_builder, |entry: DirectoryEntry| {
        match entry.r#type.as_bytes() {
            b"file" => {
                inc(&mut stats, NodeType::Directory, NodeType::Content);
            }
            b"dir" => {
                inc(&mut stats, NodeType::Directory, NodeType::Directory);
            }
            b"rev" => {
                inc(&mut stats, NodeType::Directory, NodeType::Revision);
            }
            _ => panic!("Unexpected directory entry type: {:?}", entry.r#type),
        }
    });

    stats
}

fn count_edge_types_from_ovs<R: ChunkReader + Send>(
    reader_builder: ArrowReaderBuilder<R>,
) -> EdgeStats {
    let mut stats = EdgeStats::default();

    #[derive(ArRowDeserialize, Default, Clone)]
    struct OriginVisitStatus {
        snapshot: Option<String>,
    }

    for_each_edge(reader_builder, |ovs: OriginVisitStatus| {
        if ovs.snapshot.is_some() {
            inc(&mut stats, NodeType::Origin, NodeType::Snapshot)
        }
    });

    stats
}

fn count_dir_edge_types_from_rev<R: ChunkReader + Send>(
    reader_builder: ArrowReaderBuilder<R>,
) -> EdgeStats {
    let mut stats = EdgeStats::default();

    stats[NodeType::Revision as usize][NodeType::Directory as usize] +=
        count_arrow_rows(reader_builder) as usize;

    stats
}

fn count_parent_edge_types_from_rev<R: ChunkReader + Send>(
    reader_builder: ArrowReaderBuilder<R>,
) -> EdgeStats {
    let mut stats = EdgeStats::default();

    stats[NodeType::Revision as usize][NodeType::Revision as usize] +=
        count_arrow_rows(reader_builder) as usize;

    stats
}

fn count_edge_types_from_rel<R: ChunkReader + Send>(
    reader_builder: ArrowReaderBuilder<R>,
) -> EdgeStats {
    let mut stats = EdgeStats::default();
    #[derive(ArRowDeserialize, Default, Clone)]
    struct Release {
        target_type: String,
    }

    for_each_edge(reader_builder, |entry: Release| {
        match entry.target_type.as_bytes() {
            b"content" => {
                inc(&mut stats, NodeType::Release, NodeType::Content);
            }
            b"directory" => {
                inc(&mut stats, NodeType::Release, NodeType::Directory);
            }
            b"revision" => {
                inc(&mut stats, NodeType::Release, NodeType::Revision);
            }
            b"release" => {
                inc(&mut stats, NodeType::Release, NodeType::Release);
            }
            _ => panic!("Unexpected directory entry type: {:?}", entry.target_type),
        }
    });

    stats
}

fn count_edge_types_from_snp<R: ChunkReader + Send>(
    reader_builder: ArrowReaderBuilder<R>,
) -> EdgeStats {
    let mut stats = EdgeStats::default();

    #[derive(ArRowDeserialize, Default, Clone)]
    struct SnapshotBranch {
        target_type: String,
    }

    for_each_edge(reader_builder, |branch: SnapshotBranch| {
        match branch.target_type.as_bytes() {
            b"content" => {
                inc(&mut stats, NodeType::Snapshot, NodeType::Content);
            }
            b"directory" => {
                inc(&mut stats, NodeType::Snapshot, NodeType::Directory);
            }
            b"revision" => {
                inc(&mut stats, NodeType::Snapshot, NodeType::Revision);
            }
            b"release" => {
                inc(&mut stats, NodeType::Snapshot, NodeType::Release);
            }
            b"alias" => {}
            _ => panic!("Unexpected snapshot branch type: {:?}", branch.target_type),
        }
    });

    stats
}
