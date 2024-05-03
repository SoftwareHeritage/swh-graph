// Copyright (C) 2023-2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

/// Readers for the ORC dataset.
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use ar_row::deserialize::{ArRowDeserialize, ArRowStruct};
use ar_row_derive::ArRowDeserialize;
use orc_rust::arrow_reader::ArrowReaderBuilder;
use orc_rust::projection::ProjectionMask;
use orc_rust::reader::ChunkReader;
use rayon::prelude::*;

use crate::SWHType;

const SWHID_TXT_SIZE: usize = 50;
type TextSwhid = [u8; SWHID_TXT_SIZE];

pub(crate) const ORC_BATCH_SIZE: usize = 1024;
/// The value was computed experimentally to minimize both run time and memory,
/// by running `swh-graph-extract extract-nodes` on the 2023-09-06 dataset,
/// on Software Heritage's Maxxi computer (Xeon Gold 6342 CPU @ 2.80GHz,
/// 96 threads, 4TB RAM)

pub(crate) fn get_dataset_readers<P: AsRef<Path>>(
    dataset_dir: P,
    subdirectory: &str,
) -> Result<Vec<ArrowReaderBuilder<std::fs::File>>> {
    let mut dataset_dir = dataset_dir.as_ref().to_owned();
    dataset_dir.push(subdirectory);
    std::fs::read_dir(&dataset_dir)
        .with_context(|| format!("Could not list {}", dataset_dir.display()))?
        .map(|file_path| {
            let file_path = file_path
                .with_context(|| format!("Failed to list {}", dataset_dir.display()))?
                .path();
            let file = std::fs::File::open(&file_path)
                .with_context(|| format!("Could not open {}", file_path.display()))?;
            let builder = ArrowReaderBuilder::try_new(file)
                .with_context(|| format!("Could not read {}", file_path.display()))?;
            Ok(builder)
        })
        .collect()
}

pub(crate) fn iter_arrow<R: ChunkReader, T, IntoIterU, U, F>(
    reader_builder: ArrowReaderBuilder<R>,
    mut f: F,
) -> impl Iterator<Item = U>
where
    F: FnMut(T) -> IntoIterU,
    IntoIterU: IntoIterator<Item = U>,
    T: ArRowDeserialize + ArRowStruct,
{
    let field_names = <T>::columns();
    let projection = ProjectionMask::named_roots(
        reader_builder.file_metadata().root_data_type(),
        field_names.as_slice(),
    );
    let reader = reader_builder
        .with_projection(projection)
        .with_batch_size(ORC_BATCH_SIZE)
        .build();

    reader.flat_map(move |chunk| {
        let chunk: arrow_array::RecordBatch =
            chunk.unwrap_or_else(|e| panic!("Could not read chunk: {}", e));
        let items: Vec<T> = T::from_record_batch(chunk).expect("Could not deserialize from arrow");
        items.into_iter().flat_map(&mut f).collect::<Vec<_>>()
    })
}

pub(crate) fn par_iter_arrow<R: ChunkReader + Send, T: Send, IntoIterU, U: Send, F>(
    reader_builder: ArrowReaderBuilder<R>,
    f: F,
) -> impl ParallelIterator<Item = U>
where
    F: Fn(T) -> IntoIterU + Send + Sync,
    IntoIterU: IntoIterator<Item = U> + Send + Sync,
    T: ArRowDeserialize + ArRowStruct,
{
    let field_names = <T>::columns();
    let projection = ProjectionMask::named_roots(
        reader_builder.file_metadata().root_data_type(),
        field_names.as_slice(),
    );
    let reader = reader_builder
        .with_projection(projection)
        .with_batch_size(ORC_BATCH_SIZE)
        .build();

    reader.par_bridge().flat_map_iter(move |chunk| {
        let chunk: arrow_array::RecordBatch =
            chunk.unwrap_or_else(|e| panic!("Could not read chunk: {}", e));
        let items: Vec<T> = T::from_record_batch(chunk).expect("Could not deserialize from arrow");
        items.into_iter().flat_map(&f).collect::<Vec<_>>()
    })
}

fn count_arrow_rows<R: ChunkReader>(reader_builder: ArrowReaderBuilder<R>) -> u64 {
    let empty_mask = ProjectionMask::roots(reader_builder.file_metadata().root_data_type(), []); // Don't need to read any column
    let reader = reader_builder.with_projection(empty_mask).build();
    reader.total_row_count()
}

pub fn estimate_node_count(dataset_dir: &PathBuf, allowed_node_types: &[SWHType]) -> Result<u64> {
    let mut readers = Vec::new();
    if allowed_node_types.contains(&SWHType::Directory) {
        readers.extend(get_dataset_readers(dataset_dir, "directory")?);
    }
    if allowed_node_types.contains(&SWHType::Content) {
        readers.extend(get_dataset_readers(dataset_dir, "content")?);
    }
    if allowed_node_types.contains(&SWHType::Origin) {
        readers.extend(get_dataset_readers(dataset_dir, "origin")?);
    }
    if allowed_node_types.contains(&SWHType::Release) {
        readers.extend(get_dataset_readers(dataset_dir, "release")?);
    }
    if allowed_node_types.contains(&SWHType::Revision) {
        readers.extend(get_dataset_readers(dataset_dir, "revision")?);
    }
    if allowed_node_types.contains(&SWHType::Snapshot) {
        readers.extend(get_dataset_readers(dataset_dir, "snapshot")?);
    }
    Ok(readers.into_par_iter().map(count_arrow_rows).sum())
}

pub fn estimate_edge_count(dataset_dir: &PathBuf, allowed_node_types: &[SWHType]) -> Result<u64> {
    let mut readers = Vec::new();
    if allowed_node_types.contains(&SWHType::Directory) {
        readers.extend(get_dataset_readers(dataset_dir, "directory_entry")?)
    }
    if allowed_node_types.contains(&SWHType::Origin) {
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
    if allowed_node_types.contains(&SWHType::Release) {
        readers.extend(get_dataset_readers(dataset_dir, "release")?);
    }
    if allowed_node_types.contains(&SWHType::Revision) {
        readers.extend(get_dataset_readers(dataset_dir, "revision")?);
        readers.extend(get_dataset_readers(dataset_dir, "revision_history")?);
    }
    if allowed_node_types.contains(&SWHType::Snapshot) {
        readers.extend(get_dataset_readers(dataset_dir, "snapshot_branch")?);
    }
    Ok(readers.into_par_iter().map(count_arrow_rows).sum())
}

pub fn iter_swhids(
    dataset_dir: &PathBuf,
    allowed_node_types: &[SWHType],
) -> Result<impl ParallelIterator<Item = TextSwhid>> {
    let maybe_get_dataset_readers = |dataset_dir: &PathBuf, subdirectory, node_type| {
        if allowed_node_types.contains(&node_type) {
            get_dataset_readers(dataset_dir, subdirectory)
        } else {
            Ok(Vec::new())
        }
    };

    Ok([]
        .into_par_iter()
        .chain(
            maybe_get_dataset_readers(dataset_dir, "directory", SWHType::Directory)?
                .into_par_iter()
                .flat_map(iter_swhids_from_dir),
        )
        .chain(
            maybe_get_dataset_readers(dataset_dir, "directory_entry", SWHType::Directory)?
                .into_par_iter()
                .flat_map(iter_swhids_from_dir_entry),
        )
        .chain(
            maybe_get_dataset_readers(dataset_dir, "content", SWHType::Content)?
                .into_par_iter()
                .flat_map(iter_swhids_from_cnt),
        )
        .chain(
            maybe_get_dataset_readers(dataset_dir, "origin", SWHType::Origin)?
                .into_par_iter()
                .flat_map(iter_swhids_from_ori),
        )
        .chain(
            maybe_get_dataset_readers(dataset_dir, "origin_visit_status", SWHType::Origin)?
                .into_par_iter()
                .flat_map_iter(iter_arcs_from_ovs)
                .flat_map_iter(|(src, dst)| [src, dst].into_iter()),
        )
        .chain(
            maybe_get_dataset_readers(dataset_dir, "release", SWHType::Release)?
                .into_par_iter()
                .flat_map(iter_rel_swhids_from_rel),
        )
        .chain(
            maybe_get_dataset_readers(dataset_dir, "release", SWHType::Release)?
                .into_par_iter()
                .flat_map(iter_target_swhids_from_rel),
        )
        .chain(
            maybe_get_dataset_readers(dataset_dir, "revision", SWHType::Revision)?
                .into_par_iter()
                .flat_map(iter_rev_swhids_from_rev),
        )
        .chain(
            maybe_get_dataset_readers(dataset_dir, "revision", SWHType::Revision)?
                .into_par_iter()
                .flat_map(iter_dir_swhids_from_rev),
        )
        .chain(
            maybe_get_dataset_readers(dataset_dir, "revision_history", SWHType::Revision)?
                .into_par_iter()
                .flat_map(iter_parent_swhids_from_rev),
        )
        .chain(
            maybe_get_dataset_readers(dataset_dir, "snapshot", SWHType::Snapshot)?
                .into_par_iter()
                .flat_map(iter_swhids_from_snp),
        )
        .chain(
            maybe_get_dataset_readers(dataset_dir, "snapshot_branch", SWHType::Snapshot)?
                .into_par_iter()
                .flat_map(iter_swhids_from_snp_branch),
        ))
}

fn map_swhids<R: ChunkReader + Send, T: Send, F>(
    reader_builder: ArrowReaderBuilder<R>,
    f: F,
) -> impl ParallelIterator<Item = TextSwhid>
where
    F: Fn(T) -> Option<String> + Send + Sync,
    T: ArRowDeserialize + ArRowStruct,
{
    par_iter_arrow(reader_builder, move |record: T| {
        f(record).map(|swhid| swhid.as_bytes().try_into().unwrap())
    })
}

fn iter_swhids_from_dir_entry<R: ChunkReader + Send>(
    reader_builder: ArrowReaderBuilder<R>,
) -> impl ParallelIterator<Item = TextSwhid> {
    #[derive(ArRowDeserialize, Default, Clone)]
    struct DirectoryEntry {
        r#type: String,
        target: String,
    }

    map_swhids(reader_builder, |entry: DirectoryEntry| {
        Some(match entry.r#type.as_bytes() {
            b"file" => format!("swh:1:cnt:{}", entry.target),
            b"dir" => format!("swh:1:dir:{}", entry.target),
            b"rev" => format!("swh:1:rev:{}", entry.target),
            _ => panic!("Unexpected directory entry type: {:?}", entry.r#type),
        })
    })
}

fn iter_swhids_from_dir<R: ChunkReader + Send>(
    reader_builder: ArrowReaderBuilder<R>,
) -> impl ParallelIterator<Item = TextSwhid> {
    #[derive(ArRowDeserialize, Default, Clone)]
    struct Directory {
        id: String,
    }

    map_swhids(reader_builder, |dir: Directory| {
        Some(format!("swh:1:dir:{}", dir.id))
    })
}

fn iter_swhids_from_cnt<R: ChunkReader + Send>(
    reader_builder: ArrowReaderBuilder<R>,
) -> impl ParallelIterator<Item = TextSwhid> {
    #[derive(ArRowDeserialize, Default, Clone)]
    struct Content {
        sha1_git: String,
    }

    map_swhids(reader_builder, |cnt: Content| {
        Some(format!("swh:1:cnt:{}", cnt.sha1_git))
    })
}

fn iter_swhids_from_ori<R: ChunkReader + Send>(
    reader_builder: ArrowReaderBuilder<R>,
) -> impl ParallelIterator<Item = TextSwhid> {
    #[derive(ArRowDeserialize, Default, Clone)]
    struct Origin {
        id: String,
    }

    map_swhids(reader_builder, |ori: Origin| {
        Some(format!("swh:1:ori:{}", ori.id))
    })
}

fn iter_rel_swhids_from_rel<R: ChunkReader + Send>(
    reader_builder: ArrowReaderBuilder<R>,
) -> impl ParallelIterator<Item = TextSwhid> {
    #[derive(ArRowDeserialize, Default, Clone)]
    struct Release {
        id: String,
    }

    map_swhids(reader_builder, |rel: Release| {
        Some(format!("swh:1:rel:{}", rel.id))
    })
}

fn iter_target_swhids_from_rel<R: ChunkReader + Send>(
    reader_builder: ArrowReaderBuilder<R>,
) -> impl ParallelIterator<Item = TextSwhid> {
    #[derive(ArRowDeserialize, Default, Clone)]
    struct Release {
        target: String,
        target_type: String,
    }

    map_swhids(reader_builder, |entry: Release| {
        Some(match entry.target_type.as_bytes() {
            b"content" => format!("swh:1:cnt:{}", entry.target),
            b"directory" => format!("swh:1:dir:{}", entry.target),
            b"revision" => format!("swh:1:rev:{}", entry.target),
            b"release" => format!("swh:1:rel:{}", entry.target),
            _ => panic!("Unexpected release target type: {:?}", entry.target_type),
        })
    })
}

fn iter_rev_swhids_from_rev<R: ChunkReader + Send>(
    reader_builder: ArrowReaderBuilder<R>,
) -> impl ParallelIterator<Item = TextSwhid> {
    #[derive(ArRowDeserialize, Default, Clone)]
    struct Revision {
        id: String,
    }

    map_swhids(reader_builder, |dir: Revision| {
        Some(format!("swh:1:rev:{}", dir.id))
    })
}

fn iter_dir_swhids_from_rev<R: ChunkReader + Send>(
    reader_builder: ArrowReaderBuilder<R>,
) -> impl ParallelIterator<Item = TextSwhid> {
    #[derive(ArRowDeserialize, Default, Clone)]
    struct Revision {
        directory: String,
    }

    map_swhids(reader_builder, |rev: Revision| {
        Some(format!("swh:1:dir:{}", rev.directory))
    })
}

fn iter_parent_swhids_from_rev<R: ChunkReader + Send>(
    reader_builder: ArrowReaderBuilder<R>,
) -> impl ParallelIterator<Item = TextSwhid> {
    #[derive(ArRowDeserialize, Default, Clone)]
    struct RevisionParent {
        parent_id: String,
    }

    map_swhids(reader_builder, |rev: RevisionParent| {
        Some(format!("swh:1:rev:{}", rev.parent_id))
    })
}

fn iter_swhids_from_snp<R: ChunkReader + Send>(
    reader_builder: ArrowReaderBuilder<R>,
) -> impl ParallelIterator<Item = TextSwhid> {
    #[derive(ArRowDeserialize, Default, Clone)]
    struct Snapshot {
        id: String,
    }

    map_swhids(reader_builder, |dir: Snapshot| {
        Some(format!("swh:1:snp:{}", dir.id))
    })
}

fn iter_swhids_from_snp_branch<R: ChunkReader + Send>(
    reader_builder: ArrowReaderBuilder<R>,
) -> impl ParallelIterator<Item = TextSwhid> {
    #[derive(ArRowDeserialize, Default, Clone)]
    struct SnapshotBranch {
        target: String,
        target_type: String,
    }

    map_swhids(reader_builder, |branch: SnapshotBranch| {
        match branch.target_type.as_bytes() {
            b"content" => Some(format!("swh:1:cnt:{}", branch.target)),
            b"directory" => Some(format!("swh:1:dir:{}", branch.target)),
            b"revision" => Some(format!("swh:1:rev:{}", branch.target)),
            b"release" => Some(format!("swh:1:rel:{}", branch.target)),
            b"alias" => None,
            _ => panic!("Unexpected snapshot target type: {:?}", branch.target_type),
        }
    })
}

pub fn iter_arcs(
    dataset_dir: &PathBuf,
    allowed_node_types: &[SWHType],
) -> Result<impl ParallelIterator<Item = (TextSwhid, TextSwhid)>> {
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
            maybe_get_dataset_readers(dataset_dir, "directory_entry", SWHType::Directory)?
                .into_par_iter()
                .flat_map_iter(iter_arcs_from_dir_entry),
        )
        .chain(
            maybe_get_dataset_readers(dataset_dir, "origin_visit_status", SWHType::Origin)?
                .into_par_iter()
                .flat_map_iter(iter_arcs_from_ovs),
        )
        .chain(
            maybe_get_dataset_readers(dataset_dir, "release", SWHType::Release)?
                .into_par_iter()
                .flat_map_iter(iter_arcs_from_rel),
        )
        .chain(
            maybe_get_dataset_readers(dataset_dir, "revision", SWHType::Revision)?
                .into_par_iter()
                .flat_map_iter(iter_arcs_from_rev),
        )
        .chain(
            maybe_get_dataset_readers(dataset_dir, "revision_history", SWHType::Revision)?
                .into_par_iter()
                .flat_map_iter(iter_arcs_from_rev_history),
        )
        .chain(
            maybe_get_dataset_readers(dataset_dir, "snapshot_branch", SWHType::Snapshot)?
                .into_par_iter()
                .flat_map_iter(iter_arcs_from_snp_branch),
        ))
}

fn map_arcs<R: ChunkReader + Send, T: Send, F>(
    reader_builder: ArrowReaderBuilder<R>,
    f: F,
) -> impl Iterator<Item = (TextSwhid, TextSwhid)>
where
    F: Fn(T) -> Option<(String, String)> + Send + Sync,
    T: ArRowDeserialize + ArRowStruct,
{
    iter_arrow(reader_builder, move |record: T| {
        f(record).map(|(src_swhid, dst_swhid)| {
            (
                src_swhid.as_bytes().try_into().unwrap(),
                dst_swhid.as_bytes().try_into().unwrap(),
            )
        })
    })
}

fn iter_arcs_from_dir_entry<R: ChunkReader + Send>(
    reader_builder: ArrowReaderBuilder<R>,
) -> impl Iterator<Item = (TextSwhid, TextSwhid)> {
    #[derive(ArRowDeserialize, Default, Clone)]
    struct DirectoryEntry {
        directory_id: String,
        r#type: String,
        target: String,
    }

    map_arcs(reader_builder, |entry: DirectoryEntry| {
        Some((
            format!("swh:1:dir:{}", entry.directory_id),
            match entry.r#type.as_bytes() {
                b"file" => format!("swh:1:cnt:{}", entry.target),
                b"dir" => format!("swh:1:dir:{}", entry.target),
                b"rev" => format!("swh:1:rev:{}", entry.target),
                _ => panic!("Unexpected directory entry type: {:?}", entry.r#type),
            },
        ))
    })
}

fn iter_arcs_from_ovs<R: ChunkReader + Send>(
    reader_builder: ArrowReaderBuilder<R>,
) -> impl Iterator<Item = (TextSwhid, TextSwhid)> {
    #[derive(ArRowDeserialize, Default, Clone)]
    struct OriginVisitStatus {
        origin: String,
        snapshot: Option<String>,
    }

    map_arcs(reader_builder, |ovs: OriginVisitStatus| {
        use sha1::Digest;
        let mut hasher = sha1::Sha1::new();
        hasher.update(ovs.origin.as_bytes());

        ovs.snapshot.as_ref().map(|snapshot| {
            (
                format!("swh:1:ori:{:x}", hasher.finalize(),),
                format!("swh:1:snp:{}", snapshot),
            )
        })
    })
}

fn iter_arcs_from_rel<R: ChunkReader + Send>(
    reader_builder: ArrowReaderBuilder<R>,
) -> impl Iterator<Item = (TextSwhid, TextSwhid)> {
    #[derive(ArRowDeserialize, Default, Clone)]
    struct Release {
        id: String,
        target: String,
        target_type: String,
    }

    map_arcs(reader_builder, |entry: Release| {
        Some((
            format!("swh:1:rel:{}", entry.id),
            match entry.target_type.as_bytes() {
                b"content" => format!("swh:1:cnt:{}", entry.target),
                b"directory" => format!("swh:1:dir:{}", entry.target),
                b"revision" => format!("swh:1:rev:{}", entry.target),
                b"release" => format!("swh:1:rel:{}", entry.target),
                _ => panic!("Unexpected release target type: {:?}", entry.target_type),
            },
        ))
    })
}

fn iter_arcs_from_rev<R: ChunkReader + Send>(
    reader_builder: ArrowReaderBuilder<R>,
) -> impl Iterator<Item = (TextSwhid, TextSwhid)> {
    #[derive(ArRowDeserialize, Default, Clone)]
    struct Revision {
        id: String,
        directory: String,
    }

    map_arcs(reader_builder, |rev: Revision| {
        Some((
            format!("swh:1:rev:{}", rev.id),
            format!("swh:1:dir:{}", rev.directory),
        ))
    })
}

fn iter_arcs_from_rev_history<R: ChunkReader + Send>(
    reader_builder: ArrowReaderBuilder<R>,
) -> impl Iterator<Item = (TextSwhid, TextSwhid)> {
    #[derive(ArRowDeserialize, Default, Clone)]
    struct RevisionParent {
        id: String,
        parent_id: String,
    }

    map_arcs(reader_builder, |rev: RevisionParent| {
        Some((
            format!("swh:1:rev:{}", rev.id),
            format!("swh:1:rev:{}", rev.parent_id),
        ))
    })
}

fn iter_arcs_from_snp_branch<R: ChunkReader + Send>(
    reader_builder: ArrowReaderBuilder<R>,
) -> impl Iterator<Item = (TextSwhid, TextSwhid)> {
    #[derive(ArRowDeserialize, Default, Clone)]
    struct SnapshotBranch {
        snapshot_id: String,
        target: String,
        target_type: String,
    }

    map_arcs(reader_builder, |branch: SnapshotBranch| {
        let dst = match branch.target_type.as_bytes() {
            b"content" => Some(format!("swh:1:cnt:{}", branch.target)),
            b"directory" => Some(format!("swh:1:dir:{}", branch.target)),
            b"revision" => Some(format!("swh:1:rev:{}", branch.target)),
            b"release" => Some(format!("swh:1:rel:{}", branch.target)),
            b"alias" => None,
            _ => panic!("Unexpected snapshot target type: {:?}", branch.target_type),
        };
        dst.map(|dst| (format!("swh:1:snp:{}", branch.snapshot_id), dst))
    })
}

type EdgeStats = [[usize; SWHType::NUMBER_OF_TYPES]; SWHType::NUMBER_OF_TYPES];

pub fn count_edge_types(
    dataset_dir: &PathBuf,
    allowed_node_types: &[SWHType],
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
            maybe_get_dataset_readers(dataset_dir, "directory_entry", SWHType::Directory)?
                .into_par_iter()
                .map(count_edge_types_from_dir),
        )
        .chain(
            maybe_get_dataset_readers(dataset_dir, "origin_visit_status", SWHType::Origin)?
                .into_par_iter()
                .map(count_edge_types_from_ovs),
        )
        .chain(
            maybe_get_dataset_readers(dataset_dir, "release", SWHType::Release)?
                .into_par_iter()
                .map(count_edge_types_from_rel),
        )
        .chain(
            maybe_get_dataset_readers(dataset_dir, "revision", SWHType::Revision)?
                .into_par_iter()
                .map(count_dir_edge_types_from_rev),
        )
        .chain(
            maybe_get_dataset_readers(dataset_dir, "revision_history", SWHType::Revision)?
                .into_par_iter()
                .map(count_parent_edge_types_from_rev),
        )
        .chain(
            maybe_get_dataset_readers(dataset_dir, "snapshot_branch", SWHType::Snapshot)?
                .into_par_iter()
                .map(count_edge_types_from_snp),
        ))
}

fn for_each_edge<T: Send, F, R: ChunkReader + Send>(reader_builder: ArrowReaderBuilder<R>, mut f: F)
where
    F: FnMut(T) + Send + Sync,
    T: ArRowDeserialize + ArRowStruct,
{
    iter_arrow(reader_builder, move |record: T| -> [(); 0] {
        f(record);
        []
    })
    .count();
}

fn inc(stats: &mut EdgeStats, src_type: SWHType, dst_type: SWHType) {
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
                inc(&mut stats, SWHType::Directory, SWHType::Content);
            }
            b"dir" => {
                inc(&mut stats, SWHType::Directory, SWHType::Directory);
            }
            b"rev" => {
                inc(&mut stats, SWHType::Directory, SWHType::Revision);
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
            inc(&mut stats, SWHType::Origin, SWHType::Snapshot)
        }
    });

    stats
}

fn count_dir_edge_types_from_rev<R: ChunkReader + Send>(
    reader_builder: ArrowReaderBuilder<R>,
) -> EdgeStats {
    let mut stats = EdgeStats::default();

    stats[SWHType::Revision as usize][SWHType::Directory as usize] +=
        count_arrow_rows(reader_builder) as usize;

    stats
}

fn count_parent_edge_types_from_rev<R: ChunkReader + Send>(
    reader_builder: ArrowReaderBuilder<R>,
) -> EdgeStats {
    let mut stats = EdgeStats::default();

    stats[SWHType::Revision as usize][SWHType::Revision as usize] +=
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
                inc(&mut stats, SWHType::Release, SWHType::Content);
            }
            b"directory" => {
                inc(&mut stats, SWHType::Release, SWHType::Directory);
            }
            b"revision" => {
                inc(&mut stats, SWHType::Release, SWHType::Revision);
            }
            b"release" => {
                inc(&mut stats, SWHType::Release, SWHType::Release);
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
                inc(&mut stats, SWHType::Snapshot, SWHType::Content);
            }
            b"directory" => {
                inc(&mut stats, SWHType::Snapshot, SWHType::Directory);
            }
            b"revision" => {
                inc(&mut stats, SWHType::Snapshot, SWHType::Revision);
            }
            b"release" => {
                inc(&mut stats, SWHType::Snapshot, SWHType::Release);
            }
            b"alias" => {}
            _ => panic!("Unexpected snapshot branch type: {:?}", branch.target_type),
        }
    });

    stats
}

pub fn iter_labels(
    dataset_dir: &PathBuf,
    allowed_node_types: &[SWHType],
) -> Result<impl ParallelIterator<Item = Box<[u8]>>> {
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
            maybe_get_dataset_readers(dataset_dir, "directory_entry", SWHType::Directory)?
                .into_par_iter()
                .flat_map(iter_labels_from_dir_entry),
        )
        .chain(
            maybe_get_dataset_readers(dataset_dir, "snapshot_branch", SWHType::Snapshot)?
                .into_par_iter()
                .flat_map(iter_labels_from_snp_branch),
        ))
}

fn map_labels<T: Send, F, R: ChunkReader + Send>(
    reader_builder: ArrowReaderBuilder<R>,
    f: F,
) -> impl ParallelIterator<Item = Box<[u8]>>
where
    F: Fn(T) -> Option<Box<[u8]>> + Send + Sync,
    T: ArRowDeserialize + ArRowStruct,
{
    par_iter_arrow(reader_builder, f)
}

fn iter_labels_from_dir_entry<R: ChunkReader + Send>(
    reader_builder: ArrowReaderBuilder<R>,
) -> impl ParallelIterator<Item = Box<[u8]>> {
    #[derive(ArRowDeserialize, Default, Clone)]
    struct DirectoryEntry {
        name: Box<[u8]>,
    }

    map_labels(reader_builder, |entry: DirectoryEntry| Some(entry.name))
}

fn iter_labels_from_snp_branch<R: ChunkReader + Send>(
    reader_builder: ArrowReaderBuilder<R>,
) -> impl ParallelIterator<Item = Box<[u8]>> {
    #[derive(ArRowDeserialize, Default, Clone)]
    struct SnapshotBranch {
        name: Box<[u8]>,
        target_type: String,
    }

    map_labels(reader_builder, |branch: SnapshotBranch| {
        match branch.target_type.as_bytes() {
            b"content" | b"directory" | b"revision" | b"release" => Some(branch.name),
            b"alias" => None,
            _ => panic!("Unexpected snapshot branch type: {:?}", branch.target_type),
        }
    })
}

pub fn iter_persons(
    dataset_dir: &PathBuf,
    allowed_node_types: &[SWHType],
) -> Result<impl ParallelIterator<Item = Box<[u8]>>> {
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
            maybe_get_dataset_readers(dataset_dir, "revision", SWHType::Revision)?
                .into_par_iter()
                .flat_map(iter_persons_from_rev),
        )
        .chain(
            maybe_get_dataset_readers(dataset_dir, "release", SWHType::Release)?
                .into_par_iter()
                .flat_map(iter_persons_from_rel),
        ))
}

fn map_persons<T: Send, F, R: ChunkReader + Send>(
    reader_builder: ArrowReaderBuilder<R>,
    f: F,
) -> impl ParallelIterator<Item = Box<[u8]>>
where
    F: Fn(T) -> Vec<Box<[u8]>> + Send + Sync,
    T: ArRowDeserialize + ArRowStruct,
{
    par_iter_arrow(reader_builder, f)
}

fn iter_persons_from_rev<R: ChunkReader + Send>(
    reader_builder: ArrowReaderBuilder<R>,
) -> impl ParallelIterator<Item = Box<[u8]>> {
    #[derive(ArRowDeserialize, Default, Clone)]
    struct Revision {
        author: Option<Box<[u8]>>,
        committer: Option<Box<[u8]>>,
    }

    map_persons(reader_builder, |revision: Revision| {
        let mut persons = vec![];
        if let Some(author) = revision.author {
            persons.push(author);
        }
        if let Some(committer) = revision.committer {
            persons.push(committer);
        }
        persons
    })
}

fn iter_persons_from_rel<R: ChunkReader + Send>(
    reader_builder: ArrowReaderBuilder<R>,
) -> impl ParallelIterator<Item = Box<[u8]>> {
    #[derive(ArRowDeserialize, Default, Clone)]
    struct Release {
        author: Option<Box<[u8]>>,
    }

    map_persons(reader_builder, |release: Release| {
        let mut persons = vec![];
        if let Some(author) = release.author {
            persons.push(author);
        }
        persons
    })
}
