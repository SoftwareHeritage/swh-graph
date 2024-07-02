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
use orc_rust::reader::ChunkReader;
use rayon::prelude::*;

use super::iter_arcs::iter_arcs_from_ovs;
use super::orc::{get_dataset_readers, par_iter_arrow};
use super::TextSwhid;
use crate::NodeType;

pub fn iter_swhids(
    dataset_dir: &PathBuf,
    allowed_node_types: &[NodeType],
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
            maybe_get_dataset_readers(dataset_dir, "directory", NodeType::Directory)?
                .into_par_iter()
                .flat_map(iter_swhids_from_dir),
        )
        .chain(
            maybe_get_dataset_readers(dataset_dir, "directory_entry", NodeType::Directory)?
                .into_par_iter()
                .flat_map(iter_swhids_from_dir_entry),
        )
        .chain(
            maybe_get_dataset_readers(dataset_dir, "content", NodeType::Content)?
                .into_par_iter()
                .flat_map(iter_swhids_from_cnt),
        )
        .chain(
            maybe_get_dataset_readers(dataset_dir, "origin", NodeType::Origin)?
                .into_par_iter()
                .flat_map(iter_swhids_from_ori),
        )
        .chain(
            maybe_get_dataset_readers(dataset_dir, "origin_visit_status", NodeType::Origin)?
                .into_par_iter()
                .flat_map_iter(iter_arcs_from_ovs)
                .flat_map_iter(|(src, dst)| [src, dst].into_iter()),
        )
        .chain(
            maybe_get_dataset_readers(dataset_dir, "release", NodeType::Release)?
                .into_par_iter()
                .flat_map(iter_rel_swhids_from_rel),
        )
        .chain(
            maybe_get_dataset_readers(dataset_dir, "release", NodeType::Release)?
                .into_par_iter()
                .flat_map(iter_target_swhids_from_rel),
        )
        .chain(
            maybe_get_dataset_readers(dataset_dir, "revision", NodeType::Revision)?
                .into_par_iter()
                .flat_map(iter_rev_swhids_from_rev),
        )
        .chain(
            maybe_get_dataset_readers(dataset_dir, "revision", NodeType::Revision)?
                .into_par_iter()
                .flat_map(iter_dir_swhids_from_rev),
        )
        .chain(
            maybe_get_dataset_readers(dataset_dir, "revision_history", NodeType::Revision)?
                .into_par_iter()
                .flat_map(iter_parent_swhids_from_rev),
        )
        .chain(
            maybe_get_dataset_readers(dataset_dir, "snapshot", NodeType::Snapshot)?
                .into_par_iter()
                .flat_map(iter_swhids_from_snp),
        )
        .chain(
            maybe_get_dataset_readers(dataset_dir, "snapshot_branch", NodeType::Snapshot)?
                .into_par_iter()
                .flat_map(iter_swhids_from_snp_branch),
        ))
}

fn map_swhids<R: ChunkReader + Send, T, F>(
    reader_builder: ArrowReaderBuilder<R>,
    f: F,
) -> impl ParallelIterator<Item = TextSwhid>
where
    F: Fn(T) -> Option<String> + Send + Sync,
    T: ArRowDeserialize + ArRowStruct + Send,
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
