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

use super::orc::{get_dataset_readers, iter_arrow};
use super::TextSwhid;
use crate::NodeType;
use crate::SWHID;

pub fn iter_arcs(
    dataset_dir: &PathBuf,
    allowed_node_types: &[NodeType],
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
            maybe_get_dataset_readers(dataset_dir, "directory_entry", NodeType::Directory)?
                .into_par_iter()
                .flat_map_iter(iter_arcs_from_dir_entry),
        )
        .chain(
            maybe_get_dataset_readers(dataset_dir, "origin_visit_status", NodeType::Origin)?
                .into_par_iter()
                .flat_map_iter(iter_arcs_from_ovs),
        )
        .chain(
            maybe_get_dataset_readers(dataset_dir, "release", NodeType::Release)?
                .into_par_iter()
                .flat_map_iter(iter_arcs_from_rel),
        )
        .chain(
            maybe_get_dataset_readers(dataset_dir, "revision", NodeType::Revision)?
                .into_par_iter()
                .flat_map_iter(iter_arcs_from_rev),
        )
        .chain(
            maybe_get_dataset_readers(dataset_dir, "revision_history", NodeType::Revision)?
                .into_par_iter()
                .flat_map_iter(iter_arcs_from_rev_history),
        )
        .chain(
            maybe_get_dataset_readers(dataset_dir, "snapshot_branch", NodeType::Snapshot)?
                .into_par_iter()
                .flat_map_iter(iter_arcs_from_snp_branch),
        ))
}

fn map_arcs<R: ChunkReader + Send, T, F>(
    reader_builder: ArrowReaderBuilder<R>,
    f: F,
) -> impl Iterator<Item = (TextSwhid, TextSwhid)>
where
    F: Fn(T) -> Option<(String, String)> + Send + Sync,
    T: ArRowDeserialize + ArRowStruct + Send,
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

pub(super) fn iter_arcs_from_ovs<R: ChunkReader + Send>(
    reader_builder: ArrowReaderBuilder<R>,
) -> impl Iterator<Item = (TextSwhid, TextSwhid)> {
    #[derive(ArRowDeserialize, Default, Clone)]
    struct OriginVisitStatus {
        origin: String,
        snapshot: Option<String>,
    }

    map_arcs(reader_builder, |ovs: OriginVisitStatus| {
        ovs.snapshot.as_ref().map(|snapshot| {
            (
                SWHID::from_origin_url(ovs.origin).to_string(),
                format!("swh:1:snp:{snapshot}"),
            )
        })
    })
}

pub(super) fn iter_arcs_from_rel<R: ChunkReader + Send>(
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

pub(super) fn iter_arcs_from_rev<R: ChunkReader + Send>(
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

pub(super) fn iter_arcs_from_rev_history<R: ChunkReader + Send>(
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
