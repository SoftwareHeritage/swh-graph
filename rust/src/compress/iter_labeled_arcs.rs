// Copyright (C) 2023-2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

//! Iterator on the set of all arcs in an ORC dataset, along with their label if any

use std::path::PathBuf;

use anyhow::Result;
use ar_row::deserialize::{ArRowDeserialize, ArRowStruct};
use ar_row_derive::ArRowDeserialize;
use nonmax::NonMaxU64;
use orc_rust::arrow_reader::ArrowReaderBuilder;
use orc_rust::reader::ChunkReader;
use rayon::prelude::*;

use super::iter_arcs::{iter_arcs_from_rel, iter_arcs_from_rev, iter_arcs_from_rev_history};
use super::orc::{get_dataset_readers, iter_arrow};
use super::TextSwhid;
use crate::compress::label_names::LabelNameHasher;
use crate::labels::{
    Branch, DirEntry, EdgeLabel, Permission, UntypedEdgeLabel, Visit, VisitStatus,
};
use crate::{NodeType, SWHID};

pub fn iter_labeled_arcs<'a>(
    dataset_dir: &'a PathBuf,
    allowed_node_types: &'a [NodeType],
    label_name_hasher: LabelNameHasher<'a>,
) -> Result<impl ParallelIterator<Item = (TextSwhid, TextSwhid, Option<NonMaxU64>)> + 'a> {
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
                .flat_map_iter(move |rb| iter_labeled_arcs_from_dir_entry(rb, label_name_hasher)),
        )
        .chain(
            maybe_get_dataset_readers(dataset_dir, "origin_visit_status", NodeType::Origin)?
                .into_par_iter()
                .flat_map_iter(iter_labeled_arcs_from_ovs),
        )
        .chain(
            maybe_get_dataset_readers(dataset_dir, "release", NodeType::Release)?
                .into_par_iter()
                .flat_map_iter(iter_arcs_from_rel)
                .map(|(src, dst)| (src, dst, None)),
        )
        .chain(
            maybe_get_dataset_readers(dataset_dir, "revision", NodeType::Revision)?
                .into_par_iter()
                .flat_map_iter(iter_arcs_from_rev)
                .map(|(src, dst)| (src, dst, None)),
        )
        .chain(
            maybe_get_dataset_readers(dataset_dir, "revision_history", NodeType::Revision)?
                .into_par_iter()
                .flat_map_iter(iter_arcs_from_rev_history)
                .map(|(src, dst)| (src, dst, None)),
        )
        .chain(
            maybe_get_dataset_readers(dataset_dir, "snapshot_branch", NodeType::Snapshot)?
                .into_par_iter()
                .flat_map_iter(move |rb| iter_labeled_arcs_from_snp_branch(rb, label_name_hasher)),
        ))
}

fn map_labeled_arcs<R: ChunkReader + Send, T, F>(
    reader_builder: ArrowReaderBuilder<R>,
    f: F,
) -> impl Iterator<Item = (TextSwhid, TextSwhid, Option<NonMaxU64>)>
where
    F: Fn(T) -> Option<(String, String, Option<EdgeLabel>)> + Send + Sync,
    T: Send + ArRowDeserialize + ArRowStruct,
{
    iter_arrow(reader_builder, move |record: T| {
        f(record).map(|(src_swhid, dst_swhid, label)| {
            (
                src_swhid.as_bytes().try_into().unwrap(),
                dst_swhid.as_bytes().try_into().unwrap(),
                label.map(|label| {
                    UntypedEdgeLabel::from(label)
                        .0
                        .try_into()
                        .expect("label is 0")
                }),
            )
        })
    })
}

fn iter_labeled_arcs_from_dir_entry<'a, R: ChunkReader + Send + 'a>(
    reader_builder: ArrowReaderBuilder<R>,
    label_name_hasher: LabelNameHasher<'a>,
) -> impl Iterator<Item = (TextSwhid, TextSwhid, Option<NonMaxU64>)> + 'a {
    #[derive(ArRowDeserialize, Default, Clone)]
    struct DirectoryEntry {
        directory_id: String,
        name: Box<[u8]>,
        r#type: String,
        target: String,
        perms: i32,
    }

    map_labeled_arcs(reader_builder, move |entry: DirectoryEntry| {
        Some((
            format!("swh:1:dir:{}", entry.directory_id),
            match entry.r#type.as_bytes() {
                b"file" => format!("swh:1:cnt:{}", entry.target),
                b"dir" => format!("swh:1:dir:{}", entry.target),
                b"rev" => format!("swh:1:rev:{}", entry.target),
                _ => panic!("Unexpected directory entry type: {:?}", entry.r#type),
            },
            DirEntry::new(
                u16::try_from(entry.perms)
                    .ok()
                    .and_then(Permission::from_git)
                    .unwrap_or(Permission::None),
                label_name_hasher
                    .hash(entry.name)
                    .expect("Could not hash dir entry name"),
            )
            .map(EdgeLabel::DirEntry),
        ))
    })
}

fn iter_labeled_arcs_from_ovs<R: ChunkReader + Send>(
    reader_builder: ArrowReaderBuilder<R>,
) -> impl Iterator<Item = (TextSwhid, TextSwhid, Option<NonMaxU64>)> {
    #[derive(ArRowDeserialize, Default, Clone)]
    struct OriginVisitStatus {
        origin: String,
        date: Option<ar_row::Timestamp>,
        status: String,
        snapshot: Option<String>,
    }

    map_labeled_arcs(reader_builder, |ovs: OriginVisitStatus| {
        ovs.snapshot.as_ref().map(|snapshot| {
            (
                SWHID::from_origin_url(ovs.origin).to_string(),
                format!("swh:1:snp:{snapshot}"),
                Visit::new(
                    match ovs.status.as_str() {
                        "full" => VisitStatus::Full,
                        _ => VisitStatus::Partial,
                    },
                    ovs.date
                        .unwrap_or(ar_row::Timestamp {
                            seconds: 0,
                            nanoseconds: 0,
                        })
                        .seconds
                        .try_into()
                        .expect("Negative visit date"),
                )
                .map(EdgeLabel::Visit),
            )
        })
    })
}

fn iter_labeled_arcs_from_snp_branch<'a, R: ChunkReader + Send + 'a>(
    reader_builder: ArrowReaderBuilder<R>,
    label_name_hasher: LabelNameHasher<'a>,
) -> impl Iterator<Item = (TextSwhid, TextSwhid, Option<NonMaxU64>)> + 'a {
    #[derive(ArRowDeserialize, Default, Clone)]
    struct SnapshotBranch {
        snapshot_id: String,
        name: Box<[u8]>,
        target: String,
        target_type: String,
    }

    map_labeled_arcs(reader_builder, move |branch: SnapshotBranch| {
        let dst = match branch.target_type.as_bytes() {
            b"content" => Some(format!("swh:1:cnt:{}", branch.target)),
            b"directory" => Some(format!("swh:1:dir:{}", branch.target)),
            b"revision" => Some(format!("swh:1:rev:{}", branch.target)),
            b"release" => Some(format!("swh:1:rel:{}", branch.target)),
            b"alias" => None,
            _ => panic!("Unexpected snapshot target type: {:?}", branch.target_type),
        };
        dst.map(|dst| {
            (
                format!("swh:1:snp:{}", branch.snapshot_id),
                dst,
                Branch::new(
                    label_name_hasher
                        .hash(branch.name)
                        .expect("Could not hash branch name"),
                )
                .map(EdgeLabel::Branch),
            )
        })
    })
}
