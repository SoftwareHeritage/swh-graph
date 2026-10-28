// Copyright (C) 2023-2026  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

//! Iterator on the set of all labels in an ORC dataset
use std::path::PathBuf;

use anyhow::Result;
use ar_row::deserialize::{ArRowDeserialize, ArRowStruct};
use ar_row_derive::ArRowDeserialize;

use rayon::prelude::*;

use super::ExportTableReader;
use crate::NodeType;

pub fn iter_labels<R: ExportTableReader>(
    dataset_dir: &PathBuf,
    allowed_node_types: &[NodeType],
) -> Result<impl ParallelIterator<Item = Box<[u8]>>> {
    let maybe_get_dataset_readers = |dataset_dir, subdirectory, node_type| {
        if allowed_node_types.contains(&node_type) {
            R::new(dataset_dir, subdirectory)
        } else {
            Ok(Vec::new())
        }
    };

    Ok([]
        .into_par_iter()
        .chain(
            maybe_get_dataset_readers(dataset_dir, "directory_entry", NodeType::Directory)?
                .into_par_iter()
                .flat_map(iter_labels_from_dir_entry),
        )
        .chain(
            maybe_get_dataset_readers(dataset_dir, "snapshot_branch", NodeType::Snapshot)?
                .into_par_iter()
                .flat_map(iter_labels_from_snp_branch),
        ))
}

fn map_labels<T, F, R: ExportTableReader>(
    reader: R,
    f: F,
) -> impl ParallelIterator<Item = Box<[u8]>>
where
    F: Fn(T) -> Option<Box<[u8]>> + Send + Sync,
    T: ArRowDeserialize + ArRowStruct + Send,
{
    reader.par_iter(f)
}

fn iter_labels_from_dir_entry<R: ExportTableReader>(
    reader: R,
) -> impl ParallelIterator<Item = Box<[u8]>> {
    #[derive(ArRowDeserialize, Default, Clone)]
    struct DirectoryEntry {
        name: Box<[u8]>,
    }

    map_labels(reader, |entry: DirectoryEntry| Some(entry.name))
}

fn iter_labels_from_snp_branch<R: ExportTableReader>(
    reader: R,
) -> impl ParallelIterator<Item = Box<[u8]>> {
    #[derive(ArRowDeserialize, Default, Clone)]
    struct SnapshotBranch {
        name: Box<[u8]>,
        target_type: String,
    }

    map_labels(reader, |branch: SnapshotBranch| {
        match branch.target_type.as_bytes() {
            b"content" | b"directory" | b"revision" | b"release" => Some(branch.name),
            b"alias" => None,
            _ => panic!("Unexpected snapshot branch type: {:?}", branch.target_type),
        }
    })
}
