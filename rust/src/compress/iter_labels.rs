// Copyright (C) 2023-2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

//! Iterator on the set of all labels in an ORC dataset
use std::path::PathBuf;

use anyhow::Result;
use ar_row::deserialize::{ArRowDeserialize, ArRowStruct};
use ar_row_derive::ArRowDeserialize;
use orc_rust::arrow_reader::ArrowReaderBuilder;
use orc_rust::reader::ChunkReader;
use rayon::prelude::*;

use super::orc::{get_dataset_readers, par_iter_arrow};
use crate::NodeType;

pub fn iter_labels(
    dataset_dir: &PathBuf,
    allowed_node_types: &[NodeType],
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

fn map_labels<T, F, R: ChunkReader + Send>(
    reader_builder: ArrowReaderBuilder<R>,
    f: F,
) -> impl ParallelIterator<Item = Box<[u8]>>
where
    F: Fn(T) -> Option<Box<[u8]>> + Send + Sync,
    T: ArRowDeserialize + ArRowStruct + Send,
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
