// Copyright (C) 2023-2026  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

//! Iterator on the set of all person ids in an ORC dataset
use std::path::PathBuf;

use anyhow::Result;
use ar_row::deserialize::{ArRowDeserialize, ArRowStruct};
use ar_row_derive::ArRowDeserialize;

use rayon::prelude::*;

use super::ExportTableReader;
use crate::NodeType;

pub fn iter_persons<R: ExportTableReader>(
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
            maybe_get_dataset_readers(dataset_dir, "revision", NodeType::Revision)?
                .into_par_iter()
                .flat_map(iter_persons_from_rev),
        )
        .chain(
            maybe_get_dataset_readers(dataset_dir, "release", NodeType::Release)?
                .into_par_iter()
                .flat_map(iter_persons_from_rel),
        ))
}

fn map_persons<T, F, R: ExportTableReader>(
    reader: R,
    f: F,
) -> impl ParallelIterator<Item = Box<[u8]>>
where
    F: Fn(T) -> Vec<Box<[u8]>> + Send + Sync,
    T: ArRowDeserialize + ArRowStruct + Send,
{
    reader.par_iter(f)
}

fn iter_persons_from_rev<R: ExportTableReader>(
    reader: R,
) -> impl ParallelIterator<Item = Box<[u8]>> {
    #[derive(ArRowDeserialize, Default, Clone)]
    struct Revision {
        author: Option<Box<[u8]>>,
        committer: Option<Box<[u8]>>,
    }

    map_persons(reader, |revision: Revision| {
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

fn iter_persons_from_rel<R: ExportTableReader>(
    reader: R,
) -> impl ParallelIterator<Item = Box<[u8]>> {
    #[derive(ArRowDeserialize, Default, Clone)]
    struct Release {
        author: Option<Box<[u8]>>,
    }

    map_persons(reader, |release: Release| {
        let mut persons = vec![];
        if let Some(author) = release.author {
            persons.push(author);
        }
        persons
    })
}
