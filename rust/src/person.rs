// Copyright (C) 2025-2026  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use anyhow::{ensure, Context, Result};
use epserde::deser::{Deserialize, Flags, MemCase};
use mmap_rs::Mmap;
use std::path::{Path, PathBuf};
use sux::{
    bits::{BitFieldVec, BitVec},
    dict::{elias_fano::EfSeq, EliasFano},
    rank_sel::SelectAdaptConst,
    traits::IndexedSeq,
};

type FullnamesOffsets = MemCase<
    EliasFano<
        SelectAdaptConst<BitVec<Box<[usize]>>, Box<[usize]>>,
        BitFieldVec<usize, Box<[usize]>>,
    >,
>;

pub struct FullnameMap {
    fullnames: Mmap,
    offsets: FullnamesOffsets,
    graph_path: PathBuf,
}

impl FullnameMap {
    /// Constructs a new `FullnameMap`.
    pub fn new(graph_path: PathBuf) -> Result<FullnameMap> {
        let fullnames_path = suffix_path(&graph_path, ".persons");
        let offsets_path = suffix_path(&graph_path, ".persons.ef");
        let fullnames = mmap(&fullnames_path)
            .with_context(|| format!("Could not mmap {}", fullnames_path.display()))?;
        let offsets = unsafe { <EfSeq>::mmap(&offsets_path, Flags::RANDOM_ACCESS) }
            .with_context(|| format!("Could not mmap {}", offsets_path.display()))?;
        Ok(FullnameMap {
            fullnames,
            offsets,
            graph_path,
        })
    }

    /// Maps an author ID to its corresponding full name in the SWH graph
    ///
    /// Returns the full name corresponding to the ID.
    ///
    /// # Example
    /// ```
    /// use std::path::PathBuf;
    /// use anyhow::Result;
    /// use swh_graph::person::FullnameMap;
    ///
    /// fn get_fullname(id: usize, graph_path: PathBuf) -> Result<Vec<u8>> {
    ///     Ok(FullnameMap::new(graph_path)?.map_id(id)?.to_owned())
    /// }
    /// ```
    pub fn map_id(&self, id: usize) -> Result<&[u8]> {
        let offsets = self.offsets.uncase();
        ensure!(
            id < offsets.len(),
            "Invalid id {id}, there are only {} fullnames",
            offsets.len()
        );
        self.fullnames
            .get(offsets.get(id)..offsets.get(id + 1))
            .with_context(|| {
                format!(
                    "Out-of-bound access to {}.persons, index is probably corrupted",
                    self.graph_path.display()
                )
            })
    }
}

fn suffix_path<P: AsRef<Path>, S: AsRef<std::ffi::OsStr>>(path: P, suffix: S) -> PathBuf {
    let mut path = path.as_ref().as_os_str().to_owned();
    path.push(suffix);
    path.into()
}

fn mmap(path: &Path) -> Result<Mmap> {
    let file_len = path
        .metadata()
        .with_context(|| format!("Could not stat {}", path.display()))?
        .len();
    let file =
        std::fs::File::open(path).with_context(|| format!("Could not open {}", path.display()))?;
    let data = unsafe {
        mmap_rs::MmapOptions::new(file_len as _)
            .with_context(|| format!("Could not initialize mmap of size {file_len}"))?
            .with_flags(
                mmap_rs::MmapFlags::TRANSPARENT_HUGE_PAGES | mmap_rs::MmapFlags::RANDOM_ACCESS,
            )
            .with_file(&file, 0)
            .map()
            .with_context(|| format!("Could not mmap {}", path.display()))?
    };
    Ok(data)
}
