/*
 * Copyright (C) 2023-2024  The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context, Result};

use crate::{NodeConstraint, NodeType};

pub mod mmap;
pub mod shuffle;
pub mod sort;

pub fn dir_size(path: &Path) -> Result<usize> {
    Ok(std::fs::read_dir(path)
        .with_context(|| format!("Could not list {}", path.display()))?
        .map(|entry| {
            entry
                .unwrap_or_else(|e| panic!("Could not read {} entry: {:?}", path.display(), e))
                .metadata()
                .as_ref()
                .unwrap_or_else(|e| panic!("Could not read {} entry: {:?}", path.display(), e))
                .len() as usize
        })
        .sum::<usize>())
}

/// Appends a string to a path
///
/// ```
/// # use std::path::{Path, PathBuf};
/// # use swh_graph::utils::suffix_path;
///
/// assert_eq!(
///     suffix_path(Path::new("/tmp/graph"), "-transposed"),
///     Path::new("/tmp/graph-transposed").to_owned()
/// );
/// ```
#[inline(always)]
pub fn suffix_path<P: AsRef<Path>, S: AsRef<std::ffi::OsStr>>(path: P, suffix: S) -> PathBuf {
    let mut path = path.as_ref().as_os_str().to_owned();
    path.push(suffix);
    path.into()
}

/// Given a string like `*` or `cnt,dir,rev,rel,snp,ori`, returns a list of `NodeType`
/// matching the string.
///
/// # Examples
///
/// ```
/// # use swh_graph::NodeType;
/// use swh_graph::utils::parse_allowed_node_types;
///
/// assert_eq!(parse_allowed_node_types("*").unwrap(), NodeType::all());
/// assert_eq!(parse_allowed_node_types("cnt").unwrap(), vec![NodeType::Content]);
/// assert_eq!(
///     parse_allowed_node_types("rel,rev").unwrap(),
///     vec![NodeType::Release, NodeType::Revision]
/// );
/// ```
//
// TODO make this return a NodeConstraint instead
pub fn parse_allowed_node_types(s: &str) -> Result<Vec<NodeType>> {
    s.parse::<NodeConstraint>()
        .map_err(|s| anyhow!("Could not parse --allowed-node-types {s}"))
        .map(|constr| constr.to_vec())
}

#[allow(clippy::len_without_is_empty)]
pub trait GetIndex {
    type Output;

    /// Returns the total number of items in the collections
    fn len(&self) -> usize;

    /// Returns an item of the collection
    fn get(&self, index: usize) -> Option<Self::Output>;

    /// Returns an item of the collection
    ///
    /// # Safety
    ///
    /// Undefined behavior if the index is past the end of the collection.
    unsafe fn get_unchecked(&self, index: usize) -> Self::Output;
}

impl<Item: Clone, T: std::ops::Deref<Target = [Item]>> GetIndex for T {
    type Output = Item;

    fn len(&self) -> usize {
        <[Item]>::len(self)
    }

    fn get(&self, index: usize) -> Option<Self::Output> {
        <[Item]>::get(self, index).cloned()
    }

    unsafe fn get_unchecked(&self, index: usize) -> Self::Output {
        <[Item]>::get_unchecked(self, index).clone()
    }
}
