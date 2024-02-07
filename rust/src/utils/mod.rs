/*
 * Copyright (C) 2023  The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

use crate::SWHType;

pub mod mmap;
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

/// Given a string like `*` or `cnt,dir,rev,rel,snp,ori`, returns a list of `SWHType`
/// matching the string.
pub fn parse_allowed_node_types(s: &str) -> Result<Vec<SWHType>> {
    if s == "*" {
        Ok(SWHType::all())
    } else {
        let mut types = Vec::new();
        for type_ in s.split(',') {
            types.push(
                type_
                    .try_into()
                    .context("Could not parse --allowed-node-types")?,
            );
        }
        Ok(types)
    }
}
