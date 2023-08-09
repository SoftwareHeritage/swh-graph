/*
 * Copyright (C) 2023  The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

use std::path::Path;

use anyhow::{Context, Result};

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
