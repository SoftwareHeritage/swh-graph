// Copyright (C) 2023-2025  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::path::Path;

use anyhow::{Context, Result};

use super::*;
use crate::utils::suffix_path;

pub(super) fn load_if_exists<T>(
    base_path: &Path,
    suffix: &'static str,
    f: impl FnOnce(&Path) -> Result<T>,
) -> Result<Result<T, UnavailableProperty>> {
    let path = suffix_path(base_path, suffix);
    if std::fs::exists(&path).map_err(|source| UnavailableProperty {
        path: path.clone(),
        source,
    })? {
        Ok(Ok(f(&path)?))
    } else {
        Ok(Err(UnavailableProperty {
            path,
            source: std::io::Error::new(std::io::ErrorKind::NotFound, "No such file"),
        }))
    }
}

pub(super) fn mmap(path: impl AsRef<Path>) -> Result<Mmap> {
    let path = path.as_ref();
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
