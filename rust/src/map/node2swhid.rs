// Copyright (C) 2023  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use crate::SWHID;
use anyhow::Result;
use mmap_rs::Mmap;

/// Struct to load a `.node2swhid.bin` file and convert node ids to SWHIDs.
pub struct Node2SWHID {
    data: Mmap,
}

impl Node2SWHID {
    /// Load a `.node2swhid.bin` file
    pub fn load<P: AsRef<std::path::Path>>(path: P) -> Result<Self> {
        let path = path.as_ref();
        let file_len = path.metadata()?.len();
        let file = std::fs::File::open(path)?;
        let data = unsafe {
            mmap_rs::MmapOptions::new(file_len as _)?
                .with_flags((sux::prelude::Flags::TRANSPARENT_HUGE_PAGES).mmap_flags())
                .with_file(file, 0)
                .map()?
        };
        #[cfg(target_os = "linux")]
        unsafe {
            libc::madvise(data.as_ptr() as *mut _, data.len(), libc::MADV_RANDOM)
        };
        Ok(Self { data })
    }
}

impl Node2SWHID {
    /// Convert a node_it to a SWHID
    ///
    /// # Safety
    /// This function is unsafe because it does not check that `node_id` is
    /// within bounds of the array if debug asserts are disabled
    #[inline]
    pub unsafe fn get_unchecked(&self, node_id: usize) -> SWHID {
        let offset = node_id * SWHID::BYTES_SIZE;
        let bytes = self.data.get_unchecked(offset..offset + SWHID::BYTES_SIZE);
        // this unwrap is always safe because we use the same const
        let bytes: [u8; SWHID::BYTES_SIZE] = bytes.try_into().unwrap();
        // this unwrap can only fail on a corrupted file, so it's ok to panic
        SWHID::try_from(bytes).unwrap()
    }

    /// Convert a node_it to a SWHID
    #[inline]
    pub fn get(&self, node_id: usize) -> Option<SWHID> {
        let offset = node_id * SWHID::BYTES_SIZE;
        let bytes = self.data.get(offset..offset + SWHID::BYTES_SIZE)?;
        // this unwrap is always safe because we use the same const
        let bytes: [u8; SWHID::BYTES_SIZE] = bytes.try_into().unwrap();
        // this unwrap can only fail on a corrupted file, so it's ok to panic
        Some(SWHID::try_from(bytes).unwrap())
    }

    /// Return how many node_ids are in this map
    #[allow(clippy::len_without_is_empty)] // rationale: we don't care about empty maps
    #[inline]
    pub fn len(&self) -> usize {
        self.data.len() / SWHID::BYTES_SIZE
    }
}

impl core::ops::Index<usize> for Node2SWHID {
    type Output = SWHID;
    fn index(&self, index: usize) -> &Self::Output {
        let offset = index * SWHID::BYTES_SIZE;
        let bytes = &self.data[offset..offset + SWHID::BYTES_SIZE];
        debug_assert!(core::mem::size_of::<SWHID>() == SWHID::BYTES_SIZE);
        // unsafe :( but it's ok because SWHID does not depends on endianness
        // also TODO!: check for version
        unsafe { &*(bytes.as_ptr() as *const SWHID) }
    }
}
