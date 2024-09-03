// Copyright (C) 2023-2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use crate::{OutOfBoundError, SWHID};
use anyhow::{Context, Result};
use mmap_rs::{Mmap, MmapFlags, MmapMut};

/// Struct to load a `.node2swhid.bin` file and convert node ids to SWHIDs.
pub struct Node2SWHID<B> {
    data: B,
}

impl Node2SWHID<Mmap> {
    /// Load a `.node2swhid.bin` file
    pub fn load<P: AsRef<std::path::Path>>(path: P) -> Result<Self> {
        let path = path.as_ref();
        let file_len = path
            .metadata()
            .with_context(|| format!("Could not stat {}", path.display()))?
            .len();
        let file = std::fs::File::open(path)
            .with_context(|| format!("Could not open {}", path.display()))?;
        let data = unsafe {
            mmap_rs::MmapOptions::new(file_len as _)
                .context("Could not initialize mmap")?
                .with_flags(MmapFlags::TRANSPARENT_HUGE_PAGES | MmapFlags::RANDOM_ACCESS)
                .with_file(&file, 0)
                .map()
                .with_context(|| format!("Could not mmap {}", path.display()))?
        };
        Ok(Self { data })
    }
}

impl Node2SWHID<MmapMut> {
    /// Create a new `.node2swhid.bin` file
    pub fn new<P: AsRef<std::path::Path>>(path: P, num_nodes: usize) -> Result<Self> {
        let path = path.as_ref();
        let file_len = (num_nodes * SWHID::BYTES_SIZE)
            .try_into()
            .context("File size overflowed u64")?;
        let file = std::fs::File::options()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)
            .with_context(|| format!("Could not create {}", path.display()))?;

        // fallocate the file with zeros so we can fill it without ever resizing it
        file.set_len(file_len)
            .with_context(|| format!("Could not fallocate {} with zeros", path.display()))?;

        let data = unsafe {
            mmap_rs::MmapOptions::new(file_len as _)
                .context("Could not initialize mmap")?
                .with_flags(MmapFlags::TRANSPARENT_HUGE_PAGES | MmapFlags::SHARED)
                .with_file(&file, 0)
                .map_mut()
                .with_context(|| format!("Could not mmap {}", path.display()))?
        };
        Ok(Self { data })
    }
}

impl Node2SWHID<Vec<u8>> {
    pub fn new_from_iter(swhids: impl ExactSizeIterator<Item = SWHID>) -> Self {
        let file_len = swhids.len() * SWHID::BYTES_SIZE;
        let data = vec![0; file_len];
        let mut node2swhid = Node2SWHID { data };
        for (i, swhid) in swhids.enumerate() {
            node2swhid.set(i, swhid);
        }
        node2swhid
    }
}

impl<B: AsRef<[u8]>> Node2SWHID<B> {
    /// Convert a node_id to a SWHID
    ///
    /// # Safety
    /// This function is unsafe because it does not check that `node_id` is
    /// within bounds of the array if debug asserts are disabled
    #[inline]
    pub unsafe fn get_unchecked(&self, node_id: usize) -> SWHID {
        let offset = node_id * SWHID::BYTES_SIZE;
        let bytes = self
            .data
            .as_ref()
            .get_unchecked(offset..offset + SWHID::BYTES_SIZE);
        // this unwrap is always safe because we use the same const
        let bytes: [u8; SWHID::BYTES_SIZE] = bytes.try_into().unwrap();
        // this unwrap can only fail on a corrupted file, so it's ok to panic
        SWHID::try_from(bytes).unwrap()
    }

    /// Convert a node_id to a SWHID
    #[inline]
    pub fn get(&self, node_id: usize) -> Result<SWHID, OutOfBoundError> {
        let offset = node_id * SWHID::BYTES_SIZE;
        let bytes = self
            .data
            .as_ref()
            .get(offset..offset + SWHID::BYTES_SIZE)
            .ok_or(OutOfBoundError {
                index: node_id,
                len: self.data.as_ref().len() / SWHID::BYTES_SIZE,
            })?;
        // this unwrap is always safe because we use the same const
        let bytes: [u8; SWHID::BYTES_SIZE] = bytes.try_into().unwrap();
        // this unwrap can only fail on a corrupted file, so it's ok to panic
        Ok(SWHID::try_from(bytes).unwrap())
    }

    /// Return how many node_ids are in this map
    #[allow(clippy::len_without_is_empty)] // rationale: we don't care about empty maps
    #[inline]
    pub fn len(&self) -> usize {
        self.data.as_ref().len() / SWHID::BYTES_SIZE
    }
}

impl<B: AsMut<[u8]> + AsRef<[u8]>> Node2SWHID<B> {
    /// Set a node_id to map to a given SWHID, without checking bounds
    ///
    /// # Safety
    /// This function is unsafe because it does not check that `node_id` is
    /// within bounds of the array if debug asserts are disabled
    #[inline]
    pub unsafe fn set_unchecked(&mut self, node_id: usize, swhid: SWHID) {
        let bytes: [u8; SWHID::BYTES_SIZE] = swhid.into();
        let offset = node_id * SWHID::BYTES_SIZE;
        self.data
            .as_mut()
            .get_unchecked_mut(offset..offset + SWHID::BYTES_SIZE)
            .copy_from_slice(&bytes[..]);
    }

    /// Set a node_id to map to a given SWHID
    #[inline]
    pub fn set(&mut self, node_id: usize, swhid: SWHID) {
        let bytes: [u8; SWHID::BYTES_SIZE] = swhid.into();
        let offset = node_id * SWHID::BYTES_SIZE;
        self.data
            .as_mut()
            .get_mut(offset..offset + SWHID::BYTES_SIZE)
            .expect("Tried to write past the end of Node2SWHID map")
            .copy_from_slice(&bytes[..]);
    }
}

impl<B: AsRef<[u8]>> core::ops::Index<usize> for Node2SWHID<B> {
    type Output = SWHID;
    fn index(&self, index: usize) -> &Self::Output {
        let offset = index * SWHID::BYTES_SIZE;
        let bytes = &self.data.as_ref()[offset..offset + SWHID::BYTES_SIZE];
        debug_assert!(core::mem::size_of::<SWHID>() == SWHID::BYTES_SIZE);
        // unsafe :( but it's ok because SWHID does not depends on endianness
        // also TODO!: check for version
        unsafe { &*(bytes.as_ptr() as *const SWHID) }
    }
}
