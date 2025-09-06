// Copyright (C) 2023-2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use crate::{NodeType, OutOfBoundError};
use anyhow::{Context, Result};
use log::info;
use mmap_rs::{Mmap, MmapFlags, MmapMut};
use std::path::Path;
use sux::prelude::{BitFieldSlice, BitFieldSliceCore, BitFieldSliceMut, BitFieldVec};

/// Struct to create and load a `.node2type.bin` file and convert node ids to types.
pub struct Node2Type<B> {
    data: BitFieldVec<usize, B>,
}

impl<B: AsRef<[usize]>> Node2Type<B> {
    #[inline]
    /// Get the type of a node with id `node_id` without bounds checking
    ///
    /// # Safety
    /// This function is unsafe because it does not check that `node_id` is
    /// within bounds of the array if debug asserts are disabled
    pub unsafe fn get_unchecked(&self, node_id: usize) -> NodeType {
        NodeType::try_from(self.data.get_unchecked(node_id) as u8).unwrap()
    }

    #[inline]
    /// Get the type of a node with id `node_id`
    pub fn get(&self, node_id: usize) -> Result<NodeType, OutOfBoundError> {
        NodeType::try_from(self.data.get(node_id) as u8).map_err(|_| OutOfBoundError {
            index: node_id,
            len: self.data.len(),
        })
    }
}

impl<B: AsRef<[usize]> + AsMut<[usize]>> Node2Type<B> {
    #[inline]
    /// Get the type of a node with id `node_id` without bounds checking
    ///
    /// # Safety
    /// This function is unsafe because it does not check that `node_id` is
    /// within bounds of the array if debug asserts are disabled
    pub unsafe fn set_unchecked(&mut self, node_id: usize, node_type: NodeType) {
        self.data.set_unchecked(node_id, node_type as usize);
    }

    #[inline]
    /// Set the type of a node with id `node_id`
    pub fn set(&mut self, node_id: usize, node_type: NodeType) {
        self.data.set(node_id, node_type as usize);
    }
}

/// Newtype for [`Mmap`]/[`MmapMut`] which can be dereferenced as slices of usize
///
/// instead of slices of u8, so it can be used as backend for [`BitFieldVec`].
pub struct UsizeMmap<B>(B);

impl<B: AsRef<[u8]>> AsRef<[usize]> for UsizeMmap<B> {
    fn as_ref(&self) -> &[usize] {
        bytemuck::cast_slice(self.0.as_ref())
    }
}

impl<B: AsRef<[u8]> + AsMut<[u8]>> AsMut<[usize]> for UsizeMmap<B> {
    fn as_mut(&mut self) -> &mut [usize] {
        bytemuck::cast_slice_mut(self.0.as_mut())
    }
}

impl Node2Type<UsizeMmap<MmapMut>> {
    /// Create a new `.node2type.bin` file
    pub fn new<P: AsRef<Path>>(path: P, num_nodes: usize) -> Result<Self> {
        let path = path.as_ref();
        // compute the size of the file we are creating in bytes;
        // and make it a multiple of 8 bytes so BitFieldVec can
        // read u64 words from it
        let file_len = ((num_nodes * NodeType::BITWIDTH) as u64).div_ceil(64) * 8;
        info!("The resulting file will be {} bytes long.", file_len);

        // create the file
        let node2type_file = std::fs::File::options()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)
            .with_context(|| {
                format!(
                    "While creating the .node2type.bin file: {}",
                    path.to_string_lossy()
                )
            })?;

        // fallocate the file with zeros so we can fill it without ever resizing it
        node2type_file
            .set_len(file_len)
            .with_context(|| "While fallocating the file with zeros")?;

        // create a mutable mmap to the file so we can directly write it in place
        let mmap = unsafe {
            mmap_rs::MmapOptions::new(file_len as _)
                .context("Could not initialize mmap")?
                .with_flags(MmapFlags::SHARED)
                .with_file(&node2type_file, 0)
                .map_mut()
                .with_context(|| "While mmapping the file")?
        };
        // use the BitFieldVec over the mmap
        let mmap = UsizeMmap(mmap);
        let node2type = unsafe { BitFieldVec::from_raw_parts(mmap, NodeType::BITWIDTH, num_nodes) };

        Ok(Self { data: node2type })
    }

    /// Load a mutable `.node2type.bin` file
    pub fn load_mut<P: AsRef<Path>>(path: P, num_nodes: usize) -> Result<Self> {
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
                .map_mut()?
        };

        // use the BitFieldVec over the mmap
        let data = UsizeMmap(data);
        let node2type = unsafe { BitFieldVec::from_raw_parts(data, NodeType::BITWIDTH, num_nodes) };
        Ok(Self { data: node2type })
    }
}

impl Node2Type<UsizeMmap<Mmap>> {
    /// Load a read-only `.node2type.bin` file
    pub fn load<P: AsRef<Path>>(path: P, num_nodes: usize) -> Result<Self> {
        let path = path.as_ref();
        let file_len = path
            .metadata()
            .with_context(|| format!("Could not stat {}", path.display()))?
            .len();
        let expected_file_len = ((num_nodes * NodeType::BITWIDTH).div_ceil(64) * 8) as u64;
        assert_eq!(
            file_len,
            expected_file_len,
            "Expected {} to have size {} (because graph has {} nodes), but it has size {}",
            path.display(),
            expected_file_len,
            num_nodes,
            file_len,
        );

        let file = std::fs::File::open(path)
            .with_context(|| format!("Could not open {}", path.display()))?;
        let data = unsafe {
            mmap_rs::MmapOptions::new(file_len as _)?
                .with_flags(MmapFlags::TRANSPARENT_HUGE_PAGES | MmapFlags::RANDOM_ACCESS)
                .with_file(&file, 0)
                .map()?
        };

        // use the BitFieldVec over the mmap
        let data = UsizeMmap(data);
        let node2type = unsafe { BitFieldVec::from_raw_parts(data, NodeType::BITWIDTH, num_nodes) };
        Ok(Self { data: node2type })
    }
}

impl Node2Type<Vec<usize>> {
    pub fn new_from_iter(types: impl ExactSizeIterator<Item = NodeType>) -> Self {
        let num_nodes = types.len();
        let file_len = ((num_nodes * NodeType::BITWIDTH) as u64).div_ceil(64) * 8;
        let file_len = usize::try_from(file_len).expect("num_nodes overflowed usize");
        let data = vec![0usize; file_len.div_ceil((usize::BITS / 8).try_into().unwrap())];
        let data = unsafe { BitFieldVec::from_raw_parts(data, NodeType::BITWIDTH, num_nodes) };
        let mut node2type = Node2Type { data };
        for (i, type_) in types.enumerate() {
            node2type.set(i, type_);
        }
        node2type
    }
}
