use crate::SWHType;
use anyhow::{Context, Result};
use log::info;
use mmap_rs::{Mmap, MmapFlags, MmapMut};
use std::path::Path;
use sux::prelude::{BitFieldSlice, BitFieldSliceMut, BitFieldVec};

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
    pub unsafe fn get_unchecked(&self, node_id: usize) -> SWHType {
        SWHType::try_from(self.data.get_unchecked(node_id) as u8).unwrap()
    }

    #[inline]
    /// Get the type of a node with id `node_id`
    pub fn get(&self, node_id: usize) -> Option<SWHType> {
        SWHType::try_from(self.data.get(node_id) as u8).ok()
    }
}

impl<B: AsRef<[usize]> + AsMut<[usize]>> Node2Type<B> {
    #[inline]
    /// Get the type of a node with id `node_id` without bounds checking
    ///
    /// # Safety
    /// This function is unsafe because it does not check that `node_id` is
    /// within bounds of the array if debug asserts are disabled
    pub unsafe fn set_unchecked(&mut self, node_id: usize, node_type: SWHType) {
        self.data.set_unchecked(node_id, node_type as usize);
    }

    #[inline]
    /// Set the type of a node with id `node_id`
    pub fn set(&mut self, node_id: usize, node_type: SWHType) {
        self.data.set(node_id, node_type as usize);
    }
}

/// Newtype for [`Mmap`]/[`MmapMut`] which can be dereferenced as slices of usize
///
/// instead of slices of u8, so it can be used as backend for [`BitFieldVec`].
pub struct UsizeMmap<B>(B);

impl AsRef<[usize]> for UsizeMmap<Mmap> {
    fn as_ref(&self) -> &[usize] {
        bytemuck::cast_slice(&self.0)
    }
}

impl AsMut<[usize]> for UsizeMmap<MmapMut> {
    fn as_mut(&mut self) -> &mut [usize] {
        bytemuck::cast_slice_mut(&mut self.0)
    }
}

impl AsRef<[usize]> for UsizeMmap<MmapMut> {
    fn as_ref(&self) -> &[usize] {
        bytemuck::cast_slice(&self.0)
    }
}

impl Node2Type<UsizeMmap<MmapMut>> {
    /// Create a new `.node2type.bin` file
    pub fn new<P: AsRef<Path>>(path: P, num_nodes: usize) -> Result<Self> {
        let path = path.as_ref();
        // compute the size of the file we are creating in bytes;
        // and make it a multiple of 8 bytes so BitFieldVec can
        // read u64 words from it
        let file_len = ((num_nodes * SWHType::BITWIDTH) as u64).div_ceil(64) * 8;
        info!("The resulting file will be {} bytes long.", file_len);

        // create the file
        let node2type_file = std::fs::File::options()
            .read(true)
            .write(true)
            .create(true)
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
            mmap_rs::MmapOptions::new(file_len as _)?
                .with_file(node2type_file, 0)
                .map_mut()
                .with_context(|| "While mmapping the file")?
        };
        // use the BitFieldVec over the mmap
        let mmap = UsizeMmap(mmap);
        let node2type = unsafe { BitFieldVec::from_raw_parts(mmap, SWHType::BITWIDTH, num_nodes) };

        Ok(Self { data: node2type })
    }

    /// Load a mutable `.node2type.bin` file
    pub fn load_mut<P: AsRef<Path>>(path: P, num_nodes: usize) -> Result<Self> {
        let path = path.as_ref();
        let file_len = path.metadata()?.len();
        let file = std::fs::File::open(path)?;
        let data = unsafe {
            mmap_rs::MmapOptions::new(file_len as _)?
                .with_flags(MmapFlags::TRANSPARENT_HUGE_PAGES)
                .with_file(file, 0)
                .map_mut()?
        };
        #[cfg(target_os = "linux")]
        unsafe {
            libc::madvise(data.as_ptr() as *mut _, data.len(), libc::MADV_RANDOM)
        };

        // use the BitFieldVec over the mmap
        let data = UsizeMmap(data);
        let node2type = unsafe { BitFieldVec::from_raw_parts(data, SWHType::BITWIDTH, num_nodes) };
        Ok(Self { data: node2type })
    }
}

impl Node2Type<UsizeMmap<Mmap>> {
    /// Load a read-only `.node2type.bin` file
    pub fn load<P: AsRef<Path>>(path: P, num_nodes: usize) -> Result<Self> {
        let path = path.as_ref();
        let file_len = path.metadata()?.len();
        let file = std::fs::File::open(path)?;
        let data = unsafe {
            mmap_rs::MmapOptions::new(file_len as _)?
                .with_flags(MmapFlags::TRANSPARENT_HUGE_PAGES)
                .with_file(file, 0)
                .map()?
        };
        #[cfg(target_os = "linux")]
        unsafe {
            libc::madvise(data.as_ptr() as *mut _, data.len(), libc::MADV_RANDOM)
        };

        // use the BitFieldVec over the mmap
        let data = UsizeMmap(data);
        let node2type = unsafe { BitFieldVec::from_raw_parts(data, SWHType::BITWIDTH, num_nodes) };
        Ok(Self { data: node2type })
    }
}
