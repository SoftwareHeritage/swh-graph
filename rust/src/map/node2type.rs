use crate::SWHType;
use anyhow::{Context, Result};
use log::info;
use mmap_rs::{Mmap, MmapFlags, MmapMut};
use std::path::Path;
use sux::prelude::{CompactArray, VSlice, VSliceMut};

pub trait Node2Type {
    type Buf: AsRef<[usize]> + ?Sized;

    fn as_array(&self) -> CompactArray<&Self::Buf>;

    #[inline]
    /// Get the type of a node with id `node_id` without bounds checking
    ///
    /// # Safety
    /// This function is unsafe because it does not check that `node_id` is
    /// within bounds of the array if debug asserts are disabled
    unsafe fn get_unchecked(&self, node_id: usize) -> SWHType {
        SWHType::try_from(self.as_array().get_unchecked(node_id) as u8).unwrap()
    }

    #[inline]
    /// Get the type of a node with id `node_id`
    fn get(&self, node_id: usize) -> Option<SWHType> {
        SWHType::try_from(self.as_array().get(node_id) as u8).ok()
    }
}

pub trait Node2TypeMut {
    type Buf: AsRef<[usize]> + AsMut<[usize]> + ?Sized;

    fn as_array_mut(&mut self) -> CompactArray<&mut Self::Buf>;

    #[inline]
    /// Get the type of a node with id `node_id` without bounds checking
    ///
    /// # Safety
    /// This function is unsafe because it does not check that `node_id` is
    /// within bounds of the array if debug asserts are disabled
    unsafe fn set_unchecked(&mut self, node_id: usize, node_type: SWHType) {
        self.as_array_mut()
            .set_unchecked(node_id, node_type as usize);
    }

    #[inline]
    /// Set the type of a node with id `node_id`
    fn set(&mut self, node_id: usize, node_type: SWHType) {
        self.as_array_mut().set(node_id, node_type as usize);
    }
}

/// Alternative to [`MappedNode2Type`] backed by a slice instead of a mmap.
pub struct BorrowedNode2Type<B> {
    data: B,
    num_nodes: usize,
}

impl<B: AsRef<[usize]>> Node2Type for BorrowedNode2Type<B> {
    type Buf = B;

    fn as_array(&self) -> CompactArray<&Self::Buf> {
        unsafe { CompactArray::from_raw_parts(&self.data, SWHType::BITWIDTH, self.num_nodes) }
    }
}

impl<B: AsRef<[usize]> + AsMut<[usize]>> Node2TypeMut for BorrowedNode2Type<B> {
    type Buf = B;

    fn as_array_mut(&mut self) -> CompactArray<&mut Self::Buf> {
        unsafe { CompactArray::from_raw_parts(&mut self.data, SWHType::BITWIDTH, self.num_nodes) }
    }
}

/// Struct to create and load a `.node2type.bin` file and convert node ids to types.
pub struct MappedNode2Type<B> {
    data: B,
    num_nodes: usize,
}

impl<B: AsRef<[u8]>> Node2Type for MappedNode2Type<B> {
    type Buf = [usize];

    fn as_array(&self) -> CompactArray<&Self::Buf> {
        // Cast padded &[u8] to &[usize]
        let data = bytemuck::cast_slice(self.data.as_ref());
        unsafe { CompactArray::from_raw_parts(data, SWHType::BITWIDTH, self.num_nodes) }
    }
}

impl<B: AsMut<[u8]>> Node2TypeMut for MappedNode2Type<B> {
    type Buf = [usize];

    fn as_array_mut(&mut self) -> CompactArray<&mut Self::Buf> {
        // Cast padded &mut [u8] to &mut [usize]
        let data = bytemuck::cast_slice_mut(self.data.as_mut());
        unsafe { CompactArray::from_raw_parts(data, SWHType::BITWIDTH, self.num_nodes) }
    }
}

impl MappedNode2Type<MmapMut> {
    /// Create a new `.node2type.bin` file
    pub fn new<P: AsRef<Path>>(path: P, num_nodes: usize) -> Result<Self> {
        let path = path.as_ref();
        // compute the size of the file we are creating in bytes
        let mut file_len = ((num_nodes * SWHType::BITWIDTH) as u64 + 7) / 8;
        // make the file dimension a multiple of 8 bytes so CompactArray can
        // read u64 words from it
        file_len += 8 - (file_len % 8);
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

        Ok(Self {
            data: mmap,
            num_nodes,
        })
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

        Ok(Self { data, num_nodes })
    }
}

impl MappedNode2Type<Mmap> {
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

        Ok(Self { data, num_nodes })
    }
}
