// Copyright (C) 2023  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::fs::File;
use std::marker::PhantomData;
use std::path::Path;

use anyhow::{bail, Context, Result};
use byteorder::ByteOrder;
use mmap_rs::{Mmap, MmapFlags};

/// Newtype for [`Mmap`] used to store arrays of any integers
///
/// instead of slices of u8
pub struct NumberMmap<E: ByteOrder, N: common_traits::AsBytes, B> {
    data: B,
    len: usize,
    offset: usize,
    _number: PhantomData<N>,
    _endianness: PhantomData<E>,
}

impl<E: ByteOrder, N: common_traits::AsBytes> NumberMmap<E, N, Mmap> {
    pub fn new<P: AsRef<Path>>(path: P, len: usize) -> Result<NumberMmap<E, N, Mmap>> {
        let path = path.as_ref();
        let file_len = path
            .metadata()
            .with_context(|| format!("Could not stat {}", path.display()))?
            .len();
        if file_len < (len * N::BYTES) as u64 {
            // We have to allow length > num_nodes because graphs compressed
            // with the Java implementation used zero padding at the end
            bail!(
                "{} is too short: expected at least {} bytes ({} items), got {}",
                path.display(),
                len * N::BYTES,
                len,
                file_len,
            );
        }
        let file =
            File::open(path).with_context(|| format!("Could not open {}", path.display()))?;
        Self::with_file_and_offset(path, len, file, 0)
    }

    pub fn with_file_and_offset<P: AsRef<Path>>(
        path: P,
        len: usize,
        file: File,
        offset: usize,
    ) -> Result<NumberMmap<E, N, Mmap>> {
        let path = path.as_ref();
        let file_len = len * N::BYTES;
        let data = unsafe {
            mmap_rs::MmapOptions::new(file_len as _)
                .with_context(|| format!("Could not initialize mmap of size {file_len}"))?
                .with_flags(MmapFlags::TRANSPARENT_HUGE_PAGES | MmapFlags::RANDOM_ACCESS)
                .with_file(&file, 0)
                .map()
                .with_context(|| format!("Could not mmap {}", path.display()))?
        };

        if data.len() % N::BYTES != 0 {
            bail!(
                "Cannot interpret mmap of size {} as array of {}",
                data.len(),
                std::any::type_name::<N>()
            );
        }
        Ok(NumberMmap {
            data,
            len,
            offset,
            _number: PhantomData,
            _endianness: PhantomData,
        })
    }

    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.len
    }
}

impl<E: ByteOrder, N: common_traits::AsBytes> NumberMmap<E, N, Mmap> {
    fn get_slice(&self, index: usize) -> Option<&[u8]> {
        let start = (index * N::BYTES) + self.offset;
        self.data.get(start..(start + N::BYTES))
    }

    unsafe fn get_slice_unchecked(&self, index: usize) -> &[u8] {
        let start = (index * N::BYTES) + self.offset;
        self.data.get_unchecked(start..(start + N::BYTES))
    }
}

macro_rules! impl_number_mmap {
    ($ty:ty, $fn:ident) => {
        impl<E: ByteOrder> crate::utils::GetIndex for &NumberMmap<E, $ty, Mmap> {
            type Output = $ty;

            fn len(&self) -> usize {
                NumberMmap::len(self)
            }

            /// Returns an item
            fn get(&self, index: usize) -> Option<$ty> {
                self.get_slice(index).map(E::$fn)
            }

            /// Returns an item
            ///
            /// # Safety
            ///
            /// Undefined behavior if `index >= len()`
            unsafe fn get_unchecked(&self, index: usize) -> $ty {
                E::$fn(self.get_slice_unchecked(index))
            }
        }
    };
}

impl_number_mmap!(i16, read_i16);
impl_number_mmap!(u32, read_u32);
impl_number_mmap!(i64, read_i64);
impl_number_mmap!(u64, read_u64);
