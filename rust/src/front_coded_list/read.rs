// Copyright (C) 2023  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::fs::File;
use std::io::BufReader;
use std::path::Path;

use anyhow::{bail, Context, Result};
use mmap_rs::{Mmap, MmapFlags};

use crate::utils::{suffix_path, GetIndex};

#[derive(Debug, Clone)]
/// Front coded list, it takes a list of strings and encode them in a way that
/// the common prefix between strings is encoded only once.
///
/// The encoding is done in blocks of k strings, the first string is encoded
/// without compression, the other strings are encoded with the common prefix
/// removed.
///
/// See the
/// [`it.unimi.dsi.fastutil.bytes.ByteArrayFrontCodedBigList` documentation](https://fastutil.di.unimi.it/docs/it/unimi/dsi/fastutil/bytes/ByteArrayFrontCodedBigList.html)
/// or the
/// [implementation](https://archive.softwareheritage.org/swh:1:cnt:2fc1092d2f792fcfcbf6ff9baf849f6d22e41486;origin=https://repo1.maven.org/maven2/it/unimi/dsi/fastutil;visit=swh:1:snp:8007412c404cf39fa38e3db600bdf93700410741;anchor=swh:1:rel:1ec2b63253f642eae54f1a3e5ddd20178867bc7d;path=/it/unimi/dsi/fastutil/bytes/ByteArrayFrontCodedList.java) for details
pub struct FrontCodedList<D: AsRef<[u8]>, P: AsRef<[u8]>> {
    /// The number of strings in a block, this regulates the compression vs
    /// decompression speed tradeoff
    k: usize,
    /// Number of encoded strings
    len: usize,
    /// The encoded bytestrings
    data: D,
    /// The pointer to in which byte the k-th string start, in big endian
    pointers: P,
}

impl FrontCodedList<Mmap, Mmap> {
    pub fn load<P: AsRef<Path>>(base_path: P) -> Result<Self> {
        let properties_path = suffix_path(&base_path, ".properties");
        let bytearray_path = suffix_path(&base_path, ".bytearray");
        let pointers_path = suffix_path(&base_path, ".pointers");

        // Parse properties
        let properties_file = File::open(&properties_path)
            .with_context(|| format!("Could not open {}", properties_path.display()))?;
        let map = java_properties::read(BufReader::new(properties_file)).with_context(|| {
            format!(
                "Could not parse properties from {}",
                properties_path.display()
            )
        })?;
        let len =
            map.get("n").unwrap().parse::<usize>().with_context(|| {
                format!("Could not parse 'n' from {}", properties_path.display())
            })?;
        let k = map
            .get("ratio")
            .unwrap()
            .parse::<usize>()
            .with_context(|| {
                format!("Could not parse 'ratio' from {}", properties_path.display())
            })?;

        // mmap data
        let bytearray_len = bytearray_path
            .metadata()
            .with_context(|| format!("Could not read {} stats", bytearray_path.display()))?
            .len();
        let bytearray_file = std::fs::File::open(&bytearray_path)
            .with_context(|| format!("Could not open {}", bytearray_path.display()))?;
        let data = unsafe {
            mmap_rs::MmapOptions::new(bytearray_len as _)
                .context("Could not initialize mmap")?
                .with_flags(MmapFlags::TRANSPARENT_HUGE_PAGES | MmapFlags::RANDOM_ACCESS)
                .with_file(&bytearray_file, 0)
                .map()
                .with_context(|| format!("Could not mmap {}", bytearray_path.display()))?
        };

        // mmap pointers
        let pointers_len = pointers_path
            .metadata()
            .with_context(|| format!("Could not read {} stats", pointers_path.display()))?
            .len();
        let expected_pointers_len = ((len.div_ceil(k)) * 8) as u64;
        if pointers_len != expected_pointers_len {
            bail!(
                "FCL at {} has length {} and ratio {} so {} should have length {}, but it has length {}",
                base_path.as_ref().display(),
                len,
                k,
                pointers_path.display(),
                expected_pointers_len,
                pointers_len
            );
        }
        let pointers_file = std::fs::File::open(&pointers_path)
            .with_context(|| format!("Could not open {}", pointers_path.display()))?;
        let pointers = unsafe {
            mmap_rs::MmapOptions::new(pointers_len as _)
                .context("Could not initialize mmap")?
                .with_flags(MmapFlags::TRANSPARENT_HUGE_PAGES | MmapFlags::RANDOM_ACCESS)
                .with_file(&pointers_file, 0)
                .map()
                .with_context(|| format!("Could not mmap {}", pointers_path.display()))?
        };

        Ok(FrontCodedList {
            k,
            len,
            data,
            pointers,
        })
    }
}

// Adapted from https://archive.softwareheritage.org/swh:1:cnt:08cf9306577d3948360afebfa77ee623edec7f1a;origin=https://github.com/vigna/sux-rs;visit=swh:1:snp:bed7ce7510f76c0b1e8fb995778028614bfff354;anchor=swh:1:rev:fac0e742d7a404237abca48e4aeffcde34f41e58;path=/src/dict/rear_coded_list.rs;lines=304-329
impl<D: AsRef<[u8]>, P: AsRef<[u8]>> FrontCodedList<D, P> {
    /// Write the index-th string to `result` as bytes. This is done to avoid
    /// allocating a new string for every query.
    #[inline(always)]
    pub fn get_inplace(&self, index: usize, result: &mut Vec<u8>) {
        result.clear();
        let block = index / self.k;
        let offset = index % self.k;

        let start = u64::from_be_bytes(
            self.pointers.as_ref()[block * 8..(block + 1) * 8]
                .try_into()
                .unwrap(),
        )
        .try_into()
        .expect("FCL pointer overflowed usize");
        let data = &self.data.as_ref()[start..];

        // decode the first string in the block
        let (len, mut data) = decode_int(data);
        result.extend(&data[..len as usize]);
        data = &data[len as usize..];

        for _ in 0..offset {
            let (new_suffix_len, tmp) = decode_int(data);
            let (reused_prefix_len, tmp) = decode_int(tmp);

            result.resize(reused_prefix_len as usize, 0);

            result.extend(&tmp[..new_suffix_len as usize]);

            data = &tmp[new_suffix_len as usize..];
        }
    }
}

impl<D: AsRef<[u8]>, P: AsRef<[u8]>> GetIndex for &FrontCodedList<D, P> {
    type Output = Vec<u8>;

    fn len(&self) -> usize {
        self.len
    }

    /// Returns the n-th bytestring, or `None` if `index` is larger than the length
    fn get(&self, index: usize) -> Option<Self::Output> {
        if index >= self.len {
            None
        } else {
            let mut result = Vec::with_capacity(128);
            self.get_inplace(index, &mut result);
            Some(result)
        }
    }

    /// Returns the n-th bytestring
    ///
    /// # Panics
    ///
    /// If `index` is out of bound
    unsafe fn get_unchecked(&self, index: usize) -> Self::Output {
        let mut result = Vec::with_capacity(128);
        self.get_inplace(index, &mut result);
        result
    }
}

#[inline(always)]
// Reads a varint at the beginning of the array, then returns the varint and the rest
// of the array.
//
// Adapted from https://archive.softwareheritage.org/swh:1:cnt:66c21893f9cd9686456b0127df0b9b48a0fe153d;origin=https://repo1.maven.org/maven2/it/unimi/dsi/fastutil;visit=swh:1:snp:8007412c404cf39fa38e3db600bdf93700410741;anchor=swh:1:rel:1ec2b63253f642eae54f1a3e5ddd20178867bc7d;path=/it/unimi/dsi/fastutil/bytes/ByteArrayFrontCodedList.java;lines=142-159
pub(crate) fn decode_int(data: &[u8]) -> (u32, &[u8]) {
    let high_bit_mask = 0b1000_0000u8;
    let invert = |n: u8| (!n) as u32;

    if data[0] & high_bit_mask == 0 {
        (data[0] as u32, &data[1..])
    } else if data[1] & high_bit_mask == 0 {
        ((invert(data[0]) << 7) | (data[1] as u32), &data[2..])
    } else if data[2] & high_bit_mask == 0 {
        (
            ((invert(data[0])) << 14) | (invert(data[1]) << 7) | (data[2] as u32),
            &data[3..],
        )
    } else if data[3] & high_bit_mask == 0 {
        (
            (invert(data[0]) << 21)
                | (invert(data[1]) << 14)
                | (invert(data[2]) << 7)
                | (data[3] as u32),
            &data[4..],
        )
    } else {
        (
            ((invert(data[0])) << 28)
                | (invert(data[1]) << 21)
                | (invert(data[2]) << 14)
                | (invert(data[3]) << 7)
                | (data[4] as u32),
            &data[5..],
        )
    }
}
