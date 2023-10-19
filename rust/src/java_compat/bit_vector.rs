// Copyright (C) 2023  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

/// Structure to read and write a Java`it.unimi.dsi.bits.LongArrayBitVector` object
/// in Java's native serialization format
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use anyhow::{bail, Context, Result};
use byteorder::{BigEndian, ByteOrder, LittleEndian};
use mmap_rs::Mmap;
use rayon::prelude::*;

use crate::utils::mmap::NumberMmap;

/// First bytes of a serialized `it.unimi.dsi.bits.LongArrayBitVector`,
/// followed by the number of bits
const HEADER: &[u8] = b"\xac\xed\x00\x05sr\x00$it.unimi.dsi.bits.LongArrayBitVector\x00\x00\x00\x00\x00\x00\x00\x01\x03\x00\x01\x4a\x00\x06lengthxp";

/// Separator between number of bits and data in
/// `it.unimi.dsi.bits.LongArrayBitVector` serialization
const SEPARATOR: &[u8] = b"\x77\x08";

/// Last bytes of `it.unimi.dsi.bits.LongArrayBitVector` serialization
const FOOTER: &[u8] = b"x";

const OFFSET_BYTES: usize = HEADER.len() + (u64::BITS as usize) / 8 + SEPARATOR.len();

/// Reader for serialized `it.unimi.dsi.bits.LongArrayBitVector` objects
pub struct LongArrayBitVector<B> {
    data: B,
    num_bits: usize,
}

impl LongArrayBitVector<Vec<u64>> {
    pub fn new_from_bitvec(bitvec: sux::bits::bit_vec::BitVec<Vec<usize>>) -> Self {
        let (vec, num_bits) = bitvec.into_raw_parts();

        // Sound because sux interprets values in the vector as being in big endian,
        // so this works no matter the architecture.
        let mut vec: Vec<u64> = unsafe { std::mem::transmute(vec) };

        vec.par_iter_mut()
            .for_each(|cell| *cell = u64::from_be(*cell).to_le());

        LongArrayBitVector {
            data: vec,
            num_bits: num_bits,
        }
    }

    pub fn dump<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let path = path.as_ref();
        let mut file = std::fs::File::create(path)
            .with_context(|| format!("Could not create {}", path.display()))?;

        file.write_all(HEADER)?;

        let mut length_buf = [0u8; 8];
        byteorder::BigEndian::write_u64(&mut length_buf, self.num_bits as u64);
        file.write_all(&length_buf)?;

        file.write_all(SEPARATOR)?;

        file.write_all(bytemuck::cast_slice(self.data.as_ref()))
            .with_context(|| format!("Could not write to {}", path.display()))?;

        file.write_all(FOOTER)?;

        Ok(())
    }
}

impl LongArrayBitVector<NumberMmap<LittleEndian, u64, Mmap>> {
    pub fn new_from_path<P: AsRef<Path>>(path: P, expected_num_bits: usize) -> Result<Self> {
        // Get file
        let path = path.as_ref();
        let mut file = std::fs::File::open(path)
            .with_context(|| format!("Could not open {}", path.display()))?;

        // Read and check header
        let mut header = [0; HEADER.len()];
        file.read(&mut header)
            .with_context(|| format!("Could not read header of {}", path.display()))?;
        if header != HEADER {
            bail!("{} does not have the expected header", path.display());
        }

        // Read and check length
        let mut buf = [0; u64::BITS as usize / 8];
        file.read(&mut buf)
            .with_context(|| format!("Could not read length field of {}", path.display()))?;
        let num_bits = BigEndian::read_u64(&buf);
        if num_bits != expected_num_bits.try_into().unwrap() {
            bail!(
                "expected {} bits in {}, got {}",
                num_bits,
                path.display(),
                num_bits
            );
        }

        // Read and check separator
        let mut separator = [0; SEPARATOR.len()];
        file.read(&mut separator)
            .with_context(|| format!("Could not read separator of {}", path.display()))?;
        if separator != SEPARATOR {
            bail!("{} does not have the expected header", path.display());
        }

        assert_eq!(
            OFFSET_BYTES,
            file.stream_position()
                .with_context(|| format!("Could not get position in {}", path.display()))?
                .try_into()
                .unwrap()
        );

        // Seek to end, read and check footer
        let cell_bits = u64::BITS as u64;
        let cell_bytes = cell_bits / 8;
        let num_cells = if num_bits % cell_bits == 0 {
            num_bits / cell_bits
        } else {
            num_bits / cell_bits + 1
        };
        let num_bytes = num_cells * cell_bytes;
        file.seek(SeekFrom::Current(num_bytes.try_into().unwrap()))
            .with_context(|| format!("Could not seek to footer of {}", path.display()))?;
        let mut footer = [0; FOOTER.len()];
        file.read(&mut footer)
            .with_context(|| format!("Could not read footer of {}", path.display()))?;
        if footer != FOOTER {
            bail!("{} does not have the expected header", path.display());
        }

        // mmap the bitfield
        let data: NumberMmap<_, u64, _> = NumberMmap::new_with_file_and_offset(
            path,
            num_cells.try_into().unwrap(),
            file,
            OFFSET_BYTES,
        )
        .context("Could not open is_skipped_content")?;

        Ok(LongArrayBitVector {
            num_bits: num_bits.try_into().unwrap(),
            data,
        })
    }

    pub fn get(&self, i: usize) -> Option<bool> {
        if i < self.num_bits {
            let cell_bytes = u64::BITS as usize / 8;
            //let i = i + OFFSET_BITS; // Account for the header/length/separator
            let cell_id = i / (cell_bytes * 8);
            // Safe because we checked node_id is smaller than self.num_bits,
            // and the constructor checked self.is_skipped_content has length
            // ceil(self.num_bits / cell_bytes)
            let cell = u64::from_be(unsafe { self.data.get_unchecked(cell_id) });

            let mask = 1u64 << (i % (cell_bytes * 8));

            Some(cell & mask != 0)
        } else {
            None
        }
    }
}
