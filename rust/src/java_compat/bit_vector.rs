// Copyright (C) 2023  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

//! Structure to read and write a Java `it.unimi.dsi.bits.LongArrayBitVector` object
//! in Java's native serialization format

use std::io::Write;
use std::path::Path;

use anyhow::{Context, Result};
use byteorder::ByteOrder;
use rayon::prelude::*;

/// First bytes of a serialized `it.unimi.dsi.bits.LongArrayBitVector`,
/// followed by the number of bits
const HEADER: &[u8] = b"\xac\xed\x00\x05sr\x00$it.unimi.dsi.bits.LongArrayBitVector\x00\x00\x00\x00\x00\x00\x00\x01\x03\x00\x01\x4a\x00\x06lengthxp";

/// Separator between number of bits and data in
/// `it.unimi.dsi.bits.LongArrayBitVector` serialization, which contains information
/// about the following data, see Java's serialization protocol's
/// [Terminal Symbols and Constants](https://docs.oracle.com/javase/6/docs/platform/serialization/spec/protocol.html#10152)
const TC_BLOCKDATA: u8 = 0x77;
const TC_BLOCKDATALONG: u8 = 0x7A;

/// Last bytes of `it.unimi.dsi.bits.LongArrayBitVector` serialization
const TC_ENDBLOCKDATA: u8 = 0x78;

/// Maximum size of a BLOCKDATA, as its length is an unsigned byte; larger blocks use
/// BLOCKDATALONG whose length is a signed integer
const MAX_BLOCKDATA_SIZE: usize = 256;

const DEFAULT_BLOCKDATALONG_SIZE: usize = 0x100000; // Arbitrary; OpenJDK defaults to 0x400

/// Writer for serialized `it.unimi.dsi.bits.LongArrayBitVector` objects
pub struct LongArrayBitVector<B> {
    data: B,
    num_bits: usize,
}

impl LongArrayBitVector<Vec<u64>> {
    pub fn new_from_bitvec(bitvec: sux::bits::bit_vec::BitVec<Vec<usize>>) -> Self {
        let (vec, num_bits) = bitvec.into_raw_parts();

        assert_eq!(usize::BITS, u64::BITS);
        // Sound because sux interprets values in the vector as being in big endian,
        // so this works no matter the architecture.
        let mut vec: Vec<u64> =
            bytemuck::allocation::try_cast_vec(vec).expect("Could not cast Vec<usize> to Vec<u64>");

        vec.par_iter_mut()
            .for_each(|cell| *cell = u64::from_be(*cell).to_le());

        LongArrayBitVector {
            data: vec,
            num_bits,
        }
    }

    pub fn dump<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let path = path.as_ref();
        let file = std::fs::File::create(path)
            .with_context(|| format!("Could not create {}", path.display()))?;
        let mut file = std::io::BufWriter::new(file);

        file.write_all(HEADER)?;

        let mut length_buf = [0u8; 8];
        byteorder::BigEndian::write_u64(&mut length_buf, self.num_bits as u64);
        file.write_all(&length_buf)?;

        for chunk in self.data.chunks(DEFAULT_BLOCKDATALONG_SIZE / 8) {
            let chunk_size = chunk.len() * 8; // In bytes

            if chunk_size > MAX_BLOCKDATA_SIZE {
                file.write_all(&[TC_BLOCKDATALONG])?;
                let chunk_size: i32 = chunk_size.try_into().expect("Chunk size overflows i32");
                file.write_all(&chunk_size.to_be_bytes())
                    .with_context(|| format!("Could not write to {}", path.display()))?;

                file.write_all(bytemuck::cast_slice(chunk))
                    .with_context(|| format!("Could not write to {}", path.display()))?;
            } else {
                file.write_all(&[TC_BLOCKDATA])?;
                let chunk_size: u8 = chunk_size.try_into().expect("Chunk size overflows u8");
                file.write_all(&chunk_size.to_be_bytes())
                    .with_context(|| format!("Could not write to {}", path.display()))?;

                file.write_all(bytemuck::cast_slice(chunk))
                    .with_context(|| format!("Could not write to {}", path.display()))?;
            }
        }

        file.write_all(&[TC_ENDBLOCKDATA])?;

        Ok(())
    }
}
