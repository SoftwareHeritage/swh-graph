// Copyright (C) 2023  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

//! Structure to read and write a Java `it.unimi.dsi.bits.LongArrayBitVector` object
//! in Java's native serialization format

use std::io::{Read, Write};
use std::os::fd::AsRawFd;
use std::path::Path;

use anyhow::{bail, Context, Result};
use byteorder::{BigEndian, ByteOrder};
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

/// Reader for serialized `it.unimi.dsi.bits.LongArrayBitVector` objects
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
        let mut vec: Vec<u64> = unsafe { std::mem::transmute(vec) };

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

                file.write_all(bytemuck::cast_slice(chunk.as_ref()))
                    .with_context(|| format!("Could not write to {}", path.display()))?;
            } else {
                file.write_all(&[TC_BLOCKDATA])?;
                let chunk_size: u8 = chunk_size.try_into().expect("Chunk size overflows u8");
                file.write_all(&chunk_size.to_be_bytes())
                    .with_context(|| format!("Could not write to {}", path.display()))?;

                file.write_all(bytemuck::cast_slice(chunk.as_ref()))
                    .with_context(|| format!("Could not write to {}", path.display()))?;
            }
        }

        file.write_all(&[TC_ENDBLOCKDATA])?;

        Ok(())
    }

    pub fn new_from_path<P: AsRef<Path>>(path: P, expected_num_bits: usize) -> Result<Self> {
        // Get file
        let path = path.as_ref();
        let mut file = std::fs::File::open(path)
            .with_context(|| format!("Could not open {}", path.display()))?;

        #[cfg(target_os = "linux")]
        unsafe {
            libc::posix_fadvise(
                file.as_raw_fd(),
                0,
                0,
                libc::POSIX_FADV_SEQUENTIAL | libc::POSIX_FADV_WILLNEED,
            )
        };

        // Read and check header
        let mut header = [0; HEADER.len()];
        file.read(&mut header)
            .with_context(|| format!("Could not read header of {}", path.display()))?;
        if header != HEADER {
            bail!(
                "{} does not have the expected header: {:?}",
                path.display(),
                String::from_utf8_lossy(&header)
            );
        }

        // Read and check length
        let mut buf = [0; u64::BITS as usize / 8];
        file.read(&mut buf)
            .with_context(|| format!("Could not read length field of {}", path.display()))?;
        let num_bits = BigEndian::read_u64(&buf);
        if num_bits != u64::try_from(expected_num_bits).unwrap() {
            bail!(
                "expected {} bits in {}, got {}",
                num_bits,
                path.display(),
                num_bits
            );
        }

        // Read all chunks of the bitfield until we reach TC_ENDBLOCKDATA
        let mut data = Vec::<u64>::with_capacity(expected_num_bits.div_ceil(64));
        let mut type_buf = [0; 1];
        let mut data_buf = Vec::with_capacity(1024); // OpenJDK's chunk size
        for chunk_id in 0.. {
            file.read(&mut type_buf).with_context(|| {
                format!(
                    "Could not read type of chunk {} in {}",
                    chunk_id,
                    path.display()
                )
            })?;
            match type_buf[0] {
                TC_ENDBLOCKDATA => break,
                TC_BLOCKDATA => {
                    let mut bitfield_size = [0; 1];
                    file.read(&mut bitfield_size).with_context(|| {
                        format!("Could not read bitfield size of {}", path.display())
                    })?;
                    let bitfield_size = bitfield_size[0];
                    data_buf.resize(bitfield_size.into(), 0);
                    file.read_exact(&mut data_buf)
                        .with_context(|| format!("Could not read chunk from {}", path.display()))?;
                    data.extend(bytemuck::cast_slice(&data_buf));
                }
                TC_BLOCKDATALONG => {
                    let mut bitfield_size = [0; 4];
                    file.read(&mut bitfield_size).with_context(|| {
                        format!("Could not read bitfield size of {}", path.display())
                    })?;
                    let bitfield_size = i32::from_be_bytes(bitfield_size);
                    data_buf.resize(
                        bitfield_size
                            .try_into()
                            .expect("BLOCKDATALONG size overflowed usize"),
                        0,
                    );
                    file.read_exact(&mut data_buf)
                        .with_context(|| format!("Could not read chunk from {}", path.display()))?;
                    data.extend(bytemuck::cast_slice(&data_buf));
                }
                _ => bail!("Unexpected type of bitfield chunk: 0x{:x}", type_buf[0]),
            }
        }

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
            let cell = u64::from_be(*unsafe { self.data.get_unchecked(cell_id) });

            let mask = 1u64 << (i % (cell_bytes * 8));

            Some(cell & mask != 0)
        } else {
            None
        }
    }
}
