// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::collections::HashMap;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::num::NonZeroUsize;
use std::path::Path;

use anyhow::{Context, Result};

use crate::utils::suffix_path;

#[derive(thiserror::Error, Debug, PartialEq, Eq)]
pub enum PushError {
    #[error("Found the same string twice in a row")]
    Duplicate,
    #[error("String length is 2^28 or larger")]
    TooLarge,
}

pub struct FrontCodedListBuilder {
    /// The number of strings in a block, this regulates the compression vs
    /// decompression speed tradeoff
    block_size: NonZeroUsize,
    /// Number of encoded strings
    len: usize,
    /// The encoded bytestrings
    data: Vec<u8>,
    /// The pointer to in which byte the k-th string start, in big endian
    pointers: Vec<u8>,
    last_string: Option<Vec<u8>>,
}

impl FrontCodedListBuilder {
    /// Creates a new [`FrontCodedListBuilder] with blocks of the given size
    pub fn new(block_size: NonZeroUsize) -> Self {
        Self {
            block_size,
            len: 0,
            data: Vec::new(),
            pointers: Vec::new(),
            last_string: None,
        }
    }

    /// Adds a string at the end of the Front-Coded List
    ///
    /// Returns `Err` if the string is not strictly greater than the previous one.
    pub fn push(&mut self, s: Vec<u8>) -> Result<(), PushError> {
        if self.len % self.block_size == 0 {
            // This is the first string of the block, decode all of it.
            self.pointers.extend(
                u64::try_from(self.data.len())
                    .expect("String length overflowed u64")
                    .to_be_bytes(),
            );
            push_int(&mut self.data, s.len())?;
            self.data.extend(&s);
        } else {
            // Omit the prefix
            let last_string = self
                .last_string
                .as_ref()
                .expect("Not at start of block, but last_string is unset");
            let prefix_len = longest_common_prefix(last_string, &s)?;
            let suffix_len = s.len() - prefix_len;

            push_int(&mut self.data, suffix_len)?;
            push_int(&mut self.data, prefix_len)?;
            self.data.extend(&s[prefix_len..]);
        }

        self.len += 1;
        self.last_string = Some(s);

        Ok(())
    }

    /// Writes the FCL to disk
    pub fn dump(&self, base_path: impl AsRef<Path>) -> Result<()> {
        let properties_path = suffix_path(&base_path, ".properties");
        let bytearray_path = suffix_path(&base_path, ".bytearray");
        let pointers_path = suffix_path(&base_path, ".pointers");

        let (res1, res2) = rayon::join(
            || -> Result<()> {
                let file = File::create_new(&bytearray_path)
                    .with_context(|| format!("Could not create {}", bytearray_path.display()))?;
                let mut writer = BufWriter::new(file);
                writer
                    .write_all(&self.data)
                    .context("Could not write bytearray")?;
                writer.flush().context("Could not write bytearray")?;
                Ok(())
            },
            || -> Result<()> {
                let file = File::create_new(&pointers_path)
                    .with_context(|| format!("Could not create {}", pointers_path.display()))?;
                let mut writer = BufWriter::new(file);
                writer
                    .write_all(&self.pointers)
                    .context("Could not write pointers")?;
                writer.flush().context("Could not write bytearray")?;
                Ok(())
            },
        );
        res1?;
        res2?;

        let properties_file = File::create_new(&properties_path)
            .with_context(|| format!("Could not create {}", properties_path.display()))?;
        let mut writer = BufWriter::new(properties_file);
        java_properties::write(
            &mut writer,
            &HashMap::from_iter(vec![
                ("ratio".to_string(), self.block_size.to_string()),
                ("n".to_string(), self.len.to_string()),
            ]),
        )
        .context("Could not write properties")?;
        writer.flush().context("Could not flush properties")?;

        Ok(())
    }
}

#[inline(always)]
/// Computes the longest common prefix between two strings as bytes.
///
/// Returns `Err` if the second string is not strictly greater than the first.
fn longest_common_prefix(a: &[u8], b: &[u8]) -> Result<usize, PushError> {
    let min_len = usize::min(a.len(), b.len());
    for i in 0..min_len {
        if a[i] != b[i] {
            return Ok(i);
        }
    }

    if a.len() == b.len() {
        // Both strings are equal
        return Err(PushError::Duplicate);
    }

    // a is a prefix of b
    Ok(min_len)
}

#[inline(always)]
/// Writes a varint at the end of the given vector
///
/// Returns `Err` if the given integer is 2^28 or greater
pub(crate) fn push_int(v: &mut Vec<u8>, i: usize) -> Result<(), PushError> {
    if i < 1 << 7 {
        v.push(i as u8);
        Ok(())
    } else if i < 1 << 14 {
        v.push(!(i >> 7) as u8);
        v.push((i & 0x7F) as u8);
        Ok(())
    } else if i < 1 << 21 {
        v.push(!(i >> 14) as u8);
        v.push(!((i >> 7) & 0x7F) as u8);
        v.push((i & 0x7F) as u8);
        Ok(())
    } else if i < 1 << 28 {
        v.push(!(i >> 21) as u8);
        v.push(!((i >> 14) & 0x7F) as u8);
        v.push(!((i >> 7) & 0x7F) as u8);
        v.push((i & 0x7F) as u8);
        Ok(())
    } else {
        Err(PushError::TooLarge)
    }
}

#[test]
fn test_longest_common_prefix() {
    assert_eq!(
        longest_common_prefix(b"foo", b"foo"),
        Err(PushError::Duplicate)
    );
    assert_eq!(longest_common_prefix(b"bar", b"foo"), Ok(0));
    assert_eq!(longest_common_prefix(b"", b"foo"), Ok(0));
    assert_eq!(longest_common_prefix(b"bar", b"baz"), Ok(2));
    assert_eq!(longest_common_prefix(b"baz", b"bar"), Ok(2));
    assert_eq!(longest_common_prefix(b"quux", b"quxx"), Ok(2));
    assert_eq!(longest_common_prefix(b"qux", b"quux"), Ok(2));
}
