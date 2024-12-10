// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

//! Access to precomputed depth of each node's sub-DAG and a topological order
//!
//! It is computed by `bin/generations.rs`.
//!
//! The algorithm to read it is roughly:
//!
//! 1. read first offset from the .offsets (check it's zero)
//! 2. get next offsets from the .offsets, if it is zero, stop. if not, add it to the previous offset.
//! 3. read values from the current position in the .bitstream to the offset (it can be consumed
//!    in parallel with either .par_bridge() or collecting to a Vec and .into_par_iter()).
//! 4. goto 2

use std::fs::File;
use std::io::BufWriter;
use std::path::Path;

use anyhow::{ensure, Context, Result};
use dsi_bitstream::prelude::*;
use mmap_rs::{Mmap, MmapFlags};
use rdst::RadixSort;

use swh_graph::graph::NodeId;
use swh_graph::utils::suffix_path;

pub struct GenerationsWriter {
    current_offset: u64,
    data_writer: BufBitWriter<BE, WordAdapter<u64, BufWriter<File>>>,
    offsets_writer: BufBitWriter<BE, WordAdapter<u64, BufWriter<File>>>,
}

impl GenerationsWriter {
    pub fn new(path: impl AsRef<Path>) -> Result<Self, std::io::Error> {
        let path = path.as_ref();
        let offsets_path = suffix_path(path, ".offsets");
        let data_writer = BufBitWriter::new(WordAdapter::<u64, _>::new(BufWriter::new(
            File::create(path)?,
        )));
        let mut offsets_writer = BufBitWriter::new(WordAdapter::<u64, _>::new(BufWriter::new(
            File::create(offsets_path)?,
        )));
        let current_offset = 0;
        offsets_writer.write_gamma(current_offset)?;
        Ok(GenerationsWriter {
            current_offset,
            data_writer,
            offsets_writer,
        })
    }

    /// Adds a **sorted** list of nodes as the next generation
    ///
    /// # Panics
    ///
    /// When nodes are not sorted
    pub fn push_sorted_generation(&mut self, nodes: &[NodeId]) -> Result<(), std::io::Error> {
        let mut num_written_bits = 0;
        let mut previous_node = 0;
        for &node in nodes {
            assert!(node >= previous_node, "Nodes are not sorted");
            num_written_bits += self.data_writer.write_gamma(
                (node - previous_node)
                    .try_into()
                    .expect("difference between nodes overflowed u64"),
            )?;
            previous_node = node;
        }

        let num_written_bits: u64 = num_written_bits
            .try_into()
            .expect("num_written_bits overflowed u64");

        self.offsets_writer.write_gamma(num_written_bits)?;

        self.current_offset = self
            .current_offset
            .checked_add(num_written_bits)
            .expect("file bit_size overflowed u64");

        Ok(())
    }

    /// Sorts the given list and passes it to [`Self::push_sorted_generation`]
    pub fn sort_and_push_generation(&mut self, nodes: &mut [NodeId]) -> Result<(), std::io::Error> {
        nodes.radix_sort_unstable();
        self.push_sorted_generation(nodes)
    }

    /// Flushes remaining writes, and writes the footer
    pub fn close(&mut self) -> Result<(), std::io::Error> {
        self.offsets_writer.write_gamma(0)?; // Marks the end of the file
        self.offsets_writer.flush()?;
        self.data_writer.flush()?;
        Ok(())
    }
}

impl Drop for GenerationsWriter {
    fn drop(&mut self) {
        self.close().expect("Could not close GenerationsWriter")
    }
}

pub struct GenerationsReader {
    data: Mmap,
    offsets: Mmap,
}

impl GenerationsReader {
    pub fn new(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let offsets_path = suffix_path(path, ".offsets");
        let file_len = path
            .metadata()
            .with_context(|| format!("Could not stat {}", path.display()))?
            .len();
        let offsets_file_len = offsets_path
            .metadata()
            .with_context(|| format!("Could not stat {}", offsets_path.display()))?
            .len();
        let file =
            File::open(path).with_context(|| format!("Could not open {}", path.display()))?;
        let offsets_file = File::open(&offsets_path)
            .with_context(|| format!("Could not open {}", offsets_path.display()))?;
        let data = unsafe {
            mmap_rs::MmapOptions::new(file_len as _)
                .context("Could not initialize mmap")?
                .with_flags(MmapFlags::TRANSPARENT_HUGE_PAGES)
                .with_file(&file, 0)
                .map()
                .with_context(|| format!("Could not mmap {}", path.display()))?
        };
        let offsets = unsafe {
            mmap_rs::MmapOptions::new(offsets_file_len as _)
                .context("Could not initialize mmap")?
                .with_flags(MmapFlags::TRANSPARENT_HUGE_PAGES)
                .with_file(&offsets_file, 0)
                .map()
                .with_context(|| format!("Could not mmap {}", offsets_path.display()))?
        };
        Ok(GenerationsReader { data, offsets })
    }

    /// Returns an iterator of iterators of nodes, in topological order
    pub fn iter_generations(&self) -> Result<GenerationsIterator<'_>> {
        GenerationsIterator::new(
            bytemuck::cast_slice(self.data.as_ref()),
            bytemuck::cast_slice(self.offsets.as_ref()),
        )
    }

    /// Returns an iterator of `(generation, node)`, in increasing `generation` order.
    pub fn iter_nodes(&self) -> Result<impl Iterator<Item = (u64, NodeId)> + '_> {
        Ok(self
            .iter_generations()?
            .zip(0u64..)
            .flat_map(|(generation, depth)| generation.map(move |node| (depth, node))))
    }
}

type Bbr<'a> = BufBitReader<BE, MemWordReader<u64, &'a [u64]>>;

pub struct GenerationsIterator<'a> {
    data_reader: Bbr<'a>,
    offsets_reader: Bbr<'a>,
    next_generation_offset: u64,
}

impl<'a> GenerationsIterator<'a> {
    fn new(data: &'a [u64], offsets: &'a [u64]) -> Result<Self> {
        let data_reader = BufBitReader::<BE, _>::new(MemWordReader::new(data));
        let mut offsets_reader = BufBitReader::<BE, _>::new(MemWordReader::new(offsets));
        let next_generation_offset = offsets_reader
            .read_gamma()
            .context("Could not read offset")?;
        ensure!(
            next_generation_offset == 0,
            "first offset should be 0, but is {next_generation_offset}"
        );
        Ok(Self {
            data_reader,
            offsets_reader,
            next_generation_offset,
        })
    }
}

impl<'a> Iterator for GenerationsIterator<'a> {
    type Item = GenerationIterator<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.next_generation_offset == u64::MAX {
            None
        } else {
            let mut data_reader = self.data_reader.clone();
            data_reader
                .set_bit_pos(self.next_generation_offset)
                .expect("Could not set bit_pos");
            let generation_bit_size = self
                .offsets_reader
                .read_gamma()
                .expect("Could not read offset");
            if generation_bit_size == 0 {
                // End of file
                self.next_generation_offset = u64::MAX;
                return None;
            }
            self.next_generation_offset = self
                .next_generation_offset
                .checked_add(generation_bit_size)
                .expect("next_generation_offset overflowed u64");
            Some(GenerationIterator::new(
                data_reader,
                self.next_generation_offset,
            ))
        }
    }
}

pub struct GenerationIterator<'a> {
    data_reader: Bbr<'a>,
    end_offset: u64,
    current_node: NodeId,
}

impl<'a> GenerationIterator<'a> {
    fn new(data_reader: Bbr<'a>, end_offset: u64) -> Self {
        Self {
            data_reader,
            end_offset,
            current_node: 0,
        }
    }
}

impl Iterator for GenerationIterator<'_> {
    type Item = NodeId;

    fn next(&mut self) -> Option<Self::Item> {
        if self.data_reader.bit_pos().expect("Could not get bit pos") >= self.end_offset {
            None
        } else {
            self.current_node = self
                .current_node
                .checked_add(
                    self.data_reader
                        .read_gamma()
                        .expect("Could not read node id")
                        .try_into()
                        .expect("Node id difference overflowed usize"),
                )
                .expect("Node id overflowed usize");
            Some(self.current_node)
        }
    }
}
