// Copyright (C) 2025  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::borrow::Borrow;
use std::ops::Range;
use std::slice::SliceIndex;

use anyhow::{Context, Result};
use dsi_bitstream::prelude::*;
use epserde::Epserde;
use itertools::Itertools;
use sux::dict::elias_fano::{EfSeq, EliasFanoBuilder};
use sux::prelude::IndexedSeq;
use sux::traits::BitFieldSliceCore;

use super::{EmptyNodeSet, NodeId, ReadNodeSet, SortedNodeIdSlice};

pub trait NodeMultimap {
    type NodeSet<'a>: ReadNodeSet
    where
        Self: 'a;

    fn get(&self, node: NodeId) -> Self::NodeSet<'_>;
}

pub struct EmptyNodeMultimap;

impl NodeMultimap for EmptyNodeMultimap {
    type NodeSet<'a> = EmptyNodeSet;

    #[inline(always)]
    fn get(&self, _node: NodeId) -> Self::NodeSet<'_> {
        EmptyNodeSet
    }
}

/// Builder for [`EfIndexedNodeMultimap`]
///
/// # Examples
///
/// See [`EfIndexedNodeMultimap`]
pub struct NodeMultimapBuilder {
    num_keys: usize,
    /// All node sets concatenated as a plain array
    values: Vec<NodeId>,
    /// Stores for each key the length of its set of values.
    ///
    /// By computing the cumulative sum, this gives the offset in the array where a node's set
    /// begins
    lengths: BufBitWriter<LittleEndian, MemWordWriterVec<u64, Vec<u64>>>,
}

// can be replaced with a derive once https://github.com/vigna/dsi-bitstream-rs/pull/20 is merged
impl Default for NodeMultimapBuilder {
    fn default() -> Self {
        NodeMultimapBuilder {
            num_keys: 0,
            values: Default::default(),
            lengths: BufBitWriter::new(MemWordWriterVec::new(Default::default())),
        }
    }
}

impl NodeMultimapBuilder {
    pub fn push(&mut self, value: impl IntoIterator<Item: Borrow<NodeId>>) -> &mut Self {
        self.num_keys += 1;
        let value: Vec<_> = value
            .into_iter()
            .map(|node| *node.borrow())
            .sorted()
            .collect();
        self.lengths
            .write_gamma(u64::try_from(value.len()).expect("number of node ids overflowed usize"))
            .expect("Could not write gamma");
        self.values.extend(value);
        self
    }

    pub fn build(self) -> Result<SequentialNodeMultimap> {
        let Self {
            num_keys,
            values,
            lengths,
        } = self;
        Ok(SequentialNodeMultimap {
            num_keys,
            values: values.into_boxed_slice(),
            lengths: lengths
                .into_inner()
                .expect("Could not flush to MemWordWriterVec")
                .into_inner(),
        })
    }
}

/// Similar to [`NodeMultimap`] but does not implement random-access.
///
/// Use [`build_index`](Self::build_index) to get a structure that does implement it.
///
/// # Examples
///
/// See [`EfIndexedNodeMultimap`]
pub struct SequentialNodeMultimap<V: AsRef<[usize]> = Box<[usize]>, L: AsRef<[u64]> = Vec<u64>> {
    num_keys: usize,
    /// All node sets concatenated as a plain array
    values: V,
    /// Stores for each key the length of its set of values.
    ///
    /// By computing the cumulative sum, this gives the offset in the array where a node's set
    /// begins
    lengths: L,
}

impl<V: AsRef<[usize]>, L: AsRef<[u64]>> SequentialNodeMultimap<V, L> {
    pub fn build_index(self) -> Result<EfIndexedNodeMultimap<V, EfSeq>> {
        let Self {
            num_keys,
            values,
            lengths,
        } = self;
        let mut efb = EliasFanoBuilder::new(num_keys + 1, values.len());
        let mut offset = 0usize;
        efb.push(offset);
        let mut lengths_reader = BufBitReader::<LittleEndian, _>::new(MemWordReader::new(&lengths));
        for _ in 0..num_keys {
            let length = usize::try_from(
                lengths_reader
                    .read_gamma()
                    .context("Could not read gamma")?,
            )
            .context("value set length overflowed usize")?;
            offset = offset
                .checked_add(length)
                .context("offset overflowed usize")?;
            efb.push(offset);
        }
        let offsets = efb.build_with_seq();
        Ok(EfIndexedNodeMultimap { values, offsets })
    }

    // TODO: implement serialization to a plain non-epserde format (so it can be used from non-Rust)
    // and sequential access (if we can find a use-case for that)
}

/// A compact [`NodeMultimap`] that can be serialized to disk and mmapped
///
/// # Examples
///
/// ```
/// use std::fs::File;
/// use std::io::BufWriter;
///
/// use epserde::deser::Deserialize;
/// use epserde::deser::mem_case::Flags;
/// use epserde::ser::Serialize;
/// use sux::dict::elias_fano::EfSeq;
/// use swh_graph_stdlib::collections::{
///     EfIndexedNodeMultimap,
///     NodeMultimap,
///     NodeMultimapBuilder,
///     ReadNodeSet,
///     SortedNodeIdSlice
/// };
///
/// let mut multimap_builder = NodeMultimapBuilder::default();
/// multimap_builder
///     .push(&[1, 2, 3]) // map 0 to [1, 2, 3]
///     .push(&[])        // map 1 to []
///     .push(&[0, 4])    // map 2 to [0, 4]
///     .push(&[])        // map 3 to []
///     .push(&[3, 1]);   // map 4 to [1, 3]
/// let multimap = multimap_builder
///     .build()
///     .expect("Could not build multimap")
///     .build_index()
///     .expect("Could not build multimap index");
///
/// // has the expected values
/// assert_eq!(multimap.get(0), SortedNodeIdSlice(&[1, 2, 3][..]));
/// assert!(multimap.get(0).contains(1));
/// assert!(!multimap.get(0).contains(4));
/// assert_eq!(multimap.get(1), SortedNodeIdSlice(&[][..]));
/// assert_eq!(multimap.get(2), SortedNodeIdSlice(&[0, 4][..]));
/// assert_eq!(multimap.get(3), SortedNodeIdSlice(&[][..]));
/// assert_eq!(multimap.get(4), SortedNodeIdSlice(&[1, 3][..]));
///
/// // can be serialized with epserde
/// let tempdir = tempfile::tempdir().expect("Could not get temp dir");
/// let path = tempdir.path().join("test.nodemm");
/// let mut writer = BufWriter::new(File::create(&path).unwrap());
/// multimap.serialize(&mut writer).expect("Could not serialize");
///
/// // can be deserialized with epserde
/// let multimap = EfIndexedNodeMultimap::<Box<[usize]>, EfSeq>::mmap(&path, Flags::RANDOM_ACCESS).expect("Could not deserialize");
///
/// // has the expected values
/// assert_eq!(multimap.get(0), SortedNodeIdSlice(&[1, 2, 3][..]));
/// assert_eq!(multimap.get(1), SortedNodeIdSlice(&[][..]));
/// assert_eq!(multimap.get(2), SortedNodeIdSlice(&[0, 4][..]));
/// assert_eq!(multimap.get(3), SortedNodeIdSlice(&[][..]));
/// assert_eq!(multimap.get(4), SortedNodeIdSlice(&[1, 3][..]));
/// ```
#[derive(Epserde)]
pub struct EfIndexedNodeMultimap<V: AsRef<[usize]> = Box<[usize]>, O: IndexedSeq = EfSeq> {
    values: V,
    offsets: O,
}

impl<V: AsRef<[usize]>, O: IndexedSeq<Output = usize>> NodeMultimap for EfIndexedNodeMultimap<V, O>
where
    Range<usize>: SliceIndex<[usize]>,
{
    type NodeSet<'a>
        = SortedNodeIdSlice<&'a [NodeId]>
    where
        Self: 'a;

    fn get(&self, node: NodeId) -> Self::NodeSet<'_> {
        let start_offset = self.offsets.get(node);
        let end_offset = self
            .offsets
            .get(node.checked_add(1).expect("NodeId is too usize::MAX"));
        SortedNodeIdSlice(&self.values.as_ref()[start_offset..end_offset])
    }
}
impl<V: AsRef<[usize]>, O: IndexedSeq> EfIndexedNodeMultimap<V, O> {}
