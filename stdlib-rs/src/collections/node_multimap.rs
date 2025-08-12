// Copyright (C) 2025  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::borrow::Borrow;
use std::marker::PhantomData;
use std::ops::Range;
use std::slice::SliceIndex;

use anyhow::{Context, Result};
use dsi_bitstream::prelude::*;
use epserde::Epserde;
use itertools::Itertools;
use sux::dict::elias_fano::{EfSeq, EliasFanoBuilder};
use sux::prelude::IndexedSeq;

use super::{EmptyNodeSet, NodeId, SortedSlice};

pub trait NodeMultimap<V> {
    /// Values associated to a node. Usually also implements [`NodeSet`].
    type Values<'a>: IntoIterator<Item = V>
    where
        Self: 'a;

    fn get<KB: Borrow<NodeId>>(&self, node: KB) -> Self::Values<'_>;
}

pub struct EmptyNodeMultimap;

impl NodeMultimap<NodeId> for EmptyNodeMultimap {
    type Values<'a> = EmptyNodeSet;

    #[inline(always)]
    fn get<KB: Borrow<NodeId>>(&self, _node: KB) -> Self::Values<'_> {
        EmptyNodeSet
    }
}

/// Builder for [`EfIndexedNodeMultimap`]
///
/// # Examples
///
/// See [`EfIndexedNodeMultimap`]
pub struct NodeMultimapBuilder<V> {
    num_keys: usize,
    /// All node sets concatenated as a plain array
    values: Vec<V>,
    /// Stores for each key the length of its set of values.
    ///
    /// By computing the cumulative sum, this gives the offset in the array where a node's set
    /// begins
    lengths: BufBitWriter<LittleEndian, MemWordWriterVec<u64, Vec<u64>>>,
}

// can be replaced with a derive once https://github.com/vigna/dsi-bitstream-rs/pull/20 is merged
impl<V> Default for NodeMultimapBuilder<V> {
    fn default() -> Self {
        NodeMultimapBuilder {
            num_keys: 0,
            values: Default::default(),
            lengths: BufBitWriter::new(MemWordWriterVec::new(Default::default())),
        }
    }
}

impl<V: Ord> NodeMultimapBuilder<V> {
    pub fn push(&mut self, value: impl IntoIterator<Item = V>) -> &mut Self {
        self.num_keys += 1;
        let value: Vec<_> = value.into_iter().sorted().collect();
        self.lengths
            .write_gamma(u64::try_from(value.len()).expect("number of node ids overflowed usize"))
            .expect("Could not write gamma");
        self.values.extend(value);
        self
    }

    pub fn build(self) -> Result<SequentialNodeMultimap<V>> {
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
            marker: PhantomData,
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
pub struct SequentialNodeMultimap<V, VA: AsRef<[V]> = Box<[V]>, L: AsRef<[u64]> = Vec<u64>> {
    num_keys: usize,
    /// All node sets concatenated as a plain array
    values: VA,
    /// Stores for each key the length of its set of values.
    ///
    /// By computing the cumulative sum, this gives the offset in the array where a node's set
    /// begins
    lengths: L,
    marker: PhantomData<V>,
}

impl<V: Ord, VA: AsRef<[V]>, L: AsRef<[u64]>> SequentialNodeMultimap<V, VA, L> {
    pub fn build_index(self) -> Result<EfIndexedNodeMultimap<V, VA, EfSeq>> {
        let Self {
            num_keys,
            values,
            lengths,
            marker,
        } = self;
        let mut efb = EliasFanoBuilder::new(num_keys + 1, values.as_ref().len());
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
        Ok(EfIndexedNodeMultimap {
            values,
            offsets,
            marker,
        })
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
///     SortedSlice
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
/// assert_eq!(multimap.get(0), SortedSlice(&[1, 2, 3][..]));
/// assert!(multimap.get(0).contains(1));
/// assert!(!multimap.get(0).contains(4));
/// assert_eq!(multimap.get(1), SortedSlice(&[][..]));
/// assert_eq!(multimap.get(2), SortedSlice(&[0, 4][..]));
/// assert_eq!(multimap.get(3), SortedSlice(&[][..]));
/// assert_eq!(multimap.get(4), SortedSlice(&[1, 3][..]));
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
/// assert_eq!(multimap.get(0), SortedSlice(&[1, 2, 3][..]));
/// assert_eq!(multimap.get(1), SortedSlice(&[][..]));
/// assert_eq!(multimap.get(2), SortedSlice(&[0, 4][..]));
/// assert_eq!(multimap.get(3), SortedSlice(&[][..]));
/// assert_eq!(multimap.get(4), SortedSlice(&[1, 3][..]));
/// ```
#[derive(Epserde)]
pub struct EfIndexedNodeMultimap<V = NodeId, VA: AsRef<[V]> = Box<[V]>, O: IndexedSeq = EfSeq> {
    values: VA,
    offsets: O,
    marker: PhantomData<V>,
}

impl<V: Copy + Ord + PartialOrd, VA: AsRef<[V]>, O: IndexedSeq<Output = usize>> NodeMultimap<V>
    for EfIndexedNodeMultimap<V, VA, O>
where
    Range<usize>: SliceIndex<[usize]>,
{
    type Values<'a>
        = SortedSlice<V, &'a [V]>
    where
        Self: 'a;

    fn get<KB: Borrow<NodeId>>(&self, node: KB) -> Self::Values<'_> {
        let node = *node.borrow();
        let start_offset = self.offsets.get(node);
        let end_offset = self
            .offsets
            .get(node.checked_add(1).expect("NodeId is too usize::MAX"));
        SortedSlice {
            slice: &self.values.as_ref()[start_offset..end_offset],
            marker: PhantomData,
        }
    }
}
