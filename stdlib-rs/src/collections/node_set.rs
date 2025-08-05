// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

//! Sets of values from 0 to a known maximum.
//!
//! This module contains a trait to use [`HashSet<usize>`](HashSet) and [`BitVec`]
//! interchangeably, and defines [`AdaptiveNodeSet`] which switches between the two
//! based on its content.

use std::collections::HashSet;
use std::hash::BuildHasher;

use rapidhash::RapidBuildHasher;
use sux::bits::bit_vec::BitVec;

/// Constant controlling when a [`AdaptiveNodeSet`] should be promoted from sparse to dense.
///
/// Promotion happens when the number of items in a [`AdaptiveNodeSet`] times this constant
/// is greater than the maximum value.
///
/// The value was computed experimentally, using "find-earliest-revision" (which runs
/// many BFSs of highly heterogeneous sizes) on the 2023-09-06 graph, running on
/// Software Heritage's Maxxi computer (Xeon Gold 6342 CPU @ 2.80GHz, 96 threads, 4TB RAM)
/// That graph contains 34 billion nodes, and performance increases continuously when
/// increasing up to 100 millions, then plateaus up to 1 billion; though the latter
/// uses a little less memory most of the time.
const PROMOTION_THRESHOLD: usize = 64;

pub type NodeId = usize;

/// A set of `usize` with a known maximum value
pub trait ReadNodeSet {
    fn contains(&self, node: NodeId) -> bool;
}

/// A set of `usize` with a known maximum value, that can be mutated
pub trait NodeSet: ReadNodeSet {
    fn insert(&mut self, node: NodeId);
}

pub struct EmptyNodeSet;

impl ReadNodeSet for EmptyNodeSet {
    #[inline(always)]
    fn contains(&self, _node: usize) -> bool {
        false
    }
}
impl IntoIterator for EmptyNodeSet {
    type Item = NodeId;
    type IntoIter = std::iter::Empty<NodeId>;

    #[inline(always)]
    fn into_iter(self) -> Self::IntoIter {
        std::iter::empty()
    }
}

impl<S: BuildHasher> NodeSet for HashSet<usize, S> {
    #[inline(always)]
    fn insert(&mut self, node: usize) {
        HashSet::insert(self, node);
    }
}
impl<S: BuildHasher> ReadNodeSet for HashSet<usize, S> {
    #[inline(always)]
    fn contains(&self, node: usize) -> bool {
        HashSet::contains(self, &node)
    }
}

impl NodeSet for BitVec {
    #[inline(always)]
    fn insert(&mut self, node: usize) {
        self.set(node, true);
    }
}
impl ReadNodeSet for BitVec {
    #[inline(always)]
    fn contains(&self, node: usize) -> bool {
        self.get(node)
    }
}

#[derive(Debug, PartialEq, Eq)]
/// A slice of `NodeId` whose values are in ascending order
///
/// # Safety
///
/// Unsafe code should not rely on this to be sorted, as safe code can build arbitrary instances
pub struct SortedNodeIdSlice<S: AsRef<[NodeId]> + ?Sized>(pub S);

impl<S: AsRef<[NodeId]> + ?Sized> ReadNodeSet for SortedNodeIdSlice<S> {
    /// Runs in logarithmic time, as it performs a binary search
    #[inline(always)]
    fn contains(&self, node: usize) -> bool {
        self.0.as_ref().binary_search(&node).is_ok()
    }
}

impl<S: AsRef<[NodeId]>> std::ops::Deref for SortedNodeIdSlice<S> {
    type Target = S;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'a> IntoIterator for SortedNodeIdSlice<&'a [NodeId]> {
    type Item = NodeId;
    type IntoIter = std::iter::Copied<std::slice::Iter<'a, NodeId>>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter().copied()
    }
}

impl<'a, S: AsRef<[NodeId]>> From<&'a SortedNodeIdSlice<S>> for SortedNodeIdSlice<&'a [NodeId]> {
    fn from(v: &'a SortedNodeIdSlice<S>) -> Self {
        SortedNodeIdSlice(v.0.as_ref())
    }
}

/// Implementation of [`NodeSet`] that dynamically changes the underlying representation
/// based on its content
///
/// The current implementation is initialized with a [`HashSet`], but switches to
/// [`BitVec`] once the data becomes dense.
///
/// This has the advantage of allocating little memory if there won't be many elements,
/// but avoiding the overhead of [`HashSet`] when there are.
///
/// ```
/// # use swh_graph_stdlib::collections::{AdaptiveNodeSet, NodeSet};
/// let mut node_set = AdaptiveNodeSet::new(100);
/// assert_eq!(format!("{:?}", node_set), "Sparse { max_items: 100, data: {} }");
/// node_set.insert(10);
/// assert_eq!(format!("{:?}", node_set), "Sparse { max_items: 100, data: {10} }");
/// for i in 20..30 {
///     node_set.insert(i);
/// }
/// assert_eq!(format!("{:?}", node_set), "Dense { data: BitVec { bits: [1072694272, 0], len: 100 } }");
/// ```
#[derive(Debug)]
pub enum AdaptiveNodeSet {
    Sparse {
        max_items: usize,
        data: HashSet<NodeId, RapidBuildHasher>,
    },
    Dense {
        data: BitVec,
    },
}

impl AdaptiveNodeSet {
    /// Creates an empty `AdaptiveNodeSet` that may only store node ids from `0` to `max_items-1`
    #[inline(always)]
    pub fn new(max_items: usize) -> Self {
        AdaptiveNodeSet::Sparse {
            max_items,
            data: HashSet::with_hasher(RapidBuildHasher::default()),
        }
    }

    /// Creates an empty `AdaptiveNodeSet` with at least the specified capacity
    #[inline(always)]
    pub fn with_capacity(max_items: usize, capacity: usize) -> Self {
        if capacity > max_items / PROMOTION_THRESHOLD {
            AdaptiveNodeSet::Dense {
                data: BitVec::new(max_items),
            }
        } else {
            AdaptiveNodeSet::Sparse {
                max_items,
                data: HashSet::with_capacity_and_hasher(capacity, RapidBuildHasher::default()),
            }
        }
    }
}

impl NodeSet for AdaptiveNodeSet {
    /// Adds a node to the set
    ///
    /// # Panics
    ///
    /// If `node` is larger or equal to the `max_items` value passed to [`AdaptiveNodeSet::new`].
    #[inline(always)]
    fn insert(&mut self, node: usize) {
        match self {
            AdaptiveNodeSet::Sparse { max_items, data } => {
                data.insert(node);
                if data.len() > *max_items / PROMOTION_THRESHOLD {
                    // Promote the hashset to a bitvec
                    let mut new_data = BitVec::new(*max_items);
                    for node in data.iter() {
                        new_data.insert(*node);
                    }
                    *self = AdaptiveNodeSet::Dense { data: new_data };
                }
            }
            AdaptiveNodeSet::Dense { data } => data.insert(node),
        }
    }
}

impl ReadNodeSet for AdaptiveNodeSet {
    /// Returns whether the node is part of the set
    ///
    /// # Panics
    ///
    /// If `node` is larger or equal to the `max_items` value passed to [`AdaptiveNodeSet::new`].
    #[inline(always)]
    fn contains(&self, node: usize) -> bool {
        match self {
            AdaptiveNodeSet::Sparse { max_items: _, data } => data.contains(&node),
            AdaptiveNodeSet::Dense { data } => data.contains(node),
        }
    }
}
