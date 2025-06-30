// Copyright (C) 2025  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::collections::HashSet;

use rapidhash::RapidBuildHasher;
use swh_graph::collections::NodeSet;

type NodeId = usize;

const EMPTY: usize = usize::MAX;

#[repr(C)]
union _SmallNodeSet {
    /// Always has its least significant bit set to 1.
    ///
    /// If equal to [`EMPTY`], then there is no node in the set.
    /// Otherwise, there is a single node, obtained by shifting one bit right
    node: usize,
    /// Always has its least significant bit set to 0.
    nodes: *mut HashSet<usize, RapidBuildHasher>,
}

// SAFETY: HashSet is send, so sending a pointer to it is safe
unsafe impl Send for _SmallNodeSet {}

impl Drop for _SmallNodeSet {
    fn drop(&mut self) {
        match unsafe { self.node } {
            EMPTY => {
                // the set is empty, nothing to do
            }
            value if value & 0b1 == 1 => {
                // the set has one item, nothing to do
            }
            _ => {
                // the set is a hashset
                drop(unsafe { Box::from_raw(self.nodes) })
            }
        }
    }
}

/// A [`NodeSet`] implementation optimized to store zero or one node
///
/// When storing zero or a single node, this structure takes only the size of one node.
/// When storing two nodes or more, it is equivalent to `Box<HashSet<NodeId, RapidBuildHasher>>`.
#[repr(transparent)]
pub struct SmallNodeSet(_SmallNodeSet);

impl Default for SmallNodeSet {
    fn default() -> Self {
        SmallNodeSet(_SmallNodeSet { node: EMPTY })
    }
}

impl NodeSet for SmallNodeSet {
    fn insert(&mut self, node: NodeId) {
        match unsafe { self.0.node } {
            EMPTY if node & (0b1 << (usize::BITS - 1)) == 0 => {
                // the set is empty AND the node does fits in 63 bits: set the node in the
                // 63 most significant bits
                self.0.node = (node << 1) | 0b1;
            }
            value if value & 0b1 == 1 => {
                // the set has one item: we need to promote it to a hashset
                let old_node = value >> 1;
                let hashset: HashSet<_, _> = [old_node, node].into_iter().collect();
                let hashset_ptr = Box::into_raw(Box::new(hashset));
                assert!(
                    (hashset_ptr as usize) & 0b1 == 0,
                    "hashset_ptr is not aligned: {hashset_ptr:?}"
                );
                self.0.nodes = hashset_ptr
            }
            _ => {
                // the set is already a hashset
                unsafe { &mut *self.0.nodes }.insert(node);
            }
        }
    }
    fn contains(&self, node: NodeId) -> bool {
        match unsafe { self.0.node } {
            EMPTY => {
                // the set is empty
                false
            }
            value if value & 0b1 == 1 => {
                // the set has one item
                let existing_node = value >> 1;
                node == existing_node
            }
            _ => {
                // the set is a hashset
                unsafe { &*self.0.nodes }.contains(&node)
            }
        }
    }
}
