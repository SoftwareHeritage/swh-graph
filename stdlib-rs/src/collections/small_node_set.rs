// Copyright (C) 2025  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::collections::HashSet;

use crate::collections::{NodeSet, ReadNodeSet};
use rapidhash::RapidBuildHasher;

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

impl Clone for _SmallNodeSet {
    fn clone(&self) -> Self {
        match unsafe { self.node } {
            EMPTY => _SmallNodeSet { node: EMPTY },
            value if value & 0b1 == 1 => _SmallNodeSet { node: value },
            _ => {
                let hashset = unsafe { (*self.nodes).clone() };
                let hashset_ptr = Box::into_raw(Box::new(hashset));
                assert!(
                    (hashset_ptr as usize) & 0b1 == 0,
                    "hashset_ptr is not aligned: {hashset_ptr:?}"
                );
                _SmallNodeSet { nodes: hashset_ptr }
            }
        }
    }
}

/// A [`NodeSet`] implementation optimized to store zero or one node
///
/// When storing zero or a single node, this structure takes only the size of one node.
/// When storing two nodes or more, it is equivalent to `Box<HashSet<NodeId, RapidBuildHasher>>`.
#[repr(transparent)]
#[derive(Clone)]
pub struct SmallNodeSet(_SmallNodeSet);

impl Default for SmallNodeSet {
    fn default() -> Self {
        SmallNodeSet(_SmallNodeSet { node: EMPTY })
    }
}

impl SmallNodeSet {
    pub fn iter(&self) -> Iter<'_> {
        match unsafe { self.0.node } {
            EMPTY => Iter {
                next: None,
                iter: None,
            },
            value if value & 0b1 == 1 => Iter {
                // single value
                next: Some(value >> 1),
                iter: None,
            },
            _ => {
                let mut iter = unsafe { self.get_hashset() }.iter();
                Iter {
                    next: iter.next().copied(),
                    iter: Some(iter),
                }
            }
        }
    }

    unsafe fn get_hashset(&self) -> &HashSet<NodeId, RapidBuildHasher> {
        unsafe { &*self.0.nodes }
    }

    unsafe fn get_hashset_mut(&mut self) -> &mut HashSet<NodeId, RapidBuildHasher> {
        unsafe { &mut *self.0.nodes }
    }

    pub fn len(&self) -> usize {
        match unsafe { self.0.node } {
            EMPTY => 0,
            value if value & 0b1 == 1 => 1,
            _ => unsafe { self.get_hashset() }.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        match unsafe { self.0.node } {
            EMPTY => true,
            _ => false, // we don't support deletion or with_capacity() yet
        }
    }
}

impl std::fmt::Debug for SmallNodeSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("SmallNodeSet")
            .field(&self.iter().collect::<HashSet<_>>())
            .finish()
    }
}

impl PartialEq for SmallNodeSet {
    fn eq(&self, other: &Self) -> bool {
        match unsafe { (self.0.node, other.0.node) } {
            (EMPTY, EMPTY) => true,
            (EMPTY, _) | (_, EMPTY) => false, // we don't support deletion or with_capacity() yet

            (value1, value2) if value1 & 0b1 == 1 && value2 & 0b1 == 1 => value1 == value2,
            (value1, _) if value1 & 0b1 == 1 => false, // ditto
            (_, value2) if value2 & 0b1 == 1 => false, // ditto
            (_, _) => unsafe { self.get_hashset() == other.get_hashset() },
        }
    }
}

impl Eq for SmallNodeSet {}

impl<'a> IntoIterator for &'a SmallNodeSet {
    type IntoIter = Iter<'a>;
    type Item = NodeId;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

pub struct Iter<'a> {
    next: Option<NodeId>,
    iter: Option<std::collections::hash_set::Iter<'a, NodeId>>,
}

impl Iterator for Iter<'_> {
    type Item = NodeId;

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.next?;
        self.next = self.iter.as_mut().and_then(|iter| iter.next()).copied();
        Some(next)
    }
}

impl NodeSet for SmallNodeSet {
    fn insert(&mut self, node: NodeId) {
        match unsafe { self.0.node } {
            EMPTY => {
                if node & (0b1 << (usize::BITS - 1)) == 0 {
                    // the set is empty AND the node does fits in 63 bits: set the node in the
                    // 63 most significant bits
                    self.0.node = (node << 1) | 0b1;
                } else {
                    // promote directly to hashset because the node does not fit in 63 bits
                    // (and we need the least-significant bit to indicate that this is a single
                    // node)
                    let hashset: HashSet<_, _> = [node].into_iter().collect();
                    let hashset_ptr = Box::into_raw(Box::new(hashset));
                    assert!(
                        (hashset_ptr as usize) & 0b1 == 0,
                        "hashset_ptr is not aligned: {hashset_ptr:?}"
                    );
                    self.0.nodes = hashset_ptr
                }
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
                unsafe { self.get_hashset_mut() }.insert(node);
            }
        }
    }
}
impl ReadNodeSet for SmallNodeSet {
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
                unsafe { self.get_hashset() }.contains(&node)
            }
        }
    }
}

#[macro_export]
macro_rules! smallnodeset {
    [$($node:expr),* $(,)?] => {{
        let mut nodes = SmallNodeSet::default();
        $(
            nodes.insert($node);
        )*
        nodes
    }};

}

pub use smallnodeset;
