// Copyright (C) 2025  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

#![allow(clippy::needless_borrows_for_generic_args)] // https://github.com/rust-lang/rust-clippy/issues/15333

use std::io::{Cursor, Seek};

use epserde::deser::Deserialize;
use epserde::ser::Serialize;
use sux::dict::elias_fano::EfSeq;
use swh_graph_stdlib::collections::{
    EfIndexedNodeMultimap, NodeMultimap, NodeMultimapBuilder, ReadNodeSet, SortedNodeIdSlice,
};

#[test]
fn test_multimap() {
    let mut multimap_builder = NodeMultimapBuilder::default();
    multimap_builder
        .push([1, 2, 3]) // map 0 to [1, 2, 3]
        .push(&[]) // map 1 to []
        .push([0, 4]) // map 2 to [0, 4]
        .push(&[]) // map 3 to []
        .push([3, 1]); // map 4 to [1, 3]
    let multimap = multimap_builder
        .build()
        .expect("Could not build multimap")
        .build_index()
        .expect("Could not build multimap index");

    // has the expected values
    assert_eq!(multimap.get(0), SortedNodeIdSlice(&[1, 2, 3][..]));
    assert!(multimap.get(0).contains(1));
    assert!(!multimap.get(0).contains(4));
    assert_eq!(multimap.get(1), SortedNodeIdSlice(&[][..]));
    assert_eq!(multimap.get(2), SortedNodeIdSlice(&[0, 4][..]));
    assert_eq!(multimap.get(3), SortedNodeIdSlice(&[][..]));
    assert_eq!(multimap.get(4), SortedNodeIdSlice(&[1, 3][..]));

    // can be serialized with epserde
    let mut writer = Cursor::new(Vec::new());
    multimap
        .serialize(&mut writer)
        .expect("Could not serialize");

    // can be deserialized with epserde
    writer.rewind().unwrap();
    let mut reader = writer;
    let multimap = EfIndexedNodeMultimap::<Box<[usize]>, EfSeq>::deserialize_full(&mut reader)
        .expect("Could not deserialize");

    assert_eq!(multimap.get(0), SortedNodeIdSlice(&[1, 2, 3][..]));
    assert_eq!(multimap.get(1), SortedNodeIdSlice(&[][..]));
    assert_eq!(multimap.get(2), SortedNodeIdSlice(&[0, 4][..]));
    assert_eq!(multimap.get(3), SortedNodeIdSlice(&[][..]));
    assert_eq!(multimap.get(4), SortedNodeIdSlice(&[1, 3][..]));
}
