// Copyright (C) 2025  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

#![allow(clippy::single_element_loop)]

use swh_graph::collections::NodeSet;
use swh_graph::graph::NodeId;

use swh_graph_stdlib::SmallNodeSet;

#[test]
fn test_smallnodeset_size() {
    assert_eq!(
        std::mem::size_of::<SmallNodeSet>(),
        std::mem::size_of::<NodeId>(),
        "SmallNodeSet is bigger than NodeId",
    );
}

#[test]
fn test_smallnodeset_insert_ascending() {
    let mut node_set = SmallNodeSet::default();

    for node in [
        0,
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        usize::MAX - 2,
        usize::MAX - 1,
        usize::MAX,
    ] {
        assert!(
            !node_set.contains(node),
            "empty SmallNodeSet claims to contain {node}"
        );
    }

    node_set.insert(0);

    for node in [
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        usize::MAX - 2,
        usize::MAX - 1,
        usize::MAX,
    ] {
        assert!(
            !node_set.contains(node),
            "SmallNodeSet claims to contain {node}"
        );
    }
    for node in [0] {
        assert!(
            node_set.contains(node),
            "SmallNodeSet claims not to contain {node}"
        );
    }

    node_set.insert(1);

    for node in [2, 3, 4, 5, 6, 7, usize::MAX - 2, usize::MAX - 1, usize::MAX] {
        assert!(
            !node_set.contains(node),
            "SmallNodeSet claims to contain {node}"
        );
    }
    for node in [0, 1] {
        assert!(
            node_set.contains(node),
            "SmallNodeSet claims not to contain {node}"
        );
    }

    node_set.insert(2);

    for node in [3, 4, 5, 6, 7, usize::MAX - 2, usize::MAX - 1, usize::MAX] {
        assert!(
            !node_set.contains(node),
            "SmallNodeSet claims to contain {node}"
        );
    }
    for node in [0, 1, 2] {
        assert!(
            node_set.contains(node),
            "SmallNodeSet claims not to contain {node}"
        );
    }

    node_set.insert(usize::MAX - 1);

    for node in [3, 4, 5, 6, 7, usize::MAX - 2, usize::MAX] {
        assert!(
            !node_set.contains(node),
            "SmallNodeSet claims to contain {node}"
        );
    }
    for node in [0, 1, 2, usize::MAX - 1] {
        assert!(
            node_set.contains(node),
            "SmallNodeSet claims not to contain {node}"
        );
    }

    node_set.insert(usize::MAX);

    for node in [3, 4, 5, 6, 7, usize::MAX - 2] {
        assert!(
            !node_set.contains(node),
            "SmallNodeSet claims to contain {node}"
        );
    }
    for node in [0, 1, 2, usize::MAX - 1, usize::MAX] {
        assert!(
            node_set.contains(node),
            "SmallNodeSet claims not to contain {node}"
        );
    }
}

#[test]
fn test_smallnodeset_insert_descending() {
    let mut node_set = SmallNodeSet::default();

    for node in [
        0,
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        usize::MAX - 2,
        usize::MAX - 1,
        usize::MAX,
    ] {
        assert!(
            !node_set.contains(node),
            "empty SmallNodeSet claims to contain {node}"
        );
    }

    node_set.insert(usize::MAX - 1);

    for node in [0, 1, 2, 3, 4, 5, 6, 7, usize::MAX - 2, usize::MAX] {
        assert!(
            !node_set.contains(node),
            "SmallNodeSet claims to contain {node}"
        );
    }
    for node in [usize::MAX - 1] {
        assert!(
            node_set.contains(node),
            "SmallNodeSet claims not to contain {node}"
        );
    }

    node_set.insert(usize::MAX - 2);

    for node in [0, 1, 2, 3, 4, 5, 6, 7, usize::MAX] {
        assert!(
            !node_set.contains(node),
            "SmallNodeSet claims to contain {node}"
        );
    }
    for node in [usize::MAX - 1, usize::MAX - 2] {
        assert!(
            node_set.contains(node),
            "SmallNodeSet claims not to contain {node}"
        );
    }

    node_set.insert(5);

    for node in [0, 1, 2, 3, 4, 6, 7, usize::MAX] {
        assert!(
            !node_set.contains(node),
            "SmallNodeSet claims to contain {node}"
        );
    }
    for node in [5, usize::MAX - 1, usize::MAX - 2] {
        assert!(
            node_set.contains(node),
            "SmallNodeSet claims not to contain {node}"
        );
    }

    node_set.insert(0);

    for node in [1, 2, 3, 4, 6, 7, usize::MAX] {
        assert!(
            !node_set.contains(node),
            "SmallNodeSet claims to contain {node}"
        );
    }
    for node in [0, 5, usize::MAX - 1, usize::MAX - 2] {
        assert!(
            node_set.contains(node),
            "SmallNodeSet claims not to contain {node}"
        );
    }
}

#[test]
fn test_smallnodeset_insert_usize_max_first() {
    let mut node_set = SmallNodeSet::default();

    for node in [
        0,
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        usize::MAX - 2,
        usize::MAX - 1,
        usize::MAX,
    ] {
        assert!(
            !node_set.contains(node),
            "empty SmallNodeSet claims to contain {node}"
        );
    }

    node_set.insert(usize::MAX);

    for node in [0, 1, 2, 3, 4, 5, 6, 7, usize::MAX - 2, usize::MAX - 1] {
        assert!(
            !node_set.contains(node),
            "SmallNodeSet claims to contain {node}"
        );
    }
    for node in [usize::MAX] {
        assert!(
            node_set.contains(node),
            "SmallNodeSet claims not to contain {node}"
        );
    }

    node_set.insert(usize::MAX - 1);

    for node in [0, 1, 2, 3, 4, 5, 6, 7, usize::MAX - 2] {
        assert!(
            !node_set.contains(node),
            "SmallNodeSet claims to contain {node}"
        );
    }
    for node in [usize::MAX - 1, usize::MAX] {
        assert!(
            node_set.contains(node),
            "SmallNodeSet claims not to contain {node}"
        );
    }
}

#[test]
fn test_smallnodeset_insert_usize_max_second() {
    let mut node_set = SmallNodeSet::default();

    for node in [
        0,
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        usize::MAX - 2,
        usize::MAX - 1,
        usize::MAX,
    ] {
        assert!(
            !node_set.contains(node),
            "empty SmallNodeSet claims to contain {node}"
        );
    }

    node_set.insert(usize::MAX - 1);

    for node in [0, 1, 2, 3, 4, 5, 6, 7, usize::MAX - 2, usize::MAX] {
        assert!(
            !node_set.contains(node),
            "SmallNodeSet claims to contain {node}"
        );
    }
    for node in [usize::MAX - 1] {
        assert!(
            node_set.contains(node),
            "SmallNodeSet claims not to contain {node}"
        );
    }

    node_set.insert(usize::MAX);

    for node in [0, 1, 2, 3, 4, 5, 6, 7, usize::MAX - 2] {
        assert!(
            !node_set.contains(node),
            "SmallNodeSet claims to contain {node}"
        );
    }
    for node in [usize::MAX - 1, usize::MAX] {
        assert!(
            node_set.contains(node),
            "SmallNodeSet claims not to contain {node}"
        );
    }
}
