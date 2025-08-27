// Copyright (C) 2025  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

#![allow(clippy::single_element_loop)]

use itertools::Itertools;

use swh_graph::graph::NodeId;

use swh_graph_stdlib::collections::{smallnodeset, NodeSet, ReadNodeSet, SmallNodeSet};

#[test]
fn test_smallnodeset_size() {
    assert_eq!(
        std::mem::size_of::<SmallNodeSet>(),
        std::mem::size_of::<NodeId>(),
        "SmallNodeSet is bigger than NodeId",
    );
}

#[test]
fn test_macro_and_equality() {
    assert_eq!(smallnodeset![], SmallNodeSet::default());

    assert_eq!(smallnodeset![], smallnodeset![]);

    assert_ne!(smallnodeset![123], smallnodeset![]);
    assert_ne!(smallnodeset![], smallnodeset![123]);

    assert_ne!(smallnodeset![123], smallnodeset![456]);
    assert_ne!(smallnodeset![456], smallnodeset![123]);

    assert_ne!(smallnodeset![123, 456], smallnodeset![]);
    assert_ne!(smallnodeset![], smallnodeset![123, 456]);

    assert_ne!(smallnodeset![123, 456], smallnodeset![123]);
    assert_ne!(smallnodeset![123], smallnodeset![123, 456]);

    assert_eq!(smallnodeset![usize::MAX], smallnodeset![usize::MAX]);
    assert_ne!(smallnodeset![usize::MAX], smallnodeset![456]);

    let mut nodes = SmallNodeSet::default();
    nodes.insert(123);
    assert_ne!(smallnodeset![], nodes);
    assert_eq!(smallnodeset![123], nodes);
    assert_ne!(smallnodeset![123, 456], nodes);

    nodes.insert(456);
    assert_ne!(smallnodeset![], nodes);
    assert_ne!(smallnodeset![123], nodes);
    assert_eq!(smallnodeset![123, 456], nodes);
    assert_eq!(smallnodeset![456, 123], nodes);
}

#[test]
fn test_smallnodeset_insert_ascending() {
    let mut node_set = SmallNodeSet::default();

    assert_eq!(
        node_set.iter().sorted().collect::<Vec<_>>(),
        Vec::<usize>::new()
    );
    assert_eq!(node_set.len(), 0);
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

    assert_eq!(node_set.iter().sorted().collect::<Vec<_>>(), vec![0]);
    assert_eq!(node_set.len(), 1);
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

    assert_eq!(node_set.iter().sorted().collect::<Vec<_>>(), vec![0, 1]);
    assert_eq!(node_set.len(), 2);
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

    assert_eq!(node_set.iter().sorted().collect::<Vec<_>>(), vec![0, 1, 2]);
    assert_eq!(node_set.len(), 3);
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

    assert_eq!(
        node_set.iter().sorted().collect::<Vec<_>>(),
        vec![0, 1, 2, usize::MAX - 1]
    );
    assert_eq!(node_set.len(), 4);
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

    assert_eq!(
        node_set.iter().sorted().collect::<Vec<_>>(),
        vec![0, 1, 2, usize::MAX - 1, usize::MAX]
    );
    assert_eq!(node_set.len(), 5);
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

    assert_eq!(
        node_set.iter().sorted().collect::<Vec<_>>(),
        Vec::<usize>::new()
    );
    assert_eq!(node_set.len(), 0);
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

    assert_eq!(
        node_set.iter().sorted().collect::<Vec<_>>(),
        vec![usize::MAX - 1]
    );
    assert_eq!(node_set.len(), 1);
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

    assert_eq!(
        node_set.iter().sorted().collect::<Vec<_>>(),
        vec![usize::MAX - 2, usize::MAX - 1]
    );
    assert_eq!(node_set.len(), 2);
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

    assert_eq!(
        node_set.iter().sorted().collect::<Vec<_>>(),
        vec![5, usize::MAX - 2, usize::MAX - 1]
    );
    assert_eq!(node_set.len(), 3);
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

    assert_eq!(
        node_set.iter().sorted().collect::<Vec<_>>(),
        vec![0, 5, usize::MAX - 2, usize::MAX - 1]
    );
    assert_eq!(node_set.len(), 4);
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

    assert_eq!(
        node_set.iter().sorted().collect::<Vec<_>>(),
        Vec::<usize>::new()
    );
    assert_eq!(node_set.len(), 0);
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

    assert_eq!(
        node_set.iter().sorted().collect::<Vec<_>>(),
        vec![usize::MAX]
    );
    assert_eq!(node_set.len(), 1);
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

    assert_eq!(
        node_set.iter().sorted().collect::<Vec<_>>(),
        vec![usize::MAX - 1, usize::MAX]
    );
    assert_eq!(node_set.len(), 2);
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

    assert_eq!(
        node_set.iter().sorted().collect::<Vec<_>>(),
        Vec::<usize>::new()
    );
    assert_eq!(node_set.len(), 0);
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

    assert_eq!(
        node_set.iter().sorted().collect::<Vec<_>>(),
        vec![usize::MAX - 1]
    );
    assert_eq!(node_set.len(), 1);
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

    assert_eq!(
        node_set.iter().sorted().collect::<Vec<_>>(),
        vec![usize::MAX - 1, usize::MAX]
    );
    assert_eq!(node_set.len(), 2);
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
fn test_smallnodeset_deep_clone() {
    let mut node_set = SmallNodeSet::default();

    node_set.insert(0);
    node_set.insert(1);
    node_set.insert(10);

    assert_eq!(node_set.iter().sorted().collect::<Vec<_>>(), vec![0, 1, 10]);
    assert_eq!(node_set.len(), 3);

    let mut node_set_2 = node_set.clone();

    node_set.insert(100);

    assert_eq!(
        node_set.iter().sorted().collect::<Vec<_>>(),
        vec![0, 1, 10, 100]
    );
    assert_eq!(node_set.len(), 4);
    assert_eq!(
        node_set_2.iter().sorted().collect::<Vec<_>>(),
        vec![0, 1, 10]
    );
    assert_eq!(node_set_2.len(), 3);

    node_set_2.insert(200);

    assert_eq!(
        node_set.iter().sorted().collect::<Vec<_>>(),
        vec![0, 1, 10, 100]
    );
    assert_eq!(node_set.len(), 4);
    assert_eq!(
        node_set_2.iter().sorted().collect::<Vec<_>>(),
        vec![0, 1, 10, 200]
    );
    assert_eq!(node_set_2.len(), 4);
}
