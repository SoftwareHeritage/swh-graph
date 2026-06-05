// Copyright (C) 2026  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use swh_graph::graph::{SwhGraph, SwhNullidirectionalGraph};

#[test]
fn test_nullidirectional_graph() {
    let dir = tempfile::tempdir().unwrap();
    let dir = dir.path();
    std::fs::write(dir.join("graph.nodes.count.txt"), "42\n").unwrap();
    std::fs::write(dir.join("graph.edges.count.txt"), "1000\n").unwrap();

    let graph = SwhNullidirectionalGraph::new(dir.join("graph")).unwrap();

    assert_eq!(graph.num_nodes(), 42);
    assert_eq!(graph.num_arcs(), 1000);
}
