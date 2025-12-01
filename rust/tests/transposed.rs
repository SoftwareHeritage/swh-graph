// Copyright (C) 2024 The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::path::PathBuf;

use swh_graph::graph::*;
use swh_graph::views::{Subgraph, Transposed};
use swh_graph::webgraph::graphs::vec_graph::VecGraph;

#[test]
fn test_transpose_subgraph() {
    let graph = SwhUnidirectionalGraph::from_underlying_graph(
        PathBuf::new(),
        VecGraph::from_arcs(vec![(2, 0), (2, 1), (0, 1)]),
    );
    // Delete all nodes
    let filtered = Subgraph::with_node_filter(graph, |_| false);
    let transposed = Transposed(filtered);

    assert!(!transposed.has_node(0));
    assert!(!transposed.has_node(1));
    assert!(!transposed.has_node(2));
}
