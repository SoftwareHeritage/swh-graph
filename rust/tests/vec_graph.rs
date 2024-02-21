// Copyright (C) 2024 The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::path::PathBuf;

use swh_graph::graph::*;
use swh_graph::webgraph::graphs::vec_graph::VecGraph;
use swh_graph::webgraph::labels::proj::Left;

#[test]
fn test_vec_graph() {
    let graph = SwhUnidirectionalGraph::from_underlying_graph(
        PathBuf::new(),
        Left(VecGraph::from_arc_list(vec![(2, 0), (2, 1), (0, 1)])),
    );

    assert_eq!(graph.successors(0).into_iter().collect::<Vec<_>>(), vec![1]);
    assert_eq!(
        graph.successors(1).into_iter().collect::<Vec<_>>(),
        Vec::<usize>::new()
    );
    assert_eq!(
        graph.successors(2).into_iter().collect::<Vec<_>>(),
        vec![0, 1]
    );
}
