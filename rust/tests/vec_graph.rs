// Copyright (C) 2024 The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::path::PathBuf;

use swh_graph::graph::*;
use swh_graph::properties::*;
use swh_graph::webgraph::graphs::vec_graph::VecGraph;
use swh_graph::webgraph::labels::proj::Left;
use swh_graph::{SWHType, SWHID};

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

#[test]
fn test_labeled_vec_graph() {
    let arcs: Vec<(usize, usize, &[u64])> = vec![(0, 1, &[0, 789]), (2, 0, &[123]), (2, 1, &[456])];
    let underlying_graph = VecGraph::from_labeled_arc_list(arcs);

    let graph = SwhUnidirectionalGraph::from_underlying_graph(PathBuf::new(), underlying_graph);

    assert_eq!(graph.successors(0).into_iter().collect::<Vec<_>>(), vec![1]);
    assert_eq!(
        graph.successors(1).into_iter().collect::<Vec<_>>(),
        Vec::<usize>::new()
    );
    assert_eq!(
        graph.successors(2).into_iter().collect::<Vec<_>>(),
        vec![0, 1]
    );

    let collect_successors = |node_id| {
        graph
            .labelled_successors(node_id)
            .into_iter()
            .map(|(succ, labels)| (succ, labels.collect()))
            .collect::<Vec<_>>()
    };
    assert_eq!(collect_successors(0), vec![(1, vec![0.into(), 789.into()])]);
    assert_eq!(collect_successors(1), vec![]);
    assert_eq!(
        collect_successors(2),
        vec![(0, vec![123.into()]), (1, vec![456.into()])]
    );
}

#[test]
fn test_vec_graph_maps() {
    let swhids = [
        SWHID {
            namespace_version: 1,
            node_type: SWHType::Revision,
            hash: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
        },
        SWHID {
            namespace_version: 1,
            node_type: SWHType::Revision,
            hash: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1],
        },
        SWHID {
            namespace_version: 1,
            node_type: SWHType::Content,
            hash: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2],
        },
    ];
    let graph = SwhUnidirectionalGraph::from_underlying_graph(
        PathBuf::new(),
        Left(VecGraph::from_arc_list(vec![(2, 0), (2, 1), (0, 1)])),
    )
    .init_properties()
    .load_properties(|properties| {
        properties.with_maps(VecMaps::new(swhids.iter().cloned().collect()))
    })
    .unwrap();

    // Test MPH
    assert_eq!(graph.properties().node_id(swhids[0]), Some(0));
    assert_eq!(graph.properties().node_id(swhids[1]), Some(1));
    assert_eq!(graph.properties().node_id(swhids[2]), Some(2));
    assert_eq!(
        graph.properties().node_id(SWHID {
            namespace_version: 1,
            node_type: SWHType::Content,
            hash: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 42]
        }),
        None
    );

    // Test node2swhid
    assert_eq!(graph.properties().swhid(0), Some(swhids[0]));
    assert_eq!(graph.properties().swhid(1), Some(swhids[1]));
    assert_eq!(graph.properties().swhid(2), Some(swhids[2]));

    // Test node2type
    assert_eq!(graph.properties().node_type(0), Some(SWHType::Revision));
    assert_eq!(graph.properties().node_type(1), Some(SWHType::Revision));
    assert_eq!(graph.properties().node_type(2), Some(SWHType::Content));
}
