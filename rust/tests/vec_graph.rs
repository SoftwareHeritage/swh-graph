// Copyright (C) 2024 The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::path::PathBuf;

use swh_graph::graph::*;
use swh_graph::labels::LabelNameId;
use swh_graph::properties::*;
use swh_graph::webgraph::graphs::vec_graph::{LabeledVecGraph, VecGraph};
use swh_graph::{NodeType, SWHID};

#[test]
fn test_vec_graph() {
    let graph = SwhUnidirectionalGraph::from_underlying_graph(
        PathBuf::new(),
        VecGraph::from_arcs(vec![(2, 0), (2, 1), (0, 1)]),
    );

    assert_eq!(graph.successors(0).collect::<Vec<_>>(), vec![1]);
    assert_eq!(graph.successors(1).collect::<Vec<_>>(), Vec::<usize>::new());
    assert_eq!(graph.successors(2).collect::<Vec<_>>(), vec![0, 1]);
}

#[test]
fn test_labeled_vec_graph() {
    let arcs: Vec<(usize, usize, &[u64])> = vec![(0, 1, &[0, 789]), (2, 0, &[123]), (2, 1, &[456])];
    let underlying_graph = LabeledVecGraph::from_arcs(arcs);

    let graph = SwhUnidirectionalGraph::from_underlying_graph(PathBuf::new(), underlying_graph);

    assert_eq!(graph.successors(0).collect::<Vec<_>>(), vec![1]);
    assert_eq!(graph.successors(1).collect::<Vec<_>>(), Vec::<usize>::new());
    assert_eq!(graph.successors(2).collect::<Vec<_>>(), vec![0, 1]);

    let collect_successors = |node_id| {
        graph
            .untyped_labeled_successors(node_id)
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
            node_type: NodeType::Revision,
            hash: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
        },
        SWHID {
            namespace_version: 1,
            node_type: NodeType::Revision,
            hash: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1],
        },
        SWHID {
            namespace_version: 1,
            node_type: NodeType::Content,
            hash: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2],
        },
    ];

    let graph = SwhUnidirectionalGraph::from_underlying_graph(
        PathBuf::new(),
        VecGraph::from_arcs(vec![(2, 0), (2, 1), (0, 1)]),
    )
    .init_properties()
    .load_properties(|properties| properties.with_maps(VecMaps::new(swhids.to_vec())))
    .unwrap();

    // Test MPH + order
    assert_eq!(graph.properties().node_id(swhids[0]), Ok(0));
    assert_eq!(graph.properties().node_id(swhids[1]), Ok(1));
    assert_eq!(graph.properties().node_id(swhids[2]), Ok(2));
    let unknown_swhid = SWHID {
        namespace_version: 1,
        node_type: NodeType::Content,
        hash: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 42],
    };
    assert_eq!(
        graph.properties().node_id(unknown_swhid),
        Err(NodeIdFromSwhidError::UnknownSwhid(unknown_swhid))
    );

    // Test node2swhid
    assert_eq!(graph.properties().swhid(0), swhids[0]);
    assert_eq!(graph.properties().swhid(1), swhids[1]);
    assert_eq!(graph.properties().swhid(2), swhids[2]);

    // Test node2type
    assert_eq!(graph.properties().node_type(0), NodeType::Revision);
    assert_eq!(graph.properties().node_type(1), NodeType::Revision);
    assert_eq!(graph.properties().node_type(2), NodeType::Content);
}

#[test]
fn test_vec_graph_timestamps() {
    let graph = SwhUnidirectionalGraph::from_underlying_graph(
        PathBuf::new(),
        VecGraph::from_arcs(vec![(2, 0), (2, 1), (0, 1)]),
    )
    .init_properties()
    .load_properties(|properties| {
        properties.with_timestamps(
            VecTimestamps::new(vec![
                (Some(1708451441), Some(0), Some(1708451442), Some(0)),
                (Some(1708453970), Some(1), Some(1708453971), Some(2)),
                (None, None, None, None),
            ])
            .unwrap(),
        )
    })
    .unwrap();

    assert_eq!(graph.properties().author_timestamp(0), Some(1708451441));
    assert_eq!(graph.properties().author_timestamp_offset(0), Some(0));
    assert_eq!(graph.properties().committer_timestamp(0), Some(1708451442));
    assert_eq!(graph.properties().committer_timestamp_offset(0), Some(0));

    assert_eq!(graph.properties().author_timestamp(1), Some(1708453970));
    assert_eq!(graph.properties().author_timestamp_offset(1), Some(1));
    assert_eq!(graph.properties().committer_timestamp(1), Some(1708453971));
    assert_eq!(graph.properties().committer_timestamp_offset(1), Some(2));

    assert_eq!(graph.properties().author_timestamp(2), None);
    assert_eq!(graph.properties().author_timestamp_offset(2), None);
    assert_eq!(graph.properties().committer_timestamp(2), None);
    assert_eq!(graph.properties().committer_timestamp_offset(2), None);
}

#[test]
fn test_vec_graph_persons() {
    let graph = SwhUnidirectionalGraph::from_underlying_graph(
        PathBuf::new(),
        VecGraph::from_arcs(vec![(2, 0), (2, 1), (0, 1)]),
    )
    .init_properties()
    .load_properties(|properties| {
        properties.with_persons(
            VecPersons::new(vec![
                (Some(123), Some(456)),
                (Some(789), None),
                (None, None),
            ])
            .unwrap(),
        )
    })
    .unwrap();

    assert_eq!(graph.properties().author_id(0), Some(123));
    assert_eq!(graph.properties().committer_id(0), Some(456));

    assert_eq!(graph.properties().author_id(1), Some(789));
    assert_eq!(graph.properties().committer_id(1), None);

    assert_eq!(graph.properties().author_id(2), None);
    assert_eq!(graph.properties().committer_id(2), None);
}

#[test]
fn test_vec_graph_contents() {
    let graph = SwhUnidirectionalGraph::from_underlying_graph(
        PathBuf::new(),
        VecGraph::from_arcs(vec![(2, 0), (2, 1), (0, 1), (2, 3)]),
    )
    .init_properties()
    .load_properties(|properties| {
        properties.with_contents(
            VecContents::new(vec![
                (false, None),
                (false, Some(123)),
                (false, None),
                (true, Some(100_000_000_000)),
            ])
            .unwrap(),
        )
    })
    .unwrap();

    assert!(!graph.properties().is_skipped_content(0));
    assert_eq!(graph.properties().content_length(0), None);

    assert!(!graph.properties().is_skipped_content(1));
    assert_eq!(graph.properties().content_length(1), Some(123));

    assert!(!graph.properties().is_skipped_content(2));
    assert_eq!(graph.properties().content_length(2), None);

    assert!(graph.properties().is_skipped_content(3));
    assert_eq!(graph.properties().content_length(3), Some(100_000_000_000));
}

#[test]
fn test_vec_graph_strings() {
    let graph = SwhUnidirectionalGraph::from_underlying_graph(
        PathBuf::new(),
        VecGraph::from_arcs(vec![(2, 0), (2, 1), (0, 1)]),
    )
    .init_properties()
    .load_properties(|properties| {
        properties.with_strings(
            VecStrings::new(vec![
                (Some("abc"), Some("defgh")),
                (Some(""), Some("aaaaaaaaaaaaaaaaaaa")),
                (None, None),
            ])
            .unwrap(),
        )
    })
    .unwrap();

    assert_eq!(graph.properties().message(0), Some("abc".into()));
    assert_eq!(
        graph.properties().message_base64(0),
        Some("YWJj".as_bytes())
    );
    assert_eq!(graph.properties().tag_name(0), Some("defgh".into()));
    assert_eq!(
        graph.properties().tag_name_base64(0),
        Some("ZGVmZ2g=".as_bytes())
    );

    assert_eq!(graph.properties().message(1), Some("".into()));
    assert_eq!(graph.properties().message_base64(1), Some("".as_bytes()));
    assert_eq!(
        graph.properties().tag_name(1),
        Some("aaaaaaaaaaaaaaaaaaa".into())
    );
    assert_eq!(
        graph.properties().tag_name_base64(1),
        Some("YWFhYWFhYWFhYWFhYWFhYWFhYQ==".as_bytes())
    );

    assert_eq!(graph.properties().message(2), None);
    assert_eq!(graph.properties().message_base64(2), None);
    assert_eq!(graph.properties().tag_name(2), None);
    assert_eq!(graph.properties().tag_name_base64(2), None);
}

#[test]
fn test_vec_graph_label_names() {
    let graph = SwhUnidirectionalGraph::from_underlying_graph(
        PathBuf::new(),
        VecGraph::from_arcs(vec![(2, 0), (2, 1), (0, 1)]),
    )
    .init_properties()
    .load_properties(|properties| {
        properties.with_label_names(
            VecLabelNames::new(vec!["abc", "defgh", "", "aaaaaaaaaaaaaaaaaaa"]).unwrap(),
        )
    })
    .unwrap();

    assert_eq!(
        graph.properties().label_name(LabelNameId(0)),
        b"abc".to_vec()
    );
    assert_eq!(
        graph.properties().label_name_base64(LabelNameId(0)),
        b"YWJj".to_vec()
    );

    assert_eq!(
        graph.properties().label_name(LabelNameId(1)),
        b"defgh".to_vec()
    );
    assert_eq!(
        graph.properties().label_name_base64(LabelNameId(1)),
        b"ZGVmZ2g=".to_vec()
    );

    assert_eq!(graph.properties().label_name(LabelNameId(2)), b"".to_vec());
    assert_eq!(
        graph.properties().label_name_base64(LabelNameId(2)),
        b"".to_vec()
    );

    assert_eq!(
        graph.properties().label_name(LabelNameId(3)),
        b"aaaaaaaaaaaaaaaaaaa".to_vec()
    );
    assert_eq!(
        graph.properties().label_name_base64(LabelNameId(3)),
        b"YWFhYWFhYWFhYWFhYWFhYWFhYQ==".to_vec()
    );
}
