// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::path::PathBuf;

use swh_graph::graph::*;
use swh_graph::properties::{VecMaps, VecTimestamps};
use swh_graph::swhid;
use swh_graph::views::Transposed;
use swh_graph::webgraph::graphs::vec_graph::VecGraph;
use swh_graph::webgraph::labels::proj::Left;

use swh_graph_provenance::earliest_revision::*;

#[test]
fn test_find_earliest_revision_minimal() {
    let backward_graph = SwhUnidirectionalGraph::from_underlying_graph(
        PathBuf::new(),
        Left(VecGraph::from_arc_list(vec![
            (2, 0),
            (3, 1),
            (4, 2),
            (4, 3),
        ])),
    )
    .init_properties()
    .load_properties(|properties| {
        Ok(properties
            .with_maps(VecMaps::new(vec![
                swhid!(swh:1:rev:0000000000000000000000000000000000000000),
                swhid!(swh:1:rev:0000000000000000000000000000000000000001),
                swhid!(swh:1:dir:0000000000000000000000000000000000000002),
                swhid!(swh:1:dir:0000000000000000000000000000000000000003),
                swhid!(swh:1:cnt:0000000000000000000000000000000000000004),
            ]))
            .unwrap()
            .with_timestamps(
                VecTimestamps::new(vec![
                    (Some(1708451441), Some(0), Some(1708451441), Some(0)),
                    (Some(1708453970), Some(0), Some(1708453970), Some(0)),
                    (None, None, None, None),
                    (None, None, None, None),
                    (None, None, None, None),
                    (None, None, None, None),
                ])
                .unwrap(),
            )
            .unwrap())
    })
    .unwrap();
    let graph = Transposed(&backward_graph);
    assert_eq!(
        find_earliest_revision(&graph, 4),
        Some(EarliestRevision {
            node: 0,
            ts: 1708451441,
            rev_occurrences: 2
        })
    );
}
