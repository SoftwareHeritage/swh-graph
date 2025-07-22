// Copyright (C) 2025  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use swh_graph::SWHID;

#[cfg(test)]
mod tests {
    use super::*;
    use std::iter;
    // use swh_graph::labels::EdgeLabel::Branch;
    use swh_graph::graph_builder::GraphBuilder;
    // use swh_graph::labels::Permission;
    use swh_graph::swhid;

    use swh_graph::labels::VisitStatus;

    // this graph is a based of the one found in
    // rust/tests/test_label_iterators.rs:build_graph
    //
    //
    ///// ```
    /// ori0 -->  snp2 -->  rev4
    ///          ^    \
    ///         /      \
    ///        /        \
    /// ori1 -+          -> rev5 -> rev6
    ///     \           /
    ///      \         /
    ///       \       /
    ///        -> snp3
    ///
    /// ori7 --> rev8
    ///  (disjoint graph)
    /// ```
    #[test]
    fn test_process_origins_and_build_subgraph() {
        let mut builder = GraphBuilder::default();

        builder
            .node(SWHID::from_origin_url("https://example.com/repo1"))
            .unwrap()
            .done();
        builder
            .node(SWHID::from_origin_url("https://example.com/repo2"))
            .unwrap()
            .done();
        builder
            .node(swhid!(swh:1:snp:0000000000000000000000000000000000000002))
            .unwrap()
            .done();
        builder
            .node(swhid!(swh:1:snp:0000000000000000000000000000000000000003))
            .unwrap()
            .done();
        builder
            .node(swhid!(swh:1:rev:0000000000000000000000000000000000000004))
            .unwrap()
            .done();
        builder
            .node(swhid!(swh:1:rev:0000000000000000000000000000000000000005))
            .unwrap()
            .done();
        builder
            .node(swhid!(swh:1:rev:0000000000000000000000000000000000000006))
            .unwrap()
            .done();

        builder.ori_arc(0, 2, VisitStatus::Full, 1000002000);
        builder.ori_arc(0, 2, VisitStatus::Full, 1000002001);
        builder.ori_arc(0, 3, VisitStatus::Full, 1000003000);
        builder.ori_arc(1, 2, VisitStatus::Full, 1001002000);
        builder.snp_arc(2, 4, b"refs/heads/snp2-to-rev4");
        builder.snp_arc(2, 5, b"refs/heads/snp2-to-rev5");
        builder.snp_arc(3, 5, b"refs/heads/snp3-to-rev5");
        builder.snp_arc(3, 5, b"refs/heads/snp3-to-rev5-dupe");
        builder.ori_arc(5, 6, VisitStatus::Full, 1001006000);

        // disjoint graph
        builder
            .node(SWHID::from_origin_url("https://example.com/discinnected"))
            .unwrap()
            .done();
        builder
            .node(swhid!(swh:1:rev:0000000000000000000000000000000000000008))
            .unwrap()
            .done();

        builder.ori_arc(7, 8, VisitStatus::Full, 1001007000);

        let graph = builder.done().unwrap();

        let origins = vec![
            Ok("https://example.com/repo1".to_string()),
            // this one should be found with allow_protocol_variations
            Ok("git://example.com/repo2".to_string()),
            Ok("https://unknown.com/repo".to_string()),
        ];
        let (visited, unknown_origins) =
            swh_subgrapher::process_origins_and_build_subgraph(&graph, origins.into_iter(), true)
                .unwrap();

        // Check that we found the expected nodes
        assert_eq!(visited.len(), 7); // should contain both origins and the revision
        assert_eq!(unknown_origins.len(), 1); // the unknown origin
        assert_eq!(unknown_origins[0], "https://unknown.com/repo");

        // Test with empty input
        let (empty_nodes, empty_unknown) =
            swh_subgrapher::process_origins_and_build_subgraph(&graph, iter::empty(), false)
                .unwrap();
        assert!(empty_nodes.is_empty());
        assert!(empty_unknown.is_empty());

        // Test with only invalid origins
        let invalid_origins = vec![
            Ok("https://invalid1.com".to_string()),
            Ok("https://invalid2.com".to_string()),
        ];
        let (invalid_nodes, invalid_unknown) = swh_subgrapher::process_origins_and_build_subgraph(
            &graph,
            invalid_origins.into_iter(),
            false,
        )
        .unwrap();
        assert!(invalid_nodes.is_empty());
        assert_eq!(invalid_unknown.len(), 2);

        // Test with only invalid origins
        let disjoint_origins = vec![Ok("https://example.com/discinnected".to_string())];
        let (disjoint_nodes, disjoint_unknown) =
            swh_subgrapher::process_origins_and_build_subgraph(
                &graph,
                disjoint_origins.into_iter(),
                false,
            )
            .unwrap();
        assert!(disjoint_unknown.is_empty());
        assert_eq!(disjoint_nodes.len(), 2);
    }
}
