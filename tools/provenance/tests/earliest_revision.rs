// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use anyhow::Result;

use swh_graph::graph_builder::GraphBuilder;
use swh_graph::swhid;

use swh_graph_provenance::earliest_revision::*;

#[test]
fn test_find_earliest_revision_minimal() -> Result<()> {
    let mut builder = GraphBuilder::default();
    let rev0 = builder
        .node(swhid!(swh:1:rev:0000000000000000000000000000000000000000))?
        .author_timestamp(1708451441, 0)
        .committer_timestamp(1708451441, 0)
        .done();
    let rev1 = builder
        .node(swhid!(swh:1:rev:0000000000000000000000000000000000000001))?
        .author_timestamp(1708453970, 0)
        .committer_timestamp(1708453970, 0)
        .done();
    let dir2 = builder
        .node(swhid!(swh:1:dir:0000000000000000000000000000000000000002))?
        .done();
    let dir3 = builder
        .node(swhid!(swh:1:dir:0000000000000000000000000000000000000003))?
        .done();
    let cnt4 = builder
        .node(swhid!(swh:1:cnt:0000000000000000000000000000000000000004))?
        .done();
    builder.arc(rev0, dir2);
    builder.arc(rev1, dir3);
    builder.arc(dir2, cnt4);
    builder.arc(dir3, cnt4);

    let graph = builder.done()?;

    assert_eq!(
        find_earliest_revision(&graph, cnt4),
        Some(EarliestRevision {
            node: rev0,
            ts: 1708451441,
            rev_occurrences: 2
        })
    );

    Ok(())
}
