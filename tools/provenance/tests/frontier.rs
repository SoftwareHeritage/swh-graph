// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use anyhow::Result;
use pretty_assertions::assert_eq;
use sux::bits::bit_vec::BitVec;

use swh_graph::graph::*;
use swh_graph::graph_builder::GraphBuilder;
use swh_graph::labels::Permission;
use swh_graph::swhid;

use swh_graph_provenance::frontier::*;

#[test]
fn test_dfs_with_path() -> Result<()> {
    let mut builder = GraphBuilder::default();
    let dir0 = builder
        .node(swhid!(swh:1:dir:0000000000000000000000000000000000000000))?
        .done();
    let dir1 = builder
        .node(swhid!(swh:1:dir:0000000000000000000000000000000000000001))?
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
    let cnt5 = builder
        .node(swhid!(swh:1:cnt:0000000000000000000000000000000000000005))?
        .done();
    let cnt6 = builder
        .node(swhid!(swh:1:cnt:0000000000000000000000000000000000000006))?
        .done();
    let cnt7 = builder
        .node(swhid!(swh:1:cnt:0000000000000000000000000000000000000007))?
        .done();
    let rev8 = builder
        .node(swhid!(swh:1:rev:0000000000000000000000000000000000000008))?
        .done();
    let rev9 = builder
        .node(swhid!(swh:1:rev:0000000000000000000000000000000000000009))?
        .done();
    /*
     * rev8                                    rev9
     *  |                                       |
     *  v                                       v
     * dir0 --(subdir1)--> dir1 --(subdir2)--> dir2 --(content4)--> cnt4
     *  \                   \                     \
     *   \                   \                     +--(content5)--> cnt5
     *    \                   \
     *     \                   +--(subdir3)--> dir3 --(content6)--> cnt6
     *      \
     *       +--(content7)--> cnt7
     */
    builder.l_arc(dir0, dir1, Permission::Directory, b"subdir1");
    builder.l_arc(dir1, dir2, Permission::Directory, b"subdir2");
    builder.l_arc(dir1, dir3, Permission::Directory, b"subdir3");
    builder.l_arc(dir2, cnt4, Permission::Directory, b"content4");
    builder.l_arc(dir2, cnt5, Permission::Directory, b"content5");
    builder.l_arc(dir3, cnt6, Permission::Directory, b"content6");
    builder.l_arc(dir0, cnt7, Permission::Directory, b"content7");
    builder.arc(rev8, dir0);
    builder.arc(rev9, dir2);

    let graph = builder.done()?;

    let mut dir_events = Vec::new();
    let on_directory = |dir, path_parts: PathParts| {
        dir_events.push(format!(
            "{} has path {}",
            graph.properties().swhid(dir),
            String::from_utf8(path_parts.build_path(&graph))?
        ));
        Ok(true)
    };

    let mut revrel_events = Vec::new();
    let on_revrel = |revrel, path_parts: PathParts| {
        revrel_events.push(format!(
            "{} has path {}",
            graph.properties().swhid(revrel),
            String::from_utf8(path_parts.build_path(&graph))?
        ));
        Ok(())
    };

    let mut reachable_nodes = BitVec::new(graph.num_nodes());
    reachable_nodes.fill(true);

    backward_dfs_with_path(&graph, &reachable_nodes, on_directory, on_revrel, cnt4)?;

    dir_events.sort();
    revrel_events.sort();
    assert_eq!(
        dir_events,
        vec![
            "swh:1:cnt:0000000000000000000000000000000000000004 has path ",
            "swh:1:dir:0000000000000000000000000000000000000000 has path subdir1/subdir2/content4",
            "swh:1:dir:0000000000000000000000000000000000000001 has path subdir2/content4",
            "swh:1:dir:0000000000000000000000000000000000000002 has path content4",
        ]
    );
    assert_eq!(
        revrel_events,
        vec![
            "swh:1:rev:0000000000000000000000000000000000000008 has path subdir1/subdir2/content4",
            "swh:1:rev:0000000000000000000000000000000000000009 has path content4",
        ]
    );

    Ok(())
}
