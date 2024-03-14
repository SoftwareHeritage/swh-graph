// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use anyhow::Result;
use pretty_assertions::assert_eq;

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
    /*
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

    let mut cnt_events = Vec::new();
    let on_content = |cnt, path_parts: PathParts| {
        cnt_events.push(format!(
            "{} has path {}",
            graph.properties().swhid(cnt),
            String::from_utf8(path_parts.build_path(&graph))?
        ));
        Ok(())
    };

    dfs_with_path(&graph, on_directory, on_content, dir0)?;

    dir_events.sort();
    cnt_events.sort();
    assert_eq!(
        dir_events,
        vec![
            "swh:1:dir:0000000000000000000000000000000000000000 has path ",
            "swh:1:dir:0000000000000000000000000000000000000001 has path subdir1/",
            "swh:1:dir:0000000000000000000000000000000000000002 has path subdir1/subdir2/",
            "swh:1:dir:0000000000000000000000000000000000000003 has path subdir1/subdir3/",
        ]
    );
    assert_eq!(
        cnt_events,
        vec![
            "swh:1:cnt:0000000000000000000000000000000000000004 has path subdir1/subdir2/content4",
            "swh:1:cnt:0000000000000000000000000000000000000005 has path subdir1/subdir2/content5",
            "swh:1:cnt:0000000000000000000000000000000000000006 has path subdir1/subdir3/content6",
            "swh:1:cnt:0000000000000000000000000000000000000007 has path content7",
        ]
    );

    Ok(())
}
