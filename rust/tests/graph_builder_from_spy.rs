// Copyright (C) 2026  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use anyhow::Result;

use swh_graph::graph::*;
use swh_graph::graph_builder::{build_graph_from_spy, GraphBuilder};
use swh_graph::labels::{DirEntry, Permission};
use swh_graph::swhid;
use swh_graph::views::GraphSpy;

#[test]
fn test_build_graph_from_spy() -> Result<()> {
    let mut builder = GraphBuilder::default();
    let a = builder
        .node(swhid!(swh:1:dir:0000000000000000000000000000000000000010))?
        .done();
    let b = builder
        .node(swhid!(swh:1:dir:0000000000000000000000000000000000000020))?
        .done();
    let c = builder
        .node(swhid!(swh:1:cnt:0000000000000000000000000000000000000030))?
        .content_length(42)
        .done();
    builder.dir_arc(a, b, Permission::Directory, b"tests");
    builder.dir_arc(a, c, Permission::ExecutableContent, b"run.sh");
    builder.dir_arc(b, c, Permission::Content, b"test.c");
    let graph = builder.done()?;

    let spy = GraphSpy::new(graph).with_maps();

    for _ in spy.labeled_successors(a) {}
    let _ = spy.has_node(b);

    let rebuilt = build_graph_from_spy(&spy)?;

    assert_eq!(
        0,
        rebuilt
            .properties()
            .node_id(swhid!(swh:1:dir:0000000000000000000000000000000000000010))
            .unwrap()
    );
    assert_eq!(
        1,
        rebuilt
            .properties()
            .node_id(swhid!(swh:1:dir:0000000000000000000000000000000000000020))
            .unwrap()
    );
    assert_eq!(
        2,
        rebuilt
            .properties()
            .node_id(swhid!(swh:1:cnt:0000000000000000000000000000000000000030))
            .unwrap()
    );

    // like the original
    assert_eq!(rebuilt.successors(0).collect::<Vec<_>>(), vec![1, 2]);
    // empty because not accessed
    assert_eq!(
        rebuilt.successors(1).collect::<Vec<_>>(),
        Vec::<NodeId>::new()
    );
    // empty like the original and also because not accessed
    assert_eq!(
        rebuilt.successors(2).collect::<Vec<_>>(),
        Vec::<NodeId>::new()
    );

    assert_eq!(
        rebuilt
            .labeled_successors(0)
            .map(|(succ, labels)| (succ, labels.into_iter().collect::<Vec<_>>()))
            .collect::<Vec<_>>(),
        vec![
            (
                1,
                vec![DirEntry::new(
                    Permission::Directory,
                    rebuilt
                        .properties()
                        .label_name_id(b"tests")
                        .expect("missing label name")
                )
                .unwrap()
                .into()]
            ),
            (
                2,
                vec![DirEntry::new(
                    Permission::ExecutableContent,
                    rebuilt
                        .properties()
                        .label_name_id(b"run.sh")
                        .expect("missing label name")
                )
                .unwrap()
                .into()]
            )
        ]
    );

    Ok(())
}

#[test]
fn test_build_graph_from_spy_properties() -> Result<()> {
    let mut builder = GraphBuilder::default();
    let a = builder
        .node(swhid!(swh:1:rev:0000000000000000000000000000000000000010))?
        .author_timestamp(1708950743, 60)
        .message(b"Initial commit".to_vec())
        .done();
    let b = builder
        .node(swhid!(swh:1:rev:0000000000000000000000000000000000000020))?
        .committer_timestamp(1708950821, 120)
        .done();
    builder.arc(a, b);
    let graph = builder.done()?;

    let spy = GraphSpy::new(graph);

    let _ = spy.has_node(a);
    let _ = spy.successors(a);

    let rebuilt = build_graph_from_spy(&spy)?;

    assert_eq!(
        0,
        rebuilt
            .properties()
            .node_id(swhid!(swh:1:rev:0000000000000000000000000000000000000010))
            .unwrap()
    );
    assert_eq!(
        1,
        rebuilt
            .properties()
            .node_id(swhid!(swh:1:rev:0000000000000000000000000000000000000020))
            .unwrap()
    );

    assert_eq!(rebuilt.properties().author_timestamp(0), Some(1708950743));
    assert_eq!(rebuilt.properties().author_timestamp_offset(0), Some(60));

    assert_eq!(
        rebuilt.properties().message(0),
        Some(b"Initial commit".to_vec())
    );

    let successors: Vec<_> = rebuilt.successors(0).collect();
    assert_eq!(successors, vec![1]);

    Ok(())
}

#[test]
fn test_build_graph_from_spy_omit_unused_nodes() -> Result<()> {
    let mut builder = GraphBuilder::default();
    let a = builder
        .node(swhid!(swh:1:dir:0000000000000000000000000000000000000010))?
        .done();
    let b = builder
        .node(swhid!(swh:1:dir:0000000000000000000000000000000000000020))?
        .done();
    let c = builder
        .node(swhid!(swh:1:cnt:0000000000000000000000000000000000000030))?
        .content_length(42)
        .done();
    builder.dir_arc(a, b, Permission::Directory, b"tests");
    builder.dir_arc(a, c, Permission::ExecutableContent, b"run.sh");
    builder.dir_arc(b, c, Permission::Content, b"test.c");
    let graph = builder.done()?;

    let spy = GraphSpy::new(graph).with_maps();

    // access b (and indirectly c), but not a
    for _ in spy.labeled_successors(b) {}

    let rebuilt = build_graph_from_spy(&spy)?;

    assert_eq!(
        0,
        rebuilt
            .properties()
            .node_id(swhid!(swh:1:dir:0000000000000000000000000000000000000020))
            .unwrap()
    );
    assert_eq!(
        1,
        rebuilt
            .properties()
            .node_id(swhid!(swh:1:cnt:0000000000000000000000000000000000000030))
            .unwrap()
    );
    assert_eq!(rebuilt.num_nodes(), 2);

    // like the original
    assert_eq!(rebuilt.successors(0).collect::<Vec<_>>(), vec![1]);
    // like the original and also because not accessed
    assert_eq!(
        rebuilt.successors(1).collect::<Vec<_>>(),
        Vec::<NodeId>::new()
    );

    assert_eq!(
        rebuilt
            .labeled_successors(0)
            .map(|(succ, labels)| (succ, labels.into_iter().collect::<Vec<_>>()))
            .collect::<Vec<_>>(),
        vec![(
            1,
            vec![DirEntry::new(
                Permission::Content,
                rebuilt
                    .properties()
                    .label_name_id(b"test.c")
                    .expect("missing label name")
            )
            .unwrap()
            .into()]
        ),]
    );

    Ok(())
}
