/*
 * Copyright (C) 2024  The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

#![deny(elided_lifetimes_in_paths)]

use anyhow::{Context, Result};

use swh_graph::graph::*;
use swh_graph::graph_builder::GraphBuilder;
use swh_graph::labels::Permission;
use swh_graph::swhid;

#[test]
fn test_minimal() -> Result<()> {
    let mut builder = GraphBuilder::default();
    let a = builder
        .node(swhid!(swh:1:rev:0000000000000000000000000000000000000010))?
        .done();
    let b = builder
        .node(swhid!(swh:1:rev:0000000000000000000000000000000000000020))?
        .done();
    let c = builder
        .node(swhid!(swh:1:rel:0000000000000000000000000000000000000030))?
        .done();
    builder.arc(a, b, None);
    builder.arc(a, c, None);
    builder.arc(b, c, None);
    let graph = builder.done().context("Could not make graph")?;

    assert_eq!(a, 0);
    assert_eq!(b, 1);
    assert_eq!(c, 2);

    assert_eq!(graph.num_nodes(), 3);
    assert_eq!(
        graph.properties().swhid(a),
        Some(swhid!(swh:1:rev:0000000000000000000000000000000000000010))
    );
    assert_eq!(
        graph.properties().swhid(b),
        Some(swhid!(swh:1:rev:0000000000000000000000000000000000000020))
    );
    assert_eq!(
        graph.properties().swhid(c),
        Some(swhid!(swh:1:rel:0000000000000000000000000000000000000030))
    );
    assert_eq!(graph.successors(a).collect::<Vec<_>>(), vec![b, c]);
    assert_eq!(graph.successors(b).collect::<Vec<_>>(), vec![c]);
    assert_eq!(
        graph.successors(c).collect::<Vec<_>>(),
        Vec::<NodeId>::new()
    );
    assert_eq!(
        graph.predecessors(a).collect::<Vec<_>>(),
        Vec::<NodeId>::new()
    );
    assert_eq!(graph.predecessors(b).collect::<Vec<_>>(), vec![a]);
    assert_eq!(graph.predecessors(c).collect::<Vec<_>>(), vec![a, b]);

    Ok(())
}

#[test]
fn test_duplicate_swhid() -> Result<()> {
    let mut builder = GraphBuilder::default();
    builder
        .node(swhid!(swh:1:rev:0000000000000000000000000000000000000010))?
        .done();
    assert!(builder
        .node(swhid!(swh:1:rev:0000000000000000000000000000000000000010))
        .is_err());

    Ok(())
}

#[test]
fn test_labels() -> Result<()> {
    let mut builder = GraphBuilder::default();
    let a = builder
        .node(swhid!(swh:1:dir:0000000000000000000000000000000000000010))?
        .done();
    let b = builder
        .node(swhid!(swh:1:dir:0000000000000000000000000000000000000020))?
        .done();
    let c = builder
        .node(swhid!(swh:1:cnt:0000000000000000000000000000000000000030))?
        .done();
    builder.arc(a, b, Some((Permission::Directory, b"tests".into())));
    builder.arc(
        a,
        c,
        Some((Permission::ExecutableContent, b"run.sh".into())),
    );
    builder.arc(b, c, Some((Permission::Content, b"test.c".into())));
    let graph = builder.done().context("Could not make graph")?;

    assert_eq!(a, 0);
    assert_eq!(b, 1);
    assert_eq!(c, 2);

    assert_eq!(graph.num_nodes(), 3);
    assert_eq!(
        graph.properties().swhid(a),
        Some(swhid!(swh:1:dir:0000000000000000000000000000000000000010))
    );
    assert_eq!(
        graph.properties().swhid(b),
        Some(swhid!(swh:1:dir:0000000000000000000000000000000000000020))
    );
    assert_eq!(
        graph.properties().swhid(c),
        Some(swhid!(swh:1:cnt:0000000000000000000000000000000000000030))
    );

    let collect_labels = |(succ, labels): (_, LabelledArcIterator<_>)| {
        (
            succ,
            labels
                .map(|label| {
                    (
                        label.permission(),
                        graph.properties().label_name(label.filename_id()),
                    )
                })
                .collect::<Vec<_>>(),
        )
    };

    assert_eq!(graph.successors(a).collect::<Vec<_>>(), vec![b, c]);
    assert_eq!(graph.successors(b).collect::<Vec<_>>(), vec![c]);
    assert_eq!(
        graph.successors(c).collect::<Vec<_>>(),
        Vec::<NodeId>::new()
    );

    assert_eq!(
        graph
            .labelled_successors(a)
            .map(collect_labels)
            .collect::<Vec<_>>(),
        vec![
            (
                b,
                vec![(Some(Permission::Directory), Some(b"tests".into()))]
            ),
            (
                c,
                vec![(Some(Permission::ExecutableContent), Some(b"run.sh".into()))]
            ),
        ]
    );
    assert_eq!(
        graph
            .labelled_successors(b)
            .map(collect_labels)
            .collect::<Vec<_>>(),
        vec![(c, vec![(Some(Permission::Content), Some(b"test.c".into()))]),]
    );

    assert_eq!(
        graph.predecessors(a).collect::<Vec<_>>(),
        Vec::<NodeId>::new()
    );
    assert_eq!(graph.predecessors(b).collect::<Vec<_>>(), vec![a]);
    assert_eq!(graph.predecessors(c).collect::<Vec<_>>(), vec![a, b]);

    assert_eq!(
        graph
            .labelled_predecessors(a)
            .map(collect_labels)
            .collect::<Vec<_>>(),
        vec![]
    );
    assert_eq!(
        graph
            .labelled_predecessors(b)
            .map(collect_labels)
            .collect::<Vec<_>>(),
        vec![(
            a,
            vec![(Some(Permission::Directory), Some(b"tests".into()))]
        ),]
    );
    assert_eq!(
        graph
            .labelled_predecessors(c)
            .map(collect_labels)
            .collect::<Vec<_>>(),
        vec![
            (
                a,
                vec![(Some(Permission::ExecutableContent), Some(b"run.sh".into()))]
            ),
            (b, vec![(Some(Permission::Content), Some(b"test.c".into()))]),
        ]
    );

    Ok(())
}

/// Tests two dir->cnt entries with the same label; as this triggers different
/// logic in label_name building
#[test]
fn test_duplicate_labels() -> Result<()> {
    let mut builder = GraphBuilder::default();
    let a = builder
        .node(swhid!(swh:1:dir:0000000000000000000000000000000000000010))?
        .done();
    let b = builder
        .node(swhid!(swh:1:dir:0000000000000000000000000000000000000020))?
        .done();
    let c = builder
        .node(swhid!(swh:1:cnt:0000000000000000000000000000000000000030))?
        .done();
    builder.arc(a, b, Some((Permission::Directory, b"tests".into())));
    builder.arc(
        a,
        c,
        Some((Permission::ExecutableContent, b"run.sh".into())),
    );
    builder.arc(
        b,
        c,
        Some((Permission::ExecutableContent, b"run.sh".into())),
    );
    let graph = builder.done().context("Could not make graph")?;

    let collect_labels = |(succ, labels): (_, LabelledArcIterator<_>)| {
        (
            succ,
            labels
                .map(|label| {
                    (
                        label.permission(),
                        graph.properties().label_name(label.filename_id()),
                    )
                })
                .collect::<Vec<_>>(),
        )
    };

    assert_eq!(
        graph
            .labelled_successors(a)
            .map(collect_labels)
            .collect::<Vec<_>>(),
        vec![
            (
                b,
                vec![(Some(Permission::Directory), Some(b"tests".into()))]
            ),
            (
                c,
                vec![(Some(Permission::ExecutableContent), Some(b"run.sh".into()))]
            ),
        ]
    );
    assert_eq!(
        graph
            .labelled_successors(b)
            .map(collect_labels)
            .collect::<Vec<_>>(),
        vec![(
            c,
            vec![(Some(Permission::ExecutableContent), Some(b"run.sh".into()))]
        ),]
    );

    Ok(())
}

#[test]
fn test_contents() -> Result<()> {
    let mut builder = GraphBuilder::default();
    let a = builder
        .node(swhid!(swh:1:cnt:0000000000000000000000000000000000000010))?
        .content_length(42)
        .done();
    let b = builder
        .node(swhid!(swh:1:cnt:0000000000000000000000000000000000000020))?
        .is_skipped_content(true)
        .done();
    builder.arc(a, b, None);
    let graph = builder.done().context("Could not make graph")?;

    assert_eq!(graph.properties().content_length(a), Some(42));
    assert_eq!(graph.properties().is_skipped_content(a), Some(false));

    assert_eq!(graph.properties().content_length(b), None);
    assert_eq!(graph.properties().is_skipped_content(b), Some(true));

    Ok(())
}

#[test]
fn test_persons() -> Result<()> {
    let mut builder = GraphBuilder::default();
    let a = builder
        .node(swhid!(swh:1:rev:0000000000000000000000000000000000000010))?
        .author(b"Jane Doe <jdoe@example.org>".into())
        .done();
    let b = builder
        .node(swhid!(swh:1:rev:0000000000000000000000000000000000000020))?
        .author(b"John Doe <jdoe@example.org>".into())
        .committer(b"Jane Doe <jdoe@example.org>".into())
        .done();
    builder.arc(a, b, None);
    let graph = builder.done().context("Could not make graph")?;

    assert_eq!(graph.properties().author_id(a), Some(0));
    assert_eq!(graph.properties().committer_id(a), None);

    assert_eq!(graph.properties().author_id(b), Some(1));
    assert_eq!(graph.properties().committer_id(b), Some(0)); // Same as revision a's author

    Ok(())
}

#[test]
fn test_strings() -> Result<()> {
    let mut builder = GraphBuilder::default();
    let a = builder
        .node(swhid!(swh:1:rev:0000000000000000000000000000000000000010))?
        .message(b"test revision".into())
        .done();
    let b = builder
        .node(swhid!(swh:1:rev:0000000000000000000000000000000000000020))?
        .message(b"test release".into())
        .tag_name(b"v0.1.0".into())
        .done();
    builder.arc(a, b, None);
    let graph = builder.done().context("Could not make graph")?;

    assert_eq!(graph.properties().message(a), Some(b"test revision".into()));
    assert_eq!(graph.properties().tag_name(a), None);

    assert_eq!(graph.properties().message(b), Some(b"test release".into()));
    assert_eq!(graph.properties().tag_name(b), Some(b"v0.1.0".into()));

    Ok(())
}

#[test]
fn test_timestamps() -> Result<()> {
    let mut builder = GraphBuilder::default();
    let a = builder
        .node(swhid!(swh:1:rev:0000000000000000000000000000000000000010))?
        .author_timestamp(1708950743, 60)
        .done();
    let b = builder
        .node(swhid!(swh:1:rev:0000000000000000000000000000000000000020))?
        .committer_timestamp(1708950821, 120)
        .done();
    builder.arc(a, b, None);
    let graph = builder.done().context("Could not make graph")?;

    assert_eq!(graph.properties().author_timestamp(a), Some(1708950743));
    assert_eq!(graph.properties().author_timestamp_offset(a), Some(60));
    assert_eq!(graph.properties().committer_timestamp(a), None);
    assert_eq!(graph.properties().committer_timestamp_offset(a), None);

    assert_eq!(graph.properties().author_timestamp(b), None);
    assert_eq!(graph.properties().author_timestamp_offset(b), None);
    assert_eq!(graph.properties().committer_timestamp(b), Some(1708950821));
    assert_eq!(graph.properties().committer_timestamp_offset(b), Some(120));

    Ok(())
}
