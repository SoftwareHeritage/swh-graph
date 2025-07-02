/*
 * Copyright (C) 2024  The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

#![deny(elided_lifetimes_in_paths)]

use anyhow::{Context, Result};

use swh_graph::arc_iterators::LabeledArcIterator;
use swh_graph::graph::*;
use swh_graph::graph_builder::GraphBuilder;
use swh_graph::labels::{LabelNameId, Permission, Visit, VisitStatus};
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
    builder.arc(a, b);
    builder.arc(a, c);
    builder.arc(b, c);
    let graph = builder.done().context("Could not make graph")?;

    assert_eq!(a, 0);
    assert_eq!(b, 1);
    assert_eq!(c, 2);

    assert_eq!(graph.num_nodes(), 3);
    assert_eq!(
        graph.properties().swhid(a),
        swhid!(swh:1:rev:0000000000000000000000000000000000000010)
    );
    assert_eq!(
        graph.properties().swhid(b),
        swhid!(swh:1:rev:0000000000000000000000000000000000000020)
    );
    assert_eq!(
        graph.properties().swhid(c),
        swhid!(swh:1:rel:0000000000000000000000000000000000000030)
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
fn test_dir_labels() -> Result<()> {
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
    builder.dir_arc(a, b, Permission::Directory, b"tests");
    builder.dir_arc(a, c, Permission::ExecutableContent, b"run.sh");
    builder.dir_arc(b, c, Permission::Content, b"test.c");
    let graph = builder.done().context("Could not make graph")?;

    assert_eq!(a, 0);
    assert_eq!(b, 1);
    assert_eq!(c, 2);

    assert_eq!(graph.num_nodes(), 3);
    assert_eq!(
        graph.properties().swhid(a),
        swhid!(swh:1:dir:0000000000000000000000000000000000000010)
    );
    assert_eq!(
        graph.properties().swhid(b),
        swhid!(swh:1:dir:0000000000000000000000000000000000000020)
    );
    assert_eq!(
        graph.properties().swhid(c),
        swhid!(swh:1:cnt:0000000000000000000000000000000000000030)
    );

    let collect_labels = |(succ, labels): (_, LabeledArcIterator<_>)| {
        (
            succ,
            labels
                .map(|label| {
                    let label: swh_graph::labels::DirEntry = label.into();
                    (
                        label.permission(),
                        graph.properties().label_name(label.label_name_id()),
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
            .untyped_labeled_successors(a)
            .map(collect_labels)
            .collect::<Vec<_>>(),
        vec![
            (b, vec![(Some(Permission::Directory), b"tests".into())]),
            (
                c,
                vec![(Some(Permission::ExecutableContent), b"run.sh".into())]
            ),
        ]
    );
    assert_eq!(
        graph
            .untyped_labeled_successors(b)
            .map(collect_labels)
            .collect::<Vec<_>>(),
        vec![(c, vec![(Some(Permission::Content), b"test.c".into())]),]
    );

    assert_eq!(
        graph.predecessors(a).collect::<Vec<_>>(),
        Vec::<NodeId>::new()
    );
    assert_eq!(graph.predecessors(b).collect::<Vec<_>>(), vec![a]);
    assert_eq!(graph.predecessors(c).collect::<Vec<_>>(), vec![a, b]);

    assert_eq!(
        graph
            .untyped_labeled_predecessors(a)
            .map(collect_labels)
            .collect::<Vec<_>>(),
        vec![]
    );
    assert_eq!(
        graph
            .untyped_labeled_predecessors(b)
            .map(collect_labels)
            .collect::<Vec<_>>(),
        vec![(a, vec![(Some(Permission::Directory), b"tests".into())]),]
    );
    assert_eq!(
        graph
            .untyped_labeled_predecessors(c)
            .map(collect_labels)
            .collect::<Vec<_>>(),
        vec![
            (
                a,
                vec![(Some(Permission::ExecutableContent), b"run.sh".into())]
            ),
            (b, vec![(Some(Permission::Content), b"test.c".into())]),
        ]
    );

    assert_eq!(
        graph.properties().label_name_id(b"run.sh"),
        Ok(LabelNameId(0))
    );
    assert_eq!(
        graph.properties().label_name_id(b"test.c"),
        Ok(LabelNameId(1))
    );
    assert_eq!(
        graph.properties().label_name_id(b"tests"),
        Ok(LabelNameId(2))
    );
    assert!(graph.properties().label_name_id(b"non-existent").is_err());

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
    builder.dir_arc(a, b, Permission::Directory, b"tests");
    builder.dir_arc(a, c, Permission::ExecutableContent, b"run.sh");
    builder.dir_arc(b, c, Permission::ExecutableContent, b"run.sh");
    let graph = builder.done().context("Could not make graph")?;

    let collect_labels = |(succ, labels): (_, LabeledArcIterator<_>)| {
        (
            succ,
            labels
                .map(|label| {
                    let label: swh_graph::labels::DirEntry = label.into();
                    (
                        label.permission(),
                        graph.properties().label_name(label.label_name_id()),
                    )
                })
                .collect::<Vec<_>>(),
        )
    };

    assert_eq!(
        graph
            .untyped_labeled_successors(a)
            .map(collect_labels)
            .collect::<Vec<_>>(),
        vec![
            (b, vec![(Some(Permission::Directory), b"tests".into())]),
            (
                c,
                vec![(Some(Permission::ExecutableContent), b"run.sh".into())]
            ),
        ]
    );
    assert_eq!(
        graph
            .untyped_labeled_successors(b)
            .map(collect_labels)
            .collect::<Vec<_>>(),
        vec![(
            c,
            vec![(Some(Permission::ExecutableContent), b"run.sh".into())]
        ),]
    );

    assert_eq!(
        graph.properties().label_name_id(b"run.sh"),
        Ok(LabelNameId(0))
    );
    assert_eq!(
        graph.properties().label_name_id(b"tests"),
        Ok(LabelNameId(1))
    );
    assert!(graph.properties().label_name_id(b"non-existent").is_err());

    Ok(())
}

#[test]
fn test_snp_labels() -> Result<()> {
    let mut builder = GraphBuilder::default();
    let a = builder
        .node(swhid!(swh:1:snp:0000000000000000000000000000000000000010))?
        .done();
    let b = builder
        .node(swhid!(swh:1:snp:0000000000000000000000000000000000000020))?
        .done();
    let c = builder
        .node(swhid!(swh:1:rev:0000000000000000000000000000000000000030))?
        .done();
    let d = builder
        .node(swhid!(swh:1:rev:0000000000000000000000000000000000000040))?
        .done();
    builder.snp_arc(a, c, b"refs/heads/main");
    builder.snp_arc(a, d, b"refs/heads/feature/foo");
    builder.snp_arc(b, c, b"refs/heads/main");
    let graph = builder.done().context("Could not make graph")?;

    assert_eq!(a, 0);
    assert_eq!(b, 1);
    assert_eq!(c, 2);
    assert_eq!(d, 3);

    assert_eq!(graph.num_nodes(), 4);
    assert_eq!(
        graph.properties().swhid(a),
        swhid!(swh:1:snp:0000000000000000000000000000000000000010)
    );
    assert_eq!(
        graph.properties().swhid(b),
        swhid!(swh:1:snp:0000000000000000000000000000000000000020)
    );
    assert_eq!(
        graph.properties().swhid(c),
        swhid!(swh:1:rev:0000000000000000000000000000000000000030)
    );
    assert_eq!(
        graph.properties().swhid(d),
        swhid!(swh:1:rev:0000000000000000000000000000000000000040)
    );

    let collect_labels = |(succ, labels): (_, LabeledArcIterator<_>)| {
        (
            succ,
            labels
                .map(|label| {
                    let label: swh_graph::labels::Branch = label.into();
                    graph.properties().label_name(label.label_name_id())
                })
                .collect::<Vec<_>>(),
        )
    };

    assert_eq!(graph.successors(a).collect::<Vec<_>>(), vec![c, d]);
    assert_eq!(graph.successors(b).collect::<Vec<_>>(), vec![c]);
    assert_eq!(
        graph.successors(c).collect::<Vec<_>>(),
        Vec::<NodeId>::new()
    );
    assert_eq!(
        graph.successors(d).collect::<Vec<_>>(),
        Vec::<NodeId>::new()
    );

    assert_eq!(
        graph
            .untyped_labeled_successors(a)
            .map(collect_labels)
            .collect::<Vec<_>>(),
        vec![
            (c, vec![b"refs/heads/main".into()]),
            (d, vec![b"refs/heads/feature/foo".into()]),
        ]
    );
    assert_eq!(
        graph
            .untyped_labeled_successors(b)
            .map(collect_labels)
            .collect::<Vec<_>>(),
        vec![(c, vec![b"refs/heads/main".into()]),]
    );

    assert_eq!(
        graph.predecessors(a).collect::<Vec<_>>(),
        Vec::<NodeId>::new()
    );
    assert_eq!(
        graph.predecessors(b).collect::<Vec<_>>(),
        Vec::<NodeId>::new()
    );
    assert_eq!(graph.predecessors(c).collect::<Vec<_>>(), vec![a, b]);
    assert_eq!(graph.predecessors(d).collect::<Vec<_>>(), vec![a]);

    assert_eq!(
        graph
            .untyped_labeled_predecessors(a)
            .map(collect_labels)
            .collect::<Vec<_>>(),
        vec![]
    );
    assert_eq!(
        graph
            .untyped_labeled_predecessors(c)
            .map(collect_labels)
            .collect::<Vec<_>>(),
        vec![
            (a, vec![b"refs/heads/main".into()]),
            (b, vec![b"refs/heads/main".into()]),
        ]
    );
    assert_eq!(
        graph
            .untyped_labeled_predecessors(d)
            .map(collect_labels)
            .collect::<Vec<_>>(),
        vec![(a, vec![b"refs/heads/feature/foo".into()]),]
    );

    assert_eq!(
        graph.properties().label_name_id(b"refs/heads/feature/foo"),
        Ok(LabelNameId(0))
    );
    assert_eq!(
        graph.properties().label_name_id(b"refs/heads/main"),
        Ok(LabelNameId(1))
    );
    assert!(graph.properties().label_name_id(b"non-existent").is_err());

    Ok(())
}

#[test]
fn test_ori_labels() -> Result<()> {
    let mut builder = GraphBuilder::default();
    let a = builder
        .node(swhid!(swh:1:ori:0000000000000000000000000000000000000010))?
        .done();
    let b = builder
        .node(swhid!(swh:1:ori:0000000000000000000000000000000000000020))?
        .done();
    let c = builder
        .node(swhid!(swh:1:snp:0000000000000000000000000000000000000030))?
        .done();
    let d = builder
        .node(swhid!(swh:1:snp:0000000000000000000000000000000000000040))?
        .done();
    let visit_a_c = Visit::new(VisitStatus::Full, 1719581545).unwrap();
    let visit_a_d = Visit::new(VisitStatus::Partial, 1719500000).unwrap();
    let visit_b_c = Visit::new(VisitStatus::Full, 1719581578).unwrap();
    builder.ori_arc(a, c, visit_a_c.status(), visit_a_c.timestamp());
    builder.ori_arc(a, d, visit_a_d.status(), visit_a_d.timestamp());
    builder.ori_arc(b, c, visit_b_c.status(), visit_b_c.timestamp());
    let graph = builder.done().context("Could not make graph")?;

    assert_eq!(a, 0);
    assert_eq!(b, 1);
    assert_eq!(c, 2);
    assert_eq!(d, 3);

    assert_eq!(graph.num_nodes(), 4);
    assert_eq!(
        graph.properties().swhid(a),
        swhid!(swh:1:ori:0000000000000000000000000000000000000010)
    );
    assert_eq!(
        graph.properties().swhid(b),
        swhid!(swh:1:ori:0000000000000000000000000000000000000020)
    );
    assert_eq!(
        graph.properties().swhid(c),
        swhid!(swh:1:snp:0000000000000000000000000000000000000030)
    );
    assert_eq!(
        graph.properties().swhid(d),
        swhid!(swh:1:snp:0000000000000000000000000000000000000040)
    );

    let collect_labels = |(succ, labels): (_, LabeledArcIterator<_>)| {
        (
            succ,
            labels
                .map(swh_graph::labels::Visit::from)
                .collect::<Vec<_>>(),
        )
    };

    assert_eq!(graph.successors(a).collect::<Vec<_>>(), vec![c, d]);
    assert_eq!(graph.successors(b).collect::<Vec<_>>(), vec![c]);
    assert_eq!(
        graph.successors(c).collect::<Vec<_>>(),
        Vec::<NodeId>::new()
    );
    assert_eq!(
        graph.successors(d).collect::<Vec<_>>(),
        Vec::<NodeId>::new()
    );

    assert_eq!(
        graph
            .untyped_labeled_successors(a)
            .map(collect_labels)
            .collect::<Vec<_>>(),
        vec![(c, vec![visit_a_c]), (d, vec![visit_a_d]),]
    );
    assert_eq!(
        graph
            .untyped_labeled_successors(b)
            .map(collect_labels)
            .collect::<Vec<_>>(),
        vec![(c, vec![visit_b_c]),]
    );

    assert_eq!(
        graph.predecessors(a).collect::<Vec<_>>(),
        Vec::<NodeId>::new()
    );
    assert_eq!(
        graph.predecessors(b).collect::<Vec<_>>(),
        Vec::<NodeId>::new()
    );
    assert_eq!(graph.predecessors(c).collect::<Vec<_>>(), vec![a, b]);
    assert_eq!(graph.predecessors(d).collect::<Vec<_>>(), vec![a]);

    assert_eq!(
        graph
            .untyped_labeled_predecessors(a)
            .map(collect_labels)
            .collect::<Vec<_>>(),
        vec![]
    );
    assert_eq!(
        graph
            .untyped_labeled_predecessors(c)
            .map(collect_labels)
            .collect::<Vec<_>>(),
        vec![(a, vec![visit_a_c]), (b, vec![visit_b_c]),]
    );
    assert_eq!(
        graph
            .untyped_labeled_predecessors(d)
            .map(collect_labels)
            .collect::<Vec<_>>(),
        vec![(a, vec![visit_a_d]),]
    );

    Ok(())
}

#[test]
fn test_multiarc() -> Result<()> {
    let mut builder = GraphBuilder::default();
    let a = builder
        .node(swhid!(swh:1:dir:0000000000000000000000000000000000000010))?
        .done();
    let b = builder
        .node(swhid!(swh:1:dir:0000000000000000000000000000000000000020))?
        .done();
    builder.dir_arc(a, b, Permission::Content, b"README.txt");
    builder.dir_arc(a, b, Permission::ExecutableContent, b"run.sh");
    let graph = builder.done().context("Could not make graph")?;

    assert_eq!(a, 0);
    assert_eq!(b, 1);

    let collect_labels = |(succ, labels): (_, LabeledArcIterator<_>)| {
        (
            succ,
            labels
                .map(|label| {
                    let label: swh_graph::labels::DirEntry = label.into();
                    (
                        label.permission(),
                        graph.properties().label_name(label.label_name_id()),
                    )
                })
                .collect::<Vec<_>>(),
        )
    };

    assert_eq!(graph.successors(a).collect::<Vec<_>>(), vec![b]);

    assert_eq!(
        graph
            .untyped_labeled_successors(a)
            .map(collect_labels)
            .collect::<Vec<_>>(),
        vec![(
            b,
            vec![
                (Some(Permission::Content), b"README.txt".into()),
                (Some(Permission::ExecutableContent), b"run.sh".into())
            ]
        ),]
    );

    assert_eq!(graph.predecessors(b).collect::<Vec<_>>(), vec![a]);

    assert_eq!(
        graph
            .untyped_labeled_predecessors(b)
            .map(collect_labels)
            .collect::<Vec<_>>(),
        vec![(
            a,
            vec![
                (Some(Permission::Content), b"README.txt".into()),
                (Some(Permission::ExecutableContent), b"run.sh".into())
            ]
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
    builder.arc(a, b);
    let graph = builder.done().context("Could not make graph")?;

    assert_eq!(graph.properties().content_length(a), Some(42));
    assert!(!graph.properties().is_skipped_content(a));

    assert_eq!(graph.properties().content_length(b), None);
    assert!(graph.properties().is_skipped_content(b));

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
    builder.arc(a, b);
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
    builder.arc(a, b);
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
    builder.arc(a, b);
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
