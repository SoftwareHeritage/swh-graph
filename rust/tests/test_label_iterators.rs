/*
 * Copyright (C) 2024-2025  The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

use anyhow::Result;
use rayon::prelude::*;

use swh_graph::arc_iterators::LabeledArcIterator;
use swh_graph::graph::*;
use swh_graph::graph_builder::{BuiltGraph, GraphBuilder};
use swh_graph::labels::{Branch, LabelNameId, Visit, VisitStatus};
use swh_graph::swhid;

/// ```
/// ori0 -->  snp2 -->  rev4
///          ^    \
///         /      \
///        /        \
/// ori1 -+          -> rev5
///     \           /
///      \         /
///       \       /
///        -> snp3
/// ```
fn build_graph() -> Result<BuiltGraph> {
    let mut builder = GraphBuilder::default();
    builder
        .node(swhid!(swh:1:ori:0000000000000000000000000000000000000000))?
        .done();
    builder
        .node(swhid!(swh:1:ori:0000000000000000000000000000000000000001))?
        .done();
    builder
        .node(swhid!(swh:1:snp:0000000000000000000000000000000000000002))?
        .done();
    builder
        .node(swhid!(swh:1:snp:0000000000000000000000000000000000000003))?
        .done();
    builder
        .node(swhid!(swh:1:rev:0000000000000000000000000000000000000004))?
        .done();
    builder
        .node(swhid!(swh:1:rev:0000000000000000000000000000000000000005))?
        .done();
    builder.ori_arc(0, 2, VisitStatus::Full, 1000002000);
    builder.ori_arc(0, 2, VisitStatus::Full, 1000002001);
    builder.ori_arc(0, 3, VisitStatus::Full, 1000003000);
    builder.ori_arc(1, 2, VisitStatus::Full, 1001002000);
    builder.snp_arc(2, 4, b"refs/heads/snp2-to-rev4");
    builder.snp_arc(2, 5, b"refs/heads/snp2-to-rev5");
    builder.snp_arc(3, 5, b"refs/heads/snp3-to-rev5");
    builder.snp_arc(3, 5, b"refs/heads/snp3-to-rev5-dupe");

    builder.done()
}

#[test]
fn test_num_label_names() -> Result<()> {
    let graph = build_graph()?;

    assert_eq!(graph.properties().num_label_names(), 4);
    assert_eq!(
        graph.properties().iter_label_name_ids().collect::<Vec<_>>(),
        (0..4).map(LabelNameId).collect::<Vec<_>>()
    );
    assert_eq!(
        graph
            .properties()
            .par_iter_label_name_ids()
            .collect::<Vec<_>>(),
        (0..4).map(LabelNameId).collect::<Vec<_>>()
    );

    Ok(())
}

#[test]
fn test_untyped() -> Result<()> {
    let graph = build_graph()?;

    let collect_ori_labels = |(succ, labels): (_, LabeledArcIterator<_>)| {
        (succ, labels.map(Visit::from).collect::<Vec<_>>())
    };
    let collect_snp_labels = |(succ, labels): (_, LabeledArcIterator<_>)| {
        (
            succ,
            labels
                .map(|label| {
                    let label: Branch = label.into();
                    graph.properties().label_name(label.label_name_id())
                })
                .collect::<Vec<_>>(),
        )
    };

    assert_eq!(
        graph
            .untyped_labeled_successors(0)
            .map(collect_ori_labels)
            .collect::<Vec<_>>(),
        vec![
            (
                2,
                vec![
                    Visit::new(VisitStatus::Full, 1000002000).unwrap(),
                    Visit::new(VisitStatus::Full, 1000002001).unwrap()
                ]
            ),
            (3, vec![Visit::new(VisitStatus::Full, 1000003000).unwrap()])
        ]
    );

    assert_eq!(
        graph
            .untyped_labeled_successors(2)
            .map(collect_snp_labels)
            .collect::<Vec<_>>(),
        vec![
            (4, vec![b"refs/heads/snp2-to-rev4".into()]),
            (5, vec![b"refs/heads/snp2-to-rev5".into()])
        ]
    );

    Ok(())
}

#[test]
fn test_flattened_untyped() -> Result<()> {
    let graph = build_graph()?;

    assert_eq!(
        graph
            .untyped_labeled_successors(0)
            .flatten_labels()
            .map(|(succ, label)| (succ, Visit::from(label)))
            .collect::<Vec<_>>(),
        vec![
            (2, Visit::new(VisitStatus::Full, 1000002000).unwrap()),
            (2, Visit::new(VisitStatus::Full, 1000002001).unwrap()),
            (3, Visit::new(VisitStatus::Full, 1000003000).unwrap())
        ]
    );

    assert_eq!(
        graph
            .untyped_labeled_successors(2)
            .flatten_labels()
            .map(|(succ, label)| (
                succ,
                graph
                    .properties()
                    .label_name(Branch::from(label).label_name_id())
            ))
            .collect::<Vec<_>>(),
        vec![
            (4, b"refs/heads/snp2-to-rev4".into()),
            (5, b"refs/heads/snp2-to-rev5".into())
        ]
    );

    Ok(())
}

#[test]
fn test_typed() -> Result<()> {
    let graph = build_graph()?;

    assert_eq!(
        graph
            .labeled_successors(0)
            .map(|(succ, labels)| (succ, labels.collect()))
            .collect::<Vec<_>>(),
        vec![
            (
                2,
                vec![
                    Visit::new(VisitStatus::Full, 1000002000).unwrap().into(),
                    Visit::new(VisitStatus::Full, 1000002001).unwrap().into()
                ]
            ),
            (
                3,
                vec![Visit::new(VisitStatus::Full, 1000003000).unwrap().into()]
            )
        ]
    );

    let arc_2_to_4_label = graph
        .properties()
        .label_name_id(b"refs/heads/snp2-to-rev4")
        .unwrap();
    let arc_2_to_5_label = graph
        .properties()
        .label_name_id(b"refs/heads/snp2-to-rev5")
        .unwrap();
    assert_eq!(
        graph
            .labeled_successors(2)
            .map(|(succ, labels)| (succ, labels.collect()))
            .collect::<Vec<_>>(),
        vec![
            (4, vec![Branch::new(arc_2_to_4_label).unwrap().into()]),
            (5, vec![Branch::new(arc_2_to_5_label).unwrap().into()])
        ]
    );

    Ok(())
}

#[test]
fn test_typed_flattened() -> Result<()> {
    let graph = build_graph()?;

    assert_eq!(
        graph
            .labeled_successors(0)
            .flatten_labels()
            .into_iter()
            .collect::<Vec<_>>(),
        vec![
            (2, Visit::new(VisitStatus::Full, 1000002000).unwrap().into()),
            (2, Visit::new(VisitStatus::Full, 1000002001).unwrap().into()),
            (3, Visit::new(VisitStatus::Full, 1000003000).unwrap().into()),
        ]
    );

    let arc_2_to_4_label = graph
        .properties()
        .label_name_id(b"refs/heads/snp2-to-rev4")
        .unwrap();
    let arc_2_to_5_label = graph
        .properties()
        .label_name_id(b"refs/heads/snp2-to-rev5")
        .unwrap();
    assert_eq!(
        graph
            .labeled_successors(2)
            .flatten_labels()
            .into_iter()
            .collect::<Vec<_>>(),
        vec![
            (4, Branch::new(arc_2_to_4_label).unwrap().into()),
            (5, Branch::new(arc_2_to_5_label).unwrap().into())
        ]
    );

    Ok(())
}
