/*
 * Copyright (C) 2024  The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

use anyhow::Result;
use webgraph::prelude::{Left, Right, VecGraph, Zip};

use swh_graph::arc_iterators::LabeledArcIterator;
use swh_graph::graph::*;
use swh_graph::graph_builder::GraphBuilder;
use swh_graph::labels::{Branch, Visit, VisitStatus};
use swh_graph::properties::*;
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
#[allow(clippy::type_complexity)]
fn build_graph() -> Result<
    SwhBidirectionalGraph<
        SwhGraphProperties<
            VecMaps,
            VecTimestamps,
            VecPersons,
            VecContents,
            VecStrings,
            VecLabelNames,
        >,
        Zip<Left<VecGraph>, Right<VecGraph<Vec<u64>>>>,
        Zip<Left<VecGraph>, Right<VecGraph<Vec<u64>>>>,
    >,
> {
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
                    graph.properties().label_name(label.filename_id())
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
                    .label_name(Branch::from(label).filename_id())
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
