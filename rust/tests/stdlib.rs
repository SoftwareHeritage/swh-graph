// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use anyhow::Result;
use log::info;
use swh_graph::graph::*;
use swh_graph::graph_builder::GraphBuilder;
use swh_graph::java_compat::mph::gov::GOVMPH;
use swh_graph::labels::{Visit, VisitStatus};
use swh_graph::stdlib::*;
use swh_graph::swhid;

const BASENAME: &str = "../swh/graph/example_dataset/compressed/example";

#[test]
fn test_find_root_dir() -> Result<()> {
    info!("Loading graph...");
    let graph = load_unidirectional(BASENAME)?
        .load_all_properties::<GOVMPH>()?
        .load_labels()?;
    let props = graph.properties();

    assert_eq!(
        props.swhid(
            find_root_dir(
                &graph,
                props.node_id(swhid!(swh:1:rev:0000000000000000000000000000000000000003))?
            )?
            .unwrap()
        ),
        swhid!(swh:1:dir:0000000000000000000000000000000000000002)
    );
    assert_eq!(
        props.swhid(
            find_root_dir(
                &graph,
                props.node_id(swhid!(swh:1:rel:0000000000000000000000000000000000000021))?
            )?
            .unwrap()
        ),
        swhid!(swh:1:dir:0000000000000000000000000000000000000017)
    );

    Ok(())
}

#[test]
fn test_find_latest_snp() -> Result<()> {
    let mut builder = GraphBuilder::default();
    let ori0 = builder
        .node(swhid!(swh:1:ori:0000000000000000000000000000000000000000))?
        .done();
    let snp1 = builder
        .node(swhid!(swh:1:snp:0000000000000000000000000000000000000001))?
        .done();
    let snp2 = builder
        .node(swhid!(swh:1:snp:0000000000000000000000000000000000000002))?
        .done();
    let snp3 = builder
        .node(swhid!(swh:1:snp:0000000000000000000000000000000000000003))?
        .done();
    let visit1 = Visit::new(VisitStatus::Full, 1719568024).unwrap();
    let visit2 = Visit::new(VisitStatus::Full, 1719578024).unwrap();
    let visit3 = Visit::new(VisitStatus::Partial, 1719588024).unwrap();
    builder.ori_arc(ori0, snp1, visit1.status(), visit1.timestamp());
    builder.ori_arc(ori0, snp2, visit2.status(), visit2.timestamp());
    builder.ori_arc(ori0, snp3, visit3.status(), visit3.timestamp());
    let graph = builder.done()?;
    let props = graph.properties();

    let (node, timestamp) = find_latest_snp(
        &graph,
        props.node_id(swhid!(swh:1:ori:0000000000000000000000000000000000000000))?,
    )?
    .unwrap();
    assert_eq!(
        props.swhid(node),
        swhid!(swh:1:snp:0000000000000000000000000000000000000002)
    );
    assert_eq!(timestamp, visit2.timestamp());
    Ok(())
}
