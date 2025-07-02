/*
 * Copyright (C) 2025  The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

use anyhow::{Context, Result};

use swh_graph::graph::*;
use swh_graph::graph_builder::{BuiltGraph, GraphBuilder, LabelNamesOrder};
use swh_graph::labels::LabelNameId;
use swh_graph::swhid;

#[test]
fn test_label_name_order() -> Result<()> {
    fn build_graph(mut builder: GraphBuilder) -> Result<BuiltGraph> {
        let a = builder
            .node(swhid!(swh:1:snp:0000000000000000000000000000000000000010))?
            .done();
        let b = builder
            .node(swhid!(swh:1:rev:0000000000000000000000000000000000000020))?
            .done();
        let c = builder
            .node(swhid!(swh:1:rev:0000000000000000000000000000000000000030))?
            .done();
        builder.snp_arc(a, b, "ref");
        builder.snp_arc(a, c, "sel");
        builder.snp_arc(b, c, "zzz");
        builder.done().context("Could not make graph")
    }

    let mut builder = GraphBuilder::default();
    builder.label_names_order = LabelNamesOrder::LexicographicBase64;
    let old_graph = build_graph(builder)?;

    let builder = GraphBuilder::default();
    let new_graph = build_graph(builder)?;

    // Check we did build label names in different orders
    // base64("sel") == "c2Vs" < "cmVm" < base64("ref")
    assert_eq!(
        (0..3)
            .map(|i| old_graph.properties().label_name(LabelNameId(i)))
            .collect::<Vec<_>>(),
        [b"sel", b"ref", b"zzz"]
    );
    // "ref" < "sel"
    assert_eq!(
        (0..3)
            .map(|i| new_graph.properties().label_name(LabelNameId(i)))
            .collect::<Vec<_>>(),
        [b"ref", b"sel", b"zzz"]
    );

    assert_eq!(
        old_graph.properties().label_name_id("sel"),
        Ok(LabelNameId(0))
    );
    assert_eq!(
        old_graph.properties().label_name_id("ref"),
        Ok(LabelNameId(1))
    );
    assert_eq!(
        old_graph.properties().label_name_id("zzz"),
        Ok(LabelNameId(2))
    );

    assert_eq!(
        new_graph.properties().label_name_id("ref"),
        Ok(LabelNameId(0))
    );
    assert_eq!(
        new_graph.properties().label_name_id("sel"),
        Ok(LabelNameId(1))
    );
    assert_eq!(
        new_graph.properties().label_name_id("zzz"),
        Ok(LabelNameId(2))
    );

    Ok(())
}
