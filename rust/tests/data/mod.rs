// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

//! Data library of synthetic graphs built for test purposes

use anyhow::Result;
use swh_graph::graph_builder::{self, GraphBuilder};
use swh_graph::swhid;

// See test_graph_1.dot
#[allow(dead_code)]
pub fn build_test_graph_1() -> Result<graph_builder::BuiltGraph> {
    let mut builder = GraphBuilder::default();
    let ori01 = builder
        .node(swhid!(swh:1:ori:0000000000000000000000000000000000000001))?
        .done();
    let ori02 = builder
        .node(swhid!(swh:1:ori:0000000000000000000000000000000000000002))?
        .done();
    let snp20 = builder
        .node(swhid!(swh:1:snp:0000000000000000000000000000000000000020))?
        .done();
    let snp22 = builder
        .node(swhid!(swh:1:snp:0000000000000000000000000000000000000022))?
        .done();
    let rel10 = builder
        .node(swhid!(swh:1:rel:0000000000000000000000000000000000000010))?
        .done();
    let rel19 = builder
        .node(swhid!(swh:1:rel:0000000000000000000000000000000000000019))?
        .done();
    let rel21 = builder
        .node(swhid!(swh:1:rel:0000000000000000000000000000000000000021))?
        .done();
    let rev03 = builder
        .node(swhid!(swh:1:rev:0000000000000000000000000000000000000003))?
        .done();
    let rev09 = builder
        .node(swhid!(swh:1:rev:0000000000000000000000000000000000000009))?
        .done();
    let rev13 = builder
        .node(swhid!(swh:1:rev:0000000000000000000000000000000000000013))?
        .done();
    let rev18 = builder
        .node(swhid!(swh:1:rev:0000000000000000000000000000000000000018))?
        .done();
    let dir12 = builder
        .node(swhid!(swh:1:dir:0000000000000000000000000000000000000012))?
        .done();
    let dir17 = builder
        .node(swhid!(swh:1:dir:0000000000000000000000000000000000000017))?
        .done();
    let dir02 = builder
        .node(swhid!(swh:1:dir:0000000000000000000000000000000000000002))?
        .done();
    let dir08 = builder
        .node(swhid!(swh:1:dir:0000000000000000000000000000000000000008))?
        .done();
    let dir16 = builder
        .node(swhid!(swh:1:dir:0000000000000000000000000000000000000016))?
        .done();
    let dir06 = builder
        .node(swhid!(swh:1:dir:0000000000000000000000000000000000000006))?
        .done();
    let cnt11 = builder
        .node(swhid!(swh:1:cnt:0000000000000000000000000000000000000011))?
        .done();
    let cnt14 = builder
        .node(swhid!(swh:1:cnt:0000000000000000000000000000000000000014))?
        .done();
    let cnt01 = builder
        .node(swhid!(swh:1:cnt:0000000000000000000000000000000000000001))?
        .done();
    let cnt07 = builder
        .node(swhid!(swh:1:cnt:0000000000000000000000000000000000000007))?
        .done();
    let cnt15 = builder
        .node(swhid!(swh:1:cnt:0000000000000000000000000000000000000015))?
        .done();
    let cnt04 = builder
        .node(swhid!(swh:1:cnt:0000000000000000000000000000000000000004))?
        .done();
    let cnt05 = builder
        .node(swhid!(swh:1:cnt:0000000000000000000000000000000000000005))?
        .done();

    builder.arc(ori01, snp20);
    builder.arc(ori02, snp22);
    builder.arc(snp20, rev09);
    builder.arc(snp20, rel10);
    builder.arc(snp22, rel10);
    builder.arc(snp22, rel21);
    builder.arc(rel10, rev09);
    builder.arc(rel19, rev18);
    builder.arc(rel21, rev18);
    builder.arc(rev03, dir02);
    builder.arc(rev09, rev03);
    builder.arc(rev09, dir08);
    builder.arc(rev13, rev09);
    builder.arc(rev13, dir12);
    builder.arc(rev18, dir17);
    builder.arc(dir12, dir08);
    builder.arc(dir12, cnt11);
    builder.arc(dir17, cnt14);
    builder.arc(dir17, dir16);
    builder.arc(dir02, cnt01);
    builder.arc(dir08, cnt01);
    builder.arc(dir08, dir06);
    builder.arc(dir08, cnt07);
    builder.arc(dir16, cnt15);
    builder.arc(dir06, cnt04);
    builder.arc(dir06, cnt05);

    builder.done()
}
