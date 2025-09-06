// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::collections::HashMap;

use anyhow::Result;

use swh_graph::graph::*;
use swh_graph::graph_builder::GraphBuilder;
use swh_graph::labels::{Visit, VisitStatus};
use swh_graph::mph::SwhidPthash;
use swh_graph::swhid;
use swh_graph_stdlib::*;

mod data;

const BASENAME: &str = "../swh/graph/example_dataset/compressed/example";

#[test]
#[cfg_attr(miri, ignore)] // miri does not support file-backed mmap
fn test_find_root_dir() -> Result<()> {
    let graph = SwhUnidirectionalGraph::new(BASENAME)?
        .load_all_properties::<SwhidPthash>()?
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
fn test_find_head_rev() -> Result<()> {
    let mut builder = GraphBuilder::default();
    let snp0 = builder
        .node(swhid!(swh:1:snp:0000000000000000000000000000000000000000))?
        .done();
    let rev1 = builder
        .node(swhid!(swh:1:rev:0000000000000000000000000000000000000001))?
        .done();
    let rev2 = builder
        .node(swhid!(swh:1:rev:0000000000000000000000000000000000000002))?
        .done();
    let rev3 = builder
        .node(swhid!(swh:1:rev:0000000000000000000000000000000000000003))?
        .done();
    builder.snp_arc(snp0, rev1, "refs/heads/bug");
    builder.snp_arc(snp0, rev2, "refs/heads/main");
    builder.snp_arc(snp0, rev3, "refs/heads/new-sux");
    let graph = builder.done()?;

    assert_eq!(find_head_rev(&graph, snp0)?, Some(rev2));
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

    assert_eq!(
        find_latest_snp(&graph, ori0)?,
        Some((snp2, visit2.timestamp()))
    );
    Ok(())
}

#[test]
fn test_resolve_path() -> Result<()> {
    let graph = data::build_test_fs_tree_1()?;
    let props = graph.properties();
    let root_node = props.node_id(swhid!(swh:1:dir:0000000000000000000000000000000000000000))?;

    assert!(fs_resolve_name(&graph, root_node, "does-not-exist.txt").is_err());
    assert_eq!(
        props.swhid(fs_resolve_name(&graph, root_node, "README.md")?.unwrap()),
        swhid!(swh:1:cnt:0000000000000000000000000000000000000007)
    );
    assert_eq!(
        props.swhid(fs_resolve_name(&graph, root_node, "src")?.unwrap()),
        swhid!(swh:1:dir:0000000000000000000000000000000000000001)
    );

    assert!(fs_resolve_path(&graph, root_node, "does-not-exist.txt").is_err());
    assert!(fs_resolve_path(&graph, root_node, "README.md/is-not-a-dir").is_err());
    assert_eq!(
        props.swhid(fs_resolve_name(&graph, root_node, "doc")?.unwrap()),
        swhid!(swh:1:dir:0000000000000000000000000000000000000002)
    );
    assert_eq!(
        props.swhid(fs_resolve_path(&graph, root_node, "src/main.c")?.unwrap()),
        swhid!(swh:1:cnt:0000000000000000000000000000000000000004)
    );
    assert_eq!(
        props.swhid(fs_resolve_path(&graph, root_node, "doc/ls/ls.1")?.unwrap()),
        swhid!(swh:1:cnt:0000000000000000000000000000000000000006)
    );

    Ok(())
}

#[test]
fn test_fs_ls_tree() -> Result<()> {
    let graph = data::build_test_fs_tree_1()?;
    let props = graph.properties();
    let doc_dir = props.node_id("swh:1:dir:0000000000000000000000000000000000000002")?;

    use swh_graph::labels::Permission;
    use swh_graph_stdlib::FsTree::*;
    assert_eq!(
        fs_ls_tree(&graph, doc_dir)?,
        Directory(HashMap::from([(
            Vec::from("ls"),
            (
                Directory(HashMap::from([(
                    Vec::from("ls.1"),
                    (Content, Some(Permission::Content))
                )])),
                Some(Permission::Directory)
            )
        )]))
    );
    Ok(())
}

#[test]
fn test_iter_nodes() -> Result<()> {
    let graph = data::build_test_graph_1()?;
    let props = graph.properties();
    let ori1 = props.node_id(swhid!(swh:1:ori:83404f995118bd25774f4ac14422a8f175e7a054))?;
    let ori2 = props.node_id(swhid!(swh:1:ori:8f50d3f60eae370ddbf85c86219c55108a350165))?;

    assert_eq!(iter_nodes(&graph, [ori1]).count(), 12);
    assert_eq!(iter_nodes(&graph, [ori2]).count(), 21);

    Ok(())
}

#[test]
#[allow(clippy::needless_borrows_for_generic_args)]
fn test_iter_nodes_backward_compat() -> Result<()> {
    let graph = data::build_test_graph_1()?;
    let props = graph.properties();
    let ori1 = props.node_id(swhid!(swh:1:ori:83404f995118bd25774f4ac14422a8f175e7a054))?;
    let ori2 = props.node_id(swhid!(swh:1:ori:8f50d3f60eae370ddbf85c86219c55108a350165))?;

    assert_eq!(iter_nodes(&graph, &[ori1]).count(), 12);
    assert_eq!(iter_nodes(&graph, &[ori2]).count(), 21);

    Ok(())
}
