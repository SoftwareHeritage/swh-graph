// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use anyhow::Result;
use log::info;
use swh_graph::graph::*;
use swh_graph::graph_builder::GraphBuilder;
use swh_graph::java_compat::mph::gov::GOVMPH;
use swh_graph::labels::{Permission, Visit, VisitStatus};
use swh_graph::stdlib::*;
use swh_graph::swhid;

mod data;

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

#[test]
fn test_resolve_path() -> Result<()> {
    let mut builder = GraphBuilder::default();
    let dir_root = builder
        .node(swhid!(swh:1:dir:0000000000000000000000000000000000000000))?
        .done();
    let dir_src = builder
        .node(swhid!(swh:1:dir:0000000000000000000000000000000000000001))?
        .done();
    let dir_doc = builder
        .node(swhid!(swh:1:dir:0000000000000000000000000000000000000002))?
        .done();
    let dir_doc_ls = builder
        .node(swhid!(swh:1:dir:0000000000000000000000000000000000000003))?
        .done();
    let file_main_c = builder
        .node(swhid!(swh:1:cnt:0000000000000000000000000000000000000004))?
        .done();
    let file_makefile = builder
        .node(swhid!(swh:1:cnt:0000000000000000000000000000000000000005))?
        .done();
    let file_ls_man = builder
        .node(swhid!(swh:1:cnt:0000000000000000000000000000000000000006))?
        .done();
    let file_readme = builder
        .node(swhid!(swh:1:cnt:0000000000000000000000000000000000000007))?
        .done();
    builder.dir_arc(dir_root, dir_src, Permission::Directory, "src");
    builder.dir_arc(dir_root, dir_doc, Permission::Directory, "doc");
    builder.dir_arc(dir_doc, dir_doc_ls, Permission::Directory, "ls");
    builder.dir_arc(dir_src, file_main_c, Permission::Content, "main.c");
    builder.dir_arc(dir_src, file_makefile, Permission::Content, "Makefile");
    builder.dir_arc(dir_doc_ls, file_ls_man, Permission::Content, "ls.1");
    builder.dir_arc(dir_root, file_readme, Permission::Content, "README.md");
    let graph = builder.done()?;
    // Resulting filesystem tree:
    //
    //         /
    //         ├── doc/
    //         │   └── ls/
    //         │       └── ls.1
    //         ├── README.md
    //         └── src/
    //             ├── main.c
    //             └── Makefile
    //
    let props = graph.properties();
    let root_node = props.node_id(swhid!(swh:1:dir:0000000000000000000000000000000000000000))?;

    assert!(matches!(
        fs_resolve_name(&graph, root_node, "does-not-exist.txt"),
        Err(_)
    ));
    assert_eq!(
        props.swhid(fs_resolve_name(&graph, root_node, "README.md")?.unwrap()),
        swhid!(swh:1:cnt:0000000000000000000000000000000000000007)
    );
    assert_eq!(
        props.swhid(fs_resolve_name(&graph, root_node, "src")?.unwrap()),
        swhid!(swh:1:dir:0000000000000000000000000000000000000001)
    );

    assert!(matches!(
        fs_resolve_path(&graph, root_node, "does-not-exist.txt"),
        Err(_)
    ));
    assert!(matches!(
        fs_resolve_path(&graph, root_node, "README.md/is-not-a-dir"),
        Err(_)
    ));
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
fn test_iter_nodes() -> Result<()> {
    let graph = data::build_test_graph_1()?;
    let props = graph.properties();
    let ori1 = props.node_id(swhid!(swh:1:ori:83404f995118bd25774f4ac14422a8f175e7a054))?;
    let ori2 = props.node_id(swhid!(swh:1:ori:8f50d3f60eae370ddbf85c86219c55108a350165))?;

    assert_eq!(iter_nodes(&graph, &[ori1]).count(), 12);
    assert_eq!(iter_nodes(&graph, &[ori2]).count(), 21);

    Ok(())
}
