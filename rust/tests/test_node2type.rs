// Copyright (C) 2023  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use anyhow::{Context, Result};
use log::info;
use swh_graph::map::{Node2SWHID, Node2Type};

const BASENAME: &str = "../swh/graph/example_dataset/compressed/example";

#[test]
#[cfg_attr(miri, ignore)] // miri does not support file-backed mmap
fn test_load_node2type() -> Result<()> {
    // load the node ID -> SWHID map so we can convert it to a node2type
    let node2swhid_path = format!("{BASENAME}.node2swhid.bin");
    info!("loading node ID -> SWHID map from {node2swhid_path} ...");
    let node2swhid = Node2SWHID::load(&node2swhid_path)
        .with_context(|| format!("While loading the .node2swhid.bin file: {node2swhid_path}"))?;
    let num_nodes = node2swhid.len();

    // load the node2type file
    let node2type_path = format!("{BASENAME}.node2type.bin");
    info!("loading node ID -> type map from {node2type_path} ...");
    let node2type = Node2Type::load(&node2type_path, num_nodes)
        .with_context(|| format!("While loading the .node2type.bin file: {node2type_path}"))?;

    // check that the the node2type matches with the node2swhid
    for node_id in 0..num_nodes {
        assert_eq!(
            node2swhid.get(node_id).unwrap().node_type,
            node2type.get(node_id).unwrap()
        )
    }

    Ok(())
}

#[test]
#[cfg_attr(miri, ignore)] // miri does not support file-backed mmap
fn test_new_node2type() -> Result<()> {
    // load the node ID -> SWHID map so we can convert it to a node2type
    let node2swhid_path = format!("{BASENAME}.node2swhid.bin");
    info!("loading node ID -> SWHID map from {node2swhid_path} ...");
    let node2swhid = Node2SWHID::load(&node2swhid_path)
        .with_context(|| format!("While loading the .node2swhid.bin file: {node2swhid_path}"))?;
    let num_nodes = node2swhid.len();

    let tempdir = tempfile::tempdir()?;
    let node2type_file = tempdir.path().join("tmp.node2type.bin").to_owned();
    let mut node2type = Node2Type::new(&node2type_file, num_nodes)?;

    for node_id in 0..num_nodes {
        node2type.set(node_id, node2swhid.get(node_id).unwrap().node_type);
    }

    assert_eq!(
        std::fs::read(node2type_file)?,
        std::fs::read(format!("{BASENAME}.node2type.bin"))?
    );

    Ok(())
}
