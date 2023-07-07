use anyhow::{Context, Result};
use log::info;
use swh_graph::map::{Node2SWHID, Node2Type};

const BASENAME: &str = "../swh/graph/example_dataset/compressed/example";

#[test]
fn test_node2type() -> Result<()> {
    // load the node ID -> SWHID map so we can convert it to a node2file
    let node2swhid_path = format!("{}.node2swhid.bin", BASENAME);
    info!("loading node ID -> SWHID map from {node2swhid_path} ...");
    let node2swhid = Node2SWHID::load(&node2swhid_path).with_context(|| {
        format!(
            "While loading the .node2swhid.bin file: {}",
            node2swhid_path
        )
    })?;
    let num_nodes = node2swhid.len();

    // load the node2type file
    let node2type_path = format!("{}.node2type.bin", BASENAME);
    info!("loading node ID -> type map from {node2type_path} ...");
    let node2type = Node2Type::load(&node2type_path, num_nodes as u64)
        .with_context(|| format!("While loading the .node2type.bin file: {}", node2type_path))?;

    // check that the the node2type matches with the node2swhid
    for node_id in 0..num_nodes {
        assert_eq!(
            node2swhid.get(node_id).unwrap().node_type,
            node2type.get(node_id).unwrap()
        )
    }

    Ok(())
}
