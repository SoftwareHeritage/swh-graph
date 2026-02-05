// Copyright (C) 2023  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use anyhow::{Context, Result};
use log::info;
use swh_graph::map::Node2SWHID;
use swh_graph::SWHID;

const BASENAME: &str = "../swh/graph/example_dataset/compressed/example";

const SWHIDS: &[&str] = &[
    "swh:1:ori:83404f995118bd25774f4ac14422a8f175e7a054",
    "swh:1:snp:0000000000000000000000000000000000000020",
    "swh:1:rel:0000000000000000000000000000000000000010",
    "swh:1:rev:0000000000000000000000000000000000000009",
    "swh:1:ori:8f50d3f60eae370ddbf85c86219c55108a350165",
    "swh:1:snp:0000000000000000000000000000000000000022",
    "swh:1:rev:0000000000000000000000000000000000000003",
    "swh:1:dir:0000000000000000000000000000000000000002",
    "swh:1:cnt:0000000000000000000000000000000000000001",
    "swh:1:rel:0000000000000000000000000000000000000021",
    "swh:1:rev:0000000000000000000000000000000000000018",
    "swh:1:rel:0000000000000000000000000000000000000019",
    "swh:1:dir:0000000000000000000000000000000000000008",
    "swh:1:cnt:0000000000000000000000000000000000000007",
    "swh:1:rev:0000000000000000000000000000000000000013",
    "swh:1:dir:0000000000000000000000000000000000000012",
    "swh:1:cnt:0000000000000000000000000000000000000011",
    "swh:1:dir:0000000000000000000000000000000000000006",
    "swh:1:cnt:0000000000000000000000000000000000000005",
    "swh:1:cnt:0000000000000000000000000000000000000004",
    "swh:1:dir:0000000000000000000000000000000000000017",
    "swh:1:cnt:0000000000000000000000000000000000000014",
    "swh:1:dir:0000000000000000000000000000000000000016",
    "swh:1:cnt:0000000000000000000000000000000000000015",
];

#[test]
#[cfg_attr(miri, ignore)] // miri does not support file-backed mmap
fn test_load_node2swhid() -> Result<()> {
    let node2swhid_path = format!("{BASENAME}.node2swhid.bin");
    info!("loading node ID -> SWHID map from {node2swhid_path} ...");
    let node2swhid = Node2SWHID::load(&node2swhid_path)
        .with_context(|| format!("While loading the .node2swhid.bin file: {node2swhid_path}"))?;

    assert_eq!(node2swhid.len(), 24);

    assert_eq!(
        (0..24)
            .map(|node_id| format!("{}", node2swhid.get(node_id).unwrap()))
            .collect::<Vec<String>>(),
        SWHIDS
    );

    Ok(())
}

#[test]
#[cfg_attr(miri, ignore)] // miri does not support file-backed mmap
fn test_new_node2swhid() -> Result<()> {
    let tempdir = tempfile::tempdir()?;
    let node2swhid_file = tempdir.path().join("tmp.node2swhid.bin").to_owned();
    let mut node2swhid = Node2SWHID::new(&node2swhid_file, SWHIDS.len())?;

    for (node_id, &swhid) in SWHIDS.iter().enumerate() {
        let swhid: SWHID = swhid.try_into().unwrap();
        node2swhid.set(node_id, swhid);
    }

    // Check it can be read again

    let node2swhid = Node2SWHID::load(&node2swhid_file)?;

    assert_eq!(node2swhid.len(), 24);

    assert_eq!(
        (0..24)
            .map(|node_id| format!("{}", node2swhid.get(node_id).unwrap()))
            .collect::<Vec<String>>(),
        SWHIDS
    );

    Ok(())
}
