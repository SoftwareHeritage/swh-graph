// Copyright (C) 2023  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use anyhow::{Context, Result};
use std::path::PathBuf;

use swh_graph::graph::*;
use swh_graph::java_compat::mph::gov::GOVMPH;
use swh_graph::AllSwhGraphProperties;
use swh_graph::SWHID;

const BASENAME: &str = "../swh/graph/example_dataset/compressed/example";

fn graph() -> Result<SwhUnidirectionalGraph<AllSwhGraphProperties<GOVMPH>>> {
    load_unidirectional(PathBuf::from(BASENAME))
        .context("Could not load graph")?
        .load_all_properties()
        .context("Could not load graph properties")
}

#[test]
fn test_swhids() -> Result<()> {
    let graph = graph()?;

    let mut swhids: Vec<_> = (0..24)
        .map(|n| graph.properties().swhid(n).unwrap().to_string())
        .collect();
    swhids.sort();
    assert_eq!(
        swhids,
        vec![
            "swh:1:cnt:0000000000000000000000000000000000000001",
            "swh:1:cnt:0000000000000000000000000000000000000004",
            "swh:1:cnt:0000000000000000000000000000000000000005",
            "swh:1:cnt:0000000000000000000000000000000000000007",
            "swh:1:cnt:0000000000000000000000000000000000000011",
            "swh:1:cnt:0000000000000000000000000000000000000014",
            "swh:1:cnt:0000000000000000000000000000000000000015",
            "swh:1:dir:0000000000000000000000000000000000000002",
            "swh:1:dir:0000000000000000000000000000000000000006",
            "swh:1:dir:0000000000000000000000000000000000000008",
            "swh:1:dir:0000000000000000000000000000000000000012",
            "swh:1:dir:0000000000000000000000000000000000000016",
            "swh:1:dir:0000000000000000000000000000000000000017",
            "swh:1:ori:83404f995118bd25774f4ac14422a8f175e7a054",
            "swh:1:ori:8f50d3f60eae370ddbf85c86219c55108a350165",
            "swh:1:rel:0000000000000000000000000000000000000010",
            "swh:1:rel:0000000000000000000000000000000000000019",
            "swh:1:rel:0000000000000000000000000000000000000021",
            "swh:1:rev:0000000000000000000000000000000000000003",
            "swh:1:rev:0000000000000000000000000000000000000009",
            "swh:1:rev:0000000000000000000000000000000000000013",
            "swh:1:rev:0000000000000000000000000000000000000018",
            "swh:1:snp:0000000000000000000000000000000000000020",
            "swh:1:snp:0000000000000000000000000000000000000022",
        ]
    );

    Ok(())
}

#[test]
fn test_node_id() -> Result<()> {
    let graph = graph()?;

    assert_eq!(
        graph
            .properties()
            .node_id("swh:1:rev:0000000000000000000000000000000000000003")
            .unwrap(),
        6
    );

    assert_eq!(
        graph
            .properties()
            .node_id(SWHID::try_from("swh:1:rev:0000000000000000000000000000000000000003").unwrap())
            .unwrap(),
        6
    );

    Ok(())
}

#[test]
fn test_out_of_bound_properties() -> Result<()> {
    let graph = graph()?;

    let node = graph.num_nodes(); // Non-existent node

    let properties = graph.properties();
    assert_eq!(properties.swhid(node), None);
    assert_eq!(properties.author_timestamp(node), None);
    assert_eq!(properties.author_timestamp_offset(node), None);
    assert_eq!(properties.committer_timestamp(node), None);
    assert_eq!(properties.committer_timestamp_offset(node), None);
    assert_eq!(properties.is_skipped_content(node), None);
    assert_eq!(properties.content_length(node), None);
    assert_eq!(properties.message(node), None);
    assert_eq!(properties.tag_name(node), None);
    assert_eq!(properties.author_id(node), None);
    assert_eq!(properties.committer_id(node), None);

    Ok(())
}

#[test]
fn test_content_properties() -> Result<()> {
    let graph = graph()?;

    let swhid = "swh:1:cnt:0000000000000000000000000000000000000001";
    let node = graph.properties().node_id(swhid).unwrap();

    let properties = graph.properties();
    assert_eq!(properties.swhid(node), Some(swhid.try_into().unwrap()));
    assert_eq!(properties.author_timestamp(node), None);
    assert_eq!(properties.author_timestamp_offset(node), None);
    assert_eq!(properties.committer_timestamp(node), None);
    assert_eq!(properties.committer_timestamp_offset(node), None);
    assert_eq!(properties.is_skipped_content(node), Some(false));
    assert_eq!(properties.content_length(node), Some(42));
    assert_eq!(properties.message(node), None);
    assert_eq!(properties.tag_name(node), None);
    assert_eq!(properties.author_id(node), None);
    assert_eq!(properties.committer_id(node), None);

    Ok(())
}

#[test]
fn test_skipped_content_properties() -> Result<()> {
    let graph = graph()?;

    let swhid = "swh:1:cnt:0000000000000000000000000000000000000015";
    let node = graph.properties().node_id(swhid).unwrap();

    let properties = graph.properties();
    assert_eq!(properties.swhid(node), Some(swhid.try_into().unwrap()));
    assert_eq!(properties.author_timestamp(node), None);
    assert_eq!(properties.author_timestamp_offset(node), None);
    assert_eq!(properties.committer_timestamp(node), None);
    assert_eq!(properties.committer_timestamp_offset(node), None);
    assert_eq!(properties.is_skipped_content(node), Some(true));
    assert_eq!(properties.content_length(node), Some(404));
    assert_eq!(properties.message(node), None);
    assert_eq!(properties.tag_name(node), None);
    assert_eq!(properties.author_id(node), None);
    assert_eq!(properties.committer_id(node), None);

    Ok(())
}

#[test]
fn test_revision_properties() -> Result<()> {
    let graph = graph()?;

    let swhid = "swh:1:rev:0000000000000000000000000000000000000009";
    let node = graph.properties().node_id(swhid).unwrap();

    let properties = graph.properties();
    assert_eq!(properties.swhid(node), Some(swhid.try_into().unwrap()));
    assert_eq!(properties.author_timestamp(node), Some(1111144440));
    assert_eq!(properties.author_timestamp_offset(node), Some(2 * 60));
    assert_eq!(properties.committer_timestamp(node), Some(1111155550));
    assert_eq!(properties.committer_timestamp_offset(node), Some(2 * 60));
    assert_eq!(properties.is_skipped_content(node), Some(false));
    assert_eq!(properties.content_length(node), None);
    assert_eq!(properties.message(node), Some(b"Add parser".to_vec()));
    assert_eq!(properties.tag_name(node), None);
    assert_eq!(properties.author_id(node), Some(2));
    assert_eq!(properties.committer_id(node), Some(2));

    Ok(())
}

#[test]
fn test_release_properties() -> Result<()> {
    let graph = graph()?;

    let swhid = "swh:1:rel:0000000000000000000000000000000000000010";
    let node = graph.properties().node_id(swhid).unwrap();

    let properties = graph.properties();
    assert_eq!(properties.swhid(node), Some(swhid.try_into().unwrap()));
    assert_eq!(properties.author_timestamp(node), Some(1234567890));
    assert_eq!(properties.author_timestamp_offset(node), Some(2 * 60));
    assert_eq!(properties.committer_timestamp(node), None);
    assert_eq!(properties.committer_timestamp_offset(node), None);
    assert_eq!(properties.is_skipped_content(node), Some(false));
    assert_eq!(properties.content_length(node), None);
    assert_eq!(properties.message(node), Some(b"Version 1.0".to_vec()));
    assert_eq!(properties.tag_name(node), Some(b"v1.0".to_vec()));
    assert_eq!(properties.author_id(node), Some(0));
    assert_eq!(properties.committer_id(node), None);

    Ok(())
}

#[test]
fn test_snapshot_properties() -> Result<()> {
    let graph = graph()?;

    let swhid = "swh:1:snp:0000000000000000000000000000000000000020";
    let node = graph.properties().node_id(swhid).unwrap();

    let properties = graph.properties();
    assert_eq!(properties.swhid(node), Some(swhid.try_into().unwrap()));
    assert_eq!(properties.author_timestamp(node), None);
    assert_eq!(properties.author_timestamp_offset(node), None);
    assert_eq!(properties.committer_timestamp(node), None);
    assert_eq!(properties.committer_timestamp_offset(node), None);
    assert_eq!(properties.is_skipped_content(node), Some(false));
    assert_eq!(properties.content_length(node), None);
    assert_eq!(properties.message(node), None);
    assert_eq!(properties.tag_name(node), None);
    assert_eq!(properties.author_id(node), None);
    assert_eq!(properties.committer_id(node), None);

    Ok(())
}