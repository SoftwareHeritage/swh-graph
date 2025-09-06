// Copyright (C) 2023-2025  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::collections::HashSet;
use std::path::PathBuf;

use anyhow::{Context, Result};
use rayon::prelude::*;

use swh_graph::graph::*;
use swh_graph::labels::LabelNameId;
use swh_graph::mph::SwhidPthash;
use swh_graph::properties::NodeIdFromSwhidError;
use swh_graph::AllSwhGraphProperties;
use swh_graph::{NodeType, OutOfBoundError, StrSWHIDDeserializationError, SWHID};

const BASENAME: &str = "../swh/graph/example_dataset/compressed/example";

fn graph() -> Result<SwhUnidirectionalGraph<AllSwhGraphProperties<SwhidPthash>>> {
    SwhUnidirectionalGraph::new(PathBuf::from(BASENAME))
        .context("Could not load graph")?
        .load_all_properties()
        .context("Could not load graph properties")
}

#[test]
#[cfg_attr(miri, ignore)] // miri does not support file-backed mmap
fn test_swhids() -> Result<()> {
    let graph = graph()?;
    let props = graph.properties();

    let mut swhids: Vec<_> = (0..24).map(|n| props.swhid(n).to_string()).collect();
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
#[cfg_attr(miri, ignore)] // miri does not support file-backed mmap
fn test_node_id() -> Result<()> {
    let graph = graph()?;
    let props = graph.properties();

    let unknown_swhid = SWHID {
        namespace_version: 1,
        node_type: NodeType::Content,
        hash: [
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x42,
        ],
    };

    assert_eq!(
        props.node_id("swh:1:rev:0000000000000000000000000000000000000003"),
        Ok(6)
    );

    assert_eq!(
        props.node_id("swh:1:cnt:0000000000000000000000000000000000000042"),
        Err(NodeIdFromSwhidError::UnknownSwhid(unknown_swhid))
    );

    assert_eq!(
        props.node_id("swh:1:lol:0000000000000000000000000000000000000003"),
        Err(NodeIdFromSwhidError::InvalidSwhid(
            StrSWHIDDeserializationError::Type("lol".to_string())
        ))
    );

    assert_eq!(
        props.node_id(
            SWHID::try_from("swh:1:rev:0000000000000000000000000000000000000003").unwrap()
        ),
        Ok(6)
    );

    assert_eq!(
        props.node_id(unknown_swhid),
        Err(NodeIdFromSwhidError::UnknownSwhid(unknown_swhid))
    );

    Ok(())
}

#[test]
#[cfg_attr(miri, ignore)] // miri does not support file-backed mmap
fn test_node_id_from_string_swhid() -> Result<()> {
    let graph = graph()?;
    let props = graph.properties();

    let unknown_swhid = SWHID {
        namespace_version: 1,
        node_type: NodeType::Content,
        hash: [
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x42,
        ],
    };

    assert_eq!(
        props.node_id_from_string_swhid("swh:1:rev:0000000000000000000000000000000000000003"),
        Ok(6)
    );

    assert_eq!(
        props.node_id_from_string_swhid("swh:1:cnt:0000000000000000000000000000000000000042"),
        Err(NodeIdFromSwhidError::UnknownSwhid(unknown_swhid))
    );

    assert_eq!(
        props.node_id_from_string_swhid("swh:1:lol:0000000000000000000000000000000000000003"),
        Err(NodeIdFromSwhidError::InvalidSwhid(
            StrSWHIDDeserializationError::Type("lol".to_string())
        ))
    );

    Ok(())
}

#[test]
#[cfg_attr(miri, ignore)] // miri does not support file-backed mmap
fn test_out_of_bound_properties() -> Result<()> {
    let graph = graph()?;
    let props = graph.properties();

    let node = graph.num_nodes(); // Non-existent node
    let err = OutOfBoundError {
        index: node,
        len: graph.num_nodes(),
    };

    assert_eq!(props.try_swhid(node), Err(err.clone()));
    assert_eq!(props.try_author_timestamp(node), Err(err.clone()));
    assert_eq!(props.try_author_timestamp_offset(node), Err(err.clone()));
    assert_eq!(props.try_committer_timestamp(node), Err(err.clone()));
    assert_eq!(props.try_committer_timestamp_offset(node), Err(err.clone()));
    assert_eq!(props.try_is_skipped_content(node), Err(err.clone()));
    assert_eq!(props.try_content_length(node), Err(err.clone()));
    assert_eq!(props.try_message(node), Err(err.clone()));
    assert_eq!(props.try_tag_name(node), Err(err.clone()));
    assert_eq!(props.try_author_id(node), Err(err.clone()));
    assert_eq!(props.try_committer_id(node), Err(err.clone()));

    Ok(())
}

#[test]
#[cfg_attr(miri, ignore)] // miri does not support file-backed mmap
fn test_content_properties() -> Result<()> {
    let graph = graph()?;
    let props = graph.properties();

    let swhid = "swh:1:cnt:0000000000000000000000000000000000000001";
    let node = props.node_id(swhid).unwrap();

    assert_eq!(props.swhid(node), swhid.try_into().unwrap());
    assert_eq!(props.author_timestamp(node), None);
    assert_eq!(props.author_timestamp_offset(node), None);
    assert_eq!(props.committer_timestamp(node), None);
    assert_eq!(props.committer_timestamp_offset(node), None);
    assert!(!props.is_skipped_content(node));
    assert_eq!(props.content_length(node), Some(42));
    assert_eq!(props.message(node), None);
    assert_eq!(props.tag_name(node), None);
    assert_eq!(props.author_id(node), None);
    assert_eq!(props.committer_id(node), None);

    Ok(())
}

#[test]
#[cfg_attr(miri, ignore)] // miri does not support file-backed mmap
fn test_skipped_content_properties() -> Result<()> {
    let graph = graph()?;
    let props = graph.properties();

    let swhid = "swh:1:cnt:0000000000000000000000000000000000000015";
    let node = props.node_id(swhid).unwrap();

    assert_eq!(props.swhid(node), swhid.try_into().unwrap());
    assert_eq!(props.author_timestamp(node), None);
    assert_eq!(props.author_timestamp_offset(node), None);
    assert_eq!(props.committer_timestamp(node), None);
    assert_eq!(props.committer_timestamp_offset(node), None);
    assert!(props.is_skipped_content(node));
    assert_eq!(props.content_length(node), Some(404));
    assert_eq!(props.message(node), None);
    assert_eq!(props.tag_name(node), None);
    assert_eq!(props.author_id(node), None);
    assert_eq!(props.committer_id(node), None);

    Ok(())
}

#[test]
#[cfg_attr(miri, ignore)] // miri does not support file-backed mmap
fn test_revision_properties() -> Result<()> {
    let graph = graph()?;
    let props = graph.properties();

    let swhid = "swh:1:rev:0000000000000000000000000000000000000009";
    let node = props.node_id(swhid).unwrap();

    assert_eq!(props.swhid(node), swhid.try_into().unwrap());
    assert_eq!(props.author_timestamp(node), Some(1111144440));
    assert_eq!(props.author_timestamp_offset(node), Some(2 * 60));
    assert_eq!(props.committer_timestamp(node), Some(1111155550));
    assert_eq!(props.committer_timestamp_offset(node), Some(2 * 60));
    assert!(!props.is_skipped_content(node));
    assert_eq!(props.content_length(node), None);
    assert_eq!(props.message(node), Some(b"Add parser".to_vec()));
    assert_eq!(props.tag_name(node), None);
    assert_eq!(props.author_id(node), Some(2));
    assert_eq!(props.committer_id(node), Some(2));

    Ok(())
}

#[test]
#[cfg_attr(miri, ignore)] // miri does not support file-backed mmap
fn test_release_properties() -> Result<()> {
    let graph = graph()?;
    let props = graph.properties();

    let swhid = "swh:1:rel:0000000000000000000000000000000000000010";
    let node = props.node_id(swhid).unwrap();

    assert_eq!(props.swhid(node), swhid.try_into().unwrap());
    assert_eq!(props.author_timestamp(node), Some(1234567890));
    assert_eq!(props.author_timestamp_offset(node), Some(2 * 60));
    assert_eq!(props.committer_timestamp(node), None);
    assert_eq!(props.committer_timestamp_offset(node), None);
    assert!(!props.is_skipped_content(node));
    assert_eq!(props.content_length(node), None);
    assert_eq!(props.message(node), Some(b"Version 1.0".to_vec()));
    assert_eq!(props.tag_name(node), Some(b"v1.0".to_vec()));
    assert_eq!(props.author_id(node), Some(0));
    assert_eq!(props.committer_id(node), None);

    Ok(())
}

#[test]
#[cfg_attr(miri, ignore)] // miri does not support file-backed mmap
fn test_snapshot_properties() -> Result<()> {
    let graph = graph()?;
    let props = graph.properties();

    let swhid = "swh:1:snp:0000000000000000000000000000000000000020";
    let node = props.node_id(swhid).unwrap();

    assert_eq!(props.swhid(node), swhid.try_into().unwrap());
    assert_eq!(props.author_timestamp(node), None);
    assert_eq!(props.author_timestamp_offset(node), None);
    assert_eq!(props.committer_timestamp(node), None);
    assert_eq!(props.committer_timestamp_offset(node), None);
    assert!(!props.is_skipped_content(node));
    assert_eq!(props.content_length(node), None);
    assert_eq!(props.message(node), None);
    assert_eq!(props.tag_name(node), None);
    assert_eq!(props.author_id(node), None);
    assert_eq!(props.committer_id(node), None);

    Ok(())
}

#[test]
#[cfg_attr(miri, ignore)] // miri does not support file-backed mmap
fn test_labels() -> Result<()> {
    let graph = graph()?;

    assert_eq!(graph.properties().num_label_names(), 11);
    assert_eq!(
        graph.properties().iter_label_name_ids().collect::<Vec<_>>(),
        (0..11).map(LabelNameId).collect::<Vec<_>>()
    );
    assert_eq!(
        graph
            .properties()
            .par_iter_label_name_ids()
            .map(|label_name_id| String::from_utf8_lossy(
                &graph.properties().label_name(label_name_id)
            )
            .into_owned())
            .collect::<HashSet<_>>(),
        vec![
            "README.md",
            "README.rst",
            "TODO.txt",
            "main.py",
            "old",
            "oldproject",
            "parser.c",
            "refs/heads/master",
            "refs/tags/v1.0",
            "refs/tags/v2.0-anonymous",
            "tests"
        ]
        .into_iter()
        .map(|label_name| label_name.to_string())
        .collect::<HashSet<_>>()
    );

    Ok(())
}
