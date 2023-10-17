// Copyright (C) 2023  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use anyhow::{Context, Result};
use std::path::PathBuf;

use webgraph::prelude::RandomAccessGraph;

use swh_graph::graph::SwhUnidirectionalGraph;
use swh_graph::java_compat::mph::gov::GOVMPH;
use swh_graph::properties::SwhGraphProperties;

const BASENAME: &str = "../swh/graph/example_dataset/compressed/example";

fn graph() -> Result<SwhUnidirectionalGraph<impl RandomAccessGraph, SwhGraphProperties<GOVMPH>>> {
    Ok(
        swh_graph::graph::load_unidirectional(PathBuf::from(BASENAME))
            .context("Could not load graph")?
            .load_properties::<GOVMPH>()
            .context("Could not load graph properties")?,
    )
}

#[test]
fn test_swhids() -> Result<()> {
    let graph = graph()?;

    let mut swhids: Vec<_> = (0..24)
        .into_iter()
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
fn test_author_timestamp() -> Result<()> {
    let graph = graph()?;

    let node = graph
        .properties()
        .node_id(
            &"swh:1:rev:0000000000000000000000000000000000000003"
                .try_into()
                .unwrap(),
        )
        .unwrap();

    assert_eq!(graph.properties().author_timestamp(node), Some(1111122220));
    assert_eq!(
        graph.properties().author_timestamp_offset(node),
        Some(2 * 60)
    );

    Ok(())
}
