// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use anyhow::Result;
use log::info;
use swh_graph::graph::*;
use swh_graph::java_compat::mph::gov::GOVMPH;
use swh_graph::stdlib::*;
use swh_graph::swhid;

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
    let graph = load_unidirectional(BASENAME)?
        .load_all_properties::<GOVMPH>()?
        .load_labels()?;
    let props = graph.properties();
    assert_eq!(
        props.swhid(
            find_latest_snp(
                &graph,
                props.node_id(swhid!(swh:1:ori:83404f995118bd25774f4ac14422a8f175e7a054))?
            )?
            .unwrap()
            .0
        ),
        swhid!(swh:1:snp:0000000000000000000000000000000000000020)
    );
    Ok(())
}
