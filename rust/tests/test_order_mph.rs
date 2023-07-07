// Copyright (C) 2023  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use anyhow::Result;
use dsi_progress_logger::ProgressLogger;
use log::info;
use std::io::prelude::*;
use swh_graph::map::{Node2SWHID, Order};
use webgraph::prelude::*;

const BASENAME: &str = "../swh/graph/example_dataset/compressed/example";

#[test]
fn test_order_mph() -> Result<()> {
    // Setup a stderr logger because ProgressLogger uses the `log` crate
    // to printout
    stderrlog::new()
        .verbosity(2)
        .timestamp(stderrlog::Timestamp::Second)
        .init()
        .unwrap();

    info!("loading MPH...");
    let mph = webgraph::utils::mph::GOVMPH::load(format!("{}.cmph", BASENAME))?;

    info!("loading node2swhid...");
    let node2swhid = Node2SWHID::load(format!("{}.node2swhid.bin", BASENAME))?;

    info!("loading order...");
    let order = Order::load(format!("{}.order", BASENAME))?;

    info!("opening graph.nodes.csv...");
    let file = std::io::BufReader::with_capacity(
        1 << 20,
        zstd::stream::read::Decoder::with_buffer(std::io::BufReader::with_capacity(
            1 << 20,
            std::fs::File::open(format!("{}.nodes.csv.zst", BASENAME))?,
        ))?,
    );

    info!("loading compressed graph into memory (with mmap)...");
    let graph = webgraph::bvgraph::load(BASENAME)?;

    // Setup the progress logger for
    let mut pl = ProgressLogger::default().display_memory();
    pl.item_name = "node";
    pl.local_speed = true;
    pl.expected_updates = Some(graph.num_nodes());
    pl.start("Roundtrip checking of the swhid, mph, and order...");

    for line in file.lines() {
        let line = line?;

        let mph_id = mph.get_byte_array(line.as_bytes());
        let node_id = order.get(mph_id as usize).unwrap();
        let swhid = node2swhid.get(node_id).unwrap();

        assert_eq!(line, swhid.to_string());

        pl.light_update();
    }

    pl.done();

    Ok(())
}
