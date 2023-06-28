// Copyright (C) 2023  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use anyhow::Result;
use bitvec::prelude::*;
use dsi_progress_logger::ProgressLogger;
use log::{debug, info};
use std::collections::VecDeque;
use swh_graph::map::{Node2SWHID, Order};
use webgraph::prelude::*;

const BASENAME: &str = "../swh/graph/example_dataset/compressed/example";
// const BASENAME: &str = "/home/zack/graph/2022-12-07/compressed/graph";

pub fn main() -> Result<()> {
    // Setup a stderr logger because ProgressLogger uses the `log` crate
    // to printout
    stderrlog::new()
        .verbosity(3)
        .timestamp(stderrlog::Timestamp::Second)
        .init()
        .unwrap();

    // Load the mph.
    //
    // To make this work on the old (java-compressed) version of the graph,
    // this step requires converting the old .mph files to .cmph, using
    // something like:
    //
    // $ java -classpath ~/src/swh-graph/java/target/swh-graph-3.0.1.jar ~/src/swh-graph/java/src/main/java/org/softwareheritage/graph/utils/Mph2Cmph.java graph.mph graph.cmph
    let mph_file = format!("{}.cmph", BASENAME);
    info!("loading MPH from {mph_file} ...");
    let mph = webgraph::utils::mph::GOVMPH::load(mph_file)?;

    // Lookup SWHID
    //
    // See: https://archive.softwareheritage.org/swh:1:snp:fffe49ca41c0a9d777cdeb6640922422dc379b33
    let swhid = "swh:1:snp:fffe49ca41c0a9d777cdeb6640922422dc379b33";
    info!("looking up SWHID {swhid} ...");
    let node_id = mph.get_byte_array(swhid.as_bytes()) as usize;
    info!("obtained node ID {node_id} ...");

    // Load the order permutation
    // since the mph is create before the BFS and LLP steps, this is needed to
    // get the post-order node_id
    let order_file = format!("{}.order", BASENAME);
    info!("loading order permutation from {order_file} ...");
    let order = Order::load(order_file)?;
    let node_id = order.get(node_id).unwrap();
    info!("obtained node ID {node_id} ...");

    // Load a default bvgraph with memory mapping,
    //
    // To make this work on the old (java-compressed) version of the graph,
    // this step requires creating the new .ef (Elias Fano) files using
    // something like:
    //
    // $ cargo run --release --bin build_eliasfano -- $BASENAME
    //
    // Example:
    // $ cargo run --release --bin build_eliasfano --  ~/graph/latest/compressed/graph
    // $ cargo run --release --bin build_eliasfano -- ~/graph/latest/compressed/graph-transposed
    info!("loading compressed graph from {}.graph ...", BASENAME);
    let graph = webgraph::bvgraph::load(BASENAME)?;

    let node2swhid_file = format!("{}.node2swhid.bin", BASENAME);
    info!("loading node ID -> SWHID map from {node2swhid_file} ...");
    let node2swhid = Node2SWHID::load(node2swhid_file)?;

    // Setup a queue and a visited bitmap for the visit
    let num_nodes = graph.num_nodes();
    let mut visited = bitvec![u64, Lsb0; 0; num_nodes];
    let mut queue: VecDeque<usize> = VecDeque::new();
    assert!(node_id < num_nodes);
    queue.push_back(node_id);

    // Setup the progress logger for
    let mut pl = ProgressLogger::default().display_memory();
    let mut visited_nodes = 0;
    pl.item_name = "node";
    pl.local_speed = true;
    pl.expected_updates = Some(num_nodes);
    pl.start("visiting graph ...");

    // Standard BFS
    //
    // The output of the corresponding visit using the live swh-graph Web API
    // on the above SWHID can be found at:
    // https://archive.softwareheritage.org/api/1/graph/visit/nodes/swh:1:snp:fffe49ca41c0a9d777cdeb6640922422dc379b33/
    // It consists of 344 nodes.
    while let Some(current_node) = queue.pop_front() {
        let visited_swhid = node2swhid.get(current_node).unwrap();
        debug!("{visited_swhid}");
        visited_nodes += 1;
        for succ in graph.successors(current_node) {
            if !visited[succ] {
                queue.push_back(succ);
                visited.set(succ as _, true);
                pl.light_update();
            }
        }
    }

    pl.done();
    info!("visit completed after visiting {visited_nodes} nodes.");

    Ok(())
}
