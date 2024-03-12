// Copyright (C) 2023  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use anyhow::{Context, Result};
use bitvec::prelude::*;
use clap::Parser;
use dsi_progress_logger::ProgressLogger;
use log::{debug, info};
use std::collections::VecDeque;
use std::path::PathBuf;
use swh_graph::graph::*;
use swh_graph::java_compat::mph::gov::GOVMPH;

#[derive(Parser, Debug)]
#[command(about, long_about = None)]
struct Args {
    #[arg(short, long)]
    graph: PathBuf,
    #[arg(short, long)]
    swhid: String,
}

pub fn main() -> Result<()> {
    let args = Args::parse();

    // Setup a stderr logger because ProgressLogger uses the `log` crate
    // to printout
    stderrlog::new()
        .verbosity(3)
        .timestamp(stderrlog::Timestamp::Second)
        .init()
        .unwrap();

    let graph = load_unidirectional(PathBuf::from(args.graph))
        .context("Could not load graph")?
        .init_properties()
        .load_properties(|properties| properties.load_maps::<GOVMPH>())
        .context("Could not load graph properties")?
        .load_properties(|properties| properties.load_label_names())
        .context("Could not load label names")?
        .load_labels()
        .context("Could not load labels")?;

    // Lookup SWHID
    info!("looking up SWHID {} ...", args.swhid);
    let node_id = graph
        .properties()
        .node_id(&*args.swhid)
        .context("Unknown SWHID")?;
    info!("obtained node ID {node_id} ...");

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
        let visited_swhid = graph.properties().swhid(current_node);
        debug!("{visited_swhid}");
        visited_nodes += 1;
        let mut successors = graph.labelled_successors(current_node);
        while let Some((succ, labels)) = successors.next() {
            debug!("  Successor: {}", graph.properties().swhid(succ));
            for label in labels {
                let filename = graph.properties().label_name(label.filename_id());
                debug!(
                    "    has name {:?} and perm {:?}",
                    String::from_utf8_lossy(&filename),
                    label.permission().unwrap()
                );
            }
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
