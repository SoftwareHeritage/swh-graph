// Copyright (C) 2023  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use clap::Parser;
use dsi_progress_logger::{progress_logger, ProgressLog};
use log::info;
use rayon::prelude::*;

use swh_graph::map::{Node2SWHID, Node2Type};

#[derive(Parser, Debug)]
#[command(about = "Build `.node2type.bin` from `.node2swhid.bin`. Example usage:  cargo run --bin node2type -- swh/graph/example_dataset/compressed/example ", long_about = None)]
struct Args {
    /// The basename of the graph.
    basename: String,

    /// The basename of the file to create. By default it's the same as
    /// `basename`.
    dst_basename: Option<String>,
}

pub fn main() -> Result<()> {
    let args = Args::parse();

    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    // load the node ID -> SWHID map so we can convert it to a node2file
    let node2swhid_path = format!("{}.node2swhid.bin", args.basename);
    info!("loading node ID -> SWHID map from {node2swhid_path} ...");
    let node2swhid = Node2SWHID::load(&node2swhid_path)
        .with_context(|| format!("While loading the .node2swhid.bin file: {node2swhid_path}"))?;
    let num_nodes = node2swhid.len();

    // compute the path of the file we are creating
    let node2type_path = format!(
        "{}.node2type.bin",
        args.dst_basename.unwrap_or(args.basename)
    );
    // create a new node2type file that can index `num_nodes` nodes
    let node2type = Node2Type::new(&node2type_path, num_nodes)
        .with_context(|| format!("While creating the .node2type.bin file: {node2type_path}"))?;
    let node2type = Arc::new(Mutex::new(node2type));

    // init the progress logger
    let mut pl = progress_logger!(
        display_memory = true,
        item_name = "node",
        local_speed = true,
        expected_updates = Some(num_nodes),
    );
    pl.start("iterating over node ID -> SWHID map ...");
    let pl = Arc::new(Mutex::new(pl));

    // build the file
    (0..num_nodes)
        .into_par_iter()
        // get the SWHID of the node to get the type
        .map(|node_id| (node_id, node2swhid.get(node_id).unwrap().node_type))
        // split into groups to avoid constantly polling for the write lock
        .chunks(100_000_000)
        .for_each(|chunk| {
            let mut node2type = node2type.lock().unwrap();
            for (node_id, node_type) in &chunk {
                // and write it inside node2type
                node2type.set(*node_id, *node_type);
            }
            pl.lock().unwrap().update_with_count(chunk.len());
        });

    pl.lock().unwrap().done();
    Ok(())
}
