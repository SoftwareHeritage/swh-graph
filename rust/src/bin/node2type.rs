use anyhow::{Context, Result};
use clap::Parser;
use dsi_progress_logger::ProgressLogger;
use log::info;
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

    stderrlog::new()
        .verbosity(2)
        .timestamp(stderrlog::Timestamp::Second)
        .init()
        .with_context(|| "While Initializing the stderrlog")?;

    // load the node ID -> SWHID map so we can convert it to a node2file
    let node2swhid_path = format!("{}.node2swhid.bin", args.basename);
    info!("loading node ID -> SWHID map from {node2swhid_path} ...");
    let node2swhid = Node2SWHID::load(&node2swhid_path).with_context(|| {
        format!(
            "While loading the .node2swhid.bin file: {}",
            node2swhid_path
        )
    })?;
    let num_nodes = node2swhid.len();

    // compute the path of the file we are creating
    let node2type_path = format!(
        "{}.node2type.bin",
        args.dst_basename.unwrap_or(args.basename)
    );
    // create a new node2type file that can index `num_nodes` nodes
    let mut node2type = Node2Type::new(&node2type_path, num_nodes as u64)
        .with_context(|| format!("While creating the .node2type.bin file: {}", node2type_path))?;

    // init the progress logger
    let mut pl = ProgressLogger::default().display_memory();
    pl.item_name = "node";
    pl.local_speed = true;
    pl.expected_updates = Some(num_nodes);
    pl.start("iterating over node ID -> SWHID map ...");

    // build the file
    for node_id in 0..num_nodes {
        // get the SWHID of the node to get the type and write it inside node2type
        node2type.set(node_id, node2swhid.get(node_id).unwrap().node_type);
        pl.light_update();
    }

    pl.done();
    Ok(())
}
