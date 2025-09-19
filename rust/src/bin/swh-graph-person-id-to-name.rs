// Copyright (C) 2025  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::path::PathBuf;

use anyhow::Error;
use clap::Parser;

use swh_graph::person::FullnameMap;

#[derive(Parser)]
#[command(about = "Maps an author ID to its corresponding full name in the SWH graph")]
struct Args {
    /// ID of the person to search for
    id: usize,
    /// Root path of the compressed graph (this corresponds to the `graph_path` argument that is
    /// passed during a graph compression)
    graph_path: PathBuf,
}

fn main() -> Result<(), Error> {
    let args = Args::parse();
    println!(
        "{}",
        String::from_utf8_lossy(FullnameMap::new(args.graph_path)?.map_id(args.id)?)
    );
    Ok(())
}
