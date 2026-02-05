// Copyright (C) 2025  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::fmt::Display;
use std::fs::File;
use std::io::{prelude::*, BufReader, BufWriter, Lines};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use clap::Parser;
use log::{debug, info, warn};

use swh_graph::graph::{SwhGraphWithProperties, SwhUnidirectionalGraph};
use swh_graph::mph::DynMphf;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// path location to the base graph. It should contain prefixes if they are present in the file
    /// names. Check the docs for more details
    #[arg(short, long)]
    graph: PathBuf,
    /// path to a file with a list of origins to be searched.
    /// Origins should be one by line, without any extra chars
    #[arg(short, long)]
    origins: PathBuf,
    /// in case an origin is not found in the graph, this allows the script to attempt to find it
    /// with another protocol (https:// <-> git://)
    #[arg(short = 'p', long, default_value_t = false)]
    allow_protocol_variations: bool,
    /// path to folder or file name for the output. If any origin is not found in the graph,
    /// a file named `origin_errors.txt` will be written in the same path
    #[arg(short = 'O', long)]
    output: PathBuf,
}

pub fn main() -> Result<()> {
    let args = Args::parse();

    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    debug!("Debug logging ON...");

    info!("Loading origins...");
    let origins_lines = lines_from_file(args.origins).expect("Unable to read origins file");

    info!("Loading graph...");
    let graph = SwhUnidirectionalGraph::new(args.graph)
        .context("Could not load graph")?
        .init_properties()
        .load_properties(|properties| properties.load_maps::<DynMphf>())
        .context("Could not load graph properties")?;

    let (visited, unknown_origins) = swh_subgrapher::process_origins_and_build_subgraph(
        &graph,
        origins_lines,
        args.allow_protocol_variations,
    )?;

    debug!("Writing list of nodes to '{}'...", args.output.display());

    // Call the function and handle the result
    write_items_to_file(
        visited
            .iter()
            // convert NodeID into SWHID
            .map(|node| graph.properties().swhid(*node)),
        args.output.clone(),
    )?;
    info!(
        "Successfully wrote list of nodes to '{}'.",
        args.output.display()
    );

    // if there are origins that failed to be found
    if !unknown_origins.is_empty() {
        let errors_filename = args.output.with_file_name("origin_errors.txt");

        warn!(
            "Some of the requested origins could not be found in the graph. Writing failed origins to '{}'...",
            errors_filename.display()
        );

        // Call the function and handle the result
        write_items_to_file(&unknown_origins, errors_filename)?;
    }

    Ok(())
}

// write_items_to_file can take hanshmaps and vecs
fn write_items_to_file<P, I>(items: I, filename: P) -> Result<()>
where
    P: AsRef<Path>, // Accept anything convertible to a Path reference (like &str, String, PathBuf)
    I: IntoIterator, // The input must be iterable
    I::Item: Display, // The items produced by the iterator must implement Display
{
    let filename = filename.as_ref();
    let file =
        File::create(filename).with_context(|| format!("Could not open {}", filename.display()))?;

    // Wrap the file in a BufWriter for better performance.
    // Writing directly to a file can be slow due to many small system calls.
    // BufWriter collects writes in a buffer and flushes them in larger chunks.
    let mut writer = BufWriter::new(file);
    // Iterate over the elements (strings) in the HashSet.
    for item in items {
        writeln!(writer, "{item}")
            .with_context(|| format!("Could not write to {}", filename.display()))?;
    }

    Ok(())
}

fn lines_from_file(filename: impl AsRef<Path>) -> Result<Lines<BufReader<File>>> {
    let filename = filename.as_ref();
    let file =
        File::open(filename).with_context(|| format!("Could not open {}", filename.display()))?;
    let reader = BufReader::new(file);
    // returns the iterator from BufReader::lines()
    Ok(reader.lines())
}
