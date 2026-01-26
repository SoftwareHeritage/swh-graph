// Copyright (C) 2025-2026  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::fmt::Display;
use std::fs::File;
use std::io::{prelude::*, BufReader, BufWriter, Lines};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use clap::Parser;
use dsi_progress_logger::ProgressLog;
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

    let mut pl = dsi_progress_logger::concurrent_progress_logger!(
        display_memory = true,
        item_name = "node",
        local_speed = true,
    );
    pl.start("Writing list of nodes...");

    write_items_to_file(
        visited.iter().map(|node| {
            pl.light_update();
            graph.properties().swhid(*node)
        }),
        args.output.clone(),
    )?;
    pl.done();

    // if there are origins that failed to be found
    if !unknown_origins.is_empty() {
        let errors_filename = args.output.with_file_name("origin_errors.txt");

        warn!(
            "Some of the requested origins could not be found in the graph. Writing failed origins to '{}'...",
            errors_filename.display()
        );

        write_items_to_file(unknown_origins.into_iter(), errors_filename)?;
    }

    Ok(())
}

// write_items_to_file can take hanshmaps and vecs
fn write_items_to_file<P, I>(items: I, filename: P) -> Result<()>
where
    P: AsRef<Path>,
    I: IntoIterator<Item: Display>,
{
    let filename = filename.as_ref();
    let file =
        File::create(filename).with_context(|| format!("Could not open {}", filename.display()))?;

    let mut writer = BufWriter::new(file);

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
