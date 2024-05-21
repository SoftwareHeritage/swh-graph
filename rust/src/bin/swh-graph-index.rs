/*
 * Copyright (C) 2023-2024  The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use dsi_bitstream::prelude::BE;

#[derive(Parser, Debug)]
#[command(about = "Commands to (re)generate `.ef` and `.offsets` files, allowing random access to BVGraph", long_about = None)]
struct Args {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Reads a graph file linearly and produce a .offsets file which can be used
    /// by the Java backend to randomly access the graph.
    Offsets { graph: PathBuf },

    /// Reads either a graph file linearly or .offsets file (generated and used
    /// by the Java backend to randomly access the graph), and produces a .ef file
    /// suitable to randomly access the graph from the Rust backend.
    ///
    /// Only suitable for unlabelled graphs.
    Ef { base_path: PathBuf },

    /// Reads either a graph file linearly or .offsets file (generated and used
    /// by the Java backend to randomly access the graph), and produces a .ef file
    /// suitable to randomly access the graph from the Rust backend.
    ///
    /// Only suitable for labelled graphs.
    LabelsEf {
        base_path: PathBuf,
        /// The number of elements to be inserted in the Elias-Fano
        /// starting from a label offset file. It is usually one more than
        /// the number of nodes in the graph.
        num_nodes: usize,
    },

    /// Reads either a graph file linearly, and produces a degree-cumulative function
    /// encoded as an Elias-Fano sequence in a .dcf file,
    /// suitable to distribute load while working on the graph.
    ///
    /// Only suitable for unlabelled graphs.
    Dcf { base_path: PathBuf },
}

pub fn main() -> Result<()> {
    let args = Args::parse();

    stderrlog::new()
        .verbosity(2)
        .timestamp(stderrlog::Timestamp::Second)
        .init()
        .with_context(|| "While Initializing the stderrlog")?;

    match args.command {
        Commands::Offsets { graph } => {
            use webgraph::cli::build::offsets::{build_offsets, CliArgs};
            build_offsets::<BE>(CliArgs { basename: graph })?;
        }

        Commands::Ef { base_path } => {
            use webgraph::cli::build::ef::{build_eliasfano, CliArgs};
            build_eliasfano::<BE>(CliArgs {
                basename: base_path,
                n: None,
            })?;
        }

        Commands::LabelsEf {
            base_path,
            num_nodes,
        } => {
            use webgraph::cli::build::ef::{build_eliasfano, CliArgs};
            build_eliasfano::<BE>(CliArgs {
                basename: base_path,
                n: Some(num_nodes),
            })?;
        }

        Commands::Dcf { base_path } => {
            use webgraph::cli::build::dcf::{build_dcf, CliArgs};
            build_dcf::<BE>(CliArgs {
                basename: base_path,
            })?;
        }
    }

    Ok(())
}
