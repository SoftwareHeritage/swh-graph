/*
 * Copyright (C) 2023-2025  The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

use std::path::PathBuf;

use anyhow::{ensure, Context, Result};
use clap::{Parser, Subcommand};
use dsi_bitstream::prelude::BE;

use swh_graph::utils::suffix_path;

#[derive(Parser, Debug)]
#[command(about = "Commands to (re)generate `.ef` and `.offsets` files, allowing random access to BVGraph", long_about = None)]
struct Args {
    #[clap(flatten)]
    webgraph_args: webgraph_cli::GlobalArgs,
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
    /// Only suitable for unlabeled graphs.
    Ef { base_path: PathBuf },

    /// Reads either a graph file linearly or .offsets file (generated and used
    /// by the Java backend to randomly access the graph), and produces a .ef file
    /// suitable to randomly access the graph from the Rust backend.
    ///
    /// Only suitable for labeled graphs.
    LabelsEf {
        base_path: PathBuf,
        /// The number of nodes in the graph
        num_nodes: usize,
    },

    /// Reads either a graph file linearly, and produces a degree-cumulative function
    /// encoded as an Elias-Fano sequence in a .dcf file,
    /// suitable to distribute load while working on the graph.
    ///
    /// Only suitable for unlabeled graphs.
    Dcf { base_path: PathBuf },

    /// Reads the lengths of the full names and builds the corresponding Elias-Fano
    /// offsets file.
    FullnamesEf {
        #[arg(long)]
        num_persons: usize,
        fullnames_path: PathBuf,
        lengths_path: PathBuf,
        ef_path: PathBuf,
    },
}

pub fn main() -> Result<()> {
    let args = Args::parse();

    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    match args.command {
        Commands::Offsets { graph } => {
            use webgraph_cli::build::offsets::{build_offsets, CliArgs};
            build_offsets::<BE>(args.webgraph_args, CliArgs { src: graph })?;
        }

        Commands::Ef { base_path } => {
            use webgraph_cli::build::ef::{build_eliasfano, CliArgs};
            build_eliasfano::<BE>(
                args.webgraph_args,
                CliArgs {
                    src: base_path,
                    number_of_nodes: None,
                },
            )?;
        }

        Commands::LabelsEf {
            base_path,
            num_nodes,
        } => {
            use webgraph_cli::build::ef::{build_eliasfano, CliArgs};

            // webgraph shows a very obscure error when it happens (failed `.unwrap()`
            // when reading `nodes=` property on the `.properties` file),
            // so we should catch it here.
            let offsets_path = suffix_path(&base_path, ".labeloffsets");
            ensure!(
                offsets_path.exists(),
                "{} is missing",
                offsets_path.display()
            );

            build_eliasfano::<BE>(
                args.webgraph_args,
                CliArgs {
                    src: base_path,
                    number_of_nodes: Some(num_nodes),
                },
            )?;
        }

        Commands::Dcf { base_path } => {
            use webgraph_cli::build::dcf::{build_dcf, CliArgs};
            build_dcf::<BE>(args.webgraph_args, CliArgs { src: base_path })?;
        }

        Commands::FullnamesEf {
            num_persons,
            fullnames_path,
            lengths_path,
            ef_path,
        } => {
            use dsi_bitstream::prelude::*;
            use epserde::ser::Serialize;
            use std::{fs::File, io::BufReader};
            use sux::dict::EliasFanoBuilder;

            let lengths_file = File::open(&lengths_path)
                .with_context(|| format!("Could not open {}", lengths_path.display()))?;
            let mut lengths_reader = <BufBitReader<BE, _>>::new(<WordAdapter<u64, _>>::new(
                BufReader::with_capacity(1 << 20, lengths_file),
            ));

            let max_offset = std::fs::metadata(&fullnames_path)
                .with_context(|| format!("Could not stat {}", fullnames_path.display()))?
                .len();
            let max_offset = usize::try_from(max_offset).context("offset overflowed usize")?;

            let mut ef_builder = EliasFanoBuilder::new(num_persons + 1, max_offset);
            let mut offset = 0usize;
            ef_builder.push(offset);
            for _ in 0..num_persons {
                let delta = lengths_reader
                    .read_gamma()
                    .context("Could not read gamma")?;
                offset = offset.checked_add(delta as usize).with_context(|| {
                    format!(
                        "Sum of lengths in {} overflowed usize",
                        lengths_path.display()
                    )
                })?;
                ensure!(
                    offset <= max_offset,
                    "Sum of sizes in {} is greater than the size of {}",
                    lengths_path.display(),
                    fullnames_path.display(),
                );
                ef_builder.push(offset);
            }
            let ef_offsets = ef_builder.build_with_seq();

            log::info!("Writing Elias-Fano file for full names offsets...");
            let mut ef_file = File::create(&ef_path)
                .with_context(|| format!("Could not create {}", ef_path.display()))?;
            ef_offsets
                .serialize(&mut ef_file)
                .context("Could not write full names offsets elias-fano file")?;
        }
    }

    Ok(())
}
