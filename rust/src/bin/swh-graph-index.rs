/*
 * Copyright (C) 2023-2026  The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::{bail, ensure, Context, Result};
use clap::{Parser, Subcommand};
use dsi_bitstream::prelude::BE;
use dsi_progress_logger::{concurrent_progress_logger, ProgressLog};
use rayon::prelude::*;

use swh_graph::cli::{load_mph, MphAlgorithm};
use swh_graph::utils::{suffix_path, AtomicFile};

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

    /// Given a MPH and a node2swhid.bin file, builds a .order that, when composed with the MPH,
    /// yields the same order as in node2swhid.bin.
    ///
    /// This can be used to build an alternative MPH+order for an existing graph
    RebuildOrder {
        #[arg(long)]
        mph_algo: MphAlgorithm,
        #[arg(long)]
        function: PathBuf,
        #[arg(long)]
        node2swhid: PathBuf,
        #[arg(long)]
        target_order: PathBuf,
    },
}

pub fn main() -> Result<()> {
    let args = Args::parse();

    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    match args.command {
        Commands::Offsets { graph } => {
            use webgraph_cli::build::offsets::{build_offsets, CliArgs};
            build_offsets::<BE>(args.webgraph_args, CliArgs { basename: graph })?;
        }

        Commands::Ef { base_path } => {
            use webgraph_cli::build::ef::{build_elias_fano, CliArgs};
            build_elias_fano::<BE>(
                args.webgraph_args,
                CliArgs {
                    basename: base_path,
                    number_of_nodes: None,
                },
            )?;
        }

        Commands::LabelsEf {
            base_path,
            num_nodes,
        } => {
            use webgraph_cli::build::ef::{build_elias_fano, CliArgs};

            // webgraph shows a very obscure error when it happens (failed `.unwrap()`
            // when reading `nodes=` property on the `.properties` file),
            // so we should catch it here.
            let offsets_path = suffix_path(&base_path, ".labeloffsets");
            ensure!(
                offsets_path.exists(),
                "{} is missing",
                offsets_path.display()
            );

            build_elias_fano::<BE>(
                args.webgraph_args,
                CliArgs {
                    basename: base_path,
                    number_of_nodes: Some(num_nodes),
                },
            )?;
        }

        Commands::Dcf { base_path } => {
            use webgraph_cli::build::dcf::{build_dcf, CliArgs};
            build_dcf::<BE>(
                args.webgraph_args,
                CliArgs {
                    basename: base_path,
                },
            )?;
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
            let mut ef_file = AtomicFile::create_new(&ef_path)
                .with_context(|| format!("Could not create {}", ef_path.display()))?;

            // SAFETY: this might leak some internal memory, but we only ship this .ef alongside
            // the data this process has access to.
            unsafe { ef_offsets.serialize(&mut ef_file) }
                .context("Could not write full names offsets elias-fano file")?;

            ef_file
                .commit()
                .context("Could not commit elias-fano file")?;
        }

        Commands::RebuildOrder {
            mph_algo,
            function,
            node2swhid,
            target_order,
        } => {
            use swh_graph::map::{Node2SWHID, OwnedPermutation};
            use swh_graph::mph::SwhidMphf;

            let mut permut_file = AtomicFile::create_new(&target_order)
                .with_context(|| format!("Could not open {}", target_order.display()))?;

            let mph = load_mph(mph_algo, &function)?;

            log::info!("Loading node2swhid...");
            let node2swhid = Node2SWHID::load(node2swhid)?;
            let num_nodes = node2swhid.len();

            ensure!(
                mph.num_keys() == num_nodes,
                "MPH has {} nodes, but node2swhid has {num_nodes}",
                mph.num_keys()
            );

            log::info!("Allocating order...");
            let order: Vec<_> = (0..num_nodes)
                .into_par_iter()
                .map(|_| AtomicUsize::new(usize::MAX))
                .collect();

            let mut pl = concurrent_progress_logger!(
                display_memory = true,
                item_name = "node",
                local_speed = true,
                expected_updates = Some(num_nodes),
            );
            pl.start("Inverting order...");
            (0..num_nodes)
                .into_par_iter()
                .try_for_each_with(
                    pl.clone(),
                    |pl, node_id| -> Result<_> {
                        let swhid = node2swhid.get(node_id).expect("node2swhid too small"); // already checked
                        let Some(unpermuted_node_id) = mph.hash_swhid(&swhid) else {
                            log::debug!("{swhid} exists in previous graph, but not in current graph");
                            return Ok(());
                        };

                        // assign unpermuted_node_id to a value only if it was not assigned to one
                        // already.
                        // We must not assign it to a new value if it already had one, because the previous
                        // value was already inserted in used_node_ids, which prevents using again, and
                        // this would leave a hole in the set of node ids.
                        match order[unpermuted_node_id].compare_exchange(usize::MAX, node_id, Ordering::Relaxed, Ordering::Relaxed) {
                            Ok(usize::MAX) => (),
                            Ok(ret) => unreachable!("compare_exchange return Ok({ret}) but current value is {}", usize::MAX),
                            Err(swapped_value) => {
                                panic!("Node {unpermuted_node_id} was already set to {swapped_value} but we tried to set it to {node_id}. This means it collides with {}", node2swhid.get(swapped_value).unwrap())
                            }
                        }

                        pl.light_update();

                        Ok(())
                })?;
            pl.done();

            // In release mode, this is compiled into a no-op
            let order: Vec<_> = order.into_iter().map(AtomicUsize::into_inner).collect();

            let mut pl = concurrent_progress_logger!(
                display_memory = true,
                item_name = "node",
                local_speed = true,
                expected_updates = Some(num_nodes),
            );
            pl.start("Checking all node ids are assigned...");
            let unassigned_node_id = order
                .par_iter()
                .copied()
                .enumerate()
                .find_any(|&(_unpermuted_node_id, node_id)| node_id == usize::MAX);
            if let Some((unpermuted_node_id, _)) = unassigned_node_id {
                bail!("Unpermuted node {unpermuted_node_id} does not exist in node2swhid.bin");
            }
            log::info!("Checking permutation...");
            let perm = OwnedPermutation::new(order)?;

            log::info!("Writing permutation...");
            perm.dump(&mut permut_file)
                .context("Could not write permutation")?;
            permut_file.commit().context("Could not commit perm file")?;
        }
    }

    Ok(())
}
