/*
 * Copyright (C) 2023-2024  The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

use std::fs::File;
use std::io::BufWriter;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;

use anyhow::{ensure, Context, Result};
use clap::{Parser, Subcommand, ValueEnum};
use dsi_bitstream::prelude::BE;
use dsi_progress_logger::ProgressLogger;
use ph::fmph;
use rayon::prelude::*;
use swh_graph::map::{MappedPermutation, OwnedPermutation, Permutation};
use webgraph::prelude::*;

#[derive(Parser, Debug)]
#[command(about = "Commands to run individual steps of the pipeline to compress a graph from an initial not-very-compressed BVGraph", long_about = None)]
struct Args {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Runs a BFS on the initial BVGraph to group similar node ids together
    Bfs {
        graph_dir: PathBuf,
        target_order: PathBuf,
    },

    /// Uses the permutation produced by the BFS or LLP to reorder nodes in the graph
    /// to get a more compressible graph
    Permute {
        #[arg(long, default_value_t = 102400)]
        input_batch_size: usize,
        #[arg(long, default_value_t = 1_000_000_000)]
        sort_batch_size: usize,
        #[arg(long, default_value_t = 1)]
        partitions_per_thread: usize,
        #[arg(long)]
        permutation: PathBuf,
        graph_dir: PathBuf,
        target_dir: PathBuf,
    },
    /// Produces a new graph by inverting the direction of all arcs in the source graph
    Transpose {
        #[arg(long, default_value_t = 102400)]
        input_batch_size: usize,
        #[arg(long, default_value_t = 1_000_000_000)]
        sort_batch_size: usize,
        #[arg(long, default_value_t = 1)]
        partitions_per_thread: usize,
        graph_dir: PathBuf,
        target_dir: PathBuf,
    },
    /// Combines a graph and its transposed graph into a single symmetric graph
    Simplify {
        #[arg(long, default_value_t = 102400)]
        input_batch_size: usize,
        #[arg(long, default_value_t = 1_000_000_000)]
        sort_batch_size: usize,
        #[arg(long, default_value_t = 1)]
        partitions_per_thread: usize,
        graph_dir: PathBuf,
        transposed_graph_dir: PathBuf,
        target_dir: PathBuf,
    },
    /// Reads a directed graph, transposes, and produces the corresponding symmetric graph.
    ///
    /// This is equivalent to Permute + Transpose + Simplify without generating temporary
    /// graphs
    PermuteAndSymmetrize {
        #[arg(long, default_value_t = 102400)]
        input_batch_size: usize,
        #[arg(long, default_value_t = 1_000_000_000)]
        sort_batch_size: usize,
        #[arg(long, default_value_t = 1)]
        partitions_per_thread: usize,
        #[arg(long)]
        permutation: PathBuf,
        graph_dir: PathBuf,
        target_dir: PathBuf,
    },
    /// Combines multiple permutations
    ComposeOrders {
        #[arg(long)]
        num_nodes: usize,
        #[arg(long)]
        input: Vec<PathBuf>,
        #[arg(long)]
        output: PathBuf,
    },

    /// Reads the list of SWHIDs and produces node2swhid.bin and node2type.bin
    Maps {
        #[arg(long)]
        num_nodes: usize,
        #[arg(long)]
        swhids_dir: PathBuf,
        #[arg(long)]
        mph_algo: MphAlgorithm,
        #[arg(long)]
        function: PathBuf,
        #[arg(long)]
        order: PathBuf,
        #[arg(long)]
        node2swhid: PathBuf,
        #[arg(long)]
        node2type: PathBuf,
    },

    /// Reads a zstd-compressed stream containing sorted lines, and compresses it
    /// as a single rear-coded list.
    Rcl {
        #[arg(long)]
        num_lines: usize,
        #[arg(long, default_value_t = 10000)]
        stripe_length: usize,
        input_dir: PathBuf,
        rcl: PathBuf,
    },

    HashSwhids {
        mph: PathBuf,
        swhids: Vec<String>,
    },
}

#[derive(Copy, Clone, Debug, ValueEnum)]
enum MphAlgorithm {
    Fmph,
    Cmph,
}

pub fn main() -> Result<()> {
    let args = Args::parse();

    stderrlog::new()
        .verbosity(2)
        .timestamp(stderrlog::Timestamp::Second)
        .init()
        .with_context(|| "While Initializing the stderrlog")?;

    match args.command {
        Commands::Bfs {
            graph_dir,
            target_order,
        } => {
            let mut permut_file = File::create(&target_order)
                .with_context(|| format!("Could not open {}", target_order.display()))?;

            log::info!("Loading graph");
            let graph = BVGraph::with_basename(graph_dir)
                .endianness::<BE>()
                .flags(MemoryFlags::TRANSPARENT_HUGE_PAGES | MemoryFlags::RANDOM_ACCESS)
                .load()?;
            log::info!("Graph loaded");

            swh_graph::approximate_bfs::almost_bfs_order(&graph)
                .dump(&mut permut_file)
                .context("Could not write permutation")?;
        }
        Commands::Permute {
            sort_batch_size,
            input_batch_size,
            partitions_per_thread,
            graph_dir,
            permutation,
            target_dir,
        } => {
            use swh_graph::compress::transform::transform;

            let graph = BVGraph::with_basename(graph_dir)
                .endianness::<BE>()
                .flags(MemoryFlags::TRANSPARENT_HUGE_PAGES | MemoryFlags::RANDOM_ACCESS)
                .load()?;
            let num_nodes = graph.num_nodes();

            log::info!("Loading permutation...");
            let permutation = MappedPermutation::load(num_nodes, permutation.as_path())
                .with_context(|| format!("Could not load {}", permutation.display()))?;

            log::info!("Permuting...");
            transform(
                sort_batch_size,
                input_batch_size,
                partitions_per_thread,
                graph,
                |src, dst| unsafe {
                    [(
                        permutation.get_unchecked(src),
                        permutation.get_unchecked(dst),
                    )]
                },
                target_dir,
            )?;
        }

        Commands::Transpose {
            sort_batch_size,
            input_batch_size,
            partitions_per_thread,
            graph_dir,
            target_dir,
        } => {
            use swh_graph::compress::transform::transform;

            let graph = BVGraph::with_basename(graph_dir)
                .endianness::<BE>()
                .flags(MemoryFlags::TRANSPARENT_HUGE_PAGES | MemoryFlags::RANDOM_ACCESS)
                .load()?;

            log::info!("Transposing...");
            transform(
                sort_batch_size,
                input_batch_size,
                partitions_per_thread,
                graph,
                |src, dst| [(dst, src)],
                target_dir,
            )?;
        }

        Commands::Simplify { .. } => {
            unimplemented!("simplify step (use PermuteAndSymmetrize instead)");
        }

        Commands::PermuteAndSymmetrize {
            sort_batch_size,
            input_batch_size,
            partitions_per_thread,
            graph_dir,
            permutation,
            target_dir,
        } => {
            use swh_graph::compress::transform::transform;

            let graph = BVGraph::with_basename(graph_dir)
                .endianness::<BE>()
                .flags(MemoryFlags::TRANSPARENT_HUGE_PAGES | MemoryFlags::RANDOM_ACCESS)
                .load()?;
            let num_nodes = graph.num_nodes();

            log::info!("Loading permutation...");
            let permutation = unsafe { MappedPermutation::load_unchecked(permutation.as_path()) }?;
            ensure!(
                permutation.len() == num_nodes,
                "Expected permutation to have {} nodes, got {}",
                num_nodes,
                permutation.len()
            );

            log::info!("Permuting, transposing, and symmetrizing...");
            transform(
                input_batch_size,
                sort_batch_size,
                partitions_per_thread,
                graph,
                |src, dst| {
                    assert_ne!(src, dst);
                    unsafe {
                        [
                            (
                                permutation.get_unchecked(src),
                                permutation.get_unchecked(dst),
                            ),
                            (
                                permutation.get_unchecked(dst),
                                permutation.get_unchecked(src),
                            ),
                        ]
                    }
                },
                target_dir,
            )?;
        }

        Commands::ComposeOrders {
            num_nodes,
            input,
            output,
        } => {
            let num_permutations = input.len();
            log::info!("Loading permutation 1/{}...", num_permutations);
            let mut output_file = File::create(&output)
                .with_context(|| format!("Could not open {}", output.display()))?;
            let mut inputs_iter = input.into_iter();
            let input_path = inputs_iter.next().expect("No permutation provided");
            let mut permutation = OwnedPermutation::load(num_nodes, input_path.as_path())
                .with_context(|| format!("Could not load {}", input_path.display()))?;
            for (i, next_input_path) in inputs_iter.enumerate() {
                log::info!("Composing permutation {}/{}...", i + 2, num_permutations);
                let next_permutation =
                    MappedPermutation::load(num_nodes, next_input_path.as_path())
                        .with_context(|| format!("Could not load {}", next_input_path.display()))?;
                permutation
                    .compose_in_place(next_permutation)
                    .with_context(|| format!("Could not apply {}", next_input_path.display()))?;
            }

            permutation.dump(&mut output_file)?;
        }

        Commands::Maps {
            num_nodes,
            swhids_dir,
            mph_algo,
            function,
            order,
            node2swhid,
            node2type,
        } => {
            use swh_graph::mph::SwhidMphf;

            use swh_graph::compress::zst_dir::*;
            use swh_graph::map::{Node2SWHID, Node2Type};
            use swh_graph::SWHID;

            log::info!("Loading permutation");
            let order = MappedPermutation::load(num_nodes, order.as_path())
                .with_context(|| format!("Could not load {}", order.display()))?;
            match mph_algo {
                MphAlgorithm::Fmph => {}
                _ => unimplemented!("Only --mph-algo fmph is supported"),
            }
            log::info!("Permutation loaded, reading MPH");
            let mph = fmph::Function::load(function).context("Cannot load mph")?;
            log::info!("MPH loaded, sorting arcs");

            let mut swhids: Vec<SWHID> = Vec::with_capacity(num_nodes);
            let swhids_uninit = swhids.spare_capacity_mut();

            let mut pl = ProgressLogger::default().display_memory();
            pl.item_name = "node";
            pl.local_speed = true;
            pl.expected_updates = Some(num_nodes);
            pl.start("Computing node2swhid");

            par_iter_lines_from_dir(&swhids_dir, Arc::new(Mutex::new(pl))).for_each(
                |line: [u8; 50]| {
                    let node_id = order
                        .get(mph.hash_str_array(&line).expect("Failed to hash line"))
                        .unwrap();
                    let swhid =
                        SWHID::try_from(unsafe { std::str::from_utf8_unchecked(&line[..]) })
                            .expect("Invalid SWHID");
                    assert!(
                        node_id < num_nodes,
                        "hashing {} returned {}, which is greater than the number of nodes ({})",
                        swhid,
                        node_id,
                        num_nodes
                    );

                    // Safe because we checked node_id < num_nodes
                    unsafe {
                        swhids_uninit
                            .as_ptr()
                            .add(node_id)
                            .cast_mut()
                            .write(std::mem::MaybeUninit::new(swhid));
                    }
                },
            );

            // Assuming the MPH and permutation are correct, we wrote an item at every
            // index.
            unsafe { swhids.set_len(num_nodes) };

            let mut node2swhid = Node2SWHID::new(node2swhid, num_nodes)?;
            let mut node2type = Node2Type::new(node2type, num_nodes)?;
            std::thread::scope(|s| {
                s.spawn(|| {
                    let mut pl = ProgressLogger::default().display_memory();
                    pl.item_name = "swhid";
                    pl.local_speed = true;
                    pl.expected_updates = Some(num_nodes);
                    pl.start("Writing node2swhid");
                    for i in 0..num_nodes {
                        pl.light_update();
                        unsafe { node2swhid.set_unchecked(i, *swhids.get_unchecked(i)) }
                    }
                    pl.done();
                });

                s.spawn(|| {
                    let mut pl = ProgressLogger::default().display_memory();
                    pl.item_name = "type";
                    pl.local_speed = true;
                    pl.expected_updates = Some(num_nodes);
                    pl.start("Writing node2type");
                    for i in 0..num_nodes {
                        pl.light_update();
                        unsafe { node2type.set_unchecked(i, swhids.get_unchecked(i).node_type) }
                    }
                    pl.done();
                });
            });
        }

        Commands::Rcl {
            num_lines,
            stripe_length,
            input_dir,
            rcl,
        } => {
            use epserde::ser::Serialize;
            use sux::prelude::*;

            let rcl_path = rcl;

            use swh_graph::compress::zst_dir::*;

            let mut rclb = RearCodedListBuilder::new(stripe_length);

            let mut pl = ProgressLogger::default().display_memory();
            pl.item_name = "label";
            pl.local_speed = true;
            pl.expected_updates = Some(num_lines);
            pl.start("Reading labels");
            let pl = Arc::new(Mutex::new(pl));
            iter_lines_from_dir(&input_dir, pl.clone()).for_each(|line: Vec<u8>| {
                // Each line is base64-encode, so it is guaranteed to be ASCII.
                let line = unsafe { std::str::from_utf8_unchecked(&line[..]) };
                rclb.push(line);
            });
            pl.lock().unwrap().done();

            // Disabled because of https://github.com/vigna/sux-rs/issues/18
            // rclb.print_stats();

            log::info!("Building RCL...");
            let rcl = rclb.build();

            log::info!("Writing RCL...");
            let mut rcl_file = BufWriter::new(
                File::create(&rcl_path)
                    .with_context(|| format!("Could not create {}", rcl_path.display()))?,
            );
            rcl.serialize(&mut rcl_file)
                .context("Could not write RCL")?;
        }

        Commands::HashSwhids { swhids, mph } => {
            let mut file =
                File::open(&mph).with_context(|| format!("Cannot read {}", mph.display()))?;
            let mph = fmph::Function::read(&mut file).context("Count not parse mph")?;
            for swhid in swhids {
                let swhid: [u8; 50] = swhid.as_bytes().try_into().context("Invalid SWHID size")?;

                log::info!("{}", mph.get(&swhid).context("Could not hash swhid")?);
            }
        }
    }

    Ok(())
}
