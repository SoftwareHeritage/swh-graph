/*
 * Copyright (C) 2023-2024  The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;

use anyhow::{anyhow, ensure, Context, Result};
use clap::{Parser, Subcommand, ValueEnum};
use dsi_bitstream::prelude::BE;
use dsi_progress_logger::ProgressLogger;
use ph::fmph;
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
        #[arg(long)]
        /// Must be provided iff --init-swhids is.
        mph_algo: Option<MphAlgorithm>,
        /// Path to the MPH function. Must be provided iff --init-swhids is.
        #[arg(long)]
        function: Option<PathBuf>,
        #[arg(long, requires_all=["mph_algo", "function"])]
        /// Path to a list of SWHIDs to use as the BFS starting points
        init_roots: Option<PathBuf>,
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

    /// Uses the Layered-Label Propagation algorithm to produce a .order file that
    /// can be used to permute a graph to a smaller isomorphic graph
    Llp {
        #[command(flatten)]
        args: webgraph::cli::run::llp::CliArgs,
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

    /// Builds a MPH from the given a stream of base64-encoded labels
    PthashLabels {
        #[arg(long)]
        num_labels: usize,
        labels: PathBuf,
        output_mphf: PathBuf,
    },
    /// Builds a permutation mapping label hashes to their position in the sorted stream of
    /// base64-encoded labels
    PthashLabelsOrder {
        #[arg(long)]
        num_labels: usize,
        labels: PathBuf,
        mphf: PathBuf,
        output_order: PathBuf,
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
            mph_algo,
            function,
            init_roots,
            graph_dir,
            target_order,
        } => {
            use swh_graph::mph::SwhidMphf;

            let mut permut_file = File::create(&target_order)
                .with_context(|| format!("Could not open {}", target_order.display()))?;

            log::info!("Loading graph...");
            let graph = BVGraph::with_basename(graph_dir)
                .endianness::<BE>()
                .flags(MemoryFlags::TRANSPARENT_HUGE_PAGES | MemoryFlags::RANDOM_ACCESS)
                .load()?;
            log::info!("Graph loaded");

            let start_nodes = match init_roots {
                None => Vec::new(),
                Some(init_swhids) => {
                    // 'clap' should have made them required.
                    let mph_algo = mph_algo.expect("missing --mph-algo");
                    let function = function.expect("missing --function");

                    let init_swhids = File::open(&init_swhids)
                        .with_context(|| format!("Could not open {}", init_swhids.display()))?;
                    let mph: swh_graph::mph::DynMphf = match mph_algo {
                        MphAlgorithm::Cmph => {
                            swh_graph::java_compat::mph::gov::GOVMPH::load(function)
                                .context("Cannot load mph")?
                                .into()
                        }
                        MphAlgorithm::Fmph => fmph::Function::load(function)
                            .context("Cannot load mph")?
                            .into(),
                    };

                    let mut pl = ProgressLogger::default().display_memory();
                    pl.item_name = "SWHID";
                    pl.local_speed = true;
                    pl.start("[step 0/2] Loading initial SWHIDs...");

                    let init_swhids = BufReader::new(init_swhids)
                        .lines()
                        .flat_map(|line| {
                            let mut line =
                                line.unwrap_or_else(|e| panic!("Could not parse line: {}", e));
                            while line.ends_with('\n') || line.ends_with('\r') {
                                line.pop();
                            }
                            if line.is_empty() {
                                return None;
                            }
                            assert_eq!(line.len(), 50, "Unexpected line length: {:?}", line);

                            pl.light_update();

                            Some(
                                mph.hash_str(&line)
                                    .unwrap_or_else(|| panic!("Could not hash {:?}", line)),
                            )
                        })
                        .collect();

                    pl.done();

                    init_swhids
                }
            };

            swh_graph::approximate_bfs::almost_bfs_order(&graph, &start_nodes)
                .dump(&mut permut_file)
                .context("Could not write permutation")?;
        }
        Commands::Permute {
            input_batch_size,
            sort_batch_size,
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
                input_batch_size,
                sort_batch_size,
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
            input_batch_size,
            sort_batch_size,
            partitions_per_thread,
            graph_dir,
            target_dir,
        } => {
            use swh_graph::compress::transform::transform;

            log::info!("Loading graph...");
            let graph = BVGraph::with_basename(graph_dir)
                .endianness::<BE>()
                .flags(MemoryFlags::TRANSPARENT_HUGE_PAGES | MemoryFlags::RANDOM_ACCESS)
                .load()?;

            log::info!("Transposing...");
            transform(
                input_batch_size,
                sort_batch_size,
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
            input_batch_size,
            sort_batch_size,
            partitions_per_thread,
            graph_dir,
            permutation,
            target_dir,
        } => {
            use swh_graph::compress::transform::transform;

            log::info!("Loading graph...");
            let graph = BVGraph::with_basename(graph_dir)
                .endianness::<BE>()
                .flags(MemoryFlags::TRANSPARENT_HUGE_PAGES | MemoryFlags::RANDOM_ACCESS)
                .load()?;
            let num_nodes = graph.num_nodes();

            log::info!("Loading permutation...");
            let permutation = MappedPermutation::load_unchecked(permutation.as_path())?;
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
            let input_path = inputs_iter
                .next()
                .ok_or(anyhow!("No permutation provided"))?;
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

        Commands::Llp { args } => {
            webgraph::cli::run::llp::llp::<BE>(args)?;
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

            use swh_graph::compress::maps::*;
            use swh_graph::map::{Node2SWHID, Node2Type};

            log::info!("Loading permutation");
            let order = MappedPermutation::load(num_nodes, order.as_path())
                .with_context(|| format!("Could not load {}", order.display()))?;

            log::info!("Permutation loaded, reading MPH");
            let swhids = match mph_algo {
                MphAlgorithm::Fmph => {
                    let mph = fmph::Function::load(function).context("Cannot load mph")?;
                    log::info!("MPH loaded, reading and hashing SWHIDs");
                    ordered_swhids(&swhids_dir, order, mph, num_nodes)?
                }
                MphAlgorithm::Cmph => {
                    let mph = swh_graph::java_compat::mph::gov::GOVMPH::load(function)
                        .context("Cannot load mph")?;
                    log::info!("MPH loaded, reading and hashing SWHIDs");
                    ordered_swhids(&swhids_dir, order, mph, num_nodes)?
                }
            };

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
        Commands::PthashLabels {
            num_labels,
            labels,
            output_mphf,
        } => {
            use pthash::Phf;

            let mut mphf = swh_graph::compress::label_names::build_mphf(labels, num_labels)?;
            log::info!("Saving MPHF...");
            mphf.save(&output_mphf)
                .with_context(|| format!("Could not write MPH to {}", output_mphf.display()))?;
        }
        Commands::PthashLabelsOrder {
            num_labels,
            labels,
            mphf,
            output_order,
        } => {
            let order = swh_graph::compress::label_names::build_order(labels, mphf, num_labels)?;

            log::info!("Saving order");
            let mut f = File::create(&output_order)
                .with_context(|| format!("Could not create {}", output_order.display()))?;
            order
                .dump(&mut f)
                .with_context(|| format!("Could not write order to {}", output_order.display()))?;
        }
    }

    Ok(())
}
