/*
 * Copyright (C) 2023-2024  The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::sync::Mutex;

use anyhow::{anyhow, ensure, Context, Result};
use clap::{Parser, Subcommand, ValueEnum};
use dsi_bitstream::prelude::BE;
use dsi_progress_logger::{concurrent_progress_logger, progress_logger, ProgressLog};
use rayon::prelude::*;
use sux::bits::{AtomicBitVec, BitVec};
use sux::traits::BitVecOps;
use webgraph::prelude::*;

use swh_graph::map::{MappedPermutation, OwnedPermutation, Permutation};
use swh_graph::mph::SwhidPthash;

#[derive(Parser, Debug)]
#[command(about = "Commands to run individual steps of the pipeline to compress a graph from an initial not-very-compressed BvGraph", long_about = None)]
struct Args {
    #[clap(flatten)]
    webgraph_args: webgraph_cli::GlobalArgs,
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Creates a new order using the order already computed for an other graph
    InitialOrder {
        #[arg(long)]
        mph_algo: MphAlgorithm,
        #[arg(long)]
        function: PathBuf,
        #[arg(long)]
        num_nodes: usize,
        #[arg(long)]
        previous_node2swhid: PathBuf,
        #[arg(long)]
        target_order: PathBuf,
    },
    /// Runs a BFS on the initial BvGraph to group similar node ids together
    Bfs {
        #[arg(long)]
        /// Must be provided iff --init-swhids is.
        mph_algo: Option<MphAlgorithm>,
        /// Path to the MPH function. Must be provided iff --init-swhids is.
        #[arg(long)]
        function: Option<PathBuf>,
        #[arg(long)]
        /// Path to the order used to build the input graph.
        order: Option<PathBuf>,
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
        #[arg(long, default_value_t = 1)]
        partitions_per_thread: usize,
        #[arg(long)]
        permutation: Vec<PathBuf>,
        graph_dir: PathBuf,
        target_dir: PathBuf,
    },
    /// Produces a new graph by inverting the direction of all arcs in the source graph
    Transpose {
        #[arg(long, default_value_t = 102400)]
        input_batch_size: usize,
        #[arg(long, default_value_t = 1)]
        partitions_per_thread: usize,
        graph_dir: PathBuf,
        target_dir: PathBuf,
    },
    /// Combines a graph and its transposed graph into a single symmetric graph
    Simplify {
        #[arg(long, default_value_t = 102400)]
        input_batch_size: usize,
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
        args: webgraph_cli::run::llp::CliArgs,
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

    /// Writes general statistics to graph.stats
    Stats {
        #[arg(long)]
        /// Where to load the graph from
        graph: PathBuf,
        #[arg(long)]
        /// Where to write statistics to
        stats: PathBuf,
    },

    /// Reads a UTF-8 file containing sorted lines, and compresses it as a single front-coded list.
    Fcl {
        #[arg(long)]
        num_lines: usize,
        #[arg(long, default_value_t = std::num::NonZeroUsize::new(4).unwrap())]
        stripe_length: std::num::NonZeroUsize,
        input_path: PathBuf,
        fcl: PathBuf,
    },
    /// Reads a directory of zstd-compressed UTF-8-encoded files containing sorted lines,
    /// and compresses it as a single rear-coded list.
    Rcl {
        #[arg(long)]
        num_lines: usize,
        #[arg(long, default_value_t = 10000)]
        stripe_length: usize,
        input_dir: PathBuf,
        rcl: PathBuf,
    },

    /// Builds a MPH from the given a stream of textual SWHIDs
    PthashSwhids {
        #[arg(long)]
        num_nodes: usize,
        swhids: PathBuf,
        output_mphf: PathBuf,
    },

    /// Builds a MPH from the given a stream of opaque lines
    PthashPersons {
        #[arg(long)]
        num_persons: usize,
        persons: PathBuf,
        output_mphf: PathBuf,
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
    Pthash,
    Cmph,
}

pub fn main() -> Result<()> {
    let args = Args::parse();

    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    match args.command {
        Commands::InitialOrder {
            mph_algo,
            function,
            num_nodes,
            previous_node2swhid,
            target_order,
        } => {
            use swh_graph::map::Node2SWHID;
            use swh_graph::mph::SwhidMphf;

            let mut permut_file = File::create(&target_order)
                .with_context(|| format!("Could not open {}", target_order.display()))?;

            let mph: swh_graph::mph::DynMphf = match mph_algo {
                MphAlgorithm::Cmph => swh_graph::java_compat::mph::gov::GOVMPH::load(function)
                    .context("Cannot load mph")?
                    .into(),
                MphAlgorithm::Pthash => SwhidPthash::load(function)
                    .context("Cannot load mph")?
                    .into(),
            };

            log::info!("Loading previous node2swhid...");
            let previous_node2swhid = Node2SWHID::load(previous_node2swhid)?;
            let previous_num_nodes = previous_node2swhid.len();

            if num_nodes < previous_num_nodes {
                log::warn!("Previous graph was larger ({previous_num_nodes} nodes) than the one being compressed ({num_nodes} nodes). This is safe, but will cause suboptimal results.");
            }

            log::info!("Allocating order...");
            let order: Vec<_> = (0..num_nodes)
                .into_par_iter()
                .map(|_| AtomicUsize::new(usize::MAX))
                .collect();

            log::info!("Initializing set of used node ids...");
            let used_node_ids = AtomicBitVec::new(num_nodes);
            let mut pl = concurrent_progress_logger!(
                display_memory = true,
                item_name = "node",
                local_speed = true,
                expected_updates = Some(num_nodes),
            );
            pl.start("Filling new order from previous order...");
            (0..previous_num_nodes.min(num_nodes))
                .into_par_iter()
                .try_for_each_with(
                    pl.clone(),
                    |pl, previous_node_id| -> Result<_> {
                        let swhid = previous_node2swhid.get(previous_node_id).expect("node2swhid too small"); // already checked
                        let new_unpermuted_node_id = mph.hash_swhid(&swhid).with_context(|| format!("Unknown SWHID: {swhid}"))?;

                        // assign new_unpermuted_node_id to a value only if it was not assigned to one
                        // already.
                        // We must not assign it to a new value if it already had one, because the previous
                        // value was already inserted in used_node_ids, which prevents using again, and
                        // this would leave a hole in the set of node ids.
                        match order[new_unpermuted_node_id].compare_exchange(usize::MAX, previous_node_id, Ordering::Relaxed, Ordering::Relaxed) {
                            Ok(usize::MAX) => {
                                use sux::traits::AtomicBitVecOps;
                                used_node_ids.set(previous_node_id, true, Ordering::Relaxed);
                            }
                            Ok(ret) => unreachable!("compare_exchange return Ok({ret}) but current value is {}", usize::MAX),
                            Err(swapped_value) => {
                                log::warn!("Node {new_unpermuted_node_id} was already set to {swapped_value} but we tried to set it to {previous_node_id}. This probably because {swhid} or the node it collides with ({}) was removed from the graph.", previous_node2swhid.get(swapped_value).unwrap())
                            }
                        }

                        pl.light_update();

                        Ok(())
                })?;
            pl.done();

            let used_node_ids: BitVec = BitVec::from(used_node_ids);
            let mut order: Vec<_> = order.into_iter().map(AtomicUsize::into_inner).collect(); // Compiles to no-op

            // At this point, assuming the previous graph was smaller, used_node_ids will be overwhelmingly 1s in range
            // 0..previous_num_nodes (the zeros being deleted nodes) and *only* 0s in range previous_num_nodes..num_nodes

            log::info!("Listing unused node ids...");
            let mut unused_node_ids: Vec<_> = (0..previous_num_nodes.min(num_nodes))
                .into_par_iter()
                .filter(|&node_id| !used_node_ids.get(node_id))
                .collect();

            // Now we can attribute the unused node ids to all nodes that do not have one assigned.
            // First, from the materialized list of 'unused_node_ids', then from the range
            // previous_num_nodes..num_nodes.
            // (We could do it in any order, it does not matter.)
            let mut next_unused_node_id = previous_num_nodes;

            let mut pl = progress_logger!(
                display_memory = true,
                item_name = "node",
                local_speed = true,
                expected_updates = Some(num_nodes),
            );
            pl.start("Assigning unused node ids...");
            // TODO: parallelize this loop somehow?
            for node_id in order.iter_mut() {
                if *node_id == usize::MAX {
                    // not assigned yet. pick one from unused_node_ids if there are still some
                    // available
                    if let Some(unused_node_id) = unused_node_ids.pop() {
                        *node_id = unused_node_id;
                    } else {
                        ensure!(next_unused_node_id < num_nodes, "Tried to allocate node_id {next_unused_node_id} but the graph only has {num_nodes} nodes");
                        *node_id = next_unused_node_id;
                        next_unused_node_id += 1;
                    }
                }
                pl.light_update()
            }
            pl.done();

            log::info!("Checking permutation...");
            let perm = OwnedPermutation::new(order)?;

            log::info!("Writing permutation...");
            perm.dump(&mut permut_file)
                .context("Could not write permutation")?;
        }
        Commands::Bfs {
            mph_algo,
            order,
            function,
            init_roots,
            graph_dir,
            target_order,
        } => {
            use swh_graph::mph::SwhidMphf;

            log::info!("Loading graph...");
            let graph = BvGraph::with_basename(graph_dir)
                .endianness::<BE>()
                .flags(MemoryFlags::TRANSPARENT_HUGE_PAGES | MemoryFlags::RANDOM_ACCESS)
                .load()?;
            log::info!("Graph loaded");

            let order = order
                .map(|order_path| {
                    log::info!("Mmapping order");
                    MappedPermutation::load(graph.num_nodes(), &order_path).with_context(|| {
                        format!("Could not mmap order from {}", order_path.display())
                    })
                })
                .transpose()?;

            let mut permut_file = File::create(&target_order)
                .with_context(|| format!("Could not open {}", target_order.display()))?;

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
                        MphAlgorithm::Pthash => SwhidPthash::load(function)
                            .context("Cannot load mph")?
                            .into(),
                    };

                    let mut pl = progress_logger!(
                        display_memory = true,
                        item_name = "SWHID",
                        local_speed = true,
                    );
                    pl.start("[step 0/2] Loading initial SWHIDs...");

                    let init_swhids = BufReader::new(init_swhids)
                        .lines()
                        .flat_map(|line| {
                            let mut line =
                                line.unwrap_or_else(|e| panic!("Could not parse line: {e}"));
                            while line.ends_with('\n') || line.ends_with('\r') {
                                line.pop();
                            }
                            if line.is_empty() {
                                return None;
                            }
                            assert_eq!(line.len(), 50, "Unexpected line length: {line:?}");

                            pl.light_update();

                            let mut node_id = mph
                                .hash_str(&line)
                                .unwrap_or_else(|| panic!("Could not hash {line:?}"));
                            if let Some(order) = &order {
                                node_id = order
                                    .get(node_id)
                                    .unwrap_or_else(|| panic!("Invalid node id for {line:?}"));
                            }

                            Some(node_id)
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
            partitions_per_thread,
            graph_dir,
            permutation,
            target_dir,
        } => {
            use swh_graph::compress::transform::transform;

            let graph = BvGraph::with_basename(graph_dir)
                .endianness::<BE>()
                .flags(MemoryFlags::TRANSPARENT_HUGE_PAGES | MemoryFlags::RANDOM_ACCESS)
                .load()?;
            let num_nodes = graph.num_nodes();

            let permutation = compose_permutations(&permutation, num_nodes)?;

            log::info!("Permuting...");
            transform(
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
            input_batch_size,
            partitions_per_thread,
            graph_dir,
            target_dir,
        } => {
            use swh_graph::compress::transform::transform;

            log::info!("Loading graph...");
            let graph = BvGraph::with_basename(graph_dir)
                .endianness::<BE>()
                .flags(MemoryFlags::TRANSPARENT_HUGE_PAGES | MemoryFlags::RANDOM_ACCESS)
                .load()?;

            log::info!("Transposing...");
            transform(
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
            input_batch_size,
            partitions_per_thread,
            graph_dir,
            permutation,
            target_dir,
        } => {
            use swh_graph::compress::transform::transform;

            log::info!("Loading graph...");
            let graph = BvGraph::with_basename(graph_dir)
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
            let mut output_file = File::create(&output)
                .with_context(|| format!("Could not open {}", output.display()))?;
            let permutation = compose_permutations(&input, num_nodes)?;

            permutation.dump(&mut output_file)?;
        }

        Commands::Llp { args: llp_args } => {
            webgraph_cli::run::llp::llp::<BE>(args.webgraph_args, llp_args)?;
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
            use swh_graph::compress::maps::*;
            use swh_graph::map::{Node2SWHID, Node2Type};

            log::info!("Loading permutation");
            let order = MappedPermutation::load(num_nodes, order.as_path())
                .with_context(|| format!("Could not load {}", order.display()))?;

            log::info!("Permutation loaded, reading MPH");
            let swhids = match mph_algo {
                MphAlgorithm::Pthash => {
                    let mph = SwhidPthash::load(function).context("Cannot load mph")?;
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
                    let mut pl = progress_logger!(
                        display_memory = true,
                        item_name = "SWHID",
                        local_speed = true,
                        expected_updates = Some(num_nodes),
                    );
                    pl.start("Writing node2swhid");
                    for i in 0..num_nodes {
                        pl.light_update();
                        unsafe { node2swhid.set_unchecked(i, *swhids.get_unchecked(i)) }
                    }
                    pl.done();
                });

                s.spawn(|| {
                    let mut pl = progress_logger!(
                        display_memory = true,
                        item_name = "node",
                        local_speed = true,
                        expected_updates = Some(num_nodes),
                    );
                    pl.start("Writing node2type");
                    for i in 0..num_nodes {
                        pl.light_update();
                        unsafe { node2type.set_unchecked(i, swhids.get_unchecked(i).node_type) }
                    }
                    pl.done();
                });
            });
        }

        Commands::Stats { graph, stats } => {
            use swh_graph::graph::{SwhBidirectionalGraph, SwhGraph};
            use swh_graph::views::Transposed;

            let graph = SwhBidirectionalGraph::new(graph).context("Could not load graph")?;

            let outdegrees = swh_graph::stats::outdegree(&graph);
            let indegrees = swh_graph::stats::outdegree(&Transposed(&graph));
            let statistics = [
                ("nodes", graph.num_nodes().to_string()),
                ("arcs", graph.num_arcs().to_string()),
                ("loops", 0.to_string()),
                ("minindegree", indegrees.min.to_string()),
                ("maxindegree", indegrees.max.to_string()),
                ("avgindegree", indegrees.avg.to_string()),
                ("minoutdegree", outdegrees.min.to_string()),
                ("maxoutdegree", outdegrees.max.to_string()),
                ("avgoutdegree", outdegrees.avg.to_string()),
            ]
            .into_iter()
            .map(|(k, v)| (k.to_owned(), v))
            .collect();

            let f = File::create_new(&stats)
                .with_context(|| format!("Could not create {}", stats.display()))?;
            java_properties::write(BufWriter::new(f), &statistics)
                .with_context(|| format!("Could not write statistics to {}", stats.display()))?
        }

        Commands::Fcl {
            num_lines,
            stripe_length,
            input_path,
            fcl,
        } => {
            use swh_graph::front_coded_list::FrontCodedListBuilder;
            let fcl_path = fcl;

            let mut fclb = FrontCodedListBuilder::new(stripe_length);

            let mut pl = progress_logger!(
                display_memory = true,
                item_name = "label",
                local_speed = true,
                expected_updates = Some(num_lines),
            );
            pl.start("Reading labels and building FCL");
            let pl = Arc::new(Mutex::new(pl));
            let input = File::open(&input_path)
                .with_context(|| format!("Could not open {}", input_path.display()))?;
            // Each line is base64-encoded, so it is guaranteed to be ASCII.
            for line in BufReader::new(input).lines() {
                let line = line.expect("Could not decode line");
                fclb.push(line.into_bytes())
                    .context("Could not push line to FCL")?;
            }
            pl.lock().unwrap().done();

            log::info!("Writing FCL...");
            fclb.dump(fcl_path).context("Could not write FCL")?;
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

            let mut rclb = RearCodedListBuilder::<str, true>::new(stripe_length);

            let mut pl = concurrent_progress_logger!(
                display_memory = true,
                item_name = "label",
                local_speed = true,
                expected_updates = Some(num_lines),
            );
            pl.start("Reading labels");
            iter_lines_from_dir(&input_dir, pl.clone()).for_each(|line: Vec<u8>| {
                // Each line is base64-encoded, so it is guaranteed to be ASCII.
                let line = unsafe { std::str::from_utf8_unchecked(&line[..]) };
                rclb.push(line);
            });
            pl.done();

            // Disabled because of https://github.com/vigna/sux-rs/issues/18
            // rclb.print_stats();

            log::info!("Building RCL...");
            let rcl = rclb.build();

            log::info!("Writing RCL...");
            let mut rcl_file = BufWriter::new(
                File::create(&rcl_path)
                    .with_context(|| format!("Could not create {}", rcl_path.display()))?,
            );
            // SAFETY: this might leak some internal memory, but this process does not access any
            // sensitive information.
            unsafe { rcl.serialize(&mut rcl_file) }.context("Could not write RCL")?;
        }

        Commands::PthashSwhids {
            num_nodes,
            swhids,
            output_mphf,
        } => {
            use pthash::Phf;

            let mut mphf = swh_graph::compress::mph::build_swhids_mphf(swhids, num_nodes)?;
            log::info!("Saving MPHF...");
            mphf.0
                .save(&output_mphf)
                .with_context(|| format!("Could not write MPH to {}", output_mphf.display()))?;
        }

        Commands::PthashPersons {
            num_persons,
            persons,
            output_mphf,
        } => {
            use pthash::Phf;

            let mut mphf = swh_graph::compress::persons::build_mphf(persons, num_persons)?;
            log::info!("Saving MPHF...");
            mphf.save(&output_mphf)
                .with_context(|| format!("Could not write MPH to {}", output_mphf.display()))?;
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

fn compose_permutations(
    paths: &[PathBuf],
    num_nodes: usize,
) -> Result<OwnedPermutation<Vec<usize>>> {
    let num_permutations = paths.len();
    log::info!("Loading permutation 1/{}...", num_permutations);
    let mut paths = paths.iter();
    let first_path = paths.next().ok_or(anyhow!("No permutation provided"))?;
    let mut permutation = OwnedPermutation::load(num_nodes, first_path)
        .with_context(|| format!("Could not load {}", first_path.display()))?;
    for (i, next_path) in paths.enumerate() {
        log::info!("Composing permutation {}/{}...", i + 2, num_permutations);
        let next_permutation = MappedPermutation::load(num_nodes, next_path.as_path())
            .with_context(|| format!("Could not load {}", next_path.display()))?;
        permutation
            .compose_in_place(next_permutation)
            .with_context(|| format!("Could not apply {}", next_path.display()))?;
    }
    Ok(permutation)
}
