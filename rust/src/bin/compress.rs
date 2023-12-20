/*
 * Copyright (C) 2023  The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

use std::env::temp_dir;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand, ValueEnum};
use dsi_progress_logger::ProgressLogger;
use ph::fmph;
use rayon::prelude::*;
use swh_graph::map::{MappedPermutation, OwnedPermutation, Permutation};
use swh_graph::SWHType;
use webgraph::prelude::*;

#[derive(Parser, Debug)]
#[command(about = "Commands to run individual steps of the pipeline from ORC files to compressed graph", long_about = None)]
struct Args {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Reads the list of nodes and arcs from the ORC directory and produces lists of
    /// unique SWHIDs in the given directory
    ExtractNodes {
        #[arg(value_enum, long, default_value_t = DatasetFormat::Orc)]
        format: DatasetFormat,
        #[arg(long, default_value = "*")]
        allowed_node_types: String,
        /// Size (in bytes) of each thread's buffer in memory before it sorts and
        /// flushes it to disk.
        #[arg(long, default_value = "5000000")]
        buffer_size: usize,
        dataset_dir: PathBuf,
        target_dir: PathBuf,
    },
    /// Reads the list of nodes and arcs from the ORC directory and produces lists of
    /// unique labels in the given directory
    ExtractLabels {
        #[arg(value_enum, long, default_value_t = DatasetFormat::Orc)]
        format: DatasetFormat,
        #[arg(long, default_value = "*")]
        allowed_node_types: String,
        /// Size (in bytes) of each thread's buffer in memory before it sorts and
        /// flushes it to disk.
        #[arg(long, default_value = "5000000")]
        buffer_size: usize,
        dataset_dir: PathBuf,
        target_dir: PathBuf,
    },
    /// Reads the list of authors and committers from the ORC directory and produces lists
    /// unique names (based64-encoded) in the given directory
    ExtractPersons {
        #[arg(value_enum, long, default_value_t = DatasetFormat::Orc)]
        format: DatasetFormat,
        #[arg(long, default_value = "*")]
        allowed_node_types: String,
        /// Size (in bytes) of each thread's buffer in memory before it sorts and
        /// flushes it to disk.
        #[arg(long, default_value = "5000000")]
        buffer_size: usize,
        dataset_dir: PathBuf,
        target_dir: PathBuf,
    },

    /// Reads the list of nodes from the generated unique SWHIDS and counts the number
    /// of nodes of each type
    NodeStats {
        #[arg(value_enum, long, default_value_t = DatasetFormat::Orc)]
        format: DatasetFormat,
        #[arg(long)]
        swhids_dir: PathBuf,
        target_stats: PathBuf,
        target_count: PathBuf,
    },
    /// Reads the list of arcs from the ORC directory and counts the number of arcs
    /// of each type
    EdgeStats {
        #[arg(value_enum, long, default_value_t = DatasetFormat::Orc)]
        format: DatasetFormat,
        #[arg(long, default_value = "*")]
        allowed_node_types: String,
        dataset_dir: PathBuf,
        target_stats: PathBuf,
        target_count: PathBuf,
    },

    /// Reads the list of unique SWHIDs from a directory of .zstd files
    /// and produces a Minimal Perfect Hash function
    MphSwhids {
        swhids_dir: PathBuf,
        out_mph: PathBuf,
    },
    /// Reads the list of unique persons from a directory of .zstd files
    /// and produces a Minimal Perfect Hash function
    MphPersons {
        persons_dir: PathBuf,
        out_mph: PathBuf,
    },

    /// Reads a graph file linearly and produce a .offsets file which can be used
    /// by the Java backend to randomly access the graph.
    BuildOffsets {
        graph: PathBuf,
        target: PathBuf,
    },

    /// First actual compression step
    Bv {
        #[arg(value_enum, long, default_value_t = DatasetFormat::Orc)]
        format: DatasetFormat,
        #[arg(long, default_value_t = 100_000_000)]
        sort_batch_size: usize,
        #[arg(long, default_value = "*")]
        allowed_node_types: String,
        #[arg(value_enum, long, default_value_t = MphAlgorithm::Fmph)]
        mph_algo: MphAlgorithm,
        #[arg(long)]
        function: PathBuf,
        #[arg(long)]
        num_nodes: usize,
        dataset_dir: PathBuf,
        target_dir: PathBuf,
    },

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
        #[arg(long, default_value_t = 100_000_000)]
        sort_batch_size: usize,
        #[arg(long)]
        permutation: Vec<PathBuf>,
        graph_dir: PathBuf,
        target_dir: PathBuf,
    },
    /// Produces a new graph by inverting the direction of all arcs in the source graph
    Transpose {
        #[arg(long, default_value_t = 102400)]
        input_batch_size: usize,
        #[arg(long, default_value_t = 100_000_000)]
        sort_batch_size: usize,
        graph_dir: PathBuf,
        target_dir: PathBuf,
    },
    /// Combines a graph and its transposed graph into a single symmetric graph
    Simplify {
        #[arg(long, default_value_t = 102400)]
        input_batch_size: usize,
        #[arg(long, default_value_t = 100_000_000)]
        sort_batch_size: usize,
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
        #[arg(long, default_value_t = 800_000_000)]
        /// On the 2022-12-07 dataset, --sort-batch-size 800000000 produces
        /// ~1k files, ~2.8GB each; and uses 2TB of RAM.
        sort_batch_size: usize,
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

    /// Reads the list of nodes from the ORC directory and writes properties of each
    /// node to dedicated files
    NodeProperties {
        #[arg(value_enum, long, default_value_t = DatasetFormat::Orc)]
        format: DatasetFormat,
        #[arg(long, default_value = "*")]
        allowed_node_types: String,
        #[arg(value_enum, long, default_value_t = MphAlgorithm::Fmph)]
        mph_algo: MphAlgorithm,
        #[arg(long)]
        function: PathBuf,
        #[arg(long)]
        person_function: Option<PathBuf>,
        #[arg(long)]
        order: PathBuf,
        #[arg(long)]
        num_nodes: usize,
        dataset_dir: PathBuf,
        target: PathBuf,
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

#[derive(Copy, Clone, Debug, ValueEnum)]
enum DatasetFormat {
    Orc,
}

fn parse_allowed_node_types(s: &str) -> Result<Vec<SWHType>> {
    if s == "*" {
        return Ok(SWHType::all());
    } else {
        let mut types = Vec::new();
        for type_ in s.split(",") {
            types.push(
                type_
                    .try_into()
                    .context("Could not parse --allowed-node-types")?,
            );
        }
        Ok(types)
    }
}

pub fn main() -> Result<()> {
    let args = Args::parse();

    stderrlog::new()
        .verbosity(2)
        .timestamp(stderrlog::Timestamp::Second)
        .init()
        .with_context(|| "While Initializing the stderrlog")?;

    match args.command {
        Commands::ExtractNodes {
            format: DatasetFormat::Orc,
            allowed_node_types,
            buffer_size,
            dataset_dir,
            target_dir,
        } => {
            use swh_graph::utils::sort::Sortable;
            let allowed_node_types = parse_allowed_node_types(&allowed_node_types)?;

            let expected_node_count =
                swh_graph::compress::orc::estimate_node_count(&dataset_dir, &allowed_node_types)
                    .context("Could not estimate node count")? as usize;
            let expected_edge_count =
                swh_graph::compress::orc::estimate_edge_count(&dataset_dir, &allowed_node_types)
                    .context("Could not estimate edge count")? as usize;

            let mut pl = ProgressLogger::default().display_memory();
            pl.item_name = "arc";
            pl.local_speed = true;
            pl.expected_updates = Some(expected_node_count + expected_edge_count);
            pl.start("Extracting and sorting SWHIDs");

            swh_graph::compress::orc::iter_swhids(&dataset_dir, &allowed_node_types)
                .context("Could not read nodes from input dataset")?
                .unique_sort_to_dir(
                    target_dir,
                    "swhids.txt",
                    &temp_dir(),
                    pl,
                    &[],
                    buffer_size,
                    expected_node_count,
                )
                .context("Sorting failed")?;
        }
        Commands::ExtractLabels {
            format: DatasetFormat::Orc,
            allowed_node_types,
            buffer_size,
            dataset_dir,
            target_dir,
        } => {
            use swh_graph::utils::sort::Sortable;
            let allowed_node_types = parse_allowed_node_types(&allowed_node_types)?;

            let expected_edge_count =
                swh_graph::compress::orc::estimate_edge_count(&dataset_dir, &allowed_node_types)
                    .context("Could not estimate edge count")? as usize;

            let mut pl = ProgressLogger::default().display_memory();
            pl.item_name = "arc";
            pl.local_speed = true;
            pl.expected_updates = Some(expected_edge_count);
            pl.start("Extracting and sorting labels");

            let base64 = base64_simd::STANDARD;

            swh_graph::compress::orc::iter_labels(&dataset_dir, &allowed_node_types)
                .context("Could not read labels from input dataset")?
                .map(|label| base64.encode_to_string(label).into_bytes())
                .unique_sort_to_dir(
                    target_dir,
                    "labels.csv",
                    &temp_dir(),
                    pl,
                    &[],
                    buffer_size,
                    expected_edge_count / 131, // approximation, based on 2023-09-06 graph
                )
                .context("Sorting failed")?;
        }
        Commands::ExtractPersons {
            format: DatasetFormat::Orc,
            allowed_node_types,
            buffer_size,
            dataset_dir,
            target_dir,
        } => {
            use swh_graph::utils::sort::Sortable;
            let allowed_node_types = parse_allowed_node_types(&allowed_node_types)?;

            let expected_node_count = swh_graph::compress::orc::estimate_node_count(
                &dataset_dir,
                &allowed_node_types
                    .iter()
                    .cloned()
                    .filter(|t| [SWHType::Revision, SWHType::Release].contains(t))
                    .collect::<Vec<_>>(),
            )
            .context("Could not estimate node count from input dataset")?
                as usize;

            let mut pl = ProgressLogger::default().display_memory();
            pl.item_name = "node";
            pl.local_speed = true;
            pl.expected_updates = Some(expected_node_count);
            pl.start("Extracting and sorting labels");

            let base64 = base64_simd::STANDARD;

            swh_graph::compress::orc::iter_persons(&dataset_dir, &allowed_node_types)
                .context("Could not read persons from input dataset")?
                .map(|label| base64.encode_to_string(label).into_bytes())
                .unique_sort_to_dir(
                    target_dir,
                    "persons.csv",
                    &temp_dir(),
                    pl,
                    &[],
                    buffer_size,
                    expected_node_count / 56, // approximation, based on 2023-09-06 graph
                )
                .context("Sorting failed")?;
        }
        Commands::NodeStats {
            format: DatasetFormat::Orc,
            swhids_dir,
            target_stats,
            target_count,
        } => {
            use swh_graph::compress::zst_dir::*;

            let mut stats_file = File::create(&target_stats)
                .with_context(|| format!("Could not open {}", target_stats.display()))?;
            let mut count_file = File::create(&target_count)
                .with_context(|| format!("Could not open {}", target_count.display()))?;

            let mut pl = ProgressLogger::default().display_memory();
            pl.item_name = "node";
            pl.local_speed = true;
            let bits_per_line = 20; // A little more than this, actually
            pl.expected_updates = Some(swh_graph::utils::dir_size(&swhids_dir)? / bits_per_line);
            pl.start("Computing node stats");

            let stats = par_iter_lines_from_dir(&swhids_dir, Arc::new(Mutex::new(pl)))
                .map(|line: [u8; 50]| {
                    let ty = SWHType::try_from(&line[6..9]).expect("Unexpected SWHID type");
                    let mut stats = [0usize; SWHType::NUMBER_OF_TYPES];
                    stats[ty as usize] += 1;
                    stats
                })
                .reduce(Default::default, |mut left_1d, right_1d| {
                    for (left, right) in left_1d.iter_mut().zip(right_1d.into_iter()) {
                        *left += right;
                    }
                    left_1d
                });

            let mut stats_lines = Vec::new();
            let mut total = 0;
            for ty in SWHType::all() {
                stats_lines.push(format!("{} {}\n", ty, stats[ty as usize]));
                total += stats[ty as usize];
            }
            stats_lines.sort();

            stats_file
                .write_all(&stats_lines.join("").as_bytes())
                .context("Could not write node stats")?;
            count_file
                .write_all(&format!("{}\n", total).as_bytes())
                .context("Could not write node count")?;
        }
        Commands::EdgeStats {
            format: DatasetFormat::Orc,
            allowed_node_types,
            dataset_dir,
            target_stats,
            target_count,
        } => {
            let allowed_node_types = parse_allowed_node_types(&allowed_node_types)?;

            let mut stats_file = File::create(&target_stats)
                .with_context(|| format!("Could not open {}", target_stats.display()))?;
            let mut count_file = File::create(&target_count)
                .with_context(|| format!("Could not open {}", target_count.display()))?;

            let mut pl = ProgressLogger::default().display_memory();
            pl.item_name = "arc";
            pl.local_speed = true;
            pl.expected_updates = Some(
                swh_graph::compress::orc::estimate_edge_count(&dataset_dir, &allowed_node_types)
                    .context("Could not estimate edge count from input dataset")?
                    as usize,
            );
            pl.start("Computing edge stats");
            let pl = Mutex::new(pl);

            let stats =
                swh_graph::compress::orc::count_edge_types(&dataset_dir, &allowed_node_types)
                    .context("Could not read edges from input dataset")?
                    .map(|stats_2d| {
                        pl.lock().unwrap().update_with_count(
                            stats_2d.map(|stats_1d| stats_1d.iter().sum()).iter().sum(),
                        );
                        stats_2d
                    })
                    .reduce(Default::default, |mut left_2d, right_2d| {
                        for (left_1d, right_1d) in left_2d.iter_mut().zip(right_2d.into_iter()) {
                            for (left, right) in left_1d.into_iter().zip(right_1d.into_iter()) {
                                *left += right;
                            }
                        }
                        left_2d
                    });

            let mut stats_lines = Vec::new();
            let mut total = 0;
            for src_type in SWHType::all() {
                for dst_type in SWHType::all() {
                    let count = stats[src_type as usize][dst_type as usize];
                    if count != 0 {
                        stats_lines.push(format!("{}:{} {}\n", src_type, dst_type, count));
                        total += count;
                    }
                }
            }
            stats_lines.sort();

            stats_file
                .write_all(&stats_lines.join("").as_bytes())
                .context("Could not write edge stats")?;
            count_file
                .write_all(&format!("{}\n", total).as_bytes())
                .context("Could not write edge count")?;
        }

        Commands::MphSwhids {
            swhids_dir,
            out_mph,
        } => {
            swh_graph::compress::mph::build_mph::<[u8; 50]>(swhids_dir, out_mph, "SWHID")?;
        }

        Commands::MphPersons {
            persons_dir,
            out_mph,
        } => {
            swh_graph::compress::mph::build_mph::<Vec<u8>>(persons_dir, out_mph, "person")?;
        }

        Commands::BuildOffsets { graph, target } => {
            // Adapted from https://github.com/vigna/webgraph-rs/blob/90704d6f7e77b22796757144af6fef89109f33a1/src/bin/build_offsets.rs
            use dsi_bitstream::prelude::*;
            use dsi_progress_logger::*;
            use webgraph::prelude::*;

            // Create the sequential iterator over the graph
            let seq_graph = webgraph::graph::bvgraph::load_seq(&graph)?;
            let seq_graph =
                seq_graph.map_codes_reader_builder(DynamicCodesReaderSkipperBuilder::from);
            // Create the offsets file
            let file = std::fs::File::create(&target)
                .with_context(|| format!("Could not create {}", target.display()))?;
            // create a bit writer on the file
            let mut writer = <BufBitWriter<BE, _>>::new(<WordAdapter<u64, _>>::new(
                BufWriter::with_capacity(1 << 20, file),
            ));
            // progress bar
            let mut pl = ProgressLogger::default().display_memory();
            pl.item_name = "offset";
            pl.expected_updates = Some(seq_graph.num_nodes());
            pl.start("Computing offsets...");
            // read the graph a write the offsets
            let mut offset = 0;
            let mut degs_iter = seq_graph.iter_degrees();
            for (new_offset, _node_id, _degree) in &mut degs_iter {
                // write where
                writer
                    .write_gamma((new_offset - offset) as _)
                    .context("Could not write gamma")?;
                offset = new_offset;
                // decode the next nodes so we know where the next node_id starts
                pl.light_update();
            }
            // write the last offset, this is done to avoid decoding the last node
            writer
                .write_gamma((degs_iter.get_pos() - offset) as _)
                .context("Could not write final gamma")?;
            pl.light_update();
            pl.done();
        }

        Commands::Bv {
            format: DatasetFormat::Orc,
            sort_batch_size,
            allowed_node_types,
            mph_algo,
            function,
            num_nodes,
            dataset_dir,
            target_dir,
        } => {
            let allowed_node_types = parse_allowed_node_types(&allowed_node_types)?;

            match mph_algo {
                MphAlgorithm::Fmph => swh_graph::compress::bv::bv::<ph::fmph::Function>(
                    sort_batch_size,
                    function,
                    num_nodes,
                    dataset_dir,
                    &allowed_node_types,
                    target_dir,
                )?,
                MphAlgorithm::Cmph => {
                    swh_graph::compress::bv::bv::<swh_graph::java_compat::mph::gov::GOVMPH>(
                        sort_batch_size,
                        function,
                        num_nodes,
                        dataset_dir,
                        &allowed_node_types,
                        target_dir,
                    )?
                }
            }
        }

        Commands::Bfs {
            graph_dir,
            target_order,
        } => {
            let mut permut_file = File::create(&target_order)
                .with_context(|| format!("Could not open {}", target_order.display()))?;

            println!("Loading graph");
            let graph = webgraph::graph::bvgraph::load(graph_dir)?;
            println!("Graph loaded");

            swh_graph::approximate_bfs::almost_bfs_order(&graph)
                .dump(&mut permut_file)
                .context("Could not write permutation")?;
        }
        Commands::Permute {
            sort_batch_size,
            input_batch_size,
            graph_dir,
            permutation,
            target_dir,
        } => {
            use swh_graph::compress::transform::transform;

            let graph = webgraph::graph::bvgraph::load(&graph_dir)?;
            let num_nodes = graph.num_nodes();

            log::info!("Loading permutation...");
            let mut permutations_iter = permutation.into_iter();
            let permutation_path = permutations_iter.next().expect("No permutation provided");
            let mut permutation = OwnedPermutation::load(num_nodes, permutation_path.as_path())
                .with_context(|| format!("Could not load {}", permutation_path.display()))?;
            for next_permutation_path in permutations_iter {
                let next_permutation =
                    MappedPermutation::load(num_nodes, next_permutation_path.as_path())
                        .with_context(|| {
                            format!("Could not load {}", next_permutation_path.display())
                        })?;
                permutation
                    .compose_in_place(next_permutation)
                    .with_context(|| {
                        format!("Could not apply {}", next_permutation_path.display())
                    })?;
            }

            log::info!("Permuting...");
            transform(
                sort_batch_size,
                input_batch_size,
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
            graph_dir,
            target_dir,
        } => {
            use swh_graph::compress::transform::transform;

            let graph = webgraph::graph::bvgraph::load(&graph_dir)?;

            log::info!("Transposing...");
            transform(
                sort_batch_size,
                input_batch_size,
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
            graph_dir,
            permutation,
            target_dir,
        } => {
            use swh_graph::compress::transform::transform;

            let graph = webgraph::graph::bvgraph::load(&graph_dir)?;
            let num_nodes = graph.num_nodes();

            log::info!("Loading permutation...");
            let permutation = OwnedPermutation::load(num_nodes, permutation.as_path())?;

            log::info!("Permuting, transposing, and symmetrizing...");
            transform(
                input_batch_size,
                sort_batch_size,
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

            println!("Loading permutation");
            let order = MappedPermutation::load(num_nodes, order.as_path())
                .with_context(|| format!("Could not load {}", order.display()))?;
            match mph_algo {
                MphAlgorithm::Fmph => {}
                _ => unimplemented!("Only --mph-algo fmph is supported"),
            }
            println!("Permutation loaded, reading MPH");
            let mph = fmph::Function::load(function).context("Cannot load mph")?;
            println!("MPH loaded, sorting arcs");

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
                        .get(mph.hash_str_array(&line).expect("Failed to hash line") as usize)
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
                            .offset(node_id as isize)
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

        Commands::NodeProperties {
            format: DatasetFormat::Orc,
            allowed_node_types,
            mph_algo,
            function,
            person_function,
            order,
            num_nodes,
            dataset_dir,
            target,
        } => {
            use log::info;
            use swh_graph::compress::properties::*;
            use swh_graph::mph::SwhidMphf;

            let allowed_node_types = parse_allowed_node_types(&allowed_node_types)?;

            println!("Loading permutation");
            let order = unsafe { MappedPermutation::load_unchecked(order.as_path()) }
                .with_context(|| format!("Could not load {}", order.display()))?;
            assert_eq!(order.len(), num_nodes);
            println!("Permutation loaded, reading MPH");

            fn f<MPHF: SwhidMphf + Sync>(property_writer: PropertyWriter<MPHF>) -> Result<()> {
                println!("MPH loaded, writing properties");
                info!("[ 0/ 8] author timestamps");
                property_writer
                    .write_author_timestamps()
                    .context("Failed to write author timestamps")?;
                info!("[ 1/ 8] committer timestamps");
                property_writer
                    .write_committer_timestamps()
                    .context("Failed to write committer timestamps")?;
                info!("[ 2/ 8] content lengths");
                property_writer
                    .write_content_lengths()
                    .context("Failed to write content lengths")?;
                info!("[ 3/ 8] content_is_skipped");
                property_writer
                    .write_content_is_skipped()
                    .context("Failed to write content_is_skipped")?;
                info!("[ 4/ 8] author ids");
                property_writer
                    .write_author_ids()
                    .context("Failed to write author ids")?;
                info!("[ 5/ 8] committer ids");
                property_writer
                    .write_committer_ids()
                    .context("Failed to write committer ids")?;
                info!("[ 6/ 8] messages");
                property_writer
                    .write_messages()
                    .context("Failed to write messages")?;
                info!("[ 7/ 8] tag names");
                property_writer
                    .write_tag_names()
                    .context("Failed to write tag names")?;
                info!("[ 8/ 8] done");

                Ok(())
            }

            let person_mph = if allowed_node_types.contains(&SWHType::Revision)
                || allowed_node_types.contains(&SWHType::Release)
            {
                let Some(person_function) = person_function else {
                    bail!("--person-function must be provided unless --allowed-node-types is set to contain neither 'rev' nor 'rel'.");
                };
                Some(
                    ph::fmph::Function::load(&person_function)
                        .with_context(|| format!("Could not load {}", person_function.display()))?,
                )
            } else {
                None
            };

            match mph_algo {
                MphAlgorithm::Fmph => {
                    let swhid_mph = SwhidMphf::load(function).context("Cannot load mph")?;
                    let property_writer = PropertyWriter {
                        swhid_mph,
                        person_mph,
                        order,
                        num_nodes,
                        dataset_dir,
                        allowed_node_types,
                        target,
                    };
                    f::<fmph::Function>(property_writer)?;
                }
                MphAlgorithm::Cmph => {
                    let swhid_mph = SwhidMphf::load(function).context("Cannot load mph")?;
                    let property_writer = PropertyWriter {
                        swhid_mph,
                        person_mph,
                        order,
                        num_nodes,
                        dataset_dir,
                        allowed_node_types,
                        target,
                    };
                    f::<swh_graph::java_compat::mph::gov::GOVMPH>(property_writer)?;
                }
            };
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
                println!("Feeding {}", line);
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

                println!("{}", mph.get(&swhid).context("Could not hash swhid")?);
            }
        }
    }

    Ok(())
}
