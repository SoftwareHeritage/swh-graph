/*
 * Copyright (C) 2023  The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

use std::env::temp_dir;
use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;

use anyhow::{Context, Result};
use byteorder::{BigEndian, ByteOrder};
use clap::{Parser, Subcommand, ValueEnum};
use dsi_progress_logger::ProgressLogger;
use ph::fmph;
use rayon::prelude::*;
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
        allowed_nodes_types: String,
        dataset_dir: PathBuf,
        target_dir: PathBuf,
    },
    /// Reads the list of nodes and arcs from the ORC directory and produces lists of
    /// unique labels in the given directory
    ExtractLabels {
        #[arg(value_enum, long, default_value_t = DatasetFormat::Orc)]
        format: DatasetFormat,
        #[arg(long, default_value = "*")]
        allowed_nodes_types: String,
        dataset_dir: PathBuf,
        target_dir: PathBuf,
    },
    /// Reads the list of nodes from the generated unique SWHIDS and counts the number
    /// of nodes of each type
    NodeStats {
        #[arg(value_enum, long, default_value_t = DatasetFormat::Orc)]
        format: DatasetFormat,
        #[arg(long, default_value = "*")]
        allowed_nodes_types: String,
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
        allowed_nodes_types: String,
        dataset_dir: PathBuf,
        target_stats: PathBuf,
        target_count: PathBuf,
    },
    /// Reads the list of unique SWHIDs from the ORC directory and produces a Minimal Perfect Hash function
    Mph {
        swhids_dir: PathBuf,
        out_mph: PathBuf,
    },
    /// First actual compression step
    Bv {
        #[arg(value_enum, long, default_value_t = DatasetFormat::Orc)]
        format: DatasetFormat,
        #[arg(long, default_value = "*")]
        allowed_nodes_types: String,
        #[arg(long)]
        mph: PathBuf,
        #[arg(long)]
        num_nodes: usize,
        dataset_dir: PathBuf,
        target_dir: PathBuf,
    },
    /// Computes the .offsets and .ef files for the given BVGraph
    BvOffsets {
        graph_dir: PathBuf,
        ef_path: PathBuf,
    },
    /// Runs a BFS on the initial BVGraph to group similar node ids together
    Bfs {
        graph_dir: PathBuf,
        target_order: PathBuf,
    },

    HashSwhids {
        mph: PathBuf,
        swhids: Vec<String>,
    },
}

#[derive(Copy, Clone, Debug, ValueEnum)]
enum DatasetFormat {
    Orc,
}

fn parse_allowed_node_types(s: &str) -> Result<Vec<SWHType>> {
    if s == "*" {
        return Ok(SWHType::all());
    } else {
        unimplemented!("--allowed-node-types");
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
            allowed_nodes_types,
            dataset_dir,
            target_dir,
        } => {
            use swh_graph::utils::sort::Sortable;
            let _ = parse_allowed_node_types(&allowed_nodes_types);

            let mut pl = ProgressLogger::default().display_memory();
            pl.item_name = "arc";
            pl.local_speed = true;
            pl.expected_updates = Some(
                (swh_graph::compress::orc::estimate_node_count(&dataset_dir)
                    + swh_graph::compress::orc::estimate_edge_count(&dataset_dir))
                    as usize,
            );
            pl.start("Extracting and sorting SWHIDs");

            swh_graph::compress::orc::iter_swhids(&dataset_dir)
                .unique_sort_to_dir(target_dir, "swhids.txt", &temp_dir(), pl, &[])
                .context("Sorting failed")?;
        }
        Commands::ExtractLabels {
            format: DatasetFormat::Orc,
            allowed_nodes_types,
            dataset_dir,
            target_dir,
        } => {
            use swh_graph::utils::sort::Sortable;
            let _ = parse_allowed_node_types(&allowed_nodes_types);

            let mut pl = ProgressLogger::default().display_memory();
            pl.item_name = "arc";
            pl.local_speed = true;
            pl.expected_updates =
                Some(swh_graph::compress::orc::estimate_edge_count(&dataset_dir) as usize);
            pl.start("Extracting and sorting labels");

            let base64 = base64_simd::STANDARD;

            swh_graph::compress::orc::iter_labels(&dataset_dir)
                .map(|label| base64.encode_to_string(label).into_bytes())
                .unique_sort_to_dir(target_dir, "labels.txt", &temp_dir(), pl, &[])
                .context("Sorting failed")?;
        }
        Commands::NodeStats {
            format: DatasetFormat::Orc,
            allowed_nodes_types,
            swhids_dir,
            target_stats,
            target_count,
        } => {
            use swh_graph::compress::zst_dir::*;

            let _ = parse_allowed_node_types(&allowed_nodes_types);

            use std::io::Write;

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
                    let ty = match &line[6..9] {
                        b"cnt" => SWHType::Content,
                        b"dir" => SWHType::Directory,
                        b"rev" => SWHType::Revision,
                        b"rel" => SWHType::Release,
                        b"snp" => SWHType::Snapshot,
                        b"ori" => SWHType::Origin,
                        _ => panic!(
                            "Unexpected SWHID type: {}",
                            std::str::from_utf8(&line).unwrap_or(&format!("{:?}", line))
                        ),
                    };
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
            allowed_nodes_types,
            dataset_dir,
            target_stats,
            target_count,
        } => {
            let _ = parse_allowed_node_types(&allowed_nodes_types);

            use std::io::Write;

            let mut stats_file = File::create(&target_stats)
                .with_context(|| format!("Could not open {}", target_stats.display()))?;
            let mut count_file = File::create(&target_count)
                .with_context(|| format!("Could not open {}", target_count.display()))?;

            let mut pl = ProgressLogger::default().display_memory();
            pl.item_name = "arc";
            pl.local_speed = true;
            pl.expected_updates =
                Some(swh_graph::compress::orc::estimate_edge_count(&dataset_dir) as usize);
            pl.start("Computing edge stats");
            let pl = Mutex::new(pl);

            let stats = swh_graph::compress::orc::count_edge_types(&dataset_dir)
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

        Commands::Mph {
            swhids_dir,
            out_mph,
        } => {
            use swh_graph::compress::zst_dir::*;

            let clone_threshold = 10240; // TODO: tune this
            let conf = fmph::BuildConf::default();
            let call_counts = Mutex::new(0);
            let len = Mutex::new(None);

            let get_pl = |parallel| {
                let mut call_counts = call_counts.lock().unwrap();
                *call_counts += 1;
                let mut pl = ProgressLogger::default().display_memory();
                pl.item_name = "SWHID";
                pl.local_speed = true;
                pl.expected_updates = *len.lock().unwrap();
                pl.start(&format!(
                    "{} reading SWHIDs (pass {})",
                    if parallel {
                        "parallelly"
                    } else {
                        "sequentially"
                    },
                    call_counts
                ));
                Arc::new(Mutex::new(pl))
            };

            let get_key_iter = || iter_lines_from_dir(&swhids_dir, get_pl(false));
            let get_par_key_iter = || par_iter_lines_from_dir(&swhids_dir, get_pl(true));

            *len.lock().unwrap() = Some(get_par_key_iter().count());

            let keys = fmph::keyset::CachedKeySet::<[u8; 50], _>::dynamic(
                GetParallelLineIterator {
                    len: len.lock().unwrap().unwrap(),
                    get_key_iter: &get_key_iter,
                    get_par_key_iter: &get_par_key_iter,
                },
                clone_threshold,
            );
            //let keys = fmph::keyset::CachedKeySet::dynamic(&get_key_iter, clone_threshold);
            let mph = fmph::Function::with_conf(keys, conf);

            let mut file = File::create(&out_mph)
                .with_context(|| format!("Cannot create {}", out_mph.display()))?;
            mph.write(&mut file).context("Could not write MPH file")?;
        }
        Commands::Bv {
            format: DatasetFormat::Orc,
            allowed_nodes_types,
            mph,
            num_nodes,
            dataset_dir,
            target_dir,
        } => {
            use itertools::Itertools;
            use std::cell::UnsafeCell;
            use swh_graph::compress::orc::*;

            let _ = parse_allowed_node_types(&allowed_nodes_types);

            let file =
                File::open(&mph).with_context(|| format!("Cannot read {}", mph.display()))?;
            println!("Reading MPH");
            let mph = fmph::Function::read(&mut std::io::BufReader::new(file))
                .context("Could not parse mph")?;
            println!("MPH loaded, sorting arcs");

            let mut pl = ProgressLogger::default().display_memory();
            pl.item_name = "arc";
            pl.local_speed = true;
            pl.expected_updates =
                Some(swh_graph::compress::orc::estimate_edge_count(&dataset_dir) as usize);
            pl.start("Reading arcs");

            // Sort in parallel in a bunch of SortPairs instances
            let pl = Mutex::new(pl);
            let batch_size = 10_000_000; // SortPairs creates one file per batch
            let counters = thread_local::ThreadLocal::new();
            let sorters: Vec<Result<(u64, SortPairs<()>)>> = iter_arcs(&dataset_dir)
                .inspect(|_| {
                    // This is safe because only this thread accesses this and only from
                    // here.
                    let counter = counters.get_or(|| UnsafeCell::new(0));
                    let counter: &mut usize = unsafe { &mut *counter.get() };
                    *counter += 1;
                    if *counter % 32768 == 0 {
                        // Update but avoid lock contention at the expense
                        // of precision (counts at most 32768 too many at the
                        // end of each file)
                        pl.lock().unwrap().update_with_count(32768);
                        *counter = 0
                    }
                })
                .fold(
                    || {
                        use rand::Rng;
                        let sorter_id = rand::thread_rng().gen::<u64>();
                        let mut sorter_temp_dir = temp_dir();
                        sorter_temp_dir.push(format!("sort-arcs-{}", sorter_id));
                        std::fs::create_dir(&sorter_temp_dir).with_context(|| {
                            format!(
                                "Could not create temporary directory {}",
                                sorter_temp_dir.display()
                            )
                        })?;

                        Ok((
                            sorter_id,
                            SortPairs::new(batch_size, &sorter_temp_dir)
                                .context("Could not create SortPairs")?,
                        ))
                    },
                    |acc, (src, dst)| {
                        let (sorter_id, mut sorter) = acc?;
                        let src = mph.get(&src).with_context(|| {
                            format!(
                                "Could not hash {}",
                                std::str::from_utf8(&src).unwrap_or(&format!("{:?}", src))
                            )
                        })? as usize;
                        let dst = mph.get(&dst).with_context(|| {
                            format!(
                                "Could not hash {}",
                                std::str::from_utf8(&dst).unwrap_or(&format!("{:?}", dst))
                            )
                        })? as usize;
                        assert!(src < num_nodes, "src node id is greater than {}", num_nodes);
                        assert!(dst < num_nodes, "dst node id is greater than {}", num_nodes);
                        sorter
                            .push(src, dst, ())
                            .context("Could not push arc to sorter")?;
                        Ok((sorter_id, sorter))
                    },
                )
                .collect();
            pl.lock().unwrap().done();

            let mut sorted_arc_lists = Vec::new();
            for acc in sorters {
                let (_sorter_id, mut sorter) = acc?;
                sorted_arc_lists.push(sorter.iter().context("Could not get sorted arc lists")?);
            }

            // Merge sorted arc lists into a single sorted arc list
            let sorted_arcs = itertools::kmerge(sorted_arc_lists).dedup();

            let mut pl = ProgressLogger::default().display_memory();
            pl.item_name = "node";
            pl.local_speed = true;
            pl.expected_updates = Some(num_nodes);
            pl.start("Building BVGraph");
            let pl = Mutex::new(pl);
            let counters = thread_local::ThreadLocal::new();

            let sequential_graph = COOIterToLabelledGraph::new(num_nodes, sorted_arcs);
            let adjacency_lists = sequential_graph.iter_nodes().inspect(|_| {
                let counter = counters.get_or(|| UnsafeCell::new(0));
                let counter: &mut usize = unsafe { &mut *counter.get() };
                *counter += 1;
                if *counter % 32768 == 0 {
                    // Update but avoid lock contention at the expense
                    // of precision (counts at most 32768 too many at the
                    // end of each file)
                    pl.lock().unwrap().update_with_count(32768);
                    *counter = 0
                }
            });
            let comp_flags = Default::default();
            let num_threads = num_cpus::get();

            webgraph::graph::bvgraph::parallel_compress_sequential_iter(
                target_dir,
                adjacency_lists,
                comp_flags,
                num_threads,
            )
            .context("Could not build BVGraph from arcs")?;

            pl.lock().unwrap().done();
        }

        Commands::BvOffsets { graph_dir, ef_path } => {
            use std::io::BufReader;
            use std::io::{BufWriter, Seek};
            use sux::prelude::*;
            let graph_dir2 = graph_dir.to_string_lossy();
            // Adapted from https://github.com/vigna/webgraph-rs/blob/c43563de15e30d4ee1f7a4f0096f6479b81eb26a/src/bin/build_eliasfano.rs
            let f = File::open(format!("{}.properties", graph_dir2))
                .context("Could not open .properties")?;
            let map =
                java_properties::read(BufReader::new(f)).context("Could not parse .properties")?;
            let num_nodes = map
                .get("nodes")
                .unwrap()
                .parse::<u64>()
                .context("Could not get number of nodes")?;

            let mut file =
                File::open(format!("{}.graph", graph_dir2)).context("Could not open .graph")?;
            let file_len = 8 * file
                .seek(std::io::SeekFrom::End(0))
                .context("Could not seek through .graph")?;

            let mut efb = EliasFanoBuilder::new(file_len, num_nodes + 1);

            let mut ef_file = BufWriter::new(File::create(ef_path).context("Could not open .ef")?);

            let mut pl = ProgressLogger::default().display_memory();
            pl.expected_updates = Some(num_nodes as _);
            pl.item_name = "offset";

            let seq_graph =
                webgraph::graph::bvgraph::load_seq(&graph_dir).context("Could not load BVGraph")?;
            let seq_graph =
                seq_graph.map_codes_reader_builder(DynamicCodesReaderSkipperBuilder::from);
            pl.start("Building EliasFano...");
            // read the graph a write the offsets
            for (new_offset, _node_id, _degree) in seq_graph.iter_degrees() {
                // write where
                efb.push(new_offset as _)
                    .context("Could not push EF offset")?;
                // decode the next nodes so we know where the next node_id starts
                pl.light_update();
            }
            pl.done();

            let ef = efb.build();

            let mut pl = ProgressLogger::default().display_memory();
            pl.start("Building the Index over the ones in the high-bits...");
            let ef: EliasFano<SparseIndex<BitMap<_>, _, 8>, CompactArray<_>> =
                ef.convert_to().unwrap();
            pl.done();

            let mut pl = ProgressLogger::default().display_memory();
            pl.start("Writing to disk...");
            ef.serialize(&mut ef_file)?;
            pl.done();
        }

        Commands::Bfs {
            graph_dir,
            target_order,
        } => {
            use std::io::Write;

            let mut permut_file = File::create(&target_order)
                .with_context(|| format!("Could not open {}", target_order.display()))?;

            println!("Loading graph");
            let graph = webgraph::graph::bvgraph::load(graph_dir)?;
            println!("Graph loaded");

            let permutation = swh_graph::approximate_bfs::almost_bfs_order(&graph);

            let mut pl = ProgressLogger::default().display_memory();
            pl.item_name = "byte";
            pl.local_speed = true;
            pl.expected_updates = Some(graph.num_nodes() * 8);
            pl.start("Writing permutation");

            let chunk_size = 1_000_000; // 1M of u64 -> 8MB
            let mut buf = vec![0u8; chunk_size * 8];
            for chunk in permutation.chunks(chunk_size) {
                let buf_slice = &mut buf[..chunk.len() * 8]; // no-op except for the last chunk
                if usize::BITS == u64::BITS {
                    BigEndian::write_u64_into(unsafe { std::mem::transmute(chunk) }, buf_slice);
                } else if usize::BITS == u32::BITS {
                    BigEndian::write_u32_into(unsafe { std::mem::transmute(chunk) }, buf_slice);
                } else {
                    todo!("usize::BITS == {}", usize::BITS);
                }
                permut_file
                    .write_all(buf_slice)
                    .context("Could not write permutation")?;
                pl.update_with_count(chunk.len() * 8);
            }
            pl.done();
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
