/*
 * Copyright (C) 2023  The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

#[cfg(not(feature = "compression"))]
compile_error!("Feature 'compression' must be enabled for this executable to be available.");

use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand, ValueEnum};
use dsi_progress_logger::ProgressLogger;
use faster_hex::hex_decode;
use ph::fmph;
use rayon::prelude::*;
use swh_graph::{SWHType, SWHID};

#[derive(Parser, Debug)]
#[command(about = "Commands to run individual steps of the pipeline from ORC files to compressed graph", long_about = None)]
struct Args {
    #[arg(long)]
    temp_dir: PathBuf,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Reads the list of nodes and arcs from the ORC directory and produces lists of unique SWHIDs
    /// in the given directory
    ExtractNodes {
        #[arg(value_enum, long, default_value_t = DatasetFormat::Orc)]
        format: DatasetFormat,
        #[arg(long, default_value = "*")]
        allowed_nodes_types: String,
        dataset_dir: PathBuf,
        target_dir: PathBuf,
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
    HashSwhid {
        mph: PathBuf,
        hash: String,
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
            let _ = parse_allowed_node_types(&allowed_nodes_types);

            use std::cell::UnsafeCell;
            use std::io::Write;
            use std::process::Stdio;

            let pl = Arc::new(Mutex::new(ProgressLogger::default().display_memory()));
            {
                let mut pl = pl.lock().unwrap();
                pl.item_name = "SWHID";
                pl.local_speed = true;
                pl.expected_updates = Some(
                    (swh_graph::compress::orc::estimate_node_count(&dataset_dir)
                        + swh_graph::compress::orc::estimate_edge_count(&dataset_dir))
                        as usize,
                );
                pl.start("sorting SWHIDs (pass 1)");
            }

            let sorted_files = Mutex::new(Vec::new());
            let mut sorter_buffers = thread_local::ThreadLocal::new();
            let mut sorters = thread_local::ThreadLocal::new();

            let flush_buffer = |buffer: &mut Vec<u8>| {
                // Flush to the sorter
                let sort = sorters
                    .get_or(|| {
                        let file = tempfile::NamedTempFile::new_in(&args.temp_dir)
                            .expect("Could not open temporary sorted file");
                        let path: PathBuf = file.path().into();
                        sorted_files.lock().unwrap().push(file);

                        let sort = std::process::Command::new("sort")
                            .arg("--buffer-size=100M")
                            .arg("--compress-program=zstd")
                            .arg("--unique") // Removes duplicates early to save space
                            .arg("--parallel=1") // Slightly faster as we already max out the CPU
                            .env("TMPDIR", &args.temp_dir)
                            .env("LC_ALL", "C")
                            .stdout(std::fs::File::create(path).unwrap())
                            .stdin(std::process::Stdio::piped())
                            .spawn()
                            .expect("Could not start 'sort' process");
                        UnsafeCell::new(sort)
                    })
                    .get();
                // This is safe because the main thread won't access this until this
                // one ends, and other threads don't access it.
                let sort: &mut std::process::Child = unsafe { &mut *sort };

                let stdin = sort.stdin.as_mut().unwrap();
                stdin
                    .write_all(&buffer)
                    .expect("Could not write to sort's stdin");

                pl.lock().unwrap().update_with_count(buffer.len() / 51); // SWHID length + '\n' = 51

                buffer.clear();
            };

            swh_graph::compress::orc::iter_swhids(&dataset_dir).for_each(|swhid| {
                let buffer: &UnsafeCell<Vec<_>> =
                    sorter_buffers.get_or(|| UnsafeCell::new(Vec::<u8>::with_capacity(51_000_000)));
                // This is safe because the main thread won't access this until this
                // one ends, and other threads don't access it.
                let buffer: &mut Vec<_> = unsafe { &mut *buffer.get() };

                buffer.extend(swhid);
                buffer.push(b'\n');

                if buffer.len() >= 51_000_000 {
                    flush_buffer(buffer);
                }
            });

            // Write remaining buffers
            for buffer in sorter_buffers.iter_mut() {
                // This is safe because other threads ended
                let buffer = unsafe { &mut *buffer.get() };
                flush_buffer(buffer)
            }

            // Notify sorters they reached the end of their inputs
            for sorter in &mut sorters {
                // This is safe because other threads ended
                let sorter = unsafe { &mut *sorter.get() };
                drop(sorter.stdin.take().unwrap());
            }

            // Wait for sorters to finish
            for sorter in sorters {
                // This is safe because other threads ended
                let sorter = unsafe { &mut *sorter.get() };
                sorter.wait().expect("Sorter crashed");
            }

            pl.lock().unwrap().done();

            let sorted_files = sorted_files.lock().unwrap();

            assert!(sorted_files.len() > 0, "Sorters did not run");

            let mut unique_swhids_prefix = target_dir.clone();
            unique_swhids_prefix.push("swhids.txt.");

            if target_dir.exists() {
                std::fs::remove_dir(&target_dir).unwrap_or_else(|e| {
                    panic!(
                        "Could not delete directory {}: {:?}",
                        target_dir.display(),
                        e
                    )
                });
            }
            std::fs::create_dir(&target_dir).unwrap_or_else(|e| {
                panic!(
                    "Could not create directory {}: {:?}",
                    target_dir.display(),
                    e
                )
            });

            let mut merge = std::process::Command::new("sort")
                .arg("--buffer-size=100M")
                .arg("--compress-program=zstd")
                .env("TMPDIR", &args.temp_dir)
                .env("LC_ALL", "C")
                .arg("--merge")
                .arg("--unique")
                .args(sorted_files.iter().map(|file| file.path()))
                .stdin(Stdio::null())
                .stdout(Stdio::piped())
                .spawn()
                .expect("Could not start merging 'sort' process");
            let merge_out = merge.stdout.take().unwrap();

            let mut split = std::process::Command::new("split")
                .arg("--lines=100000000") // 100M
                .arg("--suffix-length=6")
                .arg("--numeric-suffixes")
                .arg("--filter=zstdmt > $FILE")
                .arg("--additional-suffix=.zst")
                .arg("-")
                .arg(&unique_swhids_prefix)
                .stdin(Stdio::from(merge_out))
                .spawn()
                .expect("Could not start zstdmt");

            merge.wait().expect("merger crashed");
            split.wait().expect("split/zstdmt crashed");
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
                .unwrap_or_else(|e| panic!("Could not open {}: {:?}", target_stats.display(), e));
            let mut count_file = File::create(&target_count)
                .unwrap_or_else(|e| panic!("Could not open {}: {:?}", target_count.display(), e));

            let pl = Arc::new(Mutex::new(ProgressLogger::default().display_memory()));
            {
                let mut pl = pl.lock().unwrap();
                pl.item_name = "arc";
                pl.local_speed = true;
                pl.expected_updates = Some(
                    (swh_graph::compress::orc::estimate_node_count(&dataset_dir)
                        + swh_graph::compress::orc::estimate_edge_count(&dataset_dir))
                        as usize,
                );
                pl.start("Computing edge stats");
            }

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
                .expect("Could not write edge stats");
            count_file
                .write_all(&format!("{}\n", total).as_bytes())
                .expect("Could not write edge count");
        }

        Commands::Mph { .. } => {

            /*
            let conf = fmph::BuildConf::default();
            let clone_threshold = 10240; // TODO: tune this
            let keys = fmph::keyset::CachedKeySet::dynamic_par(
                MyGetParallelIterator {
                    len,
                    get_key_iter: &get_key_iter,
                    get_par_key_iter: &get_par_key_iter,
                },
                clone_threshold,
            );
            let mph = fmph::Function::with_conf(keys, conf);

            let mut file =
                File::create(&out_mph).expect(&format!("Cannot create {}", out_mph.display()));
            mph.write(&mut file).unwrap();
            */
        }
        Commands::HashSwhid { hash, mph } => {
            let mut file = File::open(&mph)
                .unwrap_or_else(|e| panic!("Cannot read {}: {:?}", mph.display(), e));
            let mph = fmph::Function::read(&mut file).expect("Count not parse mph");
            let mut swhid = SWHID {
                namespace_version: 1,
                node_type: SWHType::Content,
                hash: Default::default(),
            };
            hex_decode(hash.as_bytes(), &mut swhid.hash).expect("Could not decode swhid");

            println!("{}", mph.get(&swhid).expect("Could not hash swhid"));
        }
    }

    Ok(())
}
