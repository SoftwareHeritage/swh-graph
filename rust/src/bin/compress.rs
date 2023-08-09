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
                .unique_sort_to_dir(target_dir, "swhids.txt", &args.temp_dir, pl)
                .expect("Sorting failed");
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

        Commands::Mph {
            swhids_dir,
            out_mph,
        } => {
            use swh_graph::compress::list_swhids::*;

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

            let get_key_iter = || iter_swhids_from_dir(&swhids_dir, get_pl(false));
            let get_par_key_iter = || par_iter_swhids_from_dir(&swhids_dir, get_pl(true));

            *len.lock().unwrap() = Some(get_par_key_iter().count());

            let keys = fmph::keyset::CachedKeySet::<[u8; 50], _>::dynamic(
                GetParallelSwhidIterator {
                    len: len.lock().unwrap().unwrap(),
                    get_key_iter: &get_key_iter,
                    get_par_key_iter: &get_par_key_iter,
                },
                clone_threshold,
            );
            //let keys = fmph::keyset::CachedKeySet::dynamic(&get_key_iter, clone_threshold);
            let mph = fmph::Function::with_conf(keys, conf);

            let mut file =
                File::create(&out_mph).expect(&format!("Cannot create {}", out_mph.display()));
            mph.write(&mut file).context("Could not write MPH file")?;
        }
        Commands::HashSwhid { hash, mph } => {
            let mut file =
                File::open(&mph).with_context(|| format!("Cannot read {}", mph.display()))?;
            let mph = fmph::Function::read(&mut file).context("Count not parse mph")?;
            let mut swhid = SWHID {
                namespace_version: 1,
                node_type: SWHType::Content,
                hash: Default::default(),
            };
            hex_decode(hash.as_bytes(), &mut swhid.hash).context("Could not decode swhid")?;

            println!("{}", mph.get(&swhid).context("Could not hash swhid")?);
        }
    }

    Ok(())
}
