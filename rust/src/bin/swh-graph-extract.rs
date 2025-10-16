/*
 * Copyright (C) 2023-2024  The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::Mutex;

use anyhow::{anyhow, bail, Context, Result};
use clap::{Parser, Subcommand, ValueEnum};
use dsi_progress_logger::{concurrent_progress_logger, progress_logger, ProgressLog};
use itertools::Itertools;
use mimalloc::MiMalloc;
use rayon::prelude::*;

use swh_graph::map::{MappedPermutation, Permutation};
use swh_graph::mph::SwhidPthash;
use swh_graph::utils::parse_allowed_node_types;
use swh_graph::{NodeType, SWHID};

#[cfg_attr(not(miri), global_allocator)] // Miri does not support Mimalloc
static GLOBAL: MiMalloc = MiMalloc;

#[derive(Parser, Debug)]
#[command(about = "Commands to read ORC files and produce property files and an initial not-very-compressed BVGraph", long_about = None)]
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
        ///
        /// Higher values consume more memory during the sorting phase to save
        /// time during the merging phase.
        #[arg(long, default_value = "5000000000")]
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
        ///
        /// Higher values consume more memory during the sorting phase to save
        /// time during the merging phase.
        #[arg(long, default_value = "5000000000")]
        buffer_size: usize,
        dataset_dir: PathBuf,
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
        ///
        /// Higher values consume more memory during the sorting phase to save
        /// time during the merging phase.
        #[arg(long, default_value = "5000000000")]
        buffer_size: usize,
        dataset_dir: PathBuf,
    },
    /// Extracts the full names of authors and committers from the ORC tables containing them.
    ///
    /// All the full names are concatenated into a single byte string that is written to disk.
    /// Another file, that contains the length of each full name, is generated.
    ExtractFullnames {
        #[arg(value_enum, long, default_value_t = DatasetFormat::Orc)]
        format: DatasetFormat,
        #[arg(long)]
        person_function: PathBuf,
        dataset_dir: PathBuf,
        fullnames_path: PathBuf,
        lengths_path: PathBuf,
    },

    /// Reads the list of nodes from the generated unique SWHIDS and counts the number
    /// of nodes of each type
    NodeStats {
        #[arg(value_enum, long, default_value_t = DatasetFormat::Orc)]
        format: DatasetFormat,
        #[arg(long)]
        swhids_dir: PathBuf,
        #[arg(long)]
        target_stats: PathBuf,
        #[arg(long)]
        target_count: PathBuf,
    },
    /// Reads the list of arcs from the ORC directory and counts the number of arcs
    /// of each type
    EdgeStats {
        #[arg(value_enum, long, default_value_t = DatasetFormat::Orc)]
        format: DatasetFormat,
        #[arg(long, default_value = "*")]
        allowed_node_types: String,
        #[arg(long)]
        dataset_dir: PathBuf,
        #[arg(long)]
        target_stats: PathBuf,
        #[arg(long)]
        target_count: PathBuf,
    },

    /// Reads the list of origins and sorts it in a way that related origins are close
    /// to each other in the output order
    BfsRoots {
        #[arg(value_enum, long, default_value_t = DatasetFormat::Orc)]
        format: DatasetFormat,
        #[arg(long, default_value = "*")]
        allowed_node_types: String,
        dataset_dir: PathBuf,
        target: PathBuf,
    },

    /// Reads ORC files and produces a not-very-compressed BVGraph
    Bv {
        #[arg(value_enum, long, default_value_t = DatasetFormat::Orc)]
        format: DatasetFormat,
        #[arg(long, default_value_t = 100_000_000)]
        sort_batch_size: usize,
        #[arg(long, default_value_t = 1)]
        partitions_per_thread: usize,
        #[arg(long, default_value = "*")]
        allowed_node_types: String,
        #[arg(value_enum, long, default_value_t = MphAlgorithm::Pthash)]
        mph_algo: MphAlgorithm,
        #[arg(long)]
        function: PathBuf,
        #[arg(long)]
        num_nodes: usize,
        dataset_dir: PathBuf,
        target_dir: PathBuf,
    },
    /// Reads ORC files and produces a BVGraph with edge labels
    EdgeLabels {
        #[arg(value_enum, long, default_value_t = DatasetFormat::Orc)]
        format: DatasetFormat,
        #[arg(long, default_value_t = 100_000_000)]
        sort_batch_size: usize,
        #[arg(long, default_value_t = 1)]
        partitions_per_thread: usize,
        #[arg(long, default_value = "*")]
        allowed_node_types: String,
        #[arg(value_enum, long, default_value_t = MphAlgorithm::Pthash)]
        mph_algo: MphAlgorithm,
        #[arg(long)]
        function: PathBuf,
        #[arg(long)]
        order: PathBuf,
        #[arg(long)]
        label_name_mphf: PathBuf,
        #[arg(long)]
        label_name_order: PathBuf,
        #[arg(long)]
        num_nodes: usize,
        #[arg(long, action)]
        transposed: bool,
        dataset_dir: PathBuf,
        target_dir: PathBuf,
    },
    /// Reads the list of nodes from the ORC directory and writes properties of each
    /// node to dedicated files
    NodeProperties {
        #[arg(value_enum, long, default_value_t = DatasetFormat::Orc)]
        format: DatasetFormat,
        #[arg(long, default_value = "*")]
        allowed_node_types: String,
        #[arg(value_enum, long, default_value_t = MphAlgorithm::Pthash)]
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
}

#[derive(Copy, Clone, Debug, ValueEnum)]
enum MphAlgorithm {
    Pthash,
    Cmph,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
enum DatasetFormat {
    Orc,
}

pub fn main() -> Result<()> {
    let args = Args::parse();

    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    match args.command {
        Commands::ExtractNodes {
            format: DatasetFormat::Orc,
            allowed_node_types,
            buffer_size,
            dataset_dir,
            target_dir,
        } => {
            use std::str::FromStr;
            let allowed_node_types = parse_allowed_node_types(&allowed_node_types)?;

            let expected_node_count =
                swh_graph::compress::stats::estimate_node_count(&dataset_dir, &allowed_node_types)
                    .context("Could not estimate node count")? as usize;
            let expected_edge_count =
                swh_graph::compress::stats::estimate_edge_count(&dataset_dir, &allowed_node_types)
                    .context("Could not estimate edge count")? as usize;

            let mut pl = progress_logger!(
                display_memory = true,
                item_name = "arc",
                local_speed = true,
                expected_updates = Some(expected_node_count + expected_edge_count),
            );
            pl.start("Extracting and sorting SWHIDs");

            std::fs::create_dir(&target_dir)
                .with_context(|| format!("Could not create {}", target_dir.display()))?;

            // Write output in multiple files, so it can be processed in parallel
            // by consumers
            let mut shard_id = 0;
            let mut next_shard = || -> Result<_> {
                let path = target_dir.join(format!("nodes.txt.{shard_id:0>8}.zst"));
                shard_id += 1;
                let file = std::fs::File::create_new(&path)
                    .with_context(|| format!("Could not create {}", path.display()))?;
                let compression_level = 3;
                let zstd_encoder = zstd::stream::write::Encoder::new(file, compression_level)
                    .with_context(|| {
                        format!("Could not create ZSTD encoder for {}", path.display())
                    })?
                    .auto_finish();
                Ok(zstd_encoder)
            };
            let mut writer = next_shard()?;

            // Fetch as UTF-8 encoded arrays
            let swhids = swh_graph::compress::iter_swhids(&dataset_dir, &allowed_node_types)
                .context("Could not read nodes from input dataset")?;
            // Parse (TODO: avoid UTF-8 decoding data we generated ourselves, here)
            let swhids = swhids.map(|swhid| {
                SWHID::from_str(
                    &String::from_utf8(swhid.into_iter().collect()).expect("Non-ASCII SWHID"),
                )
                .expect("Could not parse SWHID")
            });
            // Sort and deduplicate
            let swhids = swh_graph::utils::sort::par_sort_swhids(swhids, pl, buffer_size)
                .context("Could not sort SWHIDs")?;
            // Write
            for (i, swhid) in swhids.enumerate() {
                if i % 100_000_000 == 99_999_999 {
                    writer = next_shard()?;
                }
                writer
                    .write_all(format!("{swhid}\n").as_bytes())
                    .context("Could not write SWHID")?;
            }
        }
        Commands::ExtractLabels {
            format: DatasetFormat::Orc,
            allowed_node_types,
            buffer_size,
            dataset_dir,
        } => {
            let allowed_node_types = parse_allowed_node_types(&allowed_node_types)?;

            let expected_edge_count =
                swh_graph::compress::stats::estimate_edge_count(&dataset_dir, &allowed_node_types)
                    .context("Could not estimate edge count")? as usize;

            let mut pl = progress_logger!(
                display_memory = true,
                item_name = "arc",
                local_speed = true,
                expected_updates = Some(expected_edge_count),
            );
            pl.start("Extracting and sorting labels");

            // Fetch labels data
            let labels = swh_graph::compress::iter_labels(&dataset_dir, &allowed_node_types)
                .context("Could not read labels from input dataset")?;
            // Sort and deduplicate
            let labels = swh_graph::utils::sort::par_sort_strings(labels, pl, buffer_size)
                .context("Could not sort labels")?;
            // Encode as base64
            let base64 = base64_simd::STANDARD;
            let labels = labels.map(|label| {
                base64
                    .encode_to_string(label)
                    .into_bytes()
                    .into_boxed_slice()
            });
            // Write
            for label in labels {
                println!(
                    "{}",
                    String::from_utf8(label.into()).expect("Could not decode line")
                );
            }
        }
        Commands::ExtractPersons {
            format: DatasetFormat::Orc,
            allowed_node_types,
            buffer_size,
            dataset_dir,
        } => {
            let allowed_node_types = parse_allowed_node_types(&allowed_node_types)?;

            let expected_node_count = swh_graph::compress::stats::estimate_node_count(
                &dataset_dir,
                &allowed_node_types
                    .iter()
                    .cloned()
                    .filter(|t| [NodeType::Revision, NodeType::Release].contains(t))
                    .collect::<Vec<_>>(),
            )
            .context("Could not estimate node count from input dataset")?
                as usize;

            let mut pl = progress_logger!(
                display_memory = true,
                item_name = "node",
                local_speed = true,
                expected_updates = Some(expected_node_count),
            );
            pl.start("Extracting and sorting persons");

            let persons = swh_graph::compress::iter_persons(&dataset_dir, &allowed_node_types)
                .context("Could not read persons from input dataset")?;
            // Sort and deduplicate
            let persons = swh_graph::utils::sort::par_sort_strings(persons, pl, buffer_size)
                .context("Could not sort persons")?;
            // Encode as base64
            let base64 = base64_simd::STANDARD;
            let persons = persons.map(|person| {
                base64
                    .encode_to_string(person)
                    .into_bytes()
                    .into_boxed_slice()
            });
            // Write
            for person in persons {
                println!(
                    "{}",
                    String::from_utf8(person.into()).expect("Could not decode line")
                );
            }
        }
        Commands::ExtractFullnames {
            format: DatasetFormat::Orc,
            person_function,
            dataset_dir,
            fullnames_path,
            lengths_path,
        } => {
            use dsi_bitstream::prelude::*;
            use pthash::Phf;
            use std::sync::OnceLock;

            let person_mph = swh_graph::compress::persons::PersonMphf::load(&person_function)
                .with_context(|| format!("Could not load {}", person_function.display()))?;
            let num_persons = usize::try_from(person_mph.num_keys())?;
            let person_mph = swh_graph::compress::persons::PersonHasher::new(&person_mph);

            let mut pl = concurrent_progress_logger!(display_memory = true, local_speed = true);
            pl.start("Extracting and sorting full names");

            let fullnames: Vec<OnceLock<Box<[u8]>>> = vec![OnceLock::new(); num_persons];

            log::info!("Building full names...");
            swh_graph::compress::iter_fullnames(&dataset_dir, "person")
                .context("Could not read full names from input dataset")?
                .try_for_each_with(pl.clone(), |pl, (fullname, sha256)| -> Result<()> {
                    let base64 = base64_simd::STANDARD;
                    let person_hash = usize::try_from(
                        person_mph.hash(
                            base64
                                .encode_to_string(&sha256)
                                .into_bytes()
                                .into_boxed_slice(),
                        )?,
                    )
                    .context("Person hash overflowed usize")?;
                    fullnames
                        .get(person_hash)
                        .context("Person hash is greater than the number of persons")?
                        .set(fullname)
                        .map_err(|fullname| {
                            let other_fullname = fullnames.get(person_hash).unwrap().get().unwrap();
                            anyhow!("Hash collision on SHA256 {sha256:?}, between {fullname:?} and {other_fullname:?}")
                        })?;
                    pl.update();
                    Ok(())
                })?;
            pl.done();
            let fullnames: Vec<_> = fullnames
                .into_par_iter()
                .flat_map(|fullname| fullname.into_inner())
                .collect();
            if fullnames.len() != num_persons {
                bail!(
                    "Wrong number of full names, expected {}, got {}",
                    num_persons,
                    fullnames.len()
                );
            }

            log::info!("Writing full names and lengths...");
            let mut fullnames_file = File::create(&fullnames_path)
                .with_context(|| format!("Could not create {}", fullnames_path.display()))?;
            let lengths_file = File::create(&lengths_path)
                .with_context(|| format!("Could not create {}", lengths_path.display()))?;

            let mut lengths_writer = <BufBitWriter<BE, _>>::new(<WordAdapter<u64, _>>::new(
                BufWriter::with_capacity(1 << 20, lengths_file),
            ));

            for fullname in &fullnames {
                fullnames_file.write_all(fullname)?;
                lengths_writer
                    .write_gamma(
                        u64::try_from(fullname.len()).context("Name length overflowed u64")?,
                    )
                    .context("Could not write gamma")?;
            }
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

            let bits_per_line = 20; // A little more than this, actually
            let mut pl = concurrent_progress_logger!(
                display_memory = true,
                item_name = "node",
                local_speed = true,
                expected_updates = Some(swh_graph::utils::dir_size(&swhids_dir)? / bits_per_line),
            );
            pl.start("Computing node stats");

            let stats = par_iter_lines_from_dir(&swhids_dir, pl.clone())
                .map(|line: [u8; 50]| {
                    let ty = NodeType::try_from(&line[6..9]).expect("Unexpected SWHID type");
                    let mut stats = [0usize; NodeType::NUMBER_OF_TYPES];
                    stats[ty as usize] += 1;
                    stats
                })
                .reduce(Default::default, |mut left_1d, right_1d| {
                    for (left, right) in left_1d.iter_mut().zip(right_1d.into_iter()) {
                        *left += right;
                    }
                    left_1d
                });

            pl.done();

            let mut stats_lines = Vec::new();
            let mut total = 0;
            for ty in NodeType::all() {
                stats_lines.push(format!("{} {}\n", ty, stats[ty as usize]));
                total += stats[ty as usize];
            }
            stats_lines.sort();

            stats_file
                .write_all(stats_lines.join("").as_bytes())
                .context("Could not write node stats")?;
            count_file
                .write_all(format!("{total}\n").as_bytes())
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

            let mut pl = progress_logger!(
                display_memory = true,
                item_name = "arc",
                local_speed = true,
                expected_updates = Some(
                    swh_graph::compress::stats::estimate_edge_count(
                        &dataset_dir,
                        &allowed_node_types
                    )
                    .context("Could not estimate edge count from input dataset")?
                        as usize
                ),
            );
            pl.start("Computing edge stats");
            let pl = Mutex::new(pl);

            let stats =
                swh_graph::compress::stats::count_edge_types(&dataset_dir, &allowed_node_types)
                    .context("Could not read edges from input dataset")?
                    .map(|stats_2d| {
                        pl.lock().unwrap().update_with_count(
                            stats_2d.map(|stats_1d| stats_1d.iter().sum()).iter().sum(),
                        );
                        stats_2d
                    })
                    .reduce(Default::default, |mut left_2d, right_2d| {
                        for (left_1d, right_1d) in left_2d.iter_mut().zip(right_2d.into_iter()) {
                            for (left, right) in left_1d.iter_mut().zip(right_1d.into_iter()) {
                                *left += right;
                            }
                        }
                        left_2d
                    });

            let mut stats_lines = Vec::new();
            let mut total = 0;
            for src_type in NodeType::all() {
                for dst_type in NodeType::all() {
                    let count = stats[src_type as usize][dst_type as usize];
                    if count != 0 {
                        stats_lines.push(format!("{src_type}:{dst_type} {count}\n"));
                        total += count;
                    }
                }
            }
            stats_lines.sort();

            stats_file
                .write_all(stats_lines.join("").as_bytes())
                .context("Could not write edge stats")?;
            count_file
                .write_all(format!("{total}\n").as_bytes())
                .context("Could not write edge count")?;
        }

        Commands::BfsRoots {
            format: DatasetFormat::Orc,
            allowed_node_types,
            dataset_dir,
            target,
        } => {
            let allowed_node_types = parse_allowed_node_types(&allowed_node_types)?;

            let target_file = File::create(&target)
                .with_context(|| format!("Could not open {}", target.display()))?;
            let mut target_file = BufWriter::new(target_file);

            if allowed_node_types.contains(&NodeType::Origin) {
                log::info!("Reading origins...");
                let mut origins: Vec<_> = swh_graph::compress::iter_origins(&dataset_dir)
                    .context("Could not read origins")?
                    .collect();

                // split each URL by "/", reverse the parts, and join again.
                // So for example, "https://github.com/vigna/webgraph-rs/" becomes
                // "webgraph-rs/vigna/github.com//https:"
                // This allows similar projects to be grouped together (eg. all forks
                // of webgraph-rs).
                log::info!("Sorting origins...");
                #[allow(unstable_name_collisions)] // Itertools::intersperse
                origins.par_sort_unstable_by_key(|(url, _id)| -> String {
                    url.trim_end_matches('/')
                        .split('/')
                        .rev()
                        .intersperse("/")
                        .collect()
                });

                log::info!("Writing origins...");
                for (_url, id) in origins {
                    target_file
                        .write_all(format!("{id}\n").as_bytes())
                        .context("Could not write origin")?;
                }
            }

            target_file.flush().context("Could not flush output file")?;
        }

        Commands::Bv {
            format: DatasetFormat::Orc,
            sort_batch_size,
            partitions_per_thread,
            allowed_node_types,
            mph_algo,
            function,
            num_nodes,
            dataset_dir,
            target_dir,
        } => {
            let allowed_node_types = parse_allowed_node_types(&allowed_node_types)?;

            match mph_algo {
                MphAlgorithm::Pthash => swh_graph::compress::bv::bv::<SwhidPthash>(
                    sort_batch_size,
                    partitions_per_thread,
                    function,
                    num_nodes,
                    dataset_dir,
                    &allowed_node_types,
                    target_dir,
                )?,
                MphAlgorithm::Cmph => {
                    swh_graph::compress::bv::bv::<swh_graph::java_compat::mph::gov::GOVMPH>(
                        sort_batch_size,
                        partitions_per_thread,
                        function,
                        num_nodes,
                        dataset_dir,
                        &allowed_node_types,
                        target_dir,
                    )?
                }
            }
        }

        Commands::EdgeLabels {
            format: DatasetFormat::Orc,
            sort_batch_size,
            partitions_per_thread,
            allowed_node_types,
            mph_algo,
            function,
            order,
            label_name_mphf,
            label_name_order,
            num_nodes,
            transposed,
            dataset_dir,
            target_dir,
        } => {
            use pthash::Phf;
            use swh_graph::compress::label_names::{LabelNameHasher, LabelNameMphf};
            let allowed_node_types = parse_allowed_node_types(&allowed_node_types)?;
            let order = MappedPermutation::load_unchecked(&order)
                .with_context(|| format!("Could not load {}", order.display()))?;
            let label_name_mphf = LabelNameMphf::load(&label_name_mphf)
                .with_context(|| format!("Could not load {}", label_name_mphf.display()))?;
            let label_name_order = MappedPermutation::load_unchecked(&label_name_order)
                .with_context(|| format!("Could not load {}", label_name_order.display()))?;
            let label_name_hasher = LabelNameHasher::new(&label_name_mphf, &label_name_order)?;

            let label_width = match mph_algo {
                MphAlgorithm::Pthash => swh_graph::compress::bv::edge_labels::<SwhidPthash>(
                    sort_batch_size,
                    partitions_per_thread,
                    function,
                    order,
                    label_name_hasher,
                    num_nodes,
                    dataset_dir,
                    &allowed_node_types,
                    transposed,
                    target_dir.as_ref(),
                )?,
                MphAlgorithm::Cmph => {
                    swh_graph::compress::bv::edge_labels::<swh_graph::java_compat::mph::gov::GOVMPH>(
                        sort_batch_size,
                        partitions_per_thread,
                        function,
                        order,
                        label_name_hasher,
                        num_nodes,
                        dataset_dir,
                        &allowed_node_types,
                        transposed,
                        target_dir.as_ref(),
                    )?
                }
            };

            let mut properties_path = target_dir.to_owned();
            properties_path
                .as_mut_os_string()
                .push("-labelled.properties");
            let properties_file = File::create(&properties_path)
                .with_context(|| format!("Could not create {}", properties_path.display()))?;
            java_properties::write(
                BufWriter::new(properties_file),
                &[
                    (
                        "graphclass".to_string(),
                        "it.unimi.dsi.big.webgraph.labelling.BitStreamArcLabelledImmutableGraph"
                            .to_string(),
                    ),
                    (
                        "labelspec".to_string(),
                        format!(
                            "org.softwareheritage.graph.labels.SwhLabel(DirEntry,{label_width})"
                        ),
                    ),
                    (
                        "underlyinggraph".to_string(),
                        target_dir
                            .file_name()
                            .context("Could not get graph file name")?
                            .to_str()
                            .ok_or_else(|| anyhow!("Could not decode graph file name"))?
                            .to_string(),
                    ),
                ]
                .into_iter()
                .collect(),
            )
            .with_context(|| format!("Could not write {}", properties_path.display()))?;
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
            use swh_graph::mph::{LoadableSwhidMphf, SwhidMphf};

            let allowed_node_types = parse_allowed_node_types(&allowed_node_types)?;

            info!("Loading permutation");
            let order = MappedPermutation::load_unchecked(order.as_path())
                .with_context(|| format!("Could not load {}", order.display()))?;
            assert_eq!(order.len(), num_nodes);
            info!("Permutation loaded, reading MPH");

            fn f<MPHF: SwhidMphf + Sync>(property_writer: PropertyWriter<MPHF>) -> Result<()> {
                info!("MPH loaded, writing properties");
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

            let person_mph = if allowed_node_types.contains(&NodeType::Revision)
                || allowed_node_types.contains(&NodeType::Release)
            {
                use pthash::Phf;

                let Some(person_function) = person_function else {
                    bail!("--person-function must be provided unless --allowed-node-types is set to contain neither 'rev' nor 'rel'.");
                };
                Some(
                    swh_graph::compress::persons::PersonMphf::load(&person_function)
                        .with_context(|| format!("Could not load {}", person_function.display()))?,
                )
            } else {
                None
            };

            let person_mph = person_mph
                .as_ref()
                .map(swh_graph::compress::persons::PersonHasher::new);

            match mph_algo {
                MphAlgorithm::Pthash => {
                    let swhid_mph = LoadableSwhidMphf::load(function).context("Cannot load mph")?;
                    let property_writer = PropertyWriter {
                        swhid_mph,
                        person_mph,
                        order,
                        num_nodes,
                        dataset_dir,
                        allowed_node_types,
                        target,
                    };
                    f::<SwhidPthash>(property_writer)?;
                }
                MphAlgorithm::Cmph => {
                    let swhid_mph = LoadableSwhidMphf::load(function).context("Cannot load mph")?;
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
    }

    Ok(())
}
