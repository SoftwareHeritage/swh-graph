/*
 * Copyright (C) 2024  The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

use std::path::PathBuf;

use anyhow::{anyhow, bail, Context, Result};
use clap::{Parser, Subcommand, ValueEnum};
use mmap_rs::Mmap;
use ph::fmph;

use swh_graph::java_compat::mph::gov::GOVMPH;
use swh_graph::map::Node2SWHID;
use swh_graph::map::{MappedPermutation, Permutation};
use swh_graph::mph::SwhidMphf;
use swh_graph::{OutOfBoundError, SWHID};

#[derive(Parser, Debug)]
/// Runs any of swh-graph's Minimal Perfect Hash functions
///
/// Lines in stdin are hashed one by one, and a decimal-encoded 64-bits integer is
/// written on the output for each of them.
///
/// If any of the input lines was not in the dataset used to build the MPH, then the result
/// will either be a silent hash collision or cause a non-zero exit.
struct Args {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
enum MphAlgorithm {
    Fmph,
    Cmph,
}

#[derive(Subcommand, Debug)]
enum Commands {
    Swhids {
        #[arg(long)]
        num_nodes: usize,
        #[arg(long)]
        mph_algo: MphAlgorithm,
        #[arg(long)]
        mph: PathBuf,
        #[arg(long)]
        permutation: PathBuf,
        #[arg(long)]
        /// If given, uses a `.node2swhid.bin` table to check for hash collisions.
        /// This turns all silent hash collisions into a hard error.
        node2swhid: Option<PathBuf>,
    },
    Persons {
        #[arg(long)]
        mph_algo: MphAlgorithm,
        #[arg(long)]
        mph: PathBuf,
    },
}

pub fn main() -> Result<()> {
    let args = Args::parse();

    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    match args.command {
        Commands::Swhids {
            num_nodes,
            mph_algo,
            mph,
            permutation,
            node2swhid,
        } => {
            log::info!("Loading permutation...");
            let permutation = MappedPermutation::load(num_nodes, permutation.as_path())
                .with_context(|| format!("Could not load permutation {}", permutation.display()))?;

            log::info!("Loading node2swhid...");
            let node2swhid = node2swhid
                .as_ref()
                .map(Node2SWHID::<Mmap>::load)
                .transpose()
                .with_context(|| {
                    format!(
                        "Could not load node2swhid from {}",
                        node2swhid.unwrap().display()
                    )
                })?;

            match mph_algo {
                MphAlgorithm::Fmph => hash_swhids::<fmph::Function>(mph, permutation, node2swhid),
                MphAlgorithm::Cmph => hash_swhids::<GOVMPH>(mph, permutation, node2swhid),
            }
        }
        Commands::Persons { mph_algo, mph } => match mph_algo {
            MphAlgorithm::Fmph => hash_persons::<fmph::Function>(mph),
            MphAlgorithm::Cmph => hash_persons::<GOVMPH>(mph),
        },
    }
}

fn hash_swhids<MPHF: SwhidMphf>(
    mph: PathBuf,
    permutation: MappedPermutation,
    node2swhid: Option<Node2SWHID<Mmap>>,
) -> Result<()> {
    log::info!("Loading MPH function...");
    let mph =
        MPHF::load(&mph).with_context(|| format!("Could not load MPH from {}", mph.display()))?;

    log::info!("Hashing input...");

    for (i, line) in std::io::stdin().lines().enumerate() {
        let line = line.with_context(|| format!("Could not read input line {}", i))?;
        let swhid = SWHID::try_from(line.as_str())
            .with_context(|| format!("Could not parse SWHID {}", line))?;
        let node_id = permutation
            .get(
                mph.hash_swhid(&swhid)
                    .ok_or(anyhow!("Unknown SWHID {}", swhid))?, // rejected by MPH
            )
            .ok_or(anyhow!("Unknown SWHID {}", swhid))?; // MPH result too large
        if let Some(node2swhid) = &node2swhid {
            match node2swhid.get(node_id) {
                Err(OutOfBoundError { .. }) => bail!("Unknown SWHID {}", swhid),
                Ok(swhid2) if swhid == swhid2 => (), // expected result
                _ => bail!("Unknown SWHID {}", swhid), // hash collision
            }
        }
        println!("{}", node_id);
    }

    Ok(())
}

fn hash_persons<MPHF: SwhidMphf>(mph: PathBuf) -> Result<()> {
    log::info!("Loading MPH function...");
    let mph =
        MPHF::load(&mph).with_context(|| format!("Could not load MPH from {}", mph.display()))?;

    log::info!("Hashing input...");

    for (i, line) in std::io::stdin().lines().enumerate() {
        let line = line.with_context(|| format!("Could not read input line {}", i))?;
        println!(
            "{}",
            mph.hash_str(&line)
                .ok_or(anyhow!("Unknown value {}", line))?
        );
    }

    Ok(())
}
