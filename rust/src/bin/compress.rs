/*
 * Copyright (C) 2023  The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

#[cfg(not(feature = "compression"))]
compile_error!("Feature 'compression' must be enabled for this executable to be available.");

use std::fs::File;
use std::num::NonZeroU64;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand, ValueEnum};
use dsi_progress_logger::ProgressLogger;
use faster_hex::hex_decode;
use log::info;
use ph::fmph;
use swh_graph::{SWHType, SWHID};

const ORC_BATCH_SIZE: NonZeroU64 = unsafe { NonZeroU64::new_unchecked(1024) }; // TODO: tune this?

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
    /// Reads the list of nodes from the ORC directory and produces a Minimal Perfect Hash function
    Mph {
        #[arg(value_enum, long, default_value_t = DatasetFormat::Orc)]
        format: DatasetFormat,
        #[arg(long, default_value = "*")]
        allowed_nodes_types: String,
        dataset_dir: PathBuf,
        out_mph: PathBuf,
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

macro_rules! iter_swhids {
    ($struct:ty, $type:expr, $column:ident, $readers:expr) => {
        Box::new(
            $readers
                .iter()
                .flat_map(|reader| {
                    let row_reader = reader
                        .row_reader(
                            RowReaderOptions::default().include_names([stringify!($column)]),
                        )
                        .expect("Could not read rows");
                    <$struct>::check_kind(&row_reader.selected_kind())
                        .expect("Unexpected ORC structure");
                    RowIterator::new(row_reader, ORC_BATCH_SIZE)
                })
                .map(|node: $struct| {
                    let mut swhid = SWHID {
                        namespace_version: 1,
                        node_type: $type,
                        hash: unsafe { std::mem::MaybeUninit::zeroed().assume_init() },
                    };
                    hex_decode(&node.$column.as_bytes(), &mut swhid.hash).unwrap_or_else(|e| {
                        panic!("{:x?} is not a valid sha1 hash: {:?}", node.$column, e)
                    });
                    swhid
                }),
        )
    };
}

struct SwhidsIterator<'a> {
    pl: ProgressLogger<'static>,

    // FIXME: these types are known at compile time, but they are a mouthful
    cnt_iterator: Box<dyn Iterator<Item = SWHID> + 'a>,
    dir_iterator: Box<dyn Iterator<Item = SWHID> + 'a>,
    ori_iterator: Box<dyn Iterator<Item = SWHID> + 'a>,
    rel_iterator: Box<dyn Iterator<Item = SWHID> + 'a>,
    rev_iterator: Box<dyn Iterator<Item = SWHID> + 'a>,
    snp_iterator: Box<dyn Iterator<Item = SWHID> + 'a>,

    current_type: SWHType,
}

impl<'a> Iterator for SwhidsIterator<'a> {
    type Item = SWHID;

    fn next(&mut self) -> Option<SWHID> {
        self.pl.light_update();

        match self.current_type {
            SWHType::Content => self.cnt_iterator.next().or_else(|| {
                self.current_type = SWHType::Directory;
                self.next()
            }),
            SWHType::Directory => self.dir_iterator.next().or_else(|| {
                self.current_type = SWHType::Origin;
                self.next()
            }),
            SWHType::Origin => self.ori_iterator.next().or_else(|| {
                self.current_type = SWHType::Release;
                self.next()
            }),
            SWHType::Release => self.rel_iterator.next().or_else(|| {
                self.current_type = SWHType::Revision;
                self.next()
            }),
            SWHType::Revision => self.rev_iterator.next().or_else(|| {
                self.current_type = SWHType::Snapshot;
                self.next()
            }),
            SWHType::Snapshot => self.snp_iterator.next(),
            _ => unreachable!("SwhidsIterator::current_type is {:?}", self.current_type),
        }
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
        Commands::Mph {
            format: DatasetFormat::Orc,
            allowed_nodes_types,
            dataset_dir,
            out_mph,
        } => {
            use orcxx::deserialize::CheckableKind;
            use orcxx::reader::RowReaderOptions;
            use orcxx::row_iterator::RowIterator;
            use orcxx_derive::OrcDeserialize;

            // Check early before the long-running computation
            File::create(&out_mph).expect(&format!("Cannot create {}", out_mph.display()));
            std::fs::remove_file(&out_mph)
                .expect(&format!("Could not remove {}", out_mph.display()));

            let allowed_nodes_types = parse_allowed_node_types(&allowed_nodes_types)?;

            // Open input files
            let cnt_readers = get_dataset_readers(dataset_dir.clone(), "content");
            let dir_readers = get_dataset_readers(dataset_dir.clone(), "directory");
            let ori_readers = get_dataset_readers(dataset_dir.clone(), "origin");
            let rel_readers = get_dataset_readers(dataset_dir.clone(), "release");
            let rev_readers = get_dataset_readers(dataset_dir.clone(), "revision");
            let snp_readers = get_dataset_readers(dataset_dir.clone(), "snapshot");

            // Compute total length
            let len = [
                &cnt_readers,
                &dir_readers,
                &ori_readers,
                &rel_readers,
                &rev_readers,
                &snp_readers,
            ]
            .iter()
            .map(|readers| {
                readers
                    .iter()
                    .map(|reader| {
                        let len: usize = reader
                            .row_count()
                            .try_into()
                            .expect("rows count overflows usize");
                        len
                    })
                    .sum::<usize>()
            })
            .sum::<usize>();

            // Build parsers
            #[derive(OrcDeserialize, Clone, Default)]
            struct Content {
                sha1_git: String,
            }
            #[derive(OrcDeserialize, Clone, Default)]
            struct NormalNode {
                id: String,
            }

            let get_key_iter_calls = Arc::new(Mutex::new(0));

            let get_key_iter = || SwhidsIterator {
                pl: {
                    let mut call_count = get_key_iter_calls.lock().unwrap();
                    *call_count += 1;
                    let mut pl = ProgressLogger::default().display_memory();
                    pl.item_name = "node";
                    pl.local_speed = true;
                    pl.expected_updates = Some(len);
                    pl.start(format!("iterating SWHIDs (pass {})", *call_count));
                    pl
                },

                cnt_iterator: iter_swhids!(Content, SWHType::Content, sha1_git, &cnt_readers),
                dir_iterator: iter_swhids!(NormalNode, SWHType::Directory, id, &dir_readers),
                ori_iterator: iter_swhids!(NormalNode, SWHType::Origin, id, &ori_readers),
                rel_iterator: iter_swhids!(NormalNode, SWHType::Release, id, &rel_readers),
                rev_iterator: iter_swhids!(NormalNode, SWHType::Revision, id, &rev_readers),
                snp_iterator: iter_swhids!(NormalNode, SWHType::Snapshot, id, &snp_readers),
                current_type: SWHType::Content,
            };

            let conf = fmph::BuildConf::default();
            let clone_threshold = 1024; // TODO: tune this
            let keys = fmph::keyset::CachedKeySet::dynamic_with_len(
                get_key_iter,
                len,
                true,
                clone_threshold,
            );
            let mph = fmph::Function::with_conf(keys, conf);

            let mut file =
                File::create(&out_mph).expect(&format!("Cannot create {}", out_mph.display()));
            mph.write(&mut file).unwrap();
        }
    }

    Ok(())
}

fn get_dataset_readers(mut dataset_dir: PathBuf, subdirectory: &str) -> Vec<orcxx::reader::Reader> {
    dataset_dir.push("orc");
    dataset_dir.push(subdirectory);
    std::fs::read_dir(&dataset_dir)
        .expect(&format!("Could not list {}", dataset_dir.display()))
        .map(|file_path| {
            let file_path = file_path
                .expect(&format!("Failed to list {}", dataset_dir.display()))
                .path();
            let input_stream = orcxx::reader::InputStream::from_local_file(
                file_path
                    .to_str()
                    .expect(&format!("Error decoding {}", file_path.display())),
            )
            .expect(&format!("Could not open {}", file_path.display()));
            orcxx::reader::Reader::new(input_stream)
                .expect(&format!("Could not read {}", file_path.display()))
        })
        .collect()
}
