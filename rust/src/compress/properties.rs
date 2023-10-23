// Copyright (C) 2023  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::cell::RefCell;
use std::io::{BufWriter, Write};
use std::path::PathBuf;

use anyhow::{Context, Result};
use orcxx::deserialize::{OrcDeserialize, OrcStruct};
use orcxx::reader::Reader;
use orcxx::row_iterator::RowIterator;
use orcxx_derive::OrcDeserialize;
use rayon::prelude::*;

use super::orc::{get_dataset_readers, ORC_BATCH_SIZE};
use crate::java_compat::bit_vector::LongArrayBitVector;
use crate::map::{MappedPermutation, Permutation};
use crate::mph::SwhidMphf;
use crate::properties::suffixes;
use crate::utils::suffix_path;
use crate::SWHType;

pub struct PropertyWriter<MPHF: SwhidMphf> {
    pub mph: MPHF,
    pub order: MappedPermutation,
    pub num_nodes: usize,
    pub dataset_dir: PathBuf,
    pub allowed_node_types: Vec<SWHType>,
    pub target: PathBuf,
}

impl<MPHF: SwhidMphf + Sync> PropertyWriter<MPHF> {
    fn for_each_row<Row: OrcDeserialize + Clone + OrcStruct>(
        &self,
        subdirectory: &str,
        f: impl Fn(Row) -> Result<()>,
    ) -> Result<()> {
        get_dataset_readers(self.dataset_dir.clone(), subdirectory)
            .into_iter()
            .flat_map(|reader: Reader| {
                RowIterator::<Row>::new(&reader, (ORC_BATCH_SIZE as u64).try_into().unwrap())
                    .expect("Could not open row reader")
            })
            .try_for_each(f)
    }

    fn par_for_each_row<Row: OrcDeserialize + Clone + OrcStruct + Send>(
        &self,
        subdirectory: &str,
        f: impl Fn(Row) + Send + Sync,
    ) -> impl ParallelIterator<Item = ()> {
        get_dataset_readers(self.dataset_dir.clone(), subdirectory)
            .into_par_iter()
            .flat_map(|reader: Reader| {
                RowIterator::<Row>::new(&reader, (ORC_BATCH_SIZE as u64).try_into().unwrap())
                    .expect("Could not open row reader")
                    .par_bridge()
            })
            .map(f)
    }

    fn node_id(&self, swhid: &str) -> usize {
        self.order
            .get(
                self.mph
                    .hash_str(&swhid)
                    .unwrap_or_else(|| panic!("unknown SWHID {}", swhid)),
            )
            .unwrap()
    }

    fn set<Value>(&self, vector: &Vec<Value>, swhid: &str, value: Value) {
        unsafe {
            vector
                .as_ptr()
                .offset(self.node_id(swhid) as isize)
                .cast_mut()
                .write(value)
        };
    }

    fn write<Value: bytemuck::Pod>(&self, suffix: &str, values: impl AsRef<[Value]>) -> Result<()> {
        let path = suffix_path(&self.target, suffix);
        let mut file = std::fs::File::create(&path)
            .with_context(|| format!("Could not create {}", path.display()))?;
        file.write_all(bytemuck::cast_slice(values.as_ref()))
            .with_context(|| format!("Could not write to {}", path.display()))?;

        Ok(())
    }

    pub fn write_author_timestamps(&self) -> Result<()> {
        #[derive(OrcDeserialize, Default, Clone)]
        struct Revrel {
            id: String,
            date: Option<orcxx::Timestamp>,
            date_offset: Option<i16>,
        }

        let read_rev = self.allowed_node_types.contains(&SWHType::Revision);
        let read_rel = self.allowed_node_types.contains(&SWHType::Release);

        if !read_rev && !read_rel {
            log::info!("Excluded");
            return Ok(());
        }

        log::info!("Initializing...");
        let timestamps = vec![i64::MIN.to_be(); self.num_nodes];
        let timestamp_offsets = vec![i16::MIN.to_be(); self.num_nodes];

        log::info!("Reading...");
        let f = |type_: &str, r: Revrel| {
            if let Some(date) = r.date {
                let swhid = format!("swh:1:{}:{}", type_, r.id);
                self.set(&timestamps, &swhid, date.seconds.to_be());
                if let Some(date_offset) = r.date_offset {
                    self.set(&timestamp_offsets, &swhid, date_offset.to_be());
                }
            }
        };

        if read_rev && read_rel {
            [].into_par_iter()
                .chain(self.par_for_each_row("revision", |rev: Revrel| f("rev", rev)))
                .chain(self.par_for_each_row("release", |rel: Revrel| f("rel", rel)))
                .for_each(|()| ());
        } else if read_rev {
            self.par_for_each_row("revision", |rev: Revrel| f("rev", rev))
                .for_each(|()| ());
        } else if read_rel {
            self.par_for_each_row("release", |rel: Revrel| f("rel", rel))
                .for_each(|()| ());
        } else {
            unreachable!("!read_rev && !read_rel");
        }

        log::info!("Writing...");
        self.write(suffixes::AUTHOR_TIMESTAMP, timestamps)?;
        self.write(suffixes::AUTHOR_TIMESTAMP_OFFSET, timestamp_offsets)?;

        Ok(())
    }
    pub fn write_committer_timestamps(&self) -> Result<()> {
        #[derive(OrcDeserialize, Default, Clone)]
        struct Revision {
            id: String,
            committer_date: Option<orcxx::Timestamp>,
            committer_offset: Option<i16>,
        }

        if !self.allowed_node_types.contains(&SWHType::Revision) {
            log::info!("Excluded");
            return Ok(());
        }

        log::info!("Initializing...");
        let timestamps = vec![i64::MIN.to_be(); self.num_nodes];
        let timestamp_offsets = vec![i16::MIN.to_be(); self.num_nodes];

        log::info!("Reading...");
        self.par_for_each_row("revision", |rev: Revision| {
            if let Some(date) = rev.committer_date {
                let swhid = format!("swh:1:rev:{}", rev.id);
                self.set(&timestamps, &swhid, date.seconds.to_be());
                if let Some(date_offset) = rev.committer_offset {
                    self.set(&timestamp_offsets, &swhid, date_offset.to_be());
                }
            }
        })
        .for_each(|()| ());

        log::info!("Writing...");
        self.write(suffixes::COMMITTER_TIMESTAMP, timestamps)?;
        self.write(suffixes::COMMITTER_TIMESTAMP_OFFSET, timestamp_offsets)?;

        Ok(())
    }
    pub fn write_content_lengths(&self) -> Result<()> {
        #[derive(OrcDeserialize, Default, Clone)]
        struct Content {
            sha1_git: Option<String>,
            length: Option<i64>,
        }

        if !self.allowed_node_types.contains(&SWHType::Content) {
            log::info!("Excluded");
            return Ok(());
        }

        log::info!("Initializing...");
        let lengths = vec![u64::MAX.to_be(); self.num_nodes];

        log::info!("Reading...");
        let f = |cnt: Content| {
            if let Some(id) = cnt.sha1_git {
                if let Some(length) = cnt.length {
                    let swhid = format!("swh:1:cnt:{}", id);
                    self.set(&lengths, &swhid, (length as u64).to_be());
                }
            }
        };
        [].into_par_iter()
            .chain(self.par_for_each_row("content", f))
            .chain(self.par_for_each_row("skipped_content", f))
            .for_each(|()| ());

        log::info!("Writing...");
        self.write(suffixes::CONTENT_LENGTH, lengths)?;

        Ok(())
    }
    pub fn write_content_is_skipped(&self) -> Result<()> {
        #[derive(OrcDeserialize, Default, Clone)]
        struct SkippedContent {
            sha1_git: Option<String>,
        }

        if !self.allowed_node_types.contains(&SWHType::Revision) {
            log::info!("Excluded");
            return Ok(());
        }

        log::info!("Initializing...");
        let is_skipped = sux::bits::bit_vec::BitVec::new_atomic(self.num_nodes);

        log::info!("Reading...");
        self.par_for_each_row("skipped_content", |cnt: SkippedContent| {
            if let Some(id) = cnt.sha1_git {
                let swhid = format!("swh:1:cnt:{}", id);
                is_skipped.set(
                    self.node_id(&swhid),
                    true,
                    std::sync::atomic::Ordering::Relaxed,
                );
            }
        })
        .for_each(|()| ());

        // We need to be compatible with graphs generated with Java, which used Java's
        // native serialization for it.unimi.dsi.bits.LongArrayBitVector, which
        // is little-endian on disk; but sux::bits::bit_vec::BitVec is big endian
        // in memory.
        log::info!("Converting...");
        let is_skipped: sux::bits::bit_vec::BitVec<Vec<usize>> = is_skipped.into();
        let bitvec = LongArrayBitVector::new_from_bitvec(is_skipped);

        log::info!("Writing...");
        bitvec
            .dump(suffix_path(&self.target, suffixes::CONTENT_IS_SKIPPED))
            .context("Could not write LongArrayBitVector")?;

        Ok(())
    }
    pub fn write_author_ids(&self) -> Result<()> {
        Ok(())
    }
    pub fn write_committer_ids(&self) -> Result<()> {
        Ok(())
    }
    pub fn write_messages(&self) -> Result<()> {
        #[derive(OrcDeserialize, Default, Clone)]
        struct Revrel {
            id: String,
            message: Option<Vec<u8>>,
        }
        #[derive(OrcDeserialize, Default, Clone)]
        struct Origin {
            id: String,
            url: String,
        }

        let read_rev = self.allowed_node_types.contains(&SWHType::Revision);
        let read_rel = self.allowed_node_types.contains(&SWHType::Release);
        let read_ori = self.allowed_node_types.contains(&SWHType::Origin);

        if !read_rev && !read_rel && !read_ori {
            log::info!("Excluded");
            return Ok(());
        }

        log::info!("Initializing...");
        let offsets = vec![u64::MAX.to_be(); self.num_nodes];
        let path = suffix_path(&self.target, suffixes::MESSAGE);
        let file = std::fs::File::create(&path)
            .with_context(|| format!("Could not create {}", path.display()))?;
        let writer = RefCell::new(BufWriter::new(file));

        let base64 = base64_simd::STANDARD;
        let offset = RefCell::new(0u64);

        let f = |type_: &str, id: String, message: Option<Vec<u8>>| {
            if let Some(message) = message {
                let swhid = format!("swh:1:{}:{}", type_, id);
                let mut encoded_message = base64.encode_to_string(message);
                encoded_message.push('\n');
                let encoded_message = encoded_message.as_bytes();
                writer.borrow_mut().write_all(encoded_message)?;
                self.set(&offsets, &swhid, offset.borrow().to_be());
                *offset.borrow_mut() += encoded_message.len() as u64;
            }
            Ok(())
        };

        // Can't do it in parallel because we are writing to a single file
        if read_rel {
            log::info!("Reading and writing release messages...");
            self.for_each_row("release", |rel: Revrel| f("rel", rel.id, rel.message))?;
        }
        if read_rev {
            log::info!("Reading and writing revision messages...");
            self.for_each_row("revision", |rev: Revrel| f("rev", rev.id, rev.message))?;
        }
        if read_ori {
            log::info!("Reading and writing origin URLs...");
            self.for_each_row("origin", |ori: Origin| {
                f("ori", ori.id, Some(ori.url.as_bytes().to_vec()))
            })?;
        }

        log::info!("Writing offsets...");
        self.write(suffixes::MESSAGE_OFFSET, offsets)?;
        Ok(())
    }
    pub fn write_tag_names(&self) -> Result<()> {
        #[derive(OrcDeserialize, Default, Clone)]
        struct Release {
            id: String,
            name: Vec<u8>,
        }

        if !self.allowed_node_types.contains(&SWHType::Release) {
            log::info!("Excluded");
            return Ok(());
        }

        log::info!("Initializing...");
        let offsets = vec![u64::MAX.to_be(); self.num_nodes];
        let path = suffix_path(&self.target, suffixes::TAG_NAME);
        let file = std::fs::File::create(&path)
            .with_context(|| format!("Could not create {}", path.display()))?;
        let writer = RefCell::new(BufWriter::new(file));

        log::info!("Reading and writing...");
        let base64 = base64_simd::STANDARD;
        let offset = RefCell::new(0u64);

        // Can't do it in parallel because we are writing to a single file
        self.for_each_row("release", |rel: Release| {
            let swhid = format!("swh:1:rel:{}", rel.id);
            let mut encoded_name = base64.encode_to_string(rel.name);
            encoded_name.push('\n');
            let encoded_name = encoded_name.as_bytes();
            writer.borrow_mut().write_all(encoded_name)?;
            self.set(&offsets, &swhid, offset.borrow().to_be());
            *offset.borrow_mut() += encoded_name.len() as u64;

            Ok(())
        })?;

        log::info!("Writing offsets...");
        self.write(suffixes::TAG_NAME_OFFSET, offsets)?;
        Ok(())
    }
}
