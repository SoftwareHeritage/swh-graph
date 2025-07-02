// Copyright (C) 2023-2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::atomic::Ordering;

use anyhow::{Context, Result};
use ar_row::deserialize::{ArRowDeserialize, ArRowStruct};
use ar_row_derive::ArRowDeserialize;
use common_traits::{Atomic, IntoAtomic};
use rayon::prelude::*;

use super::orc::get_dataset_readers;
use super::orc::{iter_arrow, par_iter_arrow};
use crate::map::{MappedPermutation, Permutation};
use crate::mph::SwhidMphf;
use crate::properties::suffixes;
use crate::utils::suffix_path;
use crate::NodeType;

pub struct PropertyWriter<'b, SWHIDMPHF: SwhidMphf> {
    pub swhid_mph: SWHIDMPHF,
    pub person_mph: Option<super::persons::PersonHasher<'b>>,
    pub order: MappedPermutation,
    pub num_nodes: usize,
    pub dataset_dir: PathBuf,
    pub allowed_node_types: Vec<NodeType>,
    pub target: PathBuf,
}

impl<SWHIDMPHF: SwhidMphf + Sync> PropertyWriter<'_, SWHIDMPHF> {
    fn for_each_row<Row>(&self, subdirectory: &str, f: impl FnMut(Row) -> Result<()>) -> Result<()>
    where
        Row: ArRowDeserialize + ArRowStruct + Send + Sync,
    {
        get_dataset_readers(self.dataset_dir.clone(), subdirectory)?
            .into_iter()
            .flat_map(|reader_builder| iter_arrow(reader_builder, |row: Row| [row]))
            .try_for_each(f)
    }

    fn par_for_each_row<Row>(
        &self,
        subdirectory: &str,
        f: impl Fn(Row) + Send + Sync,
    ) -> Result<impl ParallelIterator<Item = ()>>
    where
        Row: ArRowDeserialize + ArRowStruct + Clone + Send + Sync,
    {
        Ok(get_dataset_readers(self.dataset_dir.clone(), subdirectory)?
            .into_par_iter()
            .flat_map(|reader_builder| par_iter_arrow(reader_builder, |row: Row| [row]))
            .map(f))
    }

    /// Equivalent to `vec![initial_value; self.num_nodes]`, but initializes values in the vector
    /// in parallel
    ///
    /// This is 9 times faster on a NUMA machine with two Intel Xeon Gold 6342 CPUs.
    fn init_vec<T: Copy + Default + Sync>(&self, initial_value: T) -> Vec<T>
    where
        for<'a> Vec<T>: IntoParallelRefMutIterator<'a, Item = &'a mut T>,
    {
        let mut vec = vec![T::default(); self.num_nodes];
        vec.par_iter_mut().for_each(|v| *v = initial_value);
        vec
    }
    /// Same as [`Self::init_vec`] but returns a vector of atomic values
    fn init_atomic_vec<T: IntoAtomic + Copy + Default + Sync>(
        &self,
        initial_value: T,
    ) -> Vec<<T as IntoAtomic>::AtomicType>
    where
        for<'a> Vec<T>: IntoParallelRefMutIterator<'a, Item = &'a mut T>,
    {
        (0..self.num_nodes)
            .into_par_iter()
            .map(|_| initial_value.to_atomic())
            .collect()
    }

    fn node_id(&self, swhid: &str) -> usize {
        self.order
            .get(
                self.swhid_mph
                    .hash_str(swhid)
                    .unwrap_or_else(|| panic!("unknown SWHID {swhid}")),
            )
            .unwrap()
    }

    fn set_atomic<Value: Atomic>(
        &self,
        vector: &[Value],
        swhid: &str,
        value: Value::NonAtomicType,
    ) {
        vector
            .get(self.node_id(swhid))
            .expect("node_id is larger than the array")
            .store(value, Ordering::Relaxed)
    }

    fn set<Value>(&self, vector: &mut [Value], swhid: &str, value: Value) {
        *vector
            .get_mut(self.node_id(swhid))
            .expect("node_id is larger than the array") = value;
    }

    fn write<Value: bytemuck::Pod>(&self, suffix: &str, values: impl AsRef<[Value]>) -> Result<()> {
        let path = suffix_path(&self.target, suffix);
        let mut file = std::fs::File::create(&path)
            .with_context(|| format!("Could not create {}", path.display()))?;
        file.write_all(bytemuck::cast_slice(values.as_ref()))
            .with_context(|| format!("Could not write to {}", path.display()))?;

        Ok(())
    }

    fn write_atomic<Value: Atomic>(&self, suffix: &str, values: Vec<Value>) -> Result<()>
    where
        <Value as Atomic>::NonAtomicType: bytemuck::Pod,
    {
        // In release mode, this is compiled into a no-op
        let values: Vec<_> = values.into_iter().map(Value::into_inner).collect();
        self.write(suffix, values)
    }

    pub fn write_author_timestamps(&self) -> Result<()> {
        #[derive(ArRowDeserialize, Default, Clone)]
        struct Revrel {
            id: String,
            date: Option<ar_row::Timestamp>,
            date_offset: Option<i16>,
        }

        let read_rev = self.allowed_node_types.contains(&NodeType::Revision);
        let read_rel = self.allowed_node_types.contains(&NodeType::Release);

        if !read_rev && !read_rel {
            log::info!("Excluded");
            return Ok(());
        }

        log::info!("Initializing...");
        let timestamps = self.init_atomic_vec(i64::MIN.to_be());
        let timestamp_offsets = self.init_atomic_vec(i16::MIN.to_be());

        log::info!("Reading...");
        let f = |type_: &str, r: Revrel| {
            if let Some(date) = r.date {
                let swhid = format!("swh:1:{}:{}", type_, r.id);
                self.set_atomic(&timestamps, &swhid, date.seconds.to_be());
                if let Some(date_offset) = r.date_offset {
                    self.set_atomic(&timestamp_offsets, &swhid, date_offset.to_be());
                }
            }
        };

        if read_rev && read_rel {
            [].into_par_iter()
                .chain(self.par_for_each_row("revision", |rev: Revrel| f("rev", rev))?)
                .chain(self.par_for_each_row("release", |rel: Revrel| f("rel", rel))?)
                .for_each(|()| ());
        } else if read_rev {
            self.par_for_each_row("revision", |rev: Revrel| f("rev", rev))?
                .for_each(|()| ());
        } else if read_rel {
            self.par_for_each_row("release", |rel: Revrel| f("rel", rel))?
                .for_each(|()| ());
        } else {
            unreachable!("!read_rev && !read_rel");
        }

        log::info!("Writing...");
        self.write_atomic(suffixes::AUTHOR_TIMESTAMP, timestamps)?;
        self.write_atomic(suffixes::AUTHOR_TIMESTAMP_OFFSET, timestamp_offsets)?;

        Ok(())
    }
    pub fn write_committer_timestamps(&self) -> Result<()> {
        #[derive(ArRowDeserialize, Default, Clone)]
        struct Revision {
            id: String,
            committer_date: Option<ar_row::Timestamp>,
            committer_offset: Option<i16>,
        }

        if !self.allowed_node_types.contains(&NodeType::Revision) {
            log::info!("Excluded");
            return Ok(());
        }

        log::info!("Initializing...");
        let timestamps = self.init_atomic_vec(i64::MIN.to_be());
        let timestamp_offsets = self.init_atomic_vec(i16::MIN.to_be());

        log::info!("Reading...");
        self.par_for_each_row("revision", |rev: Revision| {
            if let Some(date) = rev.committer_date {
                let swhid = format!("swh:1:rev:{}", rev.id);
                self.set_atomic(&timestamps, &swhid, date.seconds.to_be());
                if let Some(date_offset) = rev.committer_offset {
                    self.set_atomic(&timestamp_offsets, &swhid, date_offset.to_be());
                }
            }
        })?
        .for_each(|()| ());

        log::info!("Writing...");
        self.write_atomic(suffixes::COMMITTER_TIMESTAMP, timestamps)?;
        self.write_atomic(suffixes::COMMITTER_TIMESTAMP_OFFSET, timestamp_offsets)?;

        Ok(())
    }
    pub fn write_content_lengths(&self) -> Result<()> {
        #[derive(ArRowDeserialize, Default, Clone)]
        struct Content {
            sha1_git: Option<String>,
            length: Option<i64>,
        }

        if !self.allowed_node_types.contains(&NodeType::Content) {
            log::info!("Excluded");
            return Ok(());
        }

        log::info!("Initializing...");
        let lengths = self.init_atomic_vec(u64::MAX.to_be());

        log::info!("Reading...");
        let f = |cnt: Content| {
            if let Some(id) = cnt.sha1_git {
                if let Some(length) = cnt.length {
                    let swhid = format!("swh:1:cnt:{id}");
                    self.set_atomic(&lengths, &swhid, (length as u64).to_be());
                }
            }
        };
        [].into_par_iter()
            .chain(self.par_for_each_row("content", f)?)
            .chain(self.par_for_each_row("skipped_content", f)?)
            .for_each(|()| ());

        log::info!("Writing...");
        self.write_atomic(suffixes::CONTENT_LENGTH, lengths)?;

        Ok(())
    }
    pub fn write_content_is_skipped(&self) -> Result<()> {
        #[derive(ArRowDeserialize, Default, Clone)]
        struct SkippedContent {
            sha1_git: Option<String>,
        }

        if !self.allowed_node_types.contains(&NodeType::Content) {
            log::info!("Excluded");
            return Ok(());
        }

        log::info!("Initializing...");
        let is_skipped = sux::bits::bit_vec::AtomicBitVec::new(self.num_nodes);

        log::info!("Reading...");
        self.par_for_each_row("skipped_content", |cnt: SkippedContent| {
            if let Some(id) = cnt.sha1_git {
                let swhid = format!("swh:1:cnt:{id}");
                is_skipped.set(
                    self.node_id(&swhid),
                    true,
                    std::sync::atomic::Ordering::Relaxed,
                );
            }
        })?
        .for_each(|()| ());

        log::info!("Converting...");
        let (bitvec, len) = is_skipped.into_raw_parts();
        assert_eq!(len, self.num_nodes);
        // Make its values big-endian
        let bitvec_be: Vec<u8> = bitvec
            .into_par_iter()
            .flat_map(|cell| cell.into_inner().to_be_bytes())
            .collect();

        log::info!("Writing...");
        self.write(suffixes::CONTENT_IS_SKIPPED, bitvec_be)?;

        Ok(())
    }
    pub fn write_author_ids(&self) -> Result<()> {
        #[derive(ArRowDeserialize, Default, Clone)]
        struct Revrel {
            id: String,
            author: Option<Box<[u8]>>,
        }

        let read_rev = self.allowed_node_types.contains(&NodeType::Revision);
        let read_rel = self.allowed_node_types.contains(&NodeType::Release);

        if !read_rev && !read_rel {
            log::info!("Excluded");
            return Ok(());
        }

        let Some(person_mph) = self.person_mph.as_ref() else {
            panic!(
                "write_author_ids is missing person MPH but allowed_node_types = {:?}",
                self.allowed_node_types
            );
        };

        log::info!("Initializing...");
        let authors = self.init_atomic_vec(u32::MAX.to_be());

        log::info!("Reading...");
        let f = |type_: &str, r: Revrel| {
            if let Some(person) = r.author {
                let swhid = format!("swh:1:{}:{}", type_, r.id);
                let base64 = base64_simd::STANDARD;
                let person = base64.encode_to_string(person).into_bytes();
                let person_id: u32 = person_mph.hash(person).expect("Unknown person");
                self.set_atomic(&authors, &swhid, person_id.to_be());
            }
        };

        if read_rev && read_rel {
            [].into_par_iter()
                .chain(self.par_for_each_row("revision", |rev: Revrel| f("rev", rev))?)
                .chain(self.par_for_each_row("release", |rel: Revrel| f("rel", rel))?)
                .for_each(|()| ());
        } else if read_rev {
            self.par_for_each_row("revision", |rev: Revrel| f("rev", rev))?
                .for_each(|()| ());
        } else if read_rel {
            self.par_for_each_row("release", |rel: Revrel| f("rel", rel))?
                .for_each(|()| ());
        } else {
            unreachable!("!read_rev && !read_rel");
        }

        log::info!("Writing...");
        self.write_atomic(suffixes::AUTHOR_ID, authors)?;
        Ok(())
    }
    pub fn write_committer_ids(&self) -> Result<()> {
        #[derive(ArRowDeserialize, Default, Clone)]
        struct Revision {
            id: String,
            committer: Option<Box<[u8]>>,
        }

        if !self.allowed_node_types.contains(&NodeType::Revision) {
            log::info!("Excluded");
            return Ok(());
        }

        let Some(person_mph) = self.person_mph.as_ref() else {
            panic!(
                "write_committer_ids is missing person MPH but allowed_node_types = {:?}",
                self.allowed_node_types
            );
        };

        log::info!("Initializing...");
        let committers = self.init_atomic_vec(u32::MAX.to_be());

        log::info!("Reading...");
        self.par_for_each_row("revision", |rev: Revision| {
            if let Some(person) = rev.committer {
                let swhid = format!("swh:1:rev:{}", rev.id);
                let base64 = base64_simd::STANDARD;
                let person = base64.encode_to_string(person).into_bytes();
                let person_id: u32 = person_mph.hash(person).expect("Unknown person");
                self.set_atomic(&committers, &swhid, person_id.to_be());
            }
        })?
        .for_each(|()| ());

        log::info!("Writing...");
        self.write_atomic(suffixes::COMMITTER_ID, committers)?;
        Ok(())
    }
    pub fn write_messages(&self) -> Result<()> {
        #[derive(ArRowDeserialize, Default, Clone)]
        struct Revrel {
            id: String,
            message: Option<Box<[u8]>>,
        }
        #[derive(ArRowDeserialize, Default, Clone)]
        struct Origin {
            id: String,
            url: String,
        }

        let read_rev = self.allowed_node_types.contains(&NodeType::Revision);
        let read_rel = self.allowed_node_types.contains(&NodeType::Release);
        let read_ori = self.allowed_node_types.contains(&NodeType::Origin);

        if !read_rev && !read_rel && !read_ori {
            log::info!("Excluded");
            return Ok(());
        }

        log::info!("Initializing...");
        let mut offsets = self.init_vec(u64::MAX.to_be());
        let path = suffix_path(&self.target, suffixes::MESSAGE);
        let file = std::fs::File::create(&path)
            .with_context(|| format!("Could not create {}", path.display()))?;
        let mut writer = BufWriter::new(file);

        let base64 = base64_simd::STANDARD;
        let mut offset = 0u64;

        let mut f = |type_: &str, id: String, message: Option<Box<[u8]>>| {
            if let Some(message) = message {
                let swhid = format!("swh:1:{type_}:{id}");
                let mut encoded_message = base64.encode_to_string(message);
                encoded_message.push('\n');
                let encoded_message = encoded_message.as_bytes();
                writer.write_all(encoded_message)?;
                self.set(&mut offsets, &swhid, offset.to_be());
                offset += encoded_message.len() as u64;
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
                f("ori", ori.id, Some(ori.url.as_bytes().into()))
            })?;
        }

        log::info!("Writing offsets...");
        self.write(suffixes::MESSAGE_OFFSET, offsets)?;
        Ok(())
    }
    pub fn write_tag_names(&self) -> Result<()> {
        #[derive(ArRowDeserialize, Default, Clone)]
        struct Release {
            id: String,
            name: Box<[u8]>,
        }

        if !self.allowed_node_types.contains(&NodeType::Release) {
            log::info!("Excluded");
            return Ok(());
        }

        log::info!("Initializing...");
        let mut offsets = self.init_vec(u64::MAX.to_be());
        let path = suffix_path(&self.target, suffixes::TAG_NAME);
        let file = std::fs::File::create(&path)
            .with_context(|| format!("Could not create {}", path.display()))?;
        let mut writer = BufWriter::new(file);

        log::info!("Reading and writing...");
        let base64 = base64_simd::STANDARD;
        let mut offset = 0u64;

        // Can't do it in parallel because we are writing to a single file
        self.for_each_row("release", |rel: Release| {
            let swhid = format!("swh:1:rel:{}", rel.id);
            let mut encoded_name = base64.encode_to_string(rel.name);
            encoded_name.push('\n');
            let encoded_name = encoded_name.as_bytes();
            writer.write_all(encoded_name)?;
            self.set(&mut offsets, &swhid, offset.to_be());
            offset += encoded_name.len() as u64;

            Ok(())
        })?;

        log::info!("Writing offsets...");
        self.write(suffixes::TAG_NAME_OFFSET, offsets)?;
        Ok(())
    }
}
