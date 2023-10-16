// Copyright (C) 2023  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::io::Write;
use std::path::PathBuf;

use anyhow::{Context, Result};
use orcxx::deserialize::{OrcDeserialize, OrcStruct};
use orcxx::reader::Reader;
use orcxx::row_iterator::RowIterator;
use orcxx_derive::OrcDeserialize;
use rayon::prelude::*;

use super::orc::{get_dataset_readers, ORC_BATCH_SIZE};
use crate::map::{MappedPermutation, Permutation};
use crate::mph::SwhidMphf;
use crate::properties::suffixes;
use crate::utils::suffix_path;

pub struct PropertyWriter<MPHF: SwhidMphf> {
    pub mph: MPHF,
    pub order: MappedPermutation,
    pub num_nodes: usize,
    pub dataset_dir: PathBuf,
    pub target: PathBuf,
}

impl<MPHF: SwhidMphf + Sync> PropertyWriter<MPHF> {
    fn for_each_row<Row: OrcDeserialize + Clone + OrcStruct + Send>(
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

    fn set<Value>(&self, vector: &Vec<Value>, swhid: &str, value: Value) {
        let node_id = self
            .order
            .get(
                self.mph
                    .hash_str(&swhid)
                    .unwrap_or_else(|| panic!("unknown SWHID {}", swhid)),
            )
            .unwrap();
        unsafe {
            vector
                .as_ptr()
                .offset(node_id as isize)
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

        log::info!("Initializing...");
        let timestamps = vec![i64::MIN.to_be(); self.num_nodes];
        let timestamp_offsets = vec![i16::MIN.to_be(); self.num_nodes];

        let f = |type_: &str, r: Revrel| {
            if let Some(date) = r.date {
                let swhid = format!("swh:1:{}:{}", type_, r.id);
                self.set(&timestamps, &swhid, date.seconds.to_be());
                if let Some(date_offset) = r.date_offset {
                    self.set(&timestamp_offsets, &swhid, date_offset.to_be());
                }
            }
        };

        log::info!("Reading...");
        [].into_par_iter()
            .chain(self.for_each_row("revision", |rev: Revrel| f("rev", rev)))
            .chain(self.for_each_row("release", |rel: Revrel| f("rel", rel)))
            .for_each(|()| ());

        log::info!("Writing...");
        self.write(suffixes::AUTHOR_TIMESTAMP, timestamps)?;
        self.write(suffixes::AUTHOR_TIMESTAMP_OFFSET, timestamp_offsets)?;

        Ok(())
    }
    pub fn write_committer_timestamps(&self) -> Result<()> {
        Ok(())
    }
    pub fn write_content_lengths(&self) -> Result<()> {
        Ok(())
    }
    pub fn write_content_is_skipped(&self) -> Result<()> {
        Ok(())
    }
    pub fn write_author_ids(&self) -> Result<()> {
        Ok(())
    }
    pub fn write_committer_ids(&self) -> Result<()> {
        Ok(())
    }
    pub fn write_messages(&self) -> Result<()> {
        Ok(())
    }
    pub fn write_tag_names(&self) -> Result<()> {
        Ok(())
    }
}
