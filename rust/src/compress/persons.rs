// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use dsi_progress_logger::{concurrent_progress_logger, ProgressLog};
use pthash::{
    BuildConfiguration, DictionaryDictionary, Hashable, Minimal, MurmurHash2_64, PartitionedPhf,
    Phf,
};

pub struct Person<T: AsRef<[u8]>>(pub T);

impl<T: AsRef<[u8]>> Hashable for Person<T> {
    type Bytes<'a>
        = &'a [u8]
    where
        T: 'a;
    fn as_bytes(&self) -> Self::Bytes<'_> {
        self.0.as_ref()
    }
}

// pthash requires 128-bits hash when using over 2^30 keys, and the 2024-05-16 production
// graph has just over 2^32 keys
pub type PersonMphf = PartitionedPhf<Minimal, MurmurHash2_64, DictionaryDictionary>;

fn iter_persons(path: &Path) -> Result<impl Iterator<Item = Person<Box<[u8]>>>> {
    let persons_file =
        File::open(path).with_context(|| format!("Could not open {}", path.display()))?;
    Ok(BufReader::new(persons_file).lines().map(move |person| {
        Person(
            person
                .expect("Could not decode persons as UTF-8")
                .into_bytes()
                .into_boxed_slice(),
        )
    }))
}

/// Reads base64-encoded persons from the path and return a MPH function for them.
pub fn build_mphf(path: PathBuf, num_persons: usize) -> Result<PersonMphf> {
    let mut pass_counter = 0;
    let iter_persons = || {
        pass_counter += 1;
        let mut pl = concurrent_progress_logger!(
            display_memory = true,
            item_name = "person",
            local_speed = true,
            expected_updates = Some(num_persons),
        );
        pl.start(format!("Reading persons (pass #{pass_counter})"));
        iter_persons(&path)
            .expect("Could not read persons")
            .inspect(move |_| pl.light_update())
    };
    let temp_dir = tempfile::tempdir().unwrap();

    // TODO: tweak those for performance
    let mut config = BuildConfiguration::new(temp_dir.path().to_owned());
    config.num_threads = num_cpus::get() as u64;

    log::info!("Building MPH with parameters: {:?}", config);

    let mut f = PersonMphf::new();
    f.build_in_internal_memory_from_bytes(iter_persons, &config)
        .context("Failed to build MPH")?;
    Ok(f)
}

#[derive(Clone, Copy)]
pub struct PersonHasher<'a> {
    mphf: &'a PersonMphf,
}

impl<'a> PersonHasher<'a> {
    pub fn new(mphf: &'a PersonMphf) -> Self {
        PersonHasher { mphf }
    }

    pub fn mphf(&self) -> &'a PersonMphf {
        self.mphf
    }

    pub fn hash<T: AsRef<[u8]>>(&self, person_name: T) -> Result<u32> {
        Ok(self
            .mphf
            .hash(Person(person_name))
            .try_into()
            .expect("person MPH overflowed"))
    }
}
