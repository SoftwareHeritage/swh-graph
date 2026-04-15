// Copyright (C) 2024-2026  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use anyhow::{anyhow, bail, ensure, Context, Result};
use dsi_progress_logger::{concurrent_progress_logger, ProgressLog};
use pthash::{BuildConfiguration, Phf};
use rayon::prelude::*;

// For backward compatibility
#[doc(hidden)]
pub use crate::person::{person_struct::PseudonymizedPerson as Person, PersonHasher, PersonMphf};

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

/// Writes the ``fullnames`` in the given order
///
/// The order must match the MPHF that will be distributed alongside it.
fn write_ordered_fullnames<S: AsRef<[u8]>>(
    fullnames: impl IntoIterator<Item = S>,
    fullnames_path: &Path,
    lengths_path: &Path,
) -> Result<()> {
    use dsi_bitstream::prelude::*;

    log::info!("Writing full names and lengths...");
    let mut fullnames_file = File::create(fullnames_path)
        .with_context(|| format!("Could not create {}", fullnames_path.display()))?;
    let lengths_file = File::create(lengths_path)
        .with_context(|| format!("Could not create {}", lengths_path.display()))?;

    let mut lengths_writer = <BufBitWriter<BE, _>>::new(<WordAdapter<u64, _>>::new(
        BufWriter::with_capacity(1 << 20, lengths_file),
    ));

    for fullname in fullnames {
        let fullname = fullname.as_ref();
        fullnames_file.write_all(fullname)?;
        lengths_writer
            .write_gamma(u64::try_from(fullname.len()).context("Name length overflowed u64")?)
            .context("Could not write gamma")?;
    }

    Ok(())
}

pub fn write_fullnames(
    person_hasher: PersonHasher,
    fullnames: impl ParallelIterator<Item = Result<(Box<[u8]>, Box<[u8]>)>>,
    fullnames_path: &Path,
    lengths_path: &Path,
) -> Result<()> {
    let num_persons = usize::try_from(person_hasher.num_persons())?;

    let mut pl = concurrent_progress_logger!(display_memory = true, local_speed = true);
    pl.start("Extracting and sorting full names");

    let sorted_fullnames: Vec<OnceLock<Box<[u8]>>> = vec![OnceLock::new(); num_persons];

    log::info!("Building full names...");
    fullnames
        .try_for_each_with(pl.clone(), |pl, item| -> Result<()> {
            let (fullname, sha256) = item?;
            let person_hash = person_hasher.hash_person_fullname(&fullname)?;
            let base64 = base64_simd::STANDARD;
            ensure!(
                person_hash ==
                person_hasher.hash_pseudonymized_person(
                    base64
                        .encode_to_string(&sha256)
                        .into_bytes()
                        .into_boxed_slice(),
                )?,
                "Inconsistent hashing scheme"
            );
            let person_hash = usize::try_from(person_hash).context("Person hash overflowed usize")?;
            sorted_fullnames
                .get(person_hash)
                .context("Person hash is greater than the number of persons")?
                .set(fullname)
                .map_err(|fullname| {
                    let other_fullname = sorted_fullnames.get(person_hash).unwrap().get().unwrap();
                    anyhow!("Hash collision on SHA256 {sha256:?}, between {fullname:?} and {other_fullname:?}")
                })?;
            pl.update();
            Ok(())
        })?;
    pl.done();
    let sorted_fullnames: Vec<_> = sorted_fullnames
        .into_par_iter()
        .flat_map(|fullname| fullname.into_inner())
        .collect();
    if sorted_fullnames.len() != num_persons {
        bail!(
            "Wrong number of full names, expected {}, got {}",
            num_persons,
            sorted_fullnames.len()
        );
    }

    write_ordered_fullnames(sorted_fullnames, fullnames_path, lengths_path)
}
