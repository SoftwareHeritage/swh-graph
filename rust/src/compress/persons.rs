// Copyright (C) 2024-2026  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::OnceLock;

use anyhow::{anyhow, bail, ensure, Context, Result};
use dsi_progress_logger::{concurrent_progress_logger, progress_logger, ProgressLog};
use ph::fmph::GOFunction;
use ph::fmph::{GOBuildConf, GOConf};
use rayon::prelude::*;

use crate::utils::AtomicFile;

// For backward compatibility
#[doc(hidden)]
pub use crate::person::{
    person_struct::PseudonymizedPerson, PersonFmphgo, PersonHasher, PersonMphf,
};

fn iter_persons(path: &Path) -> Result<impl Iterator<Item = PseudonymizedPerson<Box<[u8]>>>> {
    let file = File::open(path).with_context(|| format!("Could not open {}", path.display()))?;
    let decoder = zstd::stream::read::Decoder::new(file)
        .with_context(|| format!("Could not decompress {} as zstd", path.display()))?;
    Ok(BufReader::new(decoder).lines().map(move |person| {
        PseudonymizedPerson(
            person
                .expect("Could not decode persons as UTF-8")
                .into_bytes()
                .into_boxed_slice(),
        )
    }))
}

/// Reads base64-encoded persons from the path and return a MPH function for them.
pub fn build_mphf(path: PathBuf, num_persons: usize) -> Result<PersonFmphgo> {
    let pass_counter = AtomicU32::new(1);
    let iter_persons = || {
        let pass_counter = pass_counter.fetch_add(1, Ordering::Relaxed);
        let mut pl = progress_logger!(
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
    let get_iter = iter_persons; // no support for parallel iteration
    let clone_threshold = 1_000_000; // arbitrary
    let key_set =
        ph::fmph::keyset::CachedKeySet::dynamic_with_len(get_iter, num_persons, clone_threshold);

    let conf = GOBuildConf::new(GOConf::default_bigger());
    let mphf = GOFunction::with_conf(key_set, conf);
    let len = mphf.len();
    ensure!(
        len == num_persons,
        "Built MPHF from {num_persons}, but its range is {len}"
    );
    Ok(PersonFmphgo(mphf))
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
    let fullnames_file = AtomicFile::create_new(fullnames_path)
        .with_context(|| format!("Could not create {}", fullnames_path.display()))?;
    let lengths_file = AtomicFile::create_new(lengths_path)
        .with_context(|| format!("Could not create {}", lengths_path.display()))?;

    let mut lengths_writer = <BufBitWriter<BE, _>>::new(<WordAdapter<u64, _>>::new(
        BufWriter::with_capacity(1 << 20, lengths_file),
    ));

    let mut fullnames_writer = BufWriter::new(fullnames_file);
    for fullname in fullnames {
        let fullname = fullname.as_ref();
        fullnames_writer
            .write_all(fullname)
            .context("Could not write fullname")?;
        lengths_writer
            .write_gamma(u64::try_from(fullname.len()).context("Name length overflowed u64")?)
            .context("Could not write gamma")?;
    }
    let fullnames_file = fullnames_writer
        .into_inner()
        .map_err(|e| e.into_error())
        .context("Could not flush fullnames")?;

    fullnames_file
        .commit()
        .with_context(|| format!("Could not commit {}", fullnames_path.display()))?;
    let lengths_file = lengths_writer
        .into_inner()
        .context("Could not flush lengths")?
        .into_inner()
        .into_inner()
        .map_err(|e| e.into_error())
        .context("Could not flush lengths")?;
    lengths_file
        .commit()
        .with_context(|| format!("Could not commit {}", lengths_path.display()))?;

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
