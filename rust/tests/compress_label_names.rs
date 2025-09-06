// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

#![cfg(feature = "compression")]

use std::fs::File;
use std::io::{BufWriter, Write};

use anyhow::{Context, Result};
use pthash::Phf;

use swh_graph::compress::label_names::*;
use swh_graph::map::Permutation;

#[test]
#[cfg_attr(miri, ignore)] // use PTHash which uses the FFI, and Miri does not support that.
fn test_build_mphf_and_order() -> Result<()> {
    let tmpdir = tempfile::tempdir()?;
    let labels_path = tmpdir.path().join("labels");
    let mphf_path = tmpdir.path().join("mphf");

    let mut f = BufWriter::new(File::create(&labels_path)?);
    let labels = ["abc", "def", "ghijkl", "opqrstuv", "wyx", "z", "foo", "bar"];
    let base64 = base64_simd::STANDARD;
    for label in labels {
        f.write_all(base64.encode_to_string(label).as_bytes())?;
        f.write_all(b"\n")?;
    }
    drop(f);

    let mut mphf = build_mphf(labels_path.clone(), labels.len()).context("Could not build MPHF")?;
    assert_eq!(mphf.num_keys(), labels.len() as u64);
    mphf.save(&mphf_path).context("Could not save MPHF")?;
    let order =
        build_order(labels_path, mphf_path, labels.len()).context("Could not build order")?;

    for (i, label) in labels.iter().enumerate() {
        assert_eq!(
            order.get(mphf.hash(LabelName(label.as_bytes())) as usize),
            Some(i)
        );
    }

    Ok(())
}
