// Copyright (C) 2024-2026  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

#![cfg(feature = "compression")]

use std::fs::File;
use std::io::{BufWriter, Write};

use anyhow::{Context, Result};

use swh_graph::compress::label_names::*;
use swh_graph::map::Permutation;

#[test]
fn test_build_mphf_and_order() -> Result<()> {
    let tmpdir = tempfile::tempdir()?;
    let labels_path = tmpdir.path().join("labels.zst");
    let mphf_path = tmpdir.path().join("mphf");

    let mut f = BufWriter::new(
        zstd::Encoder::new(File::create(&labels_path)?, 1)
            .context("Could not build zstd compressor")?
            .auto_finish(),
    );
    let labels = ["abc", "def", "ghijkl", "opqrstuv", "wyx", "z", "foo", "bar"];
    let base64 = base64_simd::STANDARD;
    for label in labels {
        f.write_all(base64.encode_to_string(label).as_bytes())?;
        f.write_all(b"\n")?;
    }
    //compressed_f.into_inner().map_err(|e| e.into_error()).context("Could not flush")?.into_inner();
    drop(f);

    let mphf = build_mphf(labels_path.clone(), labels.len()).context("Could not build MPHF")?;
    assert_eq!(mphf.len(), labels.len());
    let file = File::create(&mphf_path).context("Could not create MPHF file")?;
    mphf.write(&mut BufWriter::new(file))
        .context("Could not save MPHF")?;
    let order =
        build_order(labels_path, mphf_path, labels.len()).context("Could not build order")?;

    for (i, label) in labels.iter().enumerate() {
        assert_eq!(
            order.get(mphf.get(&LabelName(label.as_bytes())).unwrap() as usize),
            Some(i)
        );
    }

    Ok(())
}
