// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

#![cfg(feature = "compression")]

use std::fs::File;
use std::io::{BufWriter, Write};

use anyhow::{Context, Result};

use swh_graph::compress::label_names::*;
use swh_graph::labels::FilenameId;

#[test]
fn test_build_hasher() -> Result<()> {
    let tmpdir = tempfile::tempdir()?;
    let labels_path = tmpdir.path().join("labels");
    let mphf_path = tmpdir.path().join("mphf");

    let f = BufWriter::new(File::create(&labels_path)?);
    let mut encoder =
        zstd::stream::write::Encoder::new(f, 1).context("Could not create encoder")?;
    let labels = ["abc", "def", "ghijkl", "opqrstuv", "wyx", "z", "foo", "bar"];
    let base64 = base64_simd::STANDARD;
    for label in labels {
        encoder.write_all(base64.encode_to_string(label).as_bytes())?;
        encoder.write_all(b"\n")?;
    }
    let f = encoder.finish().context("Could not finish compressing")?;
    drop(f);

    let mphf = build_hasher(labels_path.clone(), labels.len()).context("Could not build MPHF")?;
    assert_eq!(mphf.len(), labels.len());
    mphf.save(&mphf_path).context("Could not save MPHF")?;

    for (i, label) in labels.iter().enumerate() {
        assert_eq!(
            mphf.hash(LabelName(label.as_bytes())).unwrap(),
            FilenameId(i as u64)
        );
    }

    Ok(())
}
