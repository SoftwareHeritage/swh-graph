/*
 * SPDX-FileCopyrightText: 2023 Inria
 * SPDX-FileCopyrightText: 2023 Sebastiano Vigna
 *
 * SPDX-License-Identifier: Apache-2.0 OR LGPL-2.1-or-later
 *
 * Imported from https://archive.softwareheritage.org/swh:1:dir:5aef0244039204e3cbe1424f645c9eadcc80956f;origin=https://github.com/vigna/sux-rs;visit=swh:1:snp:855180f9102fd3d7451e98f293cdd90cff7f17d9;anchor=swh:1:rev:9cafac06c95c2d916b76dc374a6f9d976bf65456
 */

use anyhow::Result;
use std::collections::HashSet;
use std::fs::File;
use std::io::BufRead;
use std::io::BufReader;

#[test]
fn test_gov_mph() -> Result<()> {
    let m = swh_graph::java_compat::mph::gov::GOVMPH::load("tests/data/test.cmph")?;
    let reader = BufReader::new(File::open("tests/data/mph.txt")?);
    let mut s = HashSet::new();
    for line in reader.lines() {
        let line = line?;
        let p = m.get_byte_array(line.as_bytes());
        assert!(p < m.size());
        assert!(s.insert(p));
    }
    assert_eq!(s.len(), m.size() as usize);
    Ok(())
}

#[test]
fn test_gov3_sf() -> Result<()> {
    let m = swh_graph::java_compat::sf::gov3::GOV3::load("tests/data/test.csf")?;
    let reader = BufReader::new(File::open("tests/data/mph.txt")?);
    for (idx, line) in reader.lines().enumerate() {
        let line = line?;
        let p = m.get_byte_array(line.as_bytes());
        assert_eq!(p, idx as u64);
    }
    Ok(())
}
