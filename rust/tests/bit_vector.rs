// Copyright (C) 2023  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use anyhow::{Context, Result};
use sux::bits::bit_vec::BitVec;
use tempfile::tempdir;

use swh_graph::java_compat::bit_vector::LongArrayBitVector;

#[test]
fn test_write() -> Result<()> {
    let temp_dir = tempdir().context("Could not get tempdir")?;

    for i in [
        0, 1, 2, 3, 4, 10, 20, 30, 31, 32, 33, 34, 62, 63, 64, 65, 66, 98, 99,
    ] {
        let path = temp_dir.path().join(format!("bitvec_{i}.bin"));
        let mut bitvec = BitVec::<Vec<usize>>::new(100);
        bitvec.set(i, true);

        let bit_vector = LongArrayBitVector::new_from_bitvec(bitvec);
        bit_vector.dump(&path).context("Could not dump")?;
    }

    Ok(())
}
