/*
 * Copyright (C) 2023  The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

use anyhow::{Context, Result};
use rayon::prelude::*;

use swh_graph::utils::sort::*;

#[test]
fn test_par_sort_arcs() -> Result<()> {
    let tempdir = tempfile::tempdir().context("temp dir")?;
    assert_eq!(
        par_sort_arcs(
            tempdir.path(),
            100, // sort_batch_size
            vec![(1, 10), (2, 1), (1, 5), (2, 10)].into_par_iter(),
            10, // num_partition
            |buf, (src, dst)| {
                let partition_id = src / 10;
                buf.insert(partition_id, src, dst)
            }
        )
        .context("par_sort_arcs")?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>(),
        vec![(1, 5), (1, 10), (2, 1), (2, 10)],
    );

    Ok(())
}

#[test]
fn test_par_sort_arcs_empty_buffer() -> Result<()> {
    let tempdir = tempfile::tempdir().context("temp dir")?;
    assert_eq!(
        par_sort_arcs(
            tempdir.path(),
            0, // Causes buffers to be flushed immediately, so the last batch will be empty
            vec![(1, 10), (2, 1), (1, 5), (2, 10)].into_par_iter(),
            1, // num_partitions
            |buf, (src, dst)| {
                let partition_id = 0;
                buf.insert(partition_id, src, dst)
            }
        )
        .context("par_sort_arcs")?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>(),
        vec![(1, 5), (1, 10), (2, 1), (2, 10)],
    );

    Ok(())
}
