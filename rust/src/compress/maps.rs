/*
 * Copyright (C) 2023-2024  The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

use std::path::Path;

use anyhow::Result;
use crossbeam::atomic::AtomicCell;
use dsi_progress_logger::{concurrent_progress_logger, ProgressLog};
use rayon::prelude::*;

use crate::compress::zst_dir::*;
use crate::map::Permutation;
use crate::mph::SwhidMphf;
use crate::SWHID;

pub fn ordered_swhids<MPHF: SwhidMphf + Sync + Send, P: Permutation + Sync + Send>(
    swhids_dir: &Path,
    order: P,
    mph: MPHF,
    num_nodes: usize,
) -> Result<Vec<SWHID>> {
    let swhids: Vec<_> = (0..num_nodes)
        .into_par_iter()
        .map(|_| {
            AtomicCell::new(SWHID {
                node_type: crate::NodeType::Content, // serializes to 0
                hash: Default::default(),            // ditto
                namespace_version: 0, // so the compiler can memset the whole region to 0
            })
        })
        .collect();

    let mut pl = concurrent_progress_logger!(
        display_memory = true,
        item_name = "node",
        local_speed = true,
        expected_updates = Some(num_nodes),
    );
    pl.start("Computing node2swhid");

    par_iter_lines_from_dir(swhids_dir, pl.clone()).for_each(|line: [u8; 50]| {
        let node_id = order
            .get(mph.hash_str_array(&line).expect("Failed to hash line"))
            .unwrap();
        let swhid = SWHID::try_from(unsafe { std::str::from_utf8_unchecked(&line[..]) })
            .expect("Invalid SWHID");
        assert!(
            node_id < num_nodes,
            "hashing {swhid} returned {node_id}, which is greater than the number of nodes ({num_nodes})"
        );

        swhids[node_id].store(swhid);
    });

    pl.done();

    // Assuming the MPH and permutation are correct, we wrote an item at every
    // index.
    // Compiled to a no-op
    let swhids = swhids.into_iter().map(|swhid| swhid.into_inner()).collect();

    Ok(swhids)
}
