/*
 * Copyright (C) 2023-2024  The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

use std::path::Path;
use std::sync::{Arc, Mutex};

use anyhow::Result;
use dsi_progress_logger::ProgressLogger;
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
    let mut swhids: Vec<SWHID> = Vec::with_capacity(num_nodes);
    let swhids_uninit = swhids.spare_capacity_mut();

    let mut pl = ProgressLogger::default().display_memory();
    pl.item_name = "node";
    pl.local_speed = true;
    pl.expected_updates = Some(num_nodes);
    pl.start("Computing node2swhid");

    par_iter_lines_from_dir(swhids_dir, Arc::new(Mutex::new(pl))).for_each(|line: [u8; 50]| {
        let node_id = order
            .get(mph.hash_str_array(&line).expect("Failed to hash line"))
            .unwrap();
        let swhid = SWHID::try_from(unsafe { std::str::from_utf8_unchecked(&line[..]) })
            .expect("Invalid SWHID");
        assert!(
            node_id < num_nodes,
            "hashing {} returned {}, which is greater than the number of nodes ({})",
            swhid,
            node_id,
            num_nodes
        );

        // Safe because we checked node_id < num_nodes
        unsafe {
            swhids_uninit
                .as_ptr()
                .add(node_id)
                .cast_mut()
                .write(std::mem::MaybeUninit::new(swhid));
        }
    });

    // Assuming the MPH and permutation are correct, we wrote an item at every
    // index.
    unsafe { swhids.set_len(num_nodes) };

    Ok(swhids)
}
