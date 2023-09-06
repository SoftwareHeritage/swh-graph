// Copyright (C) 2023  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::io::BufWriter;
use std::path::PathBuf;
use std::sync::Mutex;

use anyhow::{Context, Result};
use dsi_bitstream::prelude::{BufferedBitStreamWrite, FileBackend, LE};
use dsi_progress_logger::ProgressLogger;
use rayon::prelude::*;
use webgraph::prelude::*;

use crate::utils::sort::par_sort_arcs;

/// Writes a new graph on disk, obtained by applying the function to all arcs
/// on the source graph.
pub fn transform<F, G, Iter>(
    input_batch_size: usize,
    sort_batch_size: usize,
    graph: G,
    transformation: F,
    target_dir: PathBuf,
) -> Result<()>
where
    F: Fn(usize, usize) -> Iter + Send + Sync,
    Iter: IntoIterator<Item = (usize, usize)>,
    G: RandomAccessGraph + Sync,
{
    // Adapted from https://github.com/vigna/webgraph-rs/blob/08969fb1ac4ea59aafdbae976af8e026a99c9ac5/src/bin/perm.rs
    let num_nodes = graph.num_nodes();

    let bit_write =
        <BufferedBitStreamWrite<LE, _>>::new(<FileBackend<u64, _>>::new(BufWriter::new(
            std::fs::File::create(target_dir).context("Could not create target graph file")?,
        )));

    let codes_writer = DynamicCodesWriter::new(bit_write, &CompFlags::default());

    let temp_dir = tempfile::tempdir().context("Could not get temporary_directory")?;

    let mut pl = ProgressLogger::default().display_memory();
    pl.item_name = "node";
    pl.expected_updates = Some(num_nodes);
    pl.local_speed = true;
    pl.start("Reading and sorting...");
    let pl = Mutex::new(pl);

    // Merge sorted arc lists into a single sorted arc list
    let sorted_arcs = par_sort_arcs(
        temp_dir.path(),
        sort_batch_size,
        (0usize..=((num_nodes - 1) / input_batch_size)).into_par_iter(),
        |sorter, batch_id| {
            let start = batch_id * input_batch_size;
            let end = (batch_id + 1) * input_batch_size;
            graph // Not using PermutedGraph in order to avoid blanket iter_nodes_from
                .iter_nodes_from(start)
                .take_while(|(node_id, _successors)| *node_id < end)
                .for_each(|(x, succ)| {
                    succ.for_each(|s| {
                        for (x, s) in transformation(x, s).into_iter() {
                            sorter.push((x, s, ()));
                        }
                    })
                });
            pl.lock().unwrap().update_with_count(end - start);
            Ok(())
        },
    )?;
    pl.lock().unwrap().done();

    let g = COOIterToGraph::new(num_nodes, sorted_arcs);

    let mut bvcomp = BVComp::new(codes_writer, 1, 4, 3, 0);
    let mut pl = ProgressLogger::default().display_memory();
    pl.item_name = "node";
    pl.expected_updates = Some(num_nodes);
    pl.local_speed = true;
    pl.start("Writing...");
    let pl = std::cell::RefCell::new(pl);

    bvcomp
        .extend(g.iter_nodes().enumerate().map(|(i, node)| {
            if i % 32768 == 0 {
                pl.borrow_mut().update_with_count(32768)
            }
            node
        }))
        .context("Could not write to BVGraph")?;
    bvcomp.flush().context("Could not flush BVGraph")?;
    pl.borrow_mut().done();

    drop(temp_dir); // Prevent early deletion

    Ok(())
}
