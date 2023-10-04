// Copyright (C) 2023  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::io::BufWriter;
use std::path::PathBuf;
use std::sync::Mutex;

use anyhow::{Context, Result};
use dsi_bitstream::prelude::{BufBitWriter, WordAdapter, BE};
use dsi_progress_logger::ProgressLogger;
use rayon::prelude::*;
use webgraph::graph::arc_list_graph::NodeIterator;
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

    let bit_write = <BufBitWriter<BE, _>>::new(WordAdapter::new(BufWriter::new(
        std::fs::File::create(&format!("{}.graph", target_dir.to_string_lossy()))
            .context("Could not create target graph file")?,
    )));

    let codes_writer = <DynamicCodesWriter<BE, _>>::new(bit_write, &CompFlags::default());

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
                .iter_from(start)
                .take_while(|(node_id, _successors)| *node_id < end)
                .for_each(|(x, succ)| {
                    succ.into_iter().for_each(|s| {
                        for (x, s) in transformation(x, s).into_iter() {
                            sorter.push((x, s));
                        }
                    })
                });
            pl.lock().unwrap().update_with_count(end - start);
            Ok(())
        },
    )?;
    pl.lock().unwrap().done();

    let mut compression_flags = CompFlags::default();
    compression_flags.compression_window = 1;
    compression_flags.min_interval_length = 4;
    compression_flags.max_ref_count = 3;
    let mut bvcomp = BVComp::new(
        codes_writer,
        compression_flags.compression_window,
        compression_flags.min_interval_length,
        compression_flags.max_ref_count,
        0,
    );
    let mut pl = ProgressLogger::default().display_memory();
    pl.item_name = "node";
    pl.expected_updates = Some(num_nodes);
    pl.local_speed = true;
    pl.start("Writing...");

    let mut adjacency_lists = NodeIterator::new(num_nodes, sorted_arcs).inspect(|_node| {
        pl.light_update();
    });

    bvcomp
        .extend::<Inspect<_, _>>(&mut adjacency_lists)
        .context("Could not write to BVGraph")?;
    bvcomp.flush().context("Could not flush BVGraph")?;
    pl.done();

    drop(temp_dir); // Prevent early deletion

    log::info!("Writing the .properties file");
    let properties = compression_flags.to_properties(num_nodes, graph.num_arcs());
    std::fs::write(
        format!("{}.properties", target_dir.to_string_lossy()),
        properties,
    )
    .context("Could not write .properties file")?;

    Ok(())
}
