// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::collections::{BinaryHeap, HashMap};
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use clap::Parser;
use dsi_progress_logger::{ProgressLog, ProgressLogger};
use rayon::prelude::*;
use serde::Serialize;

use swh_graph::graph::*;
use swh_graph::java_compat::mph::gov::GOVMPH;
use swh_graph::SWHType;
use swh_graph::SWHID;

use swh_graph::utils::dataset_writer::{CsvZstTableWriter, ParallelDatasetWriter};

#[derive(Parser, Debug)]
/** Computes, for every content object, the list of names it directories refer to it as.
 */
struct Args {
    graph_path: PathBuf,
    #[arg(short, long, action = clap::ArgAction::Count)]
    verbose: u8,
    #[arg(long, default_value_t = 0)]
    /// Maximum number of names to print for each content, or 0 if no limit.
    ///
    /// Less popular names are dropped.
    max_results: usize,
    #[arg(long, default_value_t = 0)]
    /// Ignore names with less than this number of occurrences
    min_occurrences: u64,
    #[arg(long)]
    /// Path to a directory where to write CSV files to.
    out: PathBuf,
}

#[allow(non_snake_case)]
#[derive(Debug, Serialize)]
struct OutputRecord {
    SWHID: SWHID,
    length: i64,
    #[serde(with = "serde_bytes")] // Serialize a bytestring instead of list of ints
    filename: Vec<u8>,
    occurrences: Option<u64>,
}

/// A pair orderable by the second item.
#[derive(Debug, PartialEq, Eq)]
pub struct NameWithOccurences<N: PartialEq, O: PartialOrd>(N, O);

impl<N: PartialEq, O: PartialOrd> PartialOrd for NameWithOccurences<N, O> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.1.partial_cmp(&other.1)
    }
}

impl<N: Eq, O: Ord> Ord for NameWithOccurences<N, O> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.1.cmp(&other.1)
    }
}

pub fn main() -> Result<()> {
    let args = Args::parse();

    stderrlog::new()
        .verbosity(args.verbose as usize)
        .timestamp(stderrlog::Timestamp::Second)
        .init()
        .context("While Initializing the stderrlog")?;

    log::info!("Loading graph");
    let graph = swh_graph::graph::load_bidirectional(args.graph_path)
        .context("Could not load graph")?
        .load_backward_labels()
        .context("Could not load labels")?
        .init_properties()
        .load_properties(|props| props.load_contents())
        .context("Could not load content properties")?
        .load_properties(|props| props.load_maps::<GOVMPH>())
        .context("Could not load maps")?
        .load_properties(|props| props.load_label_names())
        .context("Could not load label names")?;
    log::info!("Graph loaded.");

    let dataset_writer = ParallelDatasetWriter::new_with_schema(args.out, ())?;

    let mut pl = ProgressLogger::default();
    pl.item_name("node");
    pl.display_memory(true);
    pl.local_speed(true);
    pl.expected_updates(Some(graph.num_nodes()));
    pl.start("Writing file names");
    let pl = Arc::new(Mutex::new(pl));

    swh_graph::utils::shuffle::par_iter_shuffled_range(0..graph.num_nodes()).try_for_each_init(
        || dataset_writer.get_thread_writer().unwrap(),
        |writer, node| -> Result<()> {
            if graph.properties().node_type(node) == SWHType::Content {
                write_content_names(
                    &graph,
                    writer,
                    args.max_results.try_into().ok(),
                    args.min_occurrences,
                    node,
                )?;
            }
            if node % 32768 == 0 {
                pl.lock().unwrap().update_with_count(32768);
            }
            Ok(())
        },
    )?;

    dataset_writer.close()?;

    pl.lock().unwrap().done();

    Ok(())
}

fn write_content_names<G>(
    graph: &G,
    writer: &mut CsvZstTableWriter,
    max_results: Option<NonZeroUsize>,
    min_occurrences: u64,
    cnt: NodeId,
) -> Result<()>
where
    G: SwhGraphWithProperties + SwhLabelledBackwardGraph,
    <G as SwhGraphWithProperties>::Contents: swh_graph::properties::Contents,
    <G as SwhGraphWithProperties>::Maps: swh_graph::properties::Maps,
    <G as SwhGraphWithProperties>::LabelNames: swh_graph::properties::LabelNames,
{
    let cnt_swhid = graph.properties().swhid(cnt);
    let length: i64 = match graph.properties().content_length(cnt) {
        None => -1,
        Some(length) => length.try_into().context("Content length overflowed i64")?,
    };

    // Count the number of occurrences of each name to point to the content
    let mut names = HashMap::<_, u64>::new();
    for (dir, labels) in graph.labelled_predecessors(cnt) {
        if graph.properties().node_type(dir) != SWHType::Directory {
            continue;
        }
        for label in labels {
            *names.entry(label.filename_id()).or_default() += 1;
        }
    }

    if names.is_empty() {
        // No filename at all
        writer
            .serialize(OutputRecord {
                SWHID: cnt_swhid,
                length,
                filename: vec![],
                occurrences: None,
            })
            .context("Could not write empty record")?;
    } else if max_results.is_none() || usize::from(max_results.unwrap()) >= names.len() {
        // Print everything
        for (filename_id, occurrences) in names {
            if occurrences >= min_occurrences {
                writer
                    .serialize(OutputRecord {
                        SWHID: cnt_swhid,
                        length,
                        filename: graph.properties().label_name(filename_id),
                        occurrences: Some(occurrences),
                    })
                    .context("Could not write record")?;
            }
        }
    } else if max_results.map(usize::from) == Some(1) {
        // Print only the result with the most occurrences.
        // This case could bemerged with the one below, but avoiding the priority heap
        // has much better performance.
        let (filename_id, occurrences) = names
            .into_iter()
            .max_by_key(|(_, occurrences)| *occurrences)
            .expect("names is unexpectedly empty"); // We checked the empty case above
        writer
            .serialize(OutputRecord {
                SWHID: cnt_swhid,
                length,
                filename: graph.properties().label_name(filename_id),
                occurrences: Some(occurrences),
            })
            .context("Could not write record")?;
    } else {
        // Print only results with the most occurrences
        let mut heap: BinaryHeap<_> = names
            .into_iter()
            .filter(|(_, occurrences)| *occurrences >= min_occurrences)
            .map(|(filename_id, occurrences)| NameWithOccurences(filename_id, occurrences))
            .collect();

        // We checked the None case above
        let max_results = max_results.expect("max_results unexpected none").into();

        // FIXME: Use into_iter_sorted once https://github.com/rust-lang/rust/issues/59278
        // is stabilized.
        for _ in 0..max_results {
            let Some(NameWithOccurences(filename_id, occurrences)) = heap.pop() else {
                // Breaking instead of continuing, because the next items can't have more
                // occurrences, as we are iterating the heap in descending order
                break;
            };

            writer
                .serialize(OutputRecord {
                    SWHID: cnt_swhid,
                    length,
                    filename: graph.properties().label_name(filename_id),
                    occurrences: Some(occurrences),
                })
                .context("Could not write record")?;
        }
    }

    Ok(())
}
