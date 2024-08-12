// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use anyhow::{bail, ensure, Context, Result};
use clap::Parser;
use dsi_progress_logger::{ProgressLog, ProgressLogger};
use itertools::Itertools;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};

use swh_graph::collections::PathStack;
use swh_graph::graph::*;
use swh_graph::java_compat::mph::gov::GOVMPH;
use swh_graph::labels::FilenameId;
use swh_graph::NodeType;
use swh_graph::SWHID;

use swh_graph::utils::dataset_writer::{CsvZstTableWriter, ParallelDatasetWriter};

#[derive(Parser, Debug)]
/** Reads a list of content and directory SWHIDs, and for each of them, returns
 * their most popular path among their parents, up to the maximum given depth.
 *
 * Contents under 3 bytes are skipped, as they are too long to process (many references
 * to them) and not interesting.
 */
struct Args {
    graph_path: PathBuf,
    #[arg(short, long, action = clap::ArgAction::Count)]
    verbose: u8,
    #[arg(long)]
    /// Expected number of lines (content/directory SWHIDs) in the input,
    /// used for progress reporting.
    expected_nodes: Option<usize>,
    #[arg(long, default_value_t = 2)]
    /// Depth of parent directories to include in the name (1 = only the file's own name, 2 = the most popular name of its direct parents and itself, ...)
    depth: usize,
    #[arg(long)]
    /// Path to a directory where to write CSV files to.
    out: PathBuf,
}

#[allow(non_snake_case)]
#[derive(Deserialize)]
struct InputRecord {
    SWHID: String,
}

#[allow(non_snake_case)]
#[derive(Debug, Serialize)]
struct OutputRecord {
    SWHID: SWHID,
    length: Option<u64>,
    #[serde(with = "serde_bytes")] // Serialize a bytestring instead of list of ints
    filepath: Vec<u8>,
    occurrences: Option<u64>,
}

/// A pair orderable by the second item.
#[derive(Debug, PartialEq, Eq)]
pub struct PathWithOccurences<N: PartialEq, O: PartialOrd>(N, O);

impl<N: PartialEq, O: PartialOrd> PartialOrd for PathWithOccurences<N, O> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.1.partial_cmp(&other.1)
    }
}

impl<N: Eq, O: Ord> Ord for PathWithOccurences<N, O> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.1.cmp(&other.1)
    }
}

pub fn main() -> Result<()> {
    let args = Args::parse();

    match args.depth {
        0 => bail!("0 is not a valid depth"),
        1 => main_monomorphized::<1>(args),
        2 => main_monomorphized::<2>(args),
        3 => main_monomorphized::<3>(args),
        4 => main_monomorphized::<4>(args),
        _ => bail!("--max-depth is not implemented for values over 4."),
    }
}

/// Monomorphized implementation of [`main`]
///
/// This allows working with arrays of length known at compile-time, avoiding overhead
/// of individually storing their size.
fn main_monomorphized<const MAX_DEPTH: usize>(args: Args) -> Result<()> {
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

    let dataset_writer = ParallelDatasetWriter::with_schema(args.out, ())?;

    let mut pl = ProgressLogger::default();
    pl.item_name("node");
    pl.display_memory(true);
    pl.local_speed(true);
    pl.expected_updates(args.expected_nodes);
    pl.start("Writing file names");
    let pl = Arc::new(Mutex::new(pl));

    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_reader(std::io::stdin());

    // Makes sure the input at least has a header, even when there is no payload
    ensure!(
        reader
            .headers()
            .context("Invalid header in input")?
            .iter()
            .any(|item| item == "SWHID"),
        "Input has no 'SWHID' header"
    );

    reader.deserialize().par_bridge().try_for_each_init(
        || dataset_writer.get_thread_writer().unwrap(),
        |writer, line: Result<InputRecord, _>| -> Result<()> {
            let swhid = line.context("Could not deserialize input line")?.SWHID;
            let swhid = SWHID::try_from(swhid.as_str())
                .with_context(|| format!("Could not parse input SWHID {}", swhid))?;
            let node = graph
                .properties()
                .node_id(swhid)
                .with_context(|| format!("Input SWHID {} is not in graph", swhid))?;
            write_content_paths::<MAX_DEPTH, _>(&graph, writer, node)?;
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

fn count_content_paths<const MAX_DEPTH: usize, G>(
    graph: &G,
    leaf: NodeId,
) -> Result<HashMap<[FilenameId; MAX_DEPTH], u64>>
where
    G: SwhGraphWithProperties + SwhLabeledBackwardGraph,
    <G as SwhGraphWithProperties>::Maps: swh_graph::properties::Maps,
{
    let mut stack = vec![leaf];
    let mut path_stack = PathStack::new();
    path_stack.push([]);

    let mut paths = HashMap::<_, u64>::new();

    while let Some(node) = stack.pop() {
        let path: Box<[_]> = path_stack.pop().unwrap().collect();
        let depth = path.len();
        let mut has_parents = false;
        if depth < MAX_DEPTH {
            for (dir, labels) in graph.untyped_labeled_predecessors(node) {
                if graph.properties().node_type(dir) != NodeType::Directory {
                    continue;
                }
                for label in labels {
                    // This is either a cnt->dir or a dir->dir arc, so we know the label
                    // has to be a DirEntry
                    let label: swh_graph::labels::DirEntry = label.into();
                    // This is a dir->cnt arc, so its label has to be a DirEntry
                    path_stack.push(path.iter().copied());
                    path_stack.push_filename(label.filename_id());
                    stack.push(dir);
                }
                has_parents = true;
            }
        }
        if !has_parents {
            let mut path_array = [FilenameId(u64::MAX); MAX_DEPTH];
            for (i, &part) in path.iter().enumerate() {
                path_array[i] = part;
            }
            *paths.entry(path_array).or_insert(0) += 1;
        }
    }

    Ok(paths)
}

fn write_content_paths<const MAX_DEPTH: usize, G>(
    graph: &G,
    writer: &mut CsvZstTableWriter,
    leaf: NodeId,
) -> Result<()>
where
    G: SwhGraphWithProperties + SwhLabeledBackwardGraph,
    <G as SwhGraphWithProperties>::Contents: swh_graph::properties::Contents,
    <G as SwhGraphWithProperties>::Maps: swh_graph::properties::Maps,
    <G as SwhGraphWithProperties>::LabelNames: swh_graph::properties::LabelNames,
{
    // Count the number of occurrences of each name to point to the leaf
    let names = count_content_paths::<MAX_DEPTH, _>(graph, leaf)?;
    let leaf_swhid = graph.properties().swhid(leaf);

    let length = graph.properties().content_length(leaf);

    #[allow(unstable_name_collisions)] // 'intersperse' from itertools
    let build_path = |path: [FilenameId; MAX_DEPTH]| -> Vec<u8> {
        path.into_iter()
            .filter(|filename_id| filename_id.0 != u64::MAX) // Used as padding for shorter paths
            .map(|filename_id| graph.properties().label_name(filename_id))
            .intersperse(b"/".into())
            .flatten()
            .collect()
    };

    if names.is_empty() {
        // No filename at all
        writer
            .serialize(OutputRecord {
                SWHID: leaf_swhid,
                length,
                filepath: vec![],
                occurrences: None,
            })
            .context("Could not write empty record")?;
    } else {
        // Print only the result with the most occurrences.
        let (path, occurrences) = names
            .into_iter()
            .max_by_key(|(_, occurrences)| *occurrences)
            .expect("names is unexpectedly empty"); // We checked the empty case above
        writer
            .serialize(OutputRecord {
                SWHID: leaf_swhid,
                length,
                filepath: build_path(path),
                occurrences: Some(occurrences),
            })
            .context("Could not write record")?;
    }

    Ok(())
}
