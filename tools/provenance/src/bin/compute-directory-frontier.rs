// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::cell::RefCell;
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::{bail, ensure, Context, Result};
use clap::Parser;
use dsi_progress_logger::{ProgressLog, ProgressLogger};
use rayon::prelude::*;
use serde::Serialize;

use swh_graph::collections::{AdaptiveNodeSet, NodeSet};
use swh_graph::graph::*;
use swh_graph::java_compat::mph::gov::GOVMPH;
use swh_graph::labels::FilenameId;
use swh_graph::utils::mmap::NumberMmap;
use swh_graph::utils::GetIndex;
use swh_graph::{SWHType, SWHID};

#[derive(Parser, Debug)]
/** Given as input a binary file with, for each directory, the newest date of first
 * occurrence of any of the content in its subtree (well, DAG), ie.,
 * max_{for all content} (min_{for all occurrence of content} occurrence).
 * Produces the "provenance frontier", as defined in
 * https://gitlab.softwareheritage.org/swh/devel/swh-provenance/-/blob/ae09086a3bd45c7edbc22691945b9d61200ec3c2/swh/provenance/algos/revision.py#L210
 */
struct Args {
    graph_path: PathBuf,
    #[arg(short, long, action = clap::ArgAction::Count)]
    verbose: u8,
    #[arg(long)]
    /// Path to read the array of max timestamps from
    max_timestamps: PathBuf,
    #[arg(long)]
    directories_out: PathBuf,
}

#[allow(non_snake_case)]
#[derive(Debug, Serialize)]
struct OutputRecord {
    max_author_date: i64,
    frontier_dir_SWHID: SWHID,
    rev_author_date: chrono::DateTime<chrono::Utc>,
    rev_SWHID: SWHID,
    #[serde(with = "serde_bytes")] // Serialize a bytestring instead of list of ints
    path: Vec<u8>,
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
        .load_forward_labels()
        .context("Could not load labels")?
        .init_properties()
        .load_properties(|props| props.load_label_names())
        .context("Could not load label names")?
        .load_properties(|props| props.load_maps::<GOVMPH>())
        .context("Could not load maps")?
        .load_properties(|props| props.load_timestamps())
        .context("Could not load timestamps")?;
    log::info!("Graph loaded.");

    let max_timestamps =
        NumberMmap::<byteorder::BE, i64, _>::new(&args.max_timestamps, graph.num_nodes())
            .with_context(|| format!("Could not mmap {}", args.max_timestamps.display()))?;

    std::fs::create_dir_all(&args.directories_out)
        .with_context(|| format!("Could not create {}", args.directories_out.display()))?;

    find_frontiers(&graph, &max_timestamps, args.directories_out)
}

fn find_frontiers<G>(
    graph: &G,
    max_timestamps: impl GetIndex<Output = i64> + Sync + Copy,
    output_dir: PathBuf,
) -> Result<()>
where
    G: SwhBackwardGraph + SwhLabelledForwardGraph + SwhGraphWithProperties + Send + Sync + 'static,
    <G as SwhGraphWithProperties>::LabelNames: swh_graph::properties::LabelNames,
    <G as SwhGraphWithProperties>::Maps: swh_graph::properties::Maps,
    <G as SwhGraphWithProperties>::Timestamps: swh_graph::properties::Timestamps,
{
    let mut pl = ProgressLogger::default();
    pl.item_name("node");
    pl.display_memory(true);
    pl.local_speed(true);
    pl.expected_updates(Some(graph.num_nodes()));
    pl.start("Visiting revisions' directories...");
    let pl = Arc::new(Mutex::new(pl));

    let worker_id = AtomicU64::new(0);

    // Reuse writers across work batches, or we end up with millions of very small files
    let writers = thread_local::ThreadLocal::new();

    (0..graph.num_nodes()).into_par_iter().try_for_each_init(
        || {
            writers
                .get_or(|| {
                    let path = output_dir.join(format!(
                        "{}.csv.zst",
                        worker_id.fetch_add(1, Ordering::Relaxed)
                    ));
                    let file = std::fs::File::create(&path)
                        .with_context(|| format!("Could not create {}", path.display()))
                        .unwrap();
                    let compression_level = 3;
                    let zstd_encoder = zstd::stream::write::Encoder::new(file, compression_level)
                        .with_context(|| {
                            format!("Could not create ZSTD encoder for {}", path.display())
                        })
                        .unwrap()
                        .auto_finish();
                    RefCell::new(
                        csv::WriterBuilder::new()
                            .has_headers(true)
                            .terminator(csv::Terminator::CRLF)
                            .from_writer(zstd_encoder),
                    )
                })
                .borrow_mut()
        },
        |writer, node| {
            let node_type = graph
                .properties()
                .node_type(node)
                .expect("missing node type");

            let res = match node_type {
                SWHType::Release => {
                    // Allow releases
                    find_frontiers_in_release(graph, max_timestamps, writer, node)
                }
                SWHType::Revision => {
                    // Allow revisions only if they are a "snapshot head" (ie. one of their
                    // predecessors is a release or a snapshot)
                    if graph.predecessors(node).into_iter().any(|pred| {
                        let pred_type = graph
                            .properties()
                            .node_type(pred)
                            .expect("missing node type");
                        pred_type == SWHType::Snapshot || pred_type == SWHType::Release
                    }) {
                        // Head revision
                        find_frontiers_in_revision(graph, max_timestamps, writer, node)
                    } else {
                        Ok(()) // Ignore this non-HEAD revision
                    }
                }
                _ => Ok(()), // Ignore this non-rev/rel node
            };

            if node % 32768 == 0 {
                pl.lock().unwrap().update_with_count(32768);
            }

            res
        },
    )?;

    pl.lock().unwrap().done();

    log::info!("Visits done, finishing output");

    Ok(())
}

fn find_frontiers_in_release<G, W: Write>(
    graph: &G,
    max_timestamps: impl GetIndex<Output = i64> + Copy,
    writer: &mut csv::Writer<W>,
    rel_id: NodeId,
) -> Result<()>
where
    G: SwhLabelledForwardGraph + SwhGraphWithProperties,
    <G as SwhGraphWithProperties>::LabelNames: swh_graph::properties::LabelNames,
    <G as SwhGraphWithProperties>::Maps: swh_graph::properties::Maps,
    <G as SwhGraphWithProperties>::Timestamps: swh_graph::properties::Timestamps,
{
    let rel_swhid = graph.properties().swhid(rel_id).expect("Missing SWHID");

    let mut root_dir = None;
    let mut root_rev = None;
    for succ in graph.successors(rel_id) {
        let node_type = graph
            .properties()
            .node_type(succ)
            .expect("Missing node type");
        match node_type {
            SWHType::Directory => {
                ensure!(
                    root_dir.is_none(),
                    "{rel_swhid} has more than one directory successor",
                );
                root_dir = Some(succ);
            }
            SWHType::Revision => {
                ensure!(
                    root_rev.is_none(),
                    "{rel_swhid} has more than one revision successor",
                );
                root_rev = Some(succ);
            }
            _ => (),
        }
    }

    match (root_dir, root_rev) {
        (Some(_), Some(_)) => {
            bail!("{rel_swhid} has both a directory and a revision as successors",)
        }
        (None, Some(root_rev)) => {
            let mut root_dir = None;
            for succ in graph.successors(root_rev) {
                let node_type = graph
                    .properties()
                    .node_type(succ)
                    .expect("Missing node type");
                match node_type {
                    SWHType::Directory => {
                        let rev_swhid = graph.properties().swhid(succ).expect("Missing SWHID");
                        ensure!(
                            root_dir.is_none(),
                            "{rel_swhid} (via {rev_swhid}) has more than one directory successor",
                        );
                        root_dir = Some(succ);
                    }
                    _ => (),
                }
            }
            match root_dir {
                Some(root_dir) => find_frontiers_in_directory(
                    graph,
                    max_timestamps,
                    writer,
                    rel_id,
                    rel_swhid,
                    root_dir,
                ),
                None => Ok(()),
            }
        }
        (Some(root_dir), None) => {
            find_frontiers_in_directory(graph, max_timestamps, writer, rel_id, rel_swhid, root_dir)
        }
        (None, None) => Ok(()),
    }
}

fn find_frontiers_in_revision<G, W: Write>(
    graph: &G,
    max_timestamps: impl GetIndex<Output = i64> + Copy,
    writer: &mut csv::Writer<W>,
    rev_id: NodeId,
) -> Result<()>
where
    G: SwhLabelledForwardGraph + SwhGraphWithProperties,
    <G as SwhGraphWithProperties>::LabelNames: swh_graph::properties::LabelNames,
    <G as SwhGraphWithProperties>::Maps: swh_graph::properties::Maps,
    <G as SwhGraphWithProperties>::Timestamps: swh_graph::properties::Timestamps,
{
    let rev_swhid = graph.properties().swhid(rev_id).expect("Missing SWHID");

    let mut root_dir = None;
    for succ in graph.successors(rev_id) {
        let node_type = graph
            .properties()
            .node_type(succ)
            .expect("Missing node type");
        match node_type {
            SWHType::Directory => {
                let rev_swhid = graph.properties().swhid(succ).expect("Missing SWHID");
                ensure!(
                    root_dir.is_none(),
                    "{rev_swhid} has more than one directory successor",
                );
                root_dir = Some(succ);
            }
            _ => (),
        }
    }

    match root_dir {
        Some(root_dir) => {
            find_frontiers_in_directory(graph, max_timestamps, writer, rev_id, rev_swhid, root_dir)
        }
        None => Ok(()),
    }
}

/// Value in the path_stack between two lists of path parts
const PATH_SEPARATOR: FilenameId = FilenameId(u64::MIN);

fn find_frontiers_in_directory<G, W: Write>(
    graph: &G,
    max_timestamps: impl GetIndex<Output = i64>,
    writer: &mut csv::Writer<W>,
    revrel_id: NodeId,
    revrel_swhid: SWHID,
    root_dir_id: NodeId,
) -> Result<()>
where
    G: SwhLabelledForwardGraph + SwhGraphWithProperties,
    <G as SwhGraphWithProperties>::LabelNames: swh_graph::properties::LabelNames,
    <G as SwhGraphWithProperties>::Maps: swh_graph::properties::Maps,
    <G as SwhGraphWithProperties>::Timestamps: swh_graph::properties::Timestamps,
{
    let Some(revrel_timestamp) = graph.properties().author_timestamp(revrel_id) else {
        return Ok(());
    };
    let revrel_author_date =
        chrono::DateTime::from_timestamp(revrel_timestamp, 0).expect("Could not convert timestamp");

    let mut visited = AdaptiveNodeSet::new(graph.num_nodes());
    let mut stack = Vec::new();

    // flattened list of paths. Each list is made of parts represented by an id,
    // and lists are separated by PATH_SEPARATOR
    let mut path_stack = Vec::new();

    // Do not push the root directory directly on the stack, because it is not allowed to be considered
    // a frontier. Instead, we push the root directory's successors.

    for (succ, labels) in graph.labelled_successors(root_dir_id) {
        if graph
            .properties()
            .node_type(succ)
            .expect("Missing node type")
            == SWHType::Directory
        {
            // If the same subdir/file is present in a directory twice under the same name,
            // pick any name to represent both.
            let Some(first_label) = labels.into_iter().next() else {
                bail!(
                    "{} -> {} has no labels",
                    graph
                        .properties()
                        .swhid(root_dir_id)
                        .expect("Missing SWHID"),
                    graph.properties().swhid(succ).expect("Missing SWHID"),
                )
            };
            stack.push(succ);
            path_stack.push(PATH_SEPARATOR);
            path_stack.push(first_label.filename_id());
        }
    }

    while let Some(node) = stack.pop() {
        if visited.contains(node) {
            continue;
        }
        visited.insert(node);

        let mut path_parts = Vec::new();
        for &filename_id in path_stack.iter().rev() {
            if filename_id == PATH_SEPARATOR {
                break;
            }
            path_parts.push(filename_id);
        }

        let dir_max_timestamp = max_timestamps.get(node).expect("max_timestamps too small");
        if dir_max_timestamp == i64::MIN {
            // Somehow does not have a max timestamp. Presumably because it does not
            // have any content.
            continue;
        }

        // Detect if a node is a frontier according to
        // https://gitlab.softwareheritage.org/swh/devel/swh-provenance/-/blob/ae09086a3bd45c7edbc22691945b9d61200ec3c2/swh/provenance/algos/revision.py#L210
        let mut is_frontier = false;
        if dir_max_timestamp < revrel_timestamp {
            // All content is earlier than revision

            // No need to check if it's depth > 1, given that we excluded the root dir above */
            if graph.successors(node).into_iter().any(|succ| {
                graph
                    .properties()
                    .node_type(succ)
                    .expect("Missing node type")
                    == SWHType::Content
            }) {
                // Contains at least one blob
                is_frontier = true;
            }
        }

        if is_frontier {
            let mut path = Vec::with_capacity(path_parts.len() * 2 + 1);
            for part in path_parts {
                path.extend(
                    graph
                        .properties()
                        .label_name(part)
                        .expect("Unknown filename id"),
                );
                path.push(b'/');
            }
            writer
                .serialize(OutputRecord {
                    max_author_date: dir_max_timestamp,
                    frontier_dir_SWHID: graph.properties().swhid(node).expect("Missing SWHID"),
                    rev_author_date: revrel_author_date,
                    rev_SWHID: revrel_swhid,
                    path,
                })
                .context("Could not write record")?;
        } else {
            // Look for frontiers in subdirectories
            for (succ, labels) in graph.labelled_successors(node) {
                if visited.contains(succ) {
                    continue;
                }
                if !visited.contains(succ)
                    && graph
                        .properties()
                        .node_type(succ)
                        .expect("Missing node type")
                        == SWHType::Directory
                {
                    // If the same subdir/file is present in a directory twice under the same name,
                    // pick any name to represent both.
                    let Some(first_label) = labels.into_iter().next() else {
                        bail!(
                            "{} -> {} has no labels",
                            graph.properties().swhid(node).expect("Missing SWHID"),
                            graph.properties().swhid(succ).expect("Missing SWHID"),
                        )
                    };
                    stack.push(succ);
                    path_stack.push(PATH_SEPARATOR);
                    path_stack.extend(path_parts.iter().copied());
                    path_stack.push(first_label.filename_id());
                }
            }
        }
    }

    Ok(())
}
