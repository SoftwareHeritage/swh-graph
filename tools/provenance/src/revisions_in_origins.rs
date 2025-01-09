// Copyright (C) 2024-2025  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::{bail, Context, Result};
use dashmap::DashMap;
use dsi_progress_logger::{progress_logger, ProgressLog};
use rayon::prelude::*;
use rdst::RadixSort;
use sux::prelude::elias_fano;

use dataset_writer::{ParallelDatasetWriter, ParquetTableWriter};
use swh_graph::collections::{AdaptiveNodeSet, NodeSet};
use swh_graph::graph::*;
use swh_graph::utils::progress_logger::{BufferedProgressLogger, MinimalProgressLog};
use swh_graph::views::Subgraph;
use swh_graph::NodeType;

use crate::filters::NodeFilter;
use crate::x_in_y_dataset::RevrelInOriTableBuilder;

pub fn main<G>(
    graph: &G,
    node_filter: NodeFilter,
    dataset_writer: ParallelDatasetWriter<ParquetTableWriter<RevrelInOriTableBuilder>>,
) -> Result<()>
where
    G: SwhForwardGraph + SwhBackwardGraph + SwhGraphWithProperties + Send + Sync + 'static,
    <G as SwhGraphWithProperties>::Maps: swh_graph::properties::Maps,
{
    // For each revision, find a revision that is in the exact same set of origins. This limits
    // the number of full traversals to run.
    let (unique_representatives, revrel_to_representative) =
        compute_revrel_representatives(graph, node_filter)?;

    let mut pl = progress_logger!(
        item_name = "revrel representative",
        display_memory = true,
        local_speed = true,
        expected_updates = Some(unique_representatives.len()),
    );
    pl.start("Computing origin sets for revision/release representatives...");
    let shared_pl = Arc::new(Mutex::new(&mut pl));
    let representative_to_origin_set = DashMap::new();
    unique_representatives.into_par_iter().try_for_each_init(
        || BufferedProgressLogger::new(shared_pl.clone()),
        |pl, node| -> Result<_> {
            find_origins_from_revrel(&graph, node, &representative_to_origin_set)?;
            pl.light_update();
            Ok(())
        },
    )?;
    pl.done();

    // Don't need concurrent writes anymore
    let representative_to_origin_set: HashMap<_, _> =
        representative_to_origin_set.into_iter().collect();

    write_origins_from_revrels(graph, node_filter, dataset_writer, |node| {
        let representative = revrel_to_representative[node];
        anyhow::ensure!(
            representative != usize::MAX,
            "{} ({}) has no representative",
            graph.properties().swhid(node),
            node
        );
        representative_to_origin_set
            .get(&representative)
            .with_context(|| {
                format!(
                    "{} ({}) is a representative but has no origin set",
                    graph.properties().swhid(representative),
                    representative
                )
            })
    })
}

/// For each revision, find a revision that is in the exact same set of origins.
///
/// Returns:
/// 1. the small list of "representative" revisions
/// 2. a map from each revision to its "representative"
pub fn compute_revrel_representatives<G>(
    graph: &G,
    node_filter: NodeFilter,
) -> Result<(Vec<NodeId>, Vec<NodeId>)>
where
    G: SwhForwardGraph + SwhBackwardGraph + SwhGraphWithProperties + Send + Sync + 'static,
    <G as SwhGraphWithProperties>::Maps: swh_graph::properties::Maps,
{
    // filter out all cnt/dir, and rev/rel based on the node_filter
    let graph =
        Subgraph::with_node_filter(graph, |node| match graph.properties().node_type(node) {
            NodeType::Content | NodeType::Directory => false,
            NodeType::Revision | NodeType::Release => {
                crate::filters::is_root_revrel(graph, node_filter, node)
            }
            NodeType::Snapshot | NodeType::Origin => true,
        });

    log::info!("Listing root revisions");

    let num_revrels = (0..graph.num_nodes())
        .into_par_iter()
        .filter(|&node| {
            [NodeType::Revision, NodeType::Release].contains(&graph.properties().node_type(node))
        })
        .count();

    // compute the set of all revrels pointed directly by a snapshot
    let mut todo: HashSet<_> = (0..graph.num_nodes())
        .into_par_iter()
        .filter(|&node| {
            graph.has_node(node)
                && [NodeType::Revision, NodeType::Release]
                    .contains(&graph.properties().node_type(node))
                && (graph
                    .predecessors(node)
                    .any(|pred| graph.properties().node_type(pred) == NodeType::Snapshot)
                    || graph.predecessors(node).next().is_none())
        })
        .collect();

    let representatives = vec![usize::MAX; graph.num_nodes()];

    // compiled into a no-op
    let representatives: Vec<_> = representatives.into_iter().map(AtomicUsize::new).collect();

    let mut pl = progress_logger!(
        item_name = "revrel",
        display_memory = true,
        local_speed = true,
        expected_updates = Some(num_revrels),
    );
    pl.start("Computing representatives...");

    let mut unique_representatives = Vec::new();

    while !todo.is_empty() {
        unique_representatives.extend(todo.iter().copied());

        let shared_pl = Arc::new(Mutex::new(&mut pl));
        todo = todo
            .par_iter()
            .map_init(
                || BufferedProgressLogger::new(shared_pl.clone()),
                |pl, &root| {
                    if representatives[root].load(Ordering::SeqCst) != usize::MAX {
                        // the 'root' is already in the subgraph of an other node
                        return Ok(graph.successors(root).collect());
                    }

                    // find the subgraph of revrels reachable from 'root'; but not reachable from any
                    // revrel without going through 'root'.
                    //
                    // This is a heuristic that does not always work. For example, in this graph:
                    //
                    // C <--- B
                    // ^      ^
                    //  \     |
                    //   ---- A
                    //
                    // when we start from A, C may be visited before we see B; so C will be excluded on
                    // this basis that it is reachable from a node not (yet) in the subgraph.
                    //
                    // This is good enough, because we are mostly interested in chains, anyway.
                    let mut next_generation = HashSet::new(); // Representatives for the next while{} iteration
                    let mut stack = vec![root];
                    let mut subgraph = HashSet::new();
                    subgraph.insert(root);
                    while let Some(node) = stack.pop() {
                        representatives[node].store(root, Ordering::SeqCst);
                        for succ in graph.successors(node) {
                            if subgraph.contains(&succ) || next_generation.contains(&succ) {
                                continue;
                            }
                            if representatives[succ].load(Ordering::SeqCst) != usize::MAX {
                                // this node was actually processed before, so we don't need to do
                                // it again.
                                continue;
                            }
                            if todo.contains(&succ) {
                                // this node is about to be processed as its own representative, so we
                                // can tell early that it has other predecessors
                                continue;
                            }
                            if graph
                                .predecessors(succ)
                                .all(|succ_pred| subgraph.contains(&succ_pred))
                            {
                                // 'succ' is not reachable from a node outside the subgraph, add it to the
                                // subgraph.
                                subgraph.insert(succ);
                                //representatives[succ].store(root, Ordering::SeqCst);
                                stack.push(succ);
                            } else {
                                // 'succ' is reachable from a node outside the subgraph. Let's make it
                                // its own representative and we will iterate from it next time.
                                //queued_by[succ].store(node, Ordering::SeqCst);
                                //queued_by_root[succ].store(root, Ordering::SeqCst);
                                if representatives[succ].load(Ordering::SeqCst) == usize::MAX {
                                    // already done, it's pointless to do it again (and messes with the
                                    // ETA because it would double-count succ)
                                    next_generation.insert(succ);
                                }
                            }
                        }
                    }
                    pl.update_with_count(subgraph.len());
                    Ok(next_generation)
                },
            )
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect();
    }

    pl.done();

    // compiled into no-op
    let representatives: Vec<_> = representatives
        .into_iter()
        .map(AtomicUsize::into_inner)
        .collect();

    representatives
        .par_iter()
        .enumerate()
        .try_for_each(|(node, &representative)| {
            if graph.has_node(node) {
                match graph.properties().node_type(node) {
                    NodeType::Revision | NodeType::Release => {
                        if representative == usize::MAX {
                            bail!(
                                "{} ({}) has no representative",
                                graph.properties().swhid(node),
                                node
                            )
                        }
                    }
                    NodeType::Content
                    | NodeType::Directory
                    | NodeType::Snapshot
                    | NodeType::Origin => {
                        if representative != usize::MAX {
                            bail!(
                                "{} ({}) has a representative",
                                graph.properties().swhid(node),
                                node
                            )
                        }
                    }
                };
            }
            Ok(())
        })?;

    Ok((unique_representatives, representatives))
}

pub fn write_origins_from_revrels<'a, G>(
    graph: &G,
    node_filter: NodeFilter,
    dataset_writer: ParallelDatasetWriter<ParquetTableWriter<RevrelInOriTableBuilder>>,
    get_origins: impl Fn(NodeId) -> Result<&'a Option<elias_fano::EliasFano>> + Sync,
) -> Result<()>
where
    G: SwhBackwardGraph + SwhGraphWithProperties + Send + Sync + 'static,
    <G as SwhGraphWithProperties>::Maps: swh_graph::properties::Maps,
{
    let mut pl = progress_logger!(
        item_name = "node",
        display_memory = true,
        local_speed = true,
        expected_updates = Some(graph.num_nodes()),
    );
    pl.start("Writing revisions' origins...");

    let shared_pl = Arc::new(Mutex::new(&mut pl));
    (0..graph.num_nodes()).into_par_iter().try_for_each_init(
        || {
            (
                dataset_writer.get_thread_writer().unwrap(),
                BufferedProgressLogger::new(shared_pl.clone()),
            )
        },
        |(writer, thread_pl), node| -> Result<()> {
            if crate::filters::is_root_revrel(graph, node_filter, node) {
                if let Some(origins) = get_origins(node)? {
                    for origin in origins {
                        // get a new builder every time, because this is how the writer gets an opportunity
                        // to flush
                        let builder = writer.builder()?;
                        builder
                            .revrel
                            .append_value(node.try_into().expect("NodeId overflowed u64"));
                        builder
                            .ori
                            .append_value(origin.try_into().expect("NodeId overflowed u64"));
                    }
                }
            }
            thread_pl.light_update();
            Ok(())
        },
    )?;

    pl.done();

    log::info!("Done, finishing output");

    Ok(())
}

pub fn find_origins_from_revrel<G>(
    graph: &G,
    revrel: NodeId,
    revrel_to_origin: &DashMap<NodeId, Option<elias_fano::EliasFano>>,
) -> Result<()>
where
    G: SwhBackwardGraph + SwhGraphWithProperties,
    <G as SwhGraphWithProperties>::Maps: swh_graph::properties::Maps,
{
    let mut seen = AdaptiveNodeSet::new(graph.num_nodes());
    let mut stack = vec![revrel];

    let mut origins: HashSet<NodeId> = HashSet::new();

    while let Some(node) = stack.pop() {
        match graph.properties().node_type(node) {
            NodeType::Content => bail!(
                "Revision/release {} reachable from content {}",
                graph.properties().swhid(revrel),
                graph.properties().swhid(node)
            ),
            NodeType::Directory => continue, // revision is a submodule
            NodeType::Revision | NodeType::Release | NodeType::Snapshot => {
                if let Some(node_origins) = revrel_to_origin.get(&node) {
                    // Optimization: we already listed all origins reachable from this node,
                    // so we don't need to traverse again from it. This saves a considerable
                    // amount of time.
                    if let Some(node_origins) = node_origins.value() {
                        origins.extend(node_origins);
                    } else {
                        // no origin is reachable from this node
                    }
                } else {
                    for pred in graph.predecessors(node) {
                        if !seen.contains(pred) {
                            stack.push(pred);
                            seen.insert(pred);
                        }
                    }
                }
            }
            NodeType::Origin => {
                origins.insert(node);
            }
        }
    }

    revrel_to_origin.insert(
        revrel,
        if origins.is_empty() {
            None
        } else {
            let mut origins: Vec<_> = origins.into_iter().collect();
            origins.radix_sort_unstable();

            let mut efb =
                elias_fano::EliasFanoBuilder::new(origins.len(), *origins.last().unwrap());
            efb.extend(origins);

            Some(efb.build())
        },
    );

    Ok(())
}
