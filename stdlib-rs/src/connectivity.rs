// Copyright (C) 2025  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::sync::Arc;

use anyhow::{Context, Result};
use dashmap::DashSet;
use dsi_progress_logger::{concurrent_progress_logger, ProgressLog};
use rayon::prelude::*;
use rdst::RadixSort;
use sux::dict::elias_fano::{EfSeqDict, EliasFanoConcurrentBuilder};
use swh_graph::graph::{
    NodeId, SwhBackwardGraph, SwhForwardGraph, SwhGraph, SwhGraphWithProperties,
};
use swh_graph::properties;
use swh_graph::views::{ContiguousSubgraph, Contraction};
use webgraph::traits::labels::SortedIterator;
use webgraph_algo::sccs::Sccs;

/// A structure that gives access to connected components of a subgraph.
pub struct SubgraphSccs<G: SwhGraph> {
    graph: ContiguousSubgraph<
        G,
        EfSeqDict,
        properties::NoMaps,
        properties::NoTimestamps,
        properties::NoPersons,
        properties::NoContents,
        properties::NoStrings,
        properties::NoLabelNames,
    >,
    sccs: Sccs,
}

impl<G: SwhGraph> SubgraphSccs<G> {
    /// Given a set of nodes, computes the set connected components that contain these nodes.
    pub fn build_from_nodes<I: IntoParallelIterator<Item = NodeId>>(
        graph: G,
        nodes: I,
    ) -> Result<Self>
    where
        // FIXME: G should not need to be 'static
        G: SwhForwardGraph + SwhBackwardGraph + SwhGraphWithProperties + Sync + Send + 'static,
        for<'succ> <<G as SwhForwardGraph>::Successors<'succ> as IntoIterator>::IntoIter:
            SortedIterator,
        for<'pred> <<G as SwhBackwardGraph>::Predecessors<'pred> as IntoIterator>::IntoIter:
            SortedIterator,
    {
        let seen = DashSet::new(); // shared between DFSs to avoid duplicate work

        let mut pl = concurrent_progress_logger! {
            item_name = "node",
            local_speed = true,
            display_memory = true,
        };
        pl.start("Listing nodes in connected component");
        nodes
            .into_par_iter()
            .for_each_with(pl.clone(), |pl, start_node| {
                let mut todo = vec![start_node];

                // Find roots for this commit
                while let Some(node) = todo.pop() {
                    pl.light_update();
                    for pred in graph.successors(node) {
                        let new = seen.contains(&pred);
                        if new {
                            todo.push(pred);
                        }
                    }
                    for succ in graph.successors(node) {
                        let new = seen.contains(&succ);
                        if new {
                            todo.push(succ);
                        }
                    }
                }
            });
        pl.done();

        log::info!("Sorting reachable nodes");
        let mut reachable_nodes: Vec<_> = seen.into_par_iter().collect();
        reachable_nodes.radix_sort_unstable();

        let mut pl = concurrent_progress_logger!(
            item_name = "node",
            local_speed = true,
            display_memory = true,
            expected_updates = Some(reachable_nodes.len()),
        );
        let efb = EliasFanoConcurrentBuilder::new(reachable_nodes.len(), graph.num_nodes());
        reachable_nodes
            .into_par_iter()
            .enumerate()
            // SAFETY: 'index' is unique, and the vector is sorted
            .for_each_with(pl.clone(), |pl, (index, node)| {
                pl.light_update();
                unsafe { efb.set(index, node) }
            });
        pl.done();

        let contraction = Contraction(efb.build_with_seq_and_dict());

        // remove all nodes not in the connected components of any of the inputs
        let contracted_graph =
            Arc::new(ContiguousSubgraph::new_from_contraction(graph, contraction));
        let symmetrized_graph =
            swh_graph::views::SymmetricWebgraphAdapter(Arc::clone(&contracted_graph));

        let mut pl = concurrent_progress_logger!(
            item_name = "node",
            local_speed = true,
            display_memory = true,
            expected_updates = Some(contracted_graph.num_nodes()),
        );
        let sccs = webgraph_algo::sccs::symm_par(
            &symmetrized_graph,
            &rayon::ThreadPoolBuilder::default()
                .build()
                .context("Could not build thread pool")?,
            &mut pl,
        );
        pl.done();

        Ok(Self {
            graph: Arc::into_inner(contracted_graph)
                .expect("webgraph_algo::sccs::symm_par leaked its 'graph' argument"),
            sccs,
        })
    }

    /// Returns the number of strongly connected components.
    ///
    /// See [`Sccs::num_components`]
    pub fn num_components(&self) -> usize {
        self.sccs.num_components()
    }

    /// Given a node, returns which component it belongs to, if any
    ///
    /// See [`Sccs::compute_sizes`]
    #[inline(always)]
    pub fn component(&self, node: NodeId) -> Option<usize> {
        self.graph
            .contraction()
            .node_id_from_underlying(node)
            .and_then(|node| self.sccs.components().get(node))
            .copied()
    }

    /// Returns the sizes of all components.
    ///
    /// See [`Sccs::compute_sizes`]
    pub fn compute_sizes(&self) -> Box<[usize]> {
        self.sccs.compute_sizes()
    }

    /// Renumbers the components by decreasing size.
    ///
    /// See [`Sccs::sort_by_size`]
    pub fn sort_by_size(&mut self) -> Box<[usize]> {
        self.sccs.sort_by_size()
    }

    /// Renumbers the components by decreasing size using parallel methods.
    ///
    /// See [`Sccs::par_sort_by_size`]
    pub fn par_sort_by_size(&mut self) -> Box<[usize]> {
        self.sccs.par_sort_by_size()
    }
}
