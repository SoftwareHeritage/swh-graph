// Copyright (C) 2025  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::sync::Arc;

use anyhow::{ensure, Context, Result};
use dashmap::DashSet;
use dsi_progress_logger::{concurrent_progress_logger, ProgressLog};
use rayon::prelude::*;
use rdst::RadixSort;
use sux::dict::elias_fano::{EfSeqDict, EliasFanoConcurrentBuilder};
use swh_graph::graph::{
    NodeId, SwhBackwardGraph, SwhForwardGraph, SwhGraph, SwhGraphWithProperties,
};
use swh_graph::properties;
use swh_graph::views::contiguous_subgraph::{
    ContiguousSubgraph, Contraction, MonotoneContractionBackend,
};
use webgraph::traits::labels::SortedIterator;
use webgraph_algo::sccs::Sccs;

/// A structure that gives access to connected components of a subgraph.
pub struct SubgraphWccs<G: SwhGraph, N: MonotoneContractionBackend = EfSeqDict> {
    graph: ContiguousSubgraph<
        G,
        N,
        properties::NoMaps,
        properties::NoTimestamps,
        properties::NoPersons,
        properties::NoContents,
        properties::NoStrings,
        properties::NoLabelNames,
    >,
    sccs: Sccs,
}

impl<G: SwhGraph> SubgraphWccs<G, EfSeqDict> {
    /// Given a set of nodes, computes the connected components in the whole graph
    /// that contain these nodes.
    ///
    /// For example, if a graph is:
    ///
    /// ```ignore
    /// A -> B -> C
    ///      ^
    ///     /
    /// D --
    /// E -> F -> G
    /// ```
    ///
    /// then:
    ///
    /// * `build_from_closure([A, D, F])` and `build_from_closure([A, B, D, F])` compute
    ///   `[[A, B, C, D], [E, F, G]]`
    /// * `build_from_closure([A])` computes `[[A, B, C, D]]`
    pub fn build_from_closure<I: IntoParallelIterator<Item = NodeId>>(
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
                seen.insert(start_node);
                let mut todo = vec![start_node];

                while let Some(node) = todo.pop() {
                    pl.light_update();
                    for pred in graph.predecessors(node) {
                        let new = seen.insert(pred);
                        if new {
                            todo.push(pred);
                        }
                    }
                    for succ in graph.successors(node) {
                        let new = seen.insert(succ);
                        if new {
                            todo.push(succ);
                        }
                    }
                }
            });
        pl.done();

        let nodes: Vec<_> = seen.into_par_iter().collect();
        Self::build_from_nodes(graph, nodes)
    }

    /// Given a set of nodes, computes the connected components in the subgraph made
    /// of only these nodes.
    ///
    /// For example, if a graph is:
    ///
    /// ```ignore
    /// A -> B -> C
    ///      ^
    ///     /
    /// D --
    /// E -> F -> G
    /// ```
    ///
    /// then:
    ///
    /// * `build_from_nodes([A, D, F])` computes `[[A], [D], [F]]`
    /// * `build_from_nodes([A, B, D, F])` compute `[[A, B, D], [F]]`
    /// * `build_from_nodes([A])` computes `[[A]]`
    pub fn build_from_nodes(graph: G, mut nodes: Vec<NodeId>) -> Result<Self>
    where
        // FIXME: G should not need to be 'static
        G: SwhForwardGraph + SwhBackwardGraph + SwhGraphWithProperties + Sync + Send + 'static,
        for<'succ> <<G as SwhForwardGraph>::Successors<'succ> as IntoIterator>::IntoIter:
            SortedIterator,
        for<'pred> <<G as SwhBackwardGraph>::Predecessors<'pred> as IntoIterator>::IntoIter:
            SortedIterator,
    {
        log::info!("Sorting reachable nodes");
        nodes.radix_sort_unstable();

        unsafe { Self::build_from_sorted_nodes(graph, nodes) }
    }

    /// Same as [`Self::build_from_nodes`] but assumes the vector of nodes is sorted.
    ///
    /// # Safety
    ///
    /// Undefined behavior if the vector is not sorted
    pub unsafe fn build_from_sorted_nodes(graph: G, nodes: Vec<NodeId>) -> Result<Self>
    where
        // FIXME: G should not need to be 'static
        G: SwhForwardGraph + SwhBackwardGraph + SwhGraphWithProperties + Sync + Send + 'static,
        for<'succ> <<G as SwhForwardGraph>::Successors<'succ> as IntoIterator>::IntoIter:
            SortedIterator,
        for<'pred> <<G as SwhBackwardGraph>::Predecessors<'pred> as IntoIterator>::IntoIter:
            SortedIterator,
    {
        let mut pl = concurrent_progress_logger!(
            item_name = "node",
            local_speed = true,
            display_memory = true,
            expected_updates = Some(nodes.len()),
        );
        ensure!(!nodes.is_empty(), "Empty set of nodes"); // Makes EliasFanoConcurrentBuilder panic
        let efb = EliasFanoConcurrentBuilder::new(nodes.len(), graph.num_nodes());
        nodes
            .into_par_iter()
            .enumerate()
            // SAFETY: 'index' is unique, and the vector is sorted
            .for_each_with(pl.clone(), |pl, (index, node)| {
                pl.light_update();
                unsafe { efb.set(index, node) }
            });
        pl.done();

        let contraction = Contraction(efb.build_with_seq_and_dict());

        Self::build_from_contraction(graph, contraction)
    }
}

impl<G: SwhGraph, N: MonotoneContractionBackend> SubgraphWccs<G, N> {
    /// Same as [`Self::build_from_nodes`] but takes a [`Contraction`] as input
    /// instead of a `Vec<usize>`
    pub fn build_from_contraction(graph: G, contraction: Contraction<N>) -> Result<Self>
    where
        // FIXME: N should not need to be 'static
        N: Send + Sync + 'static,
        // FIXME: G should not need to be 'static
        G: SwhForwardGraph + SwhBackwardGraph + SwhGraphWithProperties + Sync + Send + 'static,
        for<'succ> <<G as SwhForwardGraph>::Successors<'succ> as IntoIterator>::IntoIter:
            SortedIterator,
        for<'pred> <<G as SwhBackwardGraph>::Predecessors<'pred> as IntoIterator>::IntoIter:
            SortedIterator,
    {
        // only keep selected nodes
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
            symmetrized_graph,
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
