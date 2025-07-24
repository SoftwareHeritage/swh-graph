// Copyright (C) 2025  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::sync::Arc;

use anyhow::{ensure, Context, Result};
use dashmap::DashSet;
use dsi_progress_logger::{concurrent_progress_logger, ConcurrentProgressLog, ProgressLog};
use epserde::prelude::{Deserialize, Serialize};
use epserde::Epserde;
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

#[derive(Epserde)]
/// [`SubgraphWccs`] minus the graph itself, so it can be (de)serialized without including the
/// graph itself.
pub struct SubgraphWccsState<N, S>
where
    N: MonotoneContractionBackend,
    S: std::ops::Deref<Target = Sccs>,
{
    pub contraction: Contraction<N>,
    pub sccs: S,
    /// Not enforced (to allow copying SubgraphWccs across different computers), but it is printed
    /// in case of error, to help debug mismatched graphs
    pub graph_path_hint: String,
}

/// A structure that gives access to connected components of a subgraph.
pub struct SubgraphWccs<G: SwhGraph, N: MonotoneContractionBackend = EfSeqDict> {
    subgraph: ContiguousSubgraph<
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
        pl.start("Listing nodes in connected closure");
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
        pl.start("Compressing set of reachable nodes");
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
            subgraph: Arc::into_inner(contracted_graph)
                .expect("webgraph_algo::sccs::symm_par leaked its 'graph' argument"),
            sccs,
        })
    }

    pub fn from_parts<S>(graph: G, state: SubgraphWccsState<N, S>) -> Result<Self>
    where
        G: SwhGraphWithProperties,
    {
        let SubgraphWccsState {
            contraction,
            sccs,
            graph_path_hint,
        } = state;
        let sccs = *sccs;

        // Check graph and state.contraction have compatible lengths
        let num_nodes = graph.num_nodes();
        let last_node = num_nodes - 1;
        let graph_path = graph.path().display();
        if graph.has_node(last_node) {
            // We have to skip this test if the graph does not have its last node (which can happen
            // if it is a Subgraph) because then we can't do it in constant time
            ensure!(
                contraction.node_id_from_underlying(last_node).is_some(),
                "Contraction is smaller than expected (Graph has {num_nodes} nodes and node {last_node} is in graph but not in contraction). Note: SubgraphWccs was built for {graph_path_hint} and graph is at {graph_path}"
            );
        }
        ensure!(
            contraction.node_id_from_underlying(num_nodes).is_none(),
            "Contraction is longer than expected (Graph has {num_nodes} nodes, but {num_nodes} is in the contraction). Note: SubgraphWccs was built for {graph_path_hint} and graph is at {graph_path}"
        );

        let subgraph = ContiguousSubgraph::new_from_contraction(graph, contraction);
        ensure!(
            sccs.components().len() == subgraph.num_nodes(),
            "Sccs has {} nodes but contraction has {}.",
            sccs.components().len(),
            subgraph.num_nodes()
        );

        Ok(Self { subgraph, sccs })
    }

    pub fn as_parts(&self) -> (&G, SubgraphWccsState<&N, &Sccs>) {
        (
            self.subgraph.underlying_graph(),
            SubgraphWccsState {
                contraction: Contraction(&self.subgraph.contraction().0),
                sccs: &self.sccs,
                graph_path_hint: self
                    .subgraph
                    .underlying_graph()
                    .path()
                    .display()
                    .to_string(),
            },
        )
    }

    /// Returns the total number in all connected components
    pub fn num_nodes(&self) -> usize {
        self.subgraph.num_nodes()
    }

    /// Returns an iterator on all the nodes in any connected component
    ///
    /// Order is not guaranteed.
    ///
    /// Updates the progress logger on every node id from 0 to `self.num_nodes()`,
    /// even those that are filtered out by an underlying subgraph.
    pub fn iter_nodes<'a>(
        &'a self,
        pl: impl ProgressLog + 'a,
    ) -> impl Iterator<Item = NodeId> + 'a {
        self.subgraph
            .iter_nodes(pl)
            .map(|node| self.subgraph.contraction().underlying_node_id(node))
    }
    /// Returns a parallel iterator on all the nodes in any connected component
    ///
    /// Order is not guaranteed.
    ///
    /// Updates the progress logger on every node id from 0 to `self.num_nodes()`,
    /// even those that are filtered out by an underlying subgraph.
    pub fn par_iter_nodes<'a>(
        &'a self,
        pl: impl ConcurrentProgressLog + 'a,
    ) -> impl ParallelIterator<Item = NodeId> + 'a
    where
        G: Sync + Send,
        N: Sync + Send,
    {
        self.subgraph
            .par_iter_nodes(pl)
            .map(|node| self.subgraph.contraction().underlying_node_id(node))
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
        self.subgraph
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
