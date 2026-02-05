// Copyright (C) 2025  The Software Heritage developers
// Copyright (C) 2025  Tommaso Fontana
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use webgraph::prelude::*;

use crate::graph::*;

/// Wraps a [`SwhForwardGraph`] in order to implements webgraph's [`RandomAccessGraph`]
///
/// # Example
///
/// ```
/// # #[cfg(not(miri))] // BfsOrder uses sysinfo (through dsi-progress-logger), which is not supported
/// # {
/// use std::path::PathBuf;
///
/// use webgraph::prelude::VecGraph;
/// use webgraph::visits::breadth_first::{Seq, IterEvent};
///
/// use swh_graph::graph::{SwhForwardGraph, SwhUnidirectionalGraph};
/// use swh_graph::views::WebgraphAdapter;
///
/// fn get_graph() -> impl SwhForwardGraph {
/// #    if false {
///     // loads this graph:
///     // 1 --.
///     // |    \
///     // v     v
///     // 0 --> 2
///     SwhUnidirectionalGraph::new("./example").unwrap()
/// #    ; }
/// #    SwhUnidirectionalGraph::from_underlying_graph(
/// #        PathBuf::new(),
/// #        VecGraph::from_arcs(vec![(0, 2), (1, 2), (1, 0)]),
/// #    )
/// }
///
/// let graph = get_graph();
/// let adapter = WebgraphAdapter(graph);
///
/// // We can now call generic webgraph algorithms on the adapter
/// let mut order = Seq::new(&adapter);
/// assert_eq!(order.into_iter().collect::<Vec<_>>(), vec![
///     IterEvent {
///         root: 0,
///         parent: 0,
///         node: 0,
///         distance: 0,
///     },
///     IterEvent {
///         root: 0,
///         parent: 0,
///         node: 2,
///         distance: 0
///     },
///     IterEvent {
///         root: 1,
///         parent: 1,
///         node: 1,
///         distance: 0,
///     }
/// ]);
/// # }
/// ```
pub struct WebgraphAdapter<G: SwhForwardGraph>(pub G);

impl<G: SwhForwardGraph> SequentialLabeling for WebgraphAdapter<G> {
    type Label = usize;
    type Lender<'node>
        = LenderSwhForwardGraph<'node, G>
    where
        Self: 'node;

    #[inline(always)]
    fn num_nodes(&self) -> usize {
        SwhGraph::num_nodes(&self.0)
    }

    #[inline(always)]
    fn num_arcs_hint(&self) -> Option<u64> {
        Some(SwhGraph::num_arcs(&self.0))
    }

    #[inline(always)]
    fn iter_from(&self, node_id: usize) -> Self::Lender<'_> {
        LenderSwhForwardGraph {
            graph: &self.0,
            node_id,
        }
    }
}

impl<G: SwhForwardGraph> RandomAccessLabeling for WebgraphAdapter<G> {
    type Labels<'succ>
        = <G as SwhForwardGraph>::Successors<'succ>
    where
        Self: 'succ;

    #[inline(always)]
    fn num_arcs(&self) -> u64 {
        SwhGraph::num_arcs(&self.0)
    }

    #[inline(always)]
    fn outdegree(&self, node_id: usize) -> usize {
        SwhForwardGraph::outdegree(&self.0, node_id)
    }

    #[inline(always)]
    fn labels(&self, node_id: NodeId) -> Self::Labels<'_> {
        SwhForwardGraph::successors(&self.0, node_id)
    }
}

impl<G: SwhForwardGraph> SequentialGraph for WebgraphAdapter<G> {}
impl<G: SwhForwardGraph> RandomAccessGraph for WebgraphAdapter<G> {}

/// Manual re-implementation of [`webgraph::traits::labels::IteratorImpl`]
pub struct LenderSwhForwardGraph<'node, G: SwhForwardGraph> {
    graph: &'node G,
    node_id: NodeId,
}

impl<'succ, G: SwhForwardGraph> lender::Lending<'succ> for LenderSwhForwardGraph<'_, G> {
    type Lend = (usize, G::Successors<'succ>);
}

impl<'succ, G: SwhForwardGraph> NodeLabelsLender<'succ> for LenderSwhForwardGraph<'_, G> {
    type Label = usize;
    type IntoIterator = G::Successors<'succ>;
}

// SAFETY: LenderSwhForwardGraph yields nodes in order, and each node's successors
// in the same order as the underlying iterator
unsafe impl<G: SwhForwardGraph> SortedLender for LenderSwhForwardGraph<'_, G> where
    for<'succ> <G as SwhForwardGraph>::Successors<'succ>: SortedLender
{
}

impl<G: SwhForwardGraph> lender::Lender for LenderSwhForwardGraph<'_, G> {
    fn next(&mut self) -> Option<<Self as lender::Lending<'_>>::Lend> {
        let node_id = self.node_id;
        if node_id >= self.graph.num_nodes() {
            return None;
        }
        self.node_id += 1;
        // webgraph expects contiguous node ids, so we have return this node even if
        // self.graph.has_arc(node_id) is false.
        // self.graph.successors(node_id) is an empty list in that case, so this is mostly fine.
        Some((node_id, self.graph.successors(node_id)))
    }
}

/// Similar to [`WebgraphAdapter`] but exposes a symmetric graph, ie. with each of its
/// `A -> B` arcs duplicated as a `B -> A` arcs
pub struct SymmetricWebgraphAdapter<G: SwhForwardGraph + SwhBackwardGraph>(pub G);

impl<G: SwhForwardGraph + SwhBackwardGraph> SequentialLabeling for SymmetricWebgraphAdapter<G>
where
    for<'succ> <<G as SwhForwardGraph>::Successors<'succ> as IntoIterator>::IntoIter:
        SortedIterator,
    for<'pred> <<G as SwhBackwardGraph>::Predecessors<'pred> as IntoIterator>::IntoIter:
        SortedIterator,
{
    type Label = usize;
    type Lender<'node>
        = LenderSwhSymmetricGraph<'node, G>
    where
        Self: 'node;

    #[inline(always)]
    fn num_nodes(&self) -> usize {
        SwhGraph::num_nodes(&self.0)
    }

    #[inline(always)]
    fn num_arcs_hint(&self) -> Option<u64> {
        Some(SwhGraph::num_arcs(&self.0) * 2)
    }

    #[inline(always)]
    fn iter_from(&self, node_id: usize) -> Self::Lender<'_> {
        LenderSwhSymmetricGraph {
            graph: &self.0,
            node_id,
        }
    }
}

impl<G: SwhForwardGraph + SwhBackwardGraph> RandomAccessLabeling for SymmetricWebgraphAdapter<G>
where
    for<'succ> <<G as SwhForwardGraph>::Successors<'succ> as IntoIterator>::IntoIter:
        SortedIterator,
    for<'pred> <<G as SwhBackwardGraph>::Predecessors<'pred> as IntoIterator>::IntoIter:
        SortedIterator,
{
    type Labels<'succ>
        = MergedSuccessorsForGraph<'succ, G>
    where
        Self: 'succ;

    #[inline(always)]
    fn num_arcs(&self) -> u64 {
        SwhGraph::num_arcs(&self.0) * 2
    }

    #[inline(always)]
    fn outdegree(&self, node_id: usize) -> usize {
        // We can simply add them because SWH graphs are acyclic, so no node can be both
        // a predecessor and a successor
        SwhForwardGraph::outdegree(&self.0, node_id) + SwhBackwardGraph::indegree(&self.0, node_id)
    }

    #[inline(always)]
    fn labels(&self, node_id: NodeId) -> Self::Labels<'_> {
        webgraph::graphs::union_graph::Succ::new(
            Some(<G as SwhForwardGraph>::successors(&self.0, node_id).into_iter()),
            Some(<G as SwhBackwardGraph>::predecessors(&self.0, node_id).into_iter()),
        )
    }
}

impl<G: SwhForwardGraph + SwhBackwardGraph> SequentialGraph for SymmetricWebgraphAdapter<G>
where
    for<'succ> <<G as SwhForwardGraph>::Successors<'succ> as IntoIterator>::IntoIter:
        SortedIterator,
    for<'pred> <<G as SwhBackwardGraph>::Predecessors<'pred> as IntoIterator>::IntoIter:
        SortedIterator,
{
}
impl<G: SwhForwardGraph + SwhBackwardGraph> RandomAccessGraph for SymmetricWebgraphAdapter<G>
where
    for<'succ> <<G as SwhForwardGraph>::Successors<'succ> as IntoIterator>::IntoIter:
        SortedIterator,
    for<'pred> <<G as SwhBackwardGraph>::Predecessors<'pred> as IntoIterator>::IntoIter:
        SortedIterator,
{
}

//struct MergedSuccessors<S1, S2>(S1, S2);
type MergedSuccessors<S1, S2> = webgraph::graphs::union_graph::Succ<S1, S2>;
type MergedSuccessorsForGraph<'succ, G> = MergedSuccessors<
    <<G as SwhForwardGraph>::Successors<'succ> as IntoIterator>::IntoIter,
    <<G as SwhBackwardGraph>::Predecessors<'succ> as IntoIterator>::IntoIter,
>;

/// Manual re-implementation of [`webgraph::graphs::union_graph::Iter`]
pub struct LenderSwhSymmetricGraph<'node, G: SwhForwardGraph + SwhBackwardGraph> {
    graph: &'node G,
    node_id: NodeId,
}

impl<'succ, G: SwhForwardGraph + SwhBackwardGraph> lender::Lending<'succ>
    for LenderSwhSymmetricGraph<'_, G>
{
    type Lend = (usize, MergedSuccessorsForGraph<'succ, G>);
}

impl<'succ, G: SwhForwardGraph + SwhBackwardGraph> NodeLabelsLender<'succ>
    for LenderSwhSymmetricGraph<'_, G>
{
    type Label = usize;
    type IntoIterator = MergedSuccessorsForGraph<'succ, G>;
}

// SAFETY: LenderSwhForwardGraph yields nodes in order, and each node's successors
// in the same order as the underlying iterator
unsafe impl<G: SwhForwardGraph + SwhBackwardGraph> SortedLender for LenderSwhSymmetricGraph<'_, G>
where
    for<'succ> <G as SwhForwardGraph>::Successors<'succ>: SortedLender,
    for<'succ> <G as SwhBackwardGraph>::Predecessors<'succ>: SortedLender,
{
}

impl<G: SwhForwardGraph + SwhBackwardGraph> lender::Lender for LenderSwhSymmetricGraph<'_, G> {
    fn next(&mut self) -> Option<<Self as lender::Lending<'_>>::Lend> {
        let node_id = self.node_id;
        if node_id >= self.graph.num_nodes() {
            return None;
        }
        self.node_id += 1;
        // webgraph expects contiguous node ids, so we have return this node even if
        // self.graph.has_arc(node_id) is false.
        // self.graph.successors(node_id) is an empty list in that case, so this is mostly fine.
        Some((
            node_id,
            webgraph::graphs::union_graph::Succ::new(
                Some(<G as SwhForwardGraph>::successors(self.graph, node_id).into_iter()),
                Some(<G as SwhBackwardGraph>::predecessors(self.graph, node_id).into_iter()),
            ),
        ))
    }
}
