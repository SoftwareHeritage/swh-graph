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
/// let order = webgraph::algo::BfsOrder::new(&adapter);
/// assert_eq!(order.collect::<Vec<_>>(), vec![0, 2, 1]);
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
