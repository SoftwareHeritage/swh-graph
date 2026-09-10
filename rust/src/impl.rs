// Copyright (C) 2024-2026  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

//! Boring implementations of `SwhGraph*` traits

use std::ops::Deref;
use std::path::Path;

use crate::graph::*;
use crate::properties;

macro_rules! impl_deref {
    ($type:ty) => {
        impl<G: SwhGraph> SwhGraph for $type {
            #[inline(always)]
            fn path(&self) -> &Path {
                self.deref().path()
            }
            #[inline(always)]
            fn is_transposed(&self) -> bool {
                self.deref().is_transposed()
            }
            #[inline(always)]
            fn num_nodes(&self) -> usize {
                self.deref().num_nodes()
            }
            #[inline(always)]
            fn has_node(&self, node_id: NodeId) -> bool {
                self.deref().has_node(node_id)
            }
            #[inline(always)]
            fn num_arcs(&self) -> u64 {
                self.deref().num_arcs()
            }
            #[inline(always)]
            fn has_arc(&self, src_node_id: NodeId, dst_node_id: NodeId) -> bool {
                self.deref().has_arc(src_node_id, dst_node_id)
            }
        }

        impl<G: SwhForwardGraph> SwhForwardGraph for $type {
            type Successors<'succ>
                = <G as SwhForwardGraph>::Successors<'succ>
            where
                Self: 'succ;

            #[inline(always)]
            fn successors(&self, node_id: NodeId) -> Self::Successors<'_> {
                self.deref().successors(node_id)
            }
            #[inline(always)]
            fn outdegree(&self, node_id: NodeId) -> usize {
                self.deref().outdegree(node_id)
            }
        }

        impl<G: SwhLabeledForwardGraph> SwhLabeledForwardGraph for $type {
            type LabeledArcs<'arc>
                = <G as SwhLabeledForwardGraph>::LabeledArcs<'arc>
            where
                Self: 'arc;
            type LabeledSuccessors<'succ>
                = <G as SwhLabeledForwardGraph>::LabeledSuccessors<'succ>
            where
                Self: 'succ;

            #[inline(always)]
            fn untyped_labeled_successors(&self, node_id: NodeId) -> Self::LabeledSuccessors<'_> {
                self.deref().untyped_labeled_successors(node_id)
            }
        }

        impl<G: SwhBackwardGraph> SwhBackwardGraph for $type {
            type Predecessors<'succ>
                = <G as SwhBackwardGraph>::Predecessors<'succ>
            where
                Self: 'succ;

            #[inline(always)]
            fn predecessors(&self, node_id: NodeId) -> Self::Predecessors<'_> {
                self.deref().predecessors(node_id)
            }
            #[inline(always)]
            fn indegree(&self, node_id: NodeId) -> usize {
                self.deref().indegree(node_id)
            }
        }

        impl<G: SwhLabeledBackwardGraph> SwhLabeledBackwardGraph for $type {
            type LabeledArcs<'arc>
                = <G as SwhLabeledBackwardGraph>::LabeledArcs<'arc>
            where
                Self: 'arc;
            type LabeledPredecessors<'succ>
                = <G as SwhLabeledBackwardGraph>::LabeledPredecessors<'succ>
            where
                Self: 'succ;

            #[inline(always)]
            fn untyped_labeled_predecessors(
                &self,
                node_id: NodeId,
            ) -> Self::LabeledPredecessors<'_> {
                self.deref().untyped_labeled_predecessors(node_id)
            }
        }
        impl<G: SwhGraphWithProperties> SwhGraphWithProperties for $type {
            type Maps = <G as SwhGraphWithProperties>::Maps;
            type Timestamps = <G as SwhGraphWithProperties>::Timestamps;
            type Persons = <G as SwhGraphWithProperties>::Persons;
            type Contents = <G as SwhGraphWithProperties>::Contents;
            type Strings = <G as SwhGraphWithProperties>::Strings;
            type LabelNames = <G as SwhGraphWithProperties>::LabelNames;
            #[inline(always)]
            fn properties(
                &self,
            ) -> &properties::SwhGraphProperties<
                Self::Maps,
                Self::Timestamps,
                Self::Persons,
                Self::Contents,
                Self::Strings,
                Self::LabelNames,
            > {
                self.deref().properties()
            }
        }
    };
}

impl_deref!(&G);
impl_deref!(&mut G);
impl_deref!(Box<G>);
impl_deref!(std::cell::Ref<'_, G>);
impl_deref!(std::cell::RefMut<'_, G>);
impl_deref!(std::cell::LazyCell<G>);
impl_deref!(std::rc::Rc<G>);
impl_deref!(std::sync::Arc<G>);
impl_deref!(std::sync::LazyLock<G>);
