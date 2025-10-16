// Copyright (C) 2023-2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

#![allow(clippy::type_complexity)]

use std::path::Path;

use anyhow::Result;
use dsi_bitstream::prelude::BE;
use webgraph::prelude::*;

use crate::graph::{NodeId, UnderlyingGraph};

#[cfg(not(miri))]
type DefaultUnderlyingGraphInner = BvGraph<
    DynCodesDecoderFactory<
        dsi_bitstream::prelude::BE,
        MmapHelper<u32>,
        // like webgraph::graphs::bvgraph::EF, but with `&'static [usize]` instead of
        // `Box<[usize]>`
        sux::dict::EliasFano<
            sux::rank_sel::SelectAdaptConst<
                sux::bits::BitVec<&'static [usize]>,
                &'static [usize],
                12,
                4,
            >,
            sux::bits::BitFieldVec<usize, &'static [usize]>,
        >,
    >,
>;
// Miri does not support file-backed mmap
#[cfg(miri)]
type DefaultUnderlyingGraphInner = BvGraph<
    DynCodesDecoderFactory<
        dsi_bitstream::prelude::BE,
        webgraph::prelude::MemoryFactory<dsi_bitstream::traits::BigEndian, std::boxed::Box<[u32]>>,
        // like webgraph::graphs::bvgraph::EF, but with `&'static [usize]` instead of
        // `Box<[usize]>`
        sux::dict::EliasFano<
            sux::rank_sel::SelectAdaptConst<
                sux::bits::BitVec<&'static [usize]>,
                &'static [usize],
                12,
                4,
            >,
            sux::bits::BitFieldVec<usize, &'static [usize]>,
        >,
    >,
>;

/// Newtype for [`BvGraph`] with its type parameters set to what the SWH graph needs.
///
/// This indirection reduces the size of error messages.
pub struct DefaultUnderlyingGraph(pub DefaultUnderlyingGraphInner);

impl DefaultUnderlyingGraph {
    pub fn new(basepath: impl AsRef<Path>) -> Result<DefaultUnderlyingGraph> {
        let basepath = basepath.as_ref().to_owned();
        let graph_load_config = BvGraph::with_basename(&basepath)
            .endianness::<BE>()
            .flags(MemoryFlags::TRANSPARENT_HUGE_PAGES | MemoryFlags::RANDOM_ACCESS);

        // Miri does not support file-backed mmap, so we have to copy the graph to memory
        // instead of just mapping it.
        #[cfg(miri)]
        let graph_load_config = graph_load_config.mode::<webgraph::graphs::bvgraph::LoadMem>();

        Ok(DefaultUnderlyingGraph(graph_load_config.load()?))
    }
}

impl SequentialLabeling for DefaultUnderlyingGraph {
    type Label = <DefaultUnderlyingGraphInner as SequentialLabeling>::Label;
    type Lender<'node>
        = <DefaultUnderlyingGraphInner as SequentialLabeling>::Lender<'node>
    where
        Self: 'node;

    // Required methods
    fn num_nodes(&self) -> usize {
        self.0.num_nodes()
    }
    fn iter_from(&self, from: usize) -> Self::Lender<'_> {
        self.0.iter_from(from)
    }

    // Specialized methods
    fn num_arcs_hint(&self) -> Option<u64> {
        self.0.num_arcs_hint()
    }
}
impl SequentialGraph for DefaultUnderlyingGraph {}

impl RandomAccessLabeling for DefaultUnderlyingGraph {
    type Labels<'succ>
        = <DefaultUnderlyingGraphInner as RandomAccessLabeling>::Labels<'succ>
    where
        Self: 'succ;

    fn num_arcs(&self) -> u64 {
        <DefaultUnderlyingGraphInner as RandomAccessLabeling>::num_arcs(&self.0)
    }

    fn labels(&self, node_id: usize) -> <Self as RandomAccessLabeling>::Labels<'_> {
        <DefaultUnderlyingGraphInner as RandomAccessLabeling>::labels(&self.0, node_id)
    }

    fn outdegree(&self, node_id: usize) -> usize {
        <DefaultUnderlyingGraphInner as RandomAccessLabeling>::outdegree(&self.0, node_id)
    }
}

impl RandomAccessGraph for DefaultUnderlyingGraph {}

impl UnderlyingGraph for DefaultUnderlyingGraph {
    type UnlabeledSuccessors<'succ>
        = <DefaultUnderlyingGraphInner as RandomAccessLabeling>::Labels<'succ>
    where
        Self: 'succ;

    fn num_arcs(&self) -> u64 {
        <DefaultUnderlyingGraphInner as UnderlyingGraph>::num_arcs(&self.0)
    }
    fn has_arc(&self, src_node_id: NodeId, dst_node_id: NodeId) -> bool {
        <DefaultUnderlyingGraphInner as UnderlyingGraph>::has_arc(&self.0, src_node_id, dst_node_id)
    }
    fn unlabeled_successors(&self, node_id: NodeId) -> Self::UnlabeledSuccessors<'_> {
        <DefaultUnderlyingGraphInner as UnderlyingGraph>::unlabeled_successors(&self.0, node_id)
    }
}
