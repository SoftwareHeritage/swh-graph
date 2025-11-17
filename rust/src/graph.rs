// Copyright (C) 2023-2025  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

//! Structures to manipulate the Software Heritage graph
//!
//! In order to load only what is necessary, these structures are initially created
//! by calling [`SwhUnidirectionalGraph::new`] or [`SwhBidirectionalGraph::new`], then calling methods
//! on them to progressively load additional data (`load_properties`, `load_all_properties`,
//! `load_labels`)

#![allow(clippy::type_complexity)]

use std::borrow::Borrow;
use std::collections::HashMap;
use std::iter::Iterator;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, bail, Context, Result};
use dsi_progress_logger::{ConcurrentProgressLog, ProgressLog};
use rayon::prelude::*;
use webgraph::graphs::vec_graph::LabeledVecGraph;
use webgraph::prelude::*;

use crate::arc_iterators::{
    DelabelingIterator, LabelTypingSuccessorIterator, LabeledArcIterator, LabeledSuccessorIterator,
};
// Useful trait to import as part of `use swh_graph::graph::*;`
pub use crate::arc_iterators::IntoFlattenedLabeledArcsIterator;
use crate::labeling::SwhLabeling;
use crate::labels::{EdgeLabel, UntypedEdgeLabel};
use crate::mph::LoadableSwhidMphf;
use crate::properties;
pub use crate::underlying_graph::DefaultUnderlyingGraph;
use crate::utils::shuffle::par_iter_shuffled_range;
use crate::utils::suffix_path;
use crate::NodeType;

/// Alias for [`usize`], which may become a newtype in a future version.
pub type NodeId = usize;

/// Supertrait of [`RandomAccessLabeling`] with methods to act like a [`RandomAccessGraph`].
///
/// If `Self` implements [`RandomAccessGraph`], then this is implemented as no-ops.
/// Otherwise, it "unpeels" layers of zipping until it reaches the graph at the bottom:
///
/// - If `Self` is `Zip<L, R>`, this defers to `L` (aka. `Left<Zip<L, R>`).
/// - If `Self` is `VecGraph` or `LabeledVecGraph<_>, this does the equivalent of deferring to `VecGraph`
///   (through [`DelabelingIterator`] because it cannot create a new `VecGraph` without copying)
pub trait UnderlyingGraph: RandomAccessLabeling {
    type UnlabeledSuccessors<'succ>: IntoIterator<Item = NodeId>
    where
        Self: 'succ;

    /// Workaround for some implementations of `<Self as RandomAccessLabeling>::num_arcs`
    /// being missing
    ///
    /// `Zip::num_arcs` runs `assert_eq!(self.0.num_arcs(), self.1.num_arcs());`
    /// but `BitStreamLabeling::num_arcs` always panics as of
    /// f460742fe0f776df2248a5f09a3425b81eb70b07, so we can't use that.
    fn num_arcs(&self) -> u64;
    fn has_arc(&self, src_node_id: NodeId, dst_node_id: NodeId) -> bool;
    fn unlabeled_successors(&self, node_id: NodeId) -> Self::UnlabeledSuccessors<'_>;
}

impl<F: RandomAccessDecoderFactory> UnderlyingGraph for BvGraph<F> {
    type UnlabeledSuccessors<'succ>
        = <Self as RandomAccessLabeling>::Labels<'succ>
    where
        Self: 'succ;

    fn num_arcs(&self) -> u64 {
        <Self as RandomAccessLabeling>::num_arcs(self)
    }
    fn has_arc(&self, src_node_id: NodeId, dst_node_id: NodeId) -> bool {
        <Self as RandomAccessGraph>::has_arc(self, src_node_id, dst_node_id)
    }
    fn unlabeled_successors(&self, node_id: NodeId) -> Self::UnlabeledSuccessors<'_> {
        <Self as RandomAccessGraph>::successors(self, node_id)
    }
}

impl<G: UnderlyingGraph, L: RandomAccessLabeling> UnderlyingGraph for Zip<G, L> {
    type UnlabeledSuccessors<'succ>
        = <G as UnderlyingGraph>::UnlabeledSuccessors<'succ>
    where
        Self: 'succ;

    fn num_arcs(&self) -> u64 {
        <G as UnderlyingGraph>::num_arcs(&self.0)
    }
    fn has_arc(&self, src_node_id: NodeId, dst_node_id: NodeId) -> bool {
        self.0.has_arc(src_node_id, dst_node_id)
    }
    fn unlabeled_successors(&self, node_id: NodeId) -> Self::UnlabeledSuccessors<'_> {
        self.0.unlabeled_successors(node_id)
    }
}

impl UnderlyingGraph for VecGraph {
    type UnlabeledSuccessors<'succ>
        = <Self as RandomAccessLabeling>::Labels<'succ>
    where
        Self: 'succ;

    fn num_arcs(&self) -> u64 {
        <Self as RandomAccessLabeling>::num_arcs(self)
    }
    fn has_arc(&self, src_node_id: NodeId, dst_node_id: NodeId) -> bool {
        <Self as RandomAccessGraph>::has_arc(self, src_node_id, dst_node_id)
    }
    fn unlabeled_successors(&self, node_id: NodeId) -> Self::UnlabeledSuccessors<'_> {
        <Self as RandomAccessGraph>::successors(self, node_id)
    }
}

impl<L: Clone> UnderlyingGraph for LabeledVecGraph<L> {
    type UnlabeledSuccessors<'succ>
        = DelabelingIterator<<Self as RandomAccessLabeling>::Labels<'succ>>
    where
        Self: 'succ;

    fn num_arcs(&self) -> u64 {
        <Self as RandomAccessLabeling>::num_arcs(self)
    }
    fn has_arc(&self, src_node_id: NodeId, dst_node_id: NodeId) -> bool {
        for succ in self.unlabeled_successors(src_node_id) {
            if succ == dst_node_id {
                return true;
            }
        }
        false
    }
    fn unlabeled_successors(&self, node_id: NodeId) -> Self::UnlabeledSuccessors<'_> {
        DelabelingIterator {
            successors: <Self as RandomAccessLabeling>::labels(self, node_id),
        }
    }
}

pub trait SwhGraph {
    /// Return the base path of the graph
    fn path(&self) -> &Path;

    /// Returns whether the graph is in the `ori->snp->rel,rev->dir->cnt` direction
    /// (with a few `dir->rev` arcs)
    fn is_transposed(&self) -> bool;

    /// Return the largest node id in the graph plus one.
    ///
    /// For graphs directly loaded from disk this is the actual number of nodes,
    /// but it is an overestimate for some graphs (eg. [`Subgraph`](crate::views::Subgraph)).
    ///
    /// Use [`actual_num_nodes`](Self::actual_num_nodes) if you need the exact number of
    /// nodes in the graph.
    fn num_nodes(&self) -> usize;

    /// Returns whether the given node id exists in the graph
    ///
    /// This is usually true iff `node_id < self.num_nodes()`, but may be false
    /// when using a filtered view such as [`Subgraph`](crate::views::Subgraph).
    fn has_node(&self, node_id: NodeId) -> bool {
        node_id < self.num_nodes()
    }

    /// Return the number of arcs in the graph.
    fn num_arcs(&self) -> u64;

    /// Returns the number of nodes of each type, if known.
    fn num_nodes_by_type(&self) -> Result<HashMap<NodeType, usize>> {
        let path = self.path().with_extension("nodes.stats.txt");
        std::fs::read_to_string(&path)
            .with_context(|| format!("Could not read {}", path.display()))?
            .lines()
            .map(|line| {
                let (type_, count) = line
                    .split_once(' ')
                    .with_context(|| format!("Unexpected line in {}: {}", path.display(), line))?;
                let type_: NodeType = type_
                    .parse()
                    .map_err(|_| anyhow!("Unknown node type in {}: {}", path.display(), type_))?;
                let count = count
                    .parse()
                    .map_err(|_| anyhow!("Invalid integer in {}: {}", path.display(), count))?;
                Ok((type_, count))
            })
            .collect()
    }
    /// Returns the number of nodes in the graph, if known.
    fn actual_num_nodes(&self) -> Result<usize> {
        Ok(self.num_nodes_by_type()?.values().sum())
    }
    /// Returns the number of arcs of each type, if known.
    fn num_arcs_by_type(&self) -> Result<HashMap<(NodeType, NodeType), usize>> {
        let path = self.path().with_extension("edges.stats.txt");
        std::fs::read_to_string(&path)
            .with_context(|| format!("Could not read {}", path.display()))?
            .lines()
            .map(|line| {
                let (type_, count) = line
                    .split_once(' ')
                    .with_context(|| format!("Unexpected line in {}: {}", path.display(), line))?;
                let (src_type, dst_type) = type_
                    .split_once(':')
                    .with_context(|| format!("Unexpected line in {}: {}", path.display(), line))?;
                let src_type: NodeType = src_type.parse().map_err(|_| {
                    anyhow!("Unknown node type in {}: {}", path.display(), src_type)
                })?;
                let dst_type: NodeType = dst_type.parse().map_err(|_| {
                    anyhow!("Unknown node type in {}: {}", path.display(), dst_type)
                })?;
                let count = count
                    .parse()
                    .map_err(|_| anyhow!("Invalid integer in {}: {}", path.display(), count))?;
                Ok(((src_type, dst_type), count))
            })
            .collect()
    }

    /// Return whether there is an arc going from `src_node_id` to `dst_node_id`.
    fn has_arc(&self, src_node_id: NodeId, dst_node_id: NodeId) -> bool;

    /// Returns an iterator on all the nodes
    ///
    /// Order is not guaranteed.
    ///
    /// Updates the progress logger on every node id from 0 to `self.num_nodes()`,
    /// even those that are filtered out.
    fn iter_nodes<'a>(
        &'a self,
        mut pl: impl ProgressLog + 'a,
    ) -> impl Iterator<Item = NodeId> + 'a {
        (0..self.num_nodes())
            .inspect(move |_| pl.light_update())
            .filter(|&node_id| self.has_node(node_id))
    }
    /// Returns a parallel iterator on all the nodes
    ///
    /// Order is not guaranteed.
    ///
    /// Updates the progress logger on every node id from 0 to `self.num_nodes()`,
    /// even those that are filtered out.
    fn par_iter_nodes<'a>(
        &'a self,
        pl: impl ConcurrentProgressLog + 'a,
    ) -> impl ParallelIterator<Item = NodeId> + 'a
    where
        Self: Sync,
    {
        par_iter_shuffled_range(0..self.num_nodes())
            .map_with(pl, move |pl, node_id| {
                pl.update();
                node_id
            })
            .filter(|&node_id| self.has_node(node_id))
    }
}

pub trait SwhForwardGraph: SwhGraph {
    type Successors<'succ>: IntoIterator<Item = usize>
    where
        Self: 'succ;

    /// Return an [`IntoIterator`] over the successors of a node.
    fn successors(&self, node_id: NodeId) -> Self::Successors<'_>;
    /// Return the number of successors of a node.
    fn outdegree(&self, node_id: NodeId) -> usize;
}

pub trait SwhLabeledForwardGraph: SwhForwardGraph {
    type LabeledArcs<'arc>: IntoIterator<Item = UntypedEdgeLabel>
    where
        Self: 'arc;
    type LabeledSuccessors<'node>: IntoIterator<Item = (usize, Self::LabeledArcs<'node>)>
        + IntoFlattenedLabeledArcsIterator<UntypedEdgeLabel>
    where
        Self: 'node;

    /// Return an [`IntoIterator`] over the successors of a node along with a list of labels
    /// of each arc
    fn untyped_labeled_successors(&self, node_id: NodeId) -> Self::LabeledSuccessors<'_>;

    /// Return an [`IntoIterator`] over the successors of a node along with a list of labels
    /// of each arc
    fn labeled_successors(
        &self,
        node_id: NodeId,
    ) -> impl Iterator<Item = (usize, impl Iterator<Item = EdgeLabel>)>
           + IntoFlattenedLabeledArcsIterator<EdgeLabel>
    where
        Self: SwhGraphWithProperties + Sized,
        <Self as SwhGraphWithProperties>::Maps: crate::properties::Maps,
    {
        LabelTypingSuccessorIterator {
            graph: self,
            is_transposed: false,
            src: node_id,
            successors: self.untyped_labeled_successors(node_id).into_iter(),
        }
    }
}

pub trait SwhBackwardGraph: SwhGraph {
    type Predecessors<'succ>: IntoIterator<Item = usize>
    where
        Self: 'succ;

    /// Return an [`IntoIterator`] over the predecessors of a node.
    fn predecessors(&self, node_id: NodeId) -> Self::Predecessors<'_>;
    /// Return the number of predecessors of a node.
    fn indegree(&self, node_id: NodeId) -> usize;
}

pub trait SwhLabeledBackwardGraph: SwhBackwardGraph {
    type LabeledArcs<'arc>: IntoIterator<Item = UntypedEdgeLabel>
    where
        Self: 'arc;
    type LabeledPredecessors<'node>: IntoIterator<Item = (usize, Self::LabeledArcs<'node>)>
        + IntoFlattenedLabeledArcsIterator<UntypedEdgeLabel>
    where
        Self: 'node;

    /// Return an [`IntoIterator`] over the predecessors of a node along with a list of labels
    /// of each arc
    fn untyped_labeled_predecessors(&self, node_id: NodeId) -> Self::LabeledPredecessors<'_>;

    /// Return an [`IntoIterator`] over the predecessors of a node along with a list of labels
    /// of each arc
    fn labeled_predecessors(
        &self,
        node_id: NodeId,
    ) -> impl Iterator<Item = (usize, impl Iterator<Item = crate::labels::EdgeLabel>)>
    where
        Self: SwhGraphWithProperties + Sized,
        <Self as SwhGraphWithProperties>::Maps: crate::properties::Maps,
    {
        LabelTypingSuccessorIterator {
            graph: self,
            is_transposed: true,
            src: node_id,
            successors: self.untyped_labeled_predecessors(node_id).into_iter(),
        }
    }
}

pub trait SwhGraphWithProperties: SwhGraph {
    type Maps: properties::MaybeMaps;
    type Timestamps: properties::MaybeTimestamps;
    type Persons: properties::MaybePersons;
    type Contents: properties::MaybeContents;
    type Strings: properties::MaybeStrings;
    type LabelNames: properties::MaybeLabelNames;

    fn properties(
        &self,
    ) -> &properties::SwhGraphProperties<
        Self::Maps,
        Self::Timestamps,
        Self::Persons,
        Self::Contents,
        Self::Strings,
        Self::LabelNames,
    >;
}

/// Alias for structures representing a graph with all arcs, arc labels, and node properties
/// loaded.
pub trait SwhFullGraph:
    SwhLabeledForwardGraph
    + SwhLabeledBackwardGraph
    + SwhGraphWithProperties<
        Maps: crate::properties::Maps,
        Timestamps: crate::properties::Timestamps,
        Persons: crate::properties::Persons,
        Contents: crate::properties::Contents,
        Strings: crate::properties::Strings,
        LabelNames: crate::properties::LabelNames,
    >
{
}

impl<
        G: SwhLabeledForwardGraph
            + SwhLabeledBackwardGraph
            + SwhGraphWithProperties<
                Maps: crate::properties::Maps,
                Timestamps: crate::properties::Timestamps,
                Persons: crate::properties::Persons,
                Contents: crate::properties::Contents,
                Strings: crate::properties::Strings,
                LabelNames: crate::properties::LabelNames,
            >,
    > SwhFullGraph for G
{
}

/// Class representing the compressed Software Heritage graph in a single direction.
///
/// Type parameters:
///
/// * `P` is either `()` or `properties::SwhGraphProperties`, manipulated using
///   [`load_properties`](SwhUnidirectionalGraph::load_properties) and
///   [`load_all_properties`](SwhUnidirectionalGraph::load_all_properties)
/// * G is the forward graph (either [`BvGraph`], or `Zip<BvGraph, SwhLabeling>`
///   [`load_labels`](SwhUnidirectionalGraph::load_labels)
pub struct SwhUnidirectionalGraph<P, G: UnderlyingGraph = DefaultUnderlyingGraph> {
    basepath: PathBuf,
    graph: G,
    properties: P,
}

impl SwhUnidirectionalGraph<()> {
    pub fn new(basepath: impl AsRef<Path>) -> Result<Self> {
        let basepath = basepath.as_ref().to_owned();
        let graph = DefaultUnderlyingGraph::new(&basepath)?;
        Ok(Self::from_underlying_graph(basepath, graph))
    }
}

impl<G: UnderlyingGraph> SwhUnidirectionalGraph<(), G> {
    pub fn from_underlying_graph(basepath: PathBuf, graph: G) -> Self {
        SwhUnidirectionalGraph {
            basepath,
            graph,
            properties: (),
        }
    }
}

impl<P, G: UnderlyingGraph> SwhGraph for SwhUnidirectionalGraph<P, G> {
    fn path(&self) -> &Path {
        self.basepath.as_path()
    }

    fn is_transposed(&self) -> bool {
        // Technically, users can load the 'graph-transposed' directly.
        // However, unless they rename files, this will fail to load properties, because
        // properties' file names wouldn't match the base path.
        // As 'is_transposed' is only useful when checking node types (of an arc),
        // this function is unlikely to be called in that scenario, so this should be fine.
        false
    }

    fn num_nodes(&self) -> usize {
        self.graph.num_nodes()
    }

    fn num_arcs(&self) -> u64 {
        UnderlyingGraph::num_arcs(&self.graph)
    }

    fn has_arc(&self, src_node_id: NodeId, dst_node_id: NodeId) -> bool {
        self.graph.has_arc(src_node_id, dst_node_id)
    }
}

impl<P, G: UnderlyingGraph> SwhForwardGraph for SwhUnidirectionalGraph<P, G> {
    type Successors<'succ>
        = <G as UnderlyingGraph>::UnlabeledSuccessors<'succ>
    where
        Self: 'succ;

    /// Return an [`IntoIterator`] over the successors of a node.
    fn successors(&self, node_id: NodeId) -> Self::Successors<'_> {
        self.graph.unlabeled_successors(node_id)
    }

    /// Return the number of successors of a node.
    fn outdegree(&self, node_id: NodeId) -> usize {
        self.graph.outdegree(node_id)
    }
}

impl<P, G: UnderlyingGraph> SwhLabeledForwardGraph for SwhUnidirectionalGraph<P, G>
where
    <G as SequentialLabeling>::Label: Pair<Left = NodeId, Right: IntoIterator<Item: Borrow<u64>>>,
    for<'succ> <G as RandomAccessLabeling>::Labels<'succ>:
        Iterator<Item = (usize, <<G as SequentialLabeling>::Label as Pair>::Right)>,
{
    type LabeledArcs<'arc> = LabeledArcIterator<<<<<G as RandomAccessLabeling>::Labels<'arc> as Iterator>::Item as Pair>::Right as IntoIterator>::IntoIter> where Self: 'arc;
    type LabeledSuccessors<'succ>
        = LabeledSuccessorIterator<<G as RandomAccessLabeling>::Labels<'succ>>
    where
        Self: 'succ;

    fn untyped_labeled_successors(&self, node_id: NodeId) -> Self::LabeledSuccessors<'_> {
        LabeledSuccessorIterator {
            successors: self.graph.labels(node_id),
        }
    }
}

impl<
        M: properties::MaybeMaps,
        T: properties::MaybeTimestamps,
        P: properties::MaybePersons,
        C: properties::MaybeContents,
        S: properties::MaybeStrings,
        N: properties::MaybeLabelNames,
        G: UnderlyingGraph,
    > SwhUnidirectionalGraph<properties::SwhGraphProperties<M, T, P, C, S, N>, G>
{
    /// Enriches the graph with more properties mmapped from disk
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use std::path::PathBuf;
    /// use swh_graph::java_compat::mph::gov::GOVMPH;
    ///
    /// swh_graph::graph::SwhUnidirectionalGraph::new(PathBuf::from("./graph"))
    ///     .expect("Could not load graph")
    ///     .init_properties()
    ///     .load_properties(|properties| properties.load_maps::<GOVMPH>())
    ///     .expect("Could not load SWHID maps")
    ///     .load_properties(|properties| properties.load_timestamps())
    ///     .expect("Could not load timestamps");
    /// ```
    pub fn load_properties<
        M2: properties::MaybeMaps,
        T2: properties::MaybeTimestamps,
        P2: properties::MaybePersons,
        C2: properties::MaybeContents,
        S2: properties::MaybeStrings,
        N2: properties::MaybeLabelNames,
    >(
        self,
        loader: impl FnOnce(
            properties::SwhGraphProperties<M, T, P, C, S, N>,
        ) -> Result<properties::SwhGraphProperties<M2, T2, P2, C2, S2, N2>>,
    ) -> Result<SwhUnidirectionalGraph<properties::SwhGraphProperties<M2, T2, P2, C2, S2, N2>, G>>
    {
        Ok(SwhUnidirectionalGraph {
            properties: loader(self.properties)?,
            basepath: self.basepath,
            graph: self.graph,
        })
    }
}

impl<G: UnderlyingGraph> SwhUnidirectionalGraph<(), G> {
    /// Prerequisite for `load_properties`
    pub fn init_properties(
        self,
    ) -> SwhUnidirectionalGraph<
        properties::SwhGraphProperties<
            properties::NoMaps,
            properties::NoTimestamps,
            properties::NoPersons,
            properties::NoContents,
            properties::NoStrings,
            properties::NoLabelNames,
        >,
        G,
    > {
        SwhUnidirectionalGraph {
            properties: properties::SwhGraphProperties::new(&self.basepath, self.graph.num_nodes()),
            basepath: self.basepath,
            graph: self.graph,
        }
    }

    /// Enriches the graph with all properties, mmapped from disk
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use std::path::PathBuf;
    /// use swh_graph::java_compat::mph::gov::GOVMPH;
    ///
    /// swh_graph::graph::SwhUnidirectionalGraph::new(PathBuf::from("./graph"))
    ///     .expect("Could not load graph")
    ///     .load_all_properties::<GOVMPH>()
    ///     .expect("Could not load properties");
    /// ```
    pub fn load_all_properties<MPHF: LoadableSwhidMphf>(
        self,
    ) -> Result<
        SwhUnidirectionalGraph<
            properties::SwhGraphProperties<
                properties::MappedMaps<MPHF>,
                properties::MappedTimestamps,
                properties::MappedPersons,
                properties::MappedContents,
                properties::MappedStrings,
                properties::MappedLabelNames,
            >,
            G,
        >,
    > {
        self.init_properties()
            .load_properties(|properties| properties.load_all())
    }
}

impl<
        MAPS: properties::MaybeMaps,
        TIMESTAMPS: properties::MaybeTimestamps,
        PERSONS: properties::MaybePersons,
        CONTENTS: properties::MaybeContents,
        STRINGS: properties::MaybeStrings,
        LABELNAMES: properties::MaybeLabelNames,
        G: UnderlyingGraph,
    > SwhGraphWithProperties
    for SwhUnidirectionalGraph<
        properties::SwhGraphProperties<MAPS, TIMESTAMPS, PERSONS, CONTENTS, STRINGS, LABELNAMES>,
        G,
    >
{
    type Maps = MAPS;
    type Timestamps = TIMESTAMPS;
    type Persons = PERSONS;
    type Contents = CONTENTS;
    type Strings = STRINGS;
    type LabelNames = LABELNAMES;

    fn properties(
        &self,
    ) -> &properties::SwhGraphProperties<MAPS, TIMESTAMPS, PERSONS, CONTENTS, STRINGS, LABELNAMES>
    {
        &self.properties
    }
}

impl<P, G: RandomAccessGraph + UnderlyingGraph> SwhUnidirectionalGraph<P, G> {
    /// Consumes this graph and returns a new one that implements [`SwhLabeledForwardGraph`]
    pub fn load_labels(self) -> Result<SwhUnidirectionalGraph<P, Zip<G, SwhLabeling>>> {
        Ok(SwhUnidirectionalGraph {
            // Note: keep british version of "labelled" here for compatibility with Java swh-graph
            graph: zip_labels(self.graph, suffix_path(&self.basepath, "-labelled"))
                .context("Could not load forward labels")?,
            properties: self.properties,
            basepath: self.basepath,
        })
    }
}

/// Class representing the compressed Software Heritage graph in both directions.
///
/// Type parameters:
///
/// * `P` is either `()` or `properties::SwhGraphProperties`, manipulated using
///   [`load_properties`](SwhBidirectionalGraph::load_properties) and
///   [`load_all_properties`](SwhBidirectionalGraph::load_all_properties)
/// * FG is the forward graph (either [`BvGraph`], or `Zip<BvGraph, SwhLabeling>`
///   after using [`load_forward_labels`](SwhBidirectionalGraph::load_forward_labels)
/// * BG is the backward graph (either [`BvGraph`], or `Zip<BvGraph, SwhLabeling>`
///   after using [`load_backward_labels`](SwhBidirectionalGraph::load_backward_labels)
pub struct SwhBidirectionalGraph<
    P,
    FG: UnderlyingGraph = DefaultUnderlyingGraph,
    BG: UnderlyingGraph = DefaultUnderlyingGraph,
> {
    basepath: PathBuf,
    forward_graph: FG,
    backward_graph: BG,
    properties: P,
}

impl SwhBidirectionalGraph<()> {
    pub fn new(basepath: impl AsRef<Path>) -> Result<Self> {
        let basepath = basepath.as_ref().to_owned();
        let forward_graph = DefaultUnderlyingGraph::new(&basepath)?;
        let backward_graph = DefaultUnderlyingGraph::new(suffix_path(&basepath, "-transposed"))?;
        Ok(Self::from_underlying_graphs(
            basepath,
            forward_graph,
            backward_graph,
        ))
    }
}

impl<FG: UnderlyingGraph, BG: UnderlyingGraph> SwhBidirectionalGraph<(), FG, BG> {
    pub fn from_underlying_graphs(
        basepath: PathBuf,
        forward_graph: FG,
        backward_graph: BG,
    ) -> Self {
        SwhBidirectionalGraph {
            basepath,
            backward_graph,
            forward_graph,
            properties: (),
        }
    }
}

impl<P, FG: UnderlyingGraph, BG: UnderlyingGraph> SwhGraph for SwhBidirectionalGraph<P, FG, BG> {
    fn path(&self) -> &Path {
        self.basepath.as_path()
    }

    fn is_transposed(&self) -> bool {
        // Technically, users can load the 'graph-transposed' directly.
        // However, unless they rename files, this will fail to load properties, because
        // properties' file names wouldn't match the base path.
        // As 'is_transposed' is only useful when checking node types (of an arc),
        // this function is unlikely to be called in that scenario, so this should be fine.
        false
    }

    fn num_nodes(&self) -> usize {
        self.forward_graph.num_nodes()
    }

    fn num_arcs(&self) -> u64 {
        UnderlyingGraph::num_arcs(&self.forward_graph)
    }

    fn has_arc(&self, src_node_id: NodeId, dst_node_id: NodeId) -> bool {
        self.forward_graph.has_arc(src_node_id, dst_node_id)
    }
}

impl<P, FG: UnderlyingGraph, BG: UnderlyingGraph> SwhForwardGraph
    for SwhBidirectionalGraph<P, FG, BG>
{
    type Successors<'succ>
        = <FG as UnderlyingGraph>::UnlabeledSuccessors<'succ>
    where
        Self: 'succ;

    fn successors(&self, node_id: NodeId) -> Self::Successors<'_> {
        self.forward_graph.unlabeled_successors(node_id)
    }
    fn outdegree(&self, node_id: NodeId) -> usize {
        self.forward_graph.outdegree(node_id)
    }
}

impl<P, FG: UnderlyingGraph, BG: UnderlyingGraph> SwhLabeledForwardGraph
    for SwhBidirectionalGraph<P, FG, BG>
where
    <FG as SequentialLabeling>::Label: Pair<Left = NodeId, Right: IntoIterator<Item: Borrow<u64>>>,
    for<'succ> <FG as RandomAccessLabeling>::Labels<'succ>:
        Iterator<Item = (usize, <<FG as SequentialLabeling>::Label as Pair>::Right)>,
{
    type LabeledArcs<'arc> = LabeledArcIterator<<<<<FG as RandomAccessLabeling>::Labels<'arc> as Iterator>::Item as Pair>::Right as IntoIterator>::IntoIter> where Self: 'arc;
    type LabeledSuccessors<'succ>
        = LabeledSuccessorIterator<<FG as RandomAccessLabeling>::Labels<'succ>>
    where
        Self: 'succ;

    fn untyped_labeled_successors(&self, node_id: NodeId) -> Self::LabeledSuccessors<'_> {
        LabeledSuccessorIterator {
            successors: self.forward_graph.labels(node_id),
        }
    }
}

impl<P, FG: UnderlyingGraph, BG: UnderlyingGraph> SwhBackwardGraph
    for SwhBidirectionalGraph<P, FG, BG>
{
    type Predecessors<'succ>
        = <BG as UnderlyingGraph>::UnlabeledSuccessors<'succ>
    where
        Self: 'succ;

    fn predecessors(&self, node_id: NodeId) -> Self::Predecessors<'_> {
        self.backward_graph.unlabeled_successors(node_id)
    }

    fn indegree(&self, node_id: NodeId) -> usize {
        self.backward_graph.outdegree(node_id)
    }
}

impl<P, FG: UnderlyingGraph, BG: UnderlyingGraph> SwhLabeledBackwardGraph
    for SwhBidirectionalGraph<P, FG, BG>
where
    <BG as SequentialLabeling>::Label: Pair<Left = NodeId, Right: IntoIterator<Item: Borrow<u64>>>,
    for<'succ> <BG as RandomAccessLabeling>::Labels<'succ>:
        Iterator<Item = (usize, <<BG as SequentialLabeling>::Label as Pair>::Right)>,
{
    type LabeledArcs<'arc> = LabeledArcIterator<<<<<BG as RandomAccessLabeling>::Labels<'arc> as Iterator>::Item as Pair>::Right as IntoIterator>::IntoIter> where Self: 'arc;
    type LabeledPredecessors<'succ>
        = LabeledSuccessorIterator<<BG as RandomAccessLabeling>::Labels<'succ>>
    where
        Self: 'succ;

    fn untyped_labeled_predecessors(&self, node_id: NodeId) -> Self::LabeledPredecessors<'_> {
        LabeledSuccessorIterator {
            successors: self.backward_graph.labels(node_id),
        }
    }
}

impl<
        M: properties::MaybeMaps,
        T: properties::MaybeTimestamps,
        P: properties::MaybePersons,
        C: properties::MaybeContents,
        S: properties::MaybeStrings,
        N: properties::MaybeLabelNames,
        BG: UnderlyingGraph,
        FG: UnderlyingGraph,
    > SwhBidirectionalGraph<properties::SwhGraphProperties<M, T, P, C, S, N>, FG, BG>
{
    /// Enriches the graph with more properties mmapped from disk
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use std::path::PathBuf;
    /// use swh_graph::java_compat::mph::gov::GOVMPH;
    /// use swh_graph::SwhGraphProperties;
    ///
    /// swh_graph::graph::SwhBidirectionalGraph::new(PathBuf::from("./graph"))
    ///     .expect("Could not load graph")
    ///     .init_properties()
    ///     .load_properties(SwhGraphProperties::load_maps::<GOVMPH>)
    ///     .expect("Could not load SWHID maps")
    ///     .load_properties(SwhGraphProperties::load_timestamps)
    ///     .expect("Could not load timestamps");
    /// ```
    pub fn load_properties<
        M2: properties::MaybeMaps,
        T2: properties::MaybeTimestamps,
        P2: properties::MaybePersons,
        C2: properties::MaybeContents,
        S2: properties::MaybeStrings,
        N2: properties::MaybeLabelNames,
    >(
        self,
        loader: impl FnOnce(
            properties::SwhGraphProperties<M, T, P, C, S, N>,
        ) -> Result<properties::SwhGraphProperties<M2, T2, P2, C2, S2, N2>>,
    ) -> Result<SwhBidirectionalGraph<properties::SwhGraphProperties<M2, T2, P2, C2, S2, N2>, FG, BG>>
    {
        Ok(SwhBidirectionalGraph {
            properties: loader(self.properties)?,
            basepath: self.basepath,
            forward_graph: self.forward_graph,
            backward_graph: self.backward_graph,
        })
    }
}

impl<FG: UnderlyingGraph, BG: UnderlyingGraph> SwhBidirectionalGraph<(), FG, BG> {
    /// Prerequisite for `load_properties`
    pub fn init_properties(
        self,
    ) -> SwhBidirectionalGraph<
        properties::SwhGraphProperties<
            properties::NoMaps,
            properties::NoTimestamps,
            properties::NoPersons,
            properties::NoContents,
            properties::NoStrings,
            properties::NoLabelNames,
        >,
        FG,
        BG,
    > {
        SwhBidirectionalGraph {
            properties: properties::SwhGraphProperties::new(
                &self.basepath,
                self.forward_graph.num_nodes(),
            ),
            basepath: self.basepath,
            forward_graph: self.forward_graph,
            backward_graph: self.backward_graph,
        }
    }

    /// Enriches the graph with more properties mmapped from disk
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use std::path::PathBuf;
    /// use swh_graph::java_compat::mph::gov::GOVMPH;
    ///
    /// swh_graph::graph::SwhBidirectionalGraph::new(PathBuf::from("./graph"))
    ///     .expect("Could not load graph")
    ///     .load_all_properties::<GOVMPH>()
    ///     .expect("Could not load properties");
    /// ```
    pub fn load_all_properties<MPHF: LoadableSwhidMphf>(
        self,
    ) -> Result<
        SwhBidirectionalGraph<
            properties::SwhGraphProperties<
                properties::MappedMaps<MPHF>,
                properties::MappedTimestamps,
                properties::MappedPersons,
                properties::MappedContents,
                properties::MappedStrings,
                properties::MappedLabelNames,
            >,
            FG,
            BG,
        >,
    > {
        self.init_properties()
            .load_properties(|properties| properties.load_all())
    }
}

impl<
        MAPS: properties::MaybeMaps,
        TIMESTAMPS: properties::MaybeTimestamps,
        PERSONS: properties::MaybePersons,
        CONTENTS: properties::MaybeContents,
        STRINGS: properties::MaybeStrings,
        LABELNAMES: properties::MaybeLabelNames,
        BG: UnderlyingGraph,
        FG: UnderlyingGraph,
    > SwhGraphWithProperties
    for SwhBidirectionalGraph<
        properties::SwhGraphProperties<MAPS, TIMESTAMPS, PERSONS, CONTENTS, STRINGS, LABELNAMES>,
        FG,
        BG,
    >
{
    type Maps = MAPS;
    type Timestamps = TIMESTAMPS;
    type Persons = PERSONS;
    type Contents = CONTENTS;
    type Strings = STRINGS;
    type LabelNames = LABELNAMES;

    fn properties(
        &self,
    ) -> &properties::SwhGraphProperties<MAPS, TIMESTAMPS, PERSONS, CONTENTS, STRINGS, LABELNAMES>
    {
        &self.properties
    }
}

impl<P, FG: RandomAccessGraph + UnderlyingGraph, BG: UnderlyingGraph>
    SwhBidirectionalGraph<P, FG, BG>
{
    /// Consumes this graph and returns a new one that implements [`SwhLabeledForwardGraph`]
    pub fn load_forward_labels(self) -> Result<SwhBidirectionalGraph<P, Zip<FG, SwhLabeling>, BG>> {
        Ok(SwhBidirectionalGraph {
            forward_graph: zip_labels(self.forward_graph, suffix_path(&self.basepath, "-labelled"))
                .context("Could not load forward labels")?,
            backward_graph: self.backward_graph,
            properties: self.properties,
            basepath: self.basepath,
        })
    }
}

impl<P, FG: UnderlyingGraph, BG: RandomAccessGraph + UnderlyingGraph>
    SwhBidirectionalGraph<P, FG, BG>
{
    /// Consumes this graph and returns a new one that implements [`SwhLabeledBackwardGraph`]
    pub fn load_backward_labels(
        self,
    ) -> Result<SwhBidirectionalGraph<P, FG, Zip<BG, SwhLabeling>>> {
        Ok(SwhBidirectionalGraph {
            forward_graph: self.forward_graph,
            backward_graph: zip_labels(
                self.backward_graph,
                // Note: keep british version of "labelled" here for compatibility with Java swh-graph
                suffix_path(&self.basepath, "-transposed-labelled"),
            )
            .context("Could not load backward labels")?,
            properties: self.properties,
            basepath: self.basepath,
        })
    }
}

impl<P, FG: RandomAccessGraph + UnderlyingGraph, BG: RandomAccessGraph + UnderlyingGraph>
    SwhBidirectionalGraph<P, FG, BG>
{
    /// Equivalent to calling both
    /// [`load_forward_labels`](SwhBidirectionalGraph::load_forward_labels) and
    /// [`load_backward_labels`](SwhBidirectionalGraph::load_backward_labels)
    pub fn load_labels(
        self,
    ) -> Result<SwhBidirectionalGraph<P, Zip<FG, SwhLabeling>, Zip<BG, SwhLabeling>>> {
        self.load_forward_labels()
            .context("Could not load forward labels")?
            .load_backward_labels()
            .context("Could not load backward labels")
    }
}

#[deprecated(
    since = "5.2.0",
    note = "please use `SwhUnidirectionalGraph::new` instead"
)]
/// Deprecated alias for [`SwhUnidirectionalGraph::new`]
pub fn load_unidirectional(basepath: impl AsRef<Path>) -> Result<SwhUnidirectionalGraph<()>> {
    SwhUnidirectionalGraph::new(basepath)
}

#[deprecated(
    since = "5.2.0",
    note = "please use `SwhBidirectionalGraph::new` instead"
)]
/// Deprecated alias for [`SwhBidirectionalGraph::new`]
pub fn load_bidirectional(basepath: impl AsRef<Path>) -> Result<SwhBidirectionalGraph<()>> {
    SwhBidirectionalGraph::new(basepath)
}

/// Loads a bidirectional graph, then all its properties, then its labels.
///
/// This is a shorthand for:
///
/// ```no_run
/// # use std::path::PathBuf;
/// # use anyhow::{Context, Result};
/// # use swh_graph::graph::SwhBidirectionalGraph;
/// # use swh_graph::mph::LoadableSwhidMphf;
/// #
/// # fn f<MPHF: LoadableSwhidMphf>() -> Result<()> {
/// # let basepath = PathBuf::from("./graph");
/// let graph = SwhBidirectionalGraph::new(basepath)
///     .context("Could not load graph")?
///     .load_all_properties::<MPHF>()
///     .context("Could not load properties")?
///     .load_labels()
///     .context("Could not load labels")?;
/// # Ok(())
/// # }
/// ```
///
/// When working on code made to be re-distributed, you should load only what is needed
/// (ie. load unidirectional if you don't need the backward graph, don't load properties
/// that are not needed, don't load labels unless they need to be read), so users can
/// run your code without a complete graph locally.
pub fn load_full<MPHF: LoadableSwhidMphf>(
    basepath: impl AsRef<Path>,
) -> Result<
    SwhBidirectionalGraph<
        properties::SwhGraphProperties<
            properties::MappedMaps<MPHF>,
            properties::MappedTimestamps,
            properties::MappedPersons,
            properties::MappedContents,
            properties::MappedStrings,
            properties::MappedLabelNames,
        >,
        Zip<DefaultUnderlyingGraph, SwhLabeling>,
        Zip<DefaultUnderlyingGraph, SwhLabeling>,
    >,
> {
    SwhBidirectionalGraph::new(basepath)
        .context("Could not load graph")?
        .load_all_properties()
        .context("Could not load properties")?
        .load_labels()
        .context("Could not load labels")
}

fn zip_labels<G: RandomAccessGraph + UnderlyingGraph, P: AsRef<Path>>(
    graph: G,
    base_path: P,
) -> Result<Zip<G, SwhLabeling>> {
    let properties_path = suffix_path(&base_path, ".properties");
    let f = std::fs::File::open(&properties_path)
        .with_context(|| format!("Could not open {}", properties_path.display()))?;
    let map = java_properties::read(std::io::BufReader::new(f))?;

    let graphclass = map
        .get("graphclass")
        .with_context(|| format!("Missing 'graphclass' from {}", properties_path.display()))?;
    // Note: keep british version of "labelled" here for compatibility with Java swh-graph
    if graphclass != "it.unimi.dsi.big.webgraph.labelling.BitStreamArcLabelledImmutableGraph" {
        bail!(
            "Expected graphclass in {} to be \"it.unimi.dsi.big.webgraph.labelling.BitStreamArcLabelledImmutableGraph\", got {:?}",
            properties_path.display(),
            graphclass
        );
    }

    let labelspec = map
        .get("labelspec")
        .with_context(|| format!("Missing 'labelspec' from {}", properties_path.display()))?;
    let width = labelspec
        .strip_prefix("org.softwareheritage.graph.labels.SwhLabel(DirEntry,")
        .and_then(|labelspec| labelspec.strip_suffix(')'))
        .and_then(|labelspec| labelspec.parse::<usize>().ok());
    let width = match width {
        None =>
        bail!("Expected labelspec in {} to be \"org.softwareheritage.graph.labels.SwhLabel(DirEntry,<integer>)\" (where <integer> is a small integer, usually under 30), got {:?}", properties_path.display(), labelspec),
        Some(width) => width
    };

    let labels = crate::labeling::mmap(&base_path, crate::labeling::SwhDeserializer::new(width))
        .with_context(|| {
            format!(
                "Could not load labeling from {}",
                base_path.as_ref().display()
            )
        })?;

    Ok(Zip(graph, labels))
}
