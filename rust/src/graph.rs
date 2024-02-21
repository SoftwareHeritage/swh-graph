// Copyright (C) 2023  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

//! Structures to manipulate the Software Heritage graph
//!
//! In order to load only what is necessary, these structures are initially created
//! by calling [`load_unidirectional`] or [`load_bidirectional`], then calling methods
//! on them to progressively load additional data (`load_properties`, `load_all_properties`,
//! `load_labels`)

#![allow(clippy::type_complexity)]

use std::iter::Iterator;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use dsi_bitstream::prelude::BE;
use webgraph::prelude::*;
//use webgraph::traits::{RandomAccessGraph, SequentialGraph};
use webgraph::graphs::bvgraph::EF;
use webgraph::graphs::BVGraph;
use webgraph::labels::swh_labels::{MmapReaderBuilder, SwhLabels};

use crate::labels::DirEntry;
use crate::mph::SwhidMphf;
use crate::properties;
use crate::utils::suffix_path;

/// Alias for [`usize`], which may become a newtype in a future version.
pub type NodeId = usize;

type DefaultUnderlyingGraph = BVGraph<
    DynCodesDecoderFactory<
        dsi_bitstream::prelude::BE,
        MmapBackend<u32>,
        sux::dict::EliasFano<
            sux::rank_sel::SelectFixed2<
                sux::bits::CountBitVec<&'static [usize]>,
                &'static [u64],
                8,
            >,
            sux::bits::BitFieldVec<usize, &'static [usize]>,
        >,
    >,
>;

/// Wrapper for [`RandomAccessLabeling`] with a method to return an underlying graph
/// (or itself, if it implements [`UnderlyingGraph`])
pub trait UnderlyingLabeling: RandomAccessLabeling {
    type Graph: UnderlyingGraph;

    fn underlying_graph(&self) -> &Self::Graph;
}
/// Wrapper for [`RandomAccessGraph`] which implements [`UnderlyingLabeling`] by
/// returning itself as the underlying graph
pub trait UnderlyingGraph:
    RandomAccessGraph<Label = usize> + UnderlyingLabeling<Graph = Self>
{
}

impl<F: RandomAccessDecoderFactory> UnderlyingLabeling for BVGraph<F> {
    type Graph = Self;

    #[inline(always)]
    fn underlying_graph(&self) -> &Self::Graph {
        self
    }
}
impl<F: RandomAccessDecoderFactory> UnderlyingGraph for BVGraph<F> {}

impl UnderlyingLabeling for Left<VecGraph<()>> {
    type Graph = Self;

    #[inline(always)]
    fn underlying_graph(&self) -> &Self::Graph {
        self
    }
}
impl UnderlyingGraph for Left<VecGraph<()>> {}

impl<G: UnderlyingGraph> UnderlyingLabeling for Zip<G, SwhGraphLabels> {
    type Graph = G;

    #[inline(always)]
    fn underlying_graph(&self) -> &Self::Graph {
        &self.0
    }
}

type SwhGraphLabels = SwhLabels<MmapReaderBuilder, epserde::deser::DeserType<'static, EF>>;

pub trait SwhGraph {
    /// Return the base path of the graph
    fn path(&self) -> &Path;
    /// Return the number of nodes in the graph.
    fn num_nodes(&self) -> usize;
    /// Return the number of arcs in the graph.
    fn num_arcs(&self) -> u64;
    /// Return whether there is an arc going from `src_node_id` to `dst_node_id`.
    fn has_arc(&self, src_node_id: NodeId, dst_node_id: NodeId) -> bool;
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

pub trait SwhLabelledForwardGraph: SwhForwardGraph {
    type LabelledArcs<'arc>: IntoIterator<Item = DirEntry>
    where
        Self: 'arc;
    type LabelledSuccessors<'node>: IntoIterator<Item = (usize, Self::LabelledArcs<'node>)>
    where
        Self: 'node;

    /// Return an [`IntoIterator`] over the successors of a node.
    fn labelled_successors(&self, node_id: NodeId) -> Self::LabelledSuccessors<'_>;
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

pub trait SwhLabelledBackwardGraph: SwhBackwardGraph {
    type LabelledArcs<'arc>: IntoIterator<Item = DirEntry>
    where
        Self: 'arc;
    type LabelledPredecessors<'node>: IntoIterator<Item = (usize, Self::LabelledArcs<'node>)>
    where
        Self: 'node;

    /// Return an [`IntoIterator`] over the successors of a node.
    fn labelled_predecessors(&self, node_id: NodeId) -> Self::LabelledPredecessors<'_>;
}

pub trait SwhGraphWithProperties: SwhGraph {
    type Maps: properties::MapsOption;
    type Timestamps: properties::TimestampsOption;
    type Persons: properties::PersonsOption;
    type Contents: properties::ContentsOption;
    type Strings: properties::StringsOption;
    type LabelNames: properties::LabelNamesOption;

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

/// Class representing the compressed Software Heritage graph in a single direction.
///
/// Created using [`load_unidirectional`].
///
/// Type parameters:
///
/// * `P` is either `()` or `properties::SwhGraphProperties`, manipulated using
///   [`load_properties`](SwhUnidirectionalGraph::load_properties) and
///   [`load_all_properties`](SwhUnidirectionalGraph::load_all_properties)
/// * G is the forward graph (either [`BVGraph`], or `Zip<BVGraph, SwhGraphLabels>`
///   [`load_labels`](SwhUnidirectionalGraph::load_labels)
pub struct SwhUnidirectionalGraph<P, G: UnderlyingLabeling = DefaultUnderlyingGraph> {
    basepath: PathBuf,
    graph: G,
    properties: P,
}

impl<G: UnderlyingLabeling> SwhUnidirectionalGraph<(), G> {
    pub fn from_underlying_graph(basepath: PathBuf, graph: G) -> Self {
        SwhUnidirectionalGraph {
            basepath,
            graph,
            properties: (),
        }
    }
}

impl<P, G: UnderlyingLabeling> SwhGraph for SwhUnidirectionalGraph<P, G> {
    fn path(&self) -> &Path {
        self.basepath.as_path()
    }

    fn num_nodes(&self) -> usize {
        self.graph.underlying_graph().num_nodes()
    }

    fn num_arcs(&self) -> u64 {
        self.graph.underlying_graph().num_arcs()
    }

    fn has_arc(&self, src_node_id: NodeId, dst_node_id: NodeId) -> bool {
        self.graph
            .underlying_graph()
            .has_arc(src_node_id, dst_node_id)
    }
}

impl<P, G: UnderlyingLabeling> SwhForwardGraph for SwhUnidirectionalGraph<P, G> {
    type Successors<'succ> = <<G as UnderlyingLabeling>::Graph as RandomAccessLabeling>::Labels<'succ> where Self: 'succ;

    /// Return an [`IntoIterator`] over the successors of a node.
    fn successors(&self, node_id: NodeId) -> Self::Successors<'_> {
        self.graph.underlying_graph().successors(node_id)
    }

    /// Return the number of successors of a node.
    fn outdegree(&self, node_id: NodeId) -> usize {
        self.graph.underlying_graph().outdegree(node_id)
    }
}

impl<P, G: UnderlyingGraph> SwhLabelledForwardGraph
    for SwhUnidirectionalGraph<P, Zip<G, SwhGraphLabels>>
{
    type LabelledArcs<'arc> = LabelledArcIterator where Self: 'arc;
    type LabelledSuccessors<'succ> = LabelledSuccessorIterator<'succ, G> where Self: 'succ;

    fn labelled_successors(&self, node_id: NodeId) -> Self::LabelledSuccessors<'_> {
        LabelledSuccessorIterator {
            successors: self.graph.successors(node_id),
        }
    }
}

impl<
        M: properties::MapsOption,
        T: properties::TimestampsOption,
        P: properties::PersonsOption,
        C: properties::ContentsOption,
        S: properties::StringsOption,
        N: properties::LabelNamesOption,
        G: UnderlyingLabeling,
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
    /// swh_graph::graph::load_unidirectional(PathBuf::from("./graph"))
    ///     .expect("Could not load graph")
    ///     .init_properties()
    ///     .load_properties(|properties| properties.load_maps::<GOVMPH>())
    ///     .expect("Could not load SWHID maps")
    ///     .load_properties(|properties| properties.load_timestamps())
    ///     .expect("Could not load timestamps");
    /// ```
    pub fn load_properties<
        M2: properties::MapsOption,
        T2: properties::TimestampsOption,
        P2: properties::PersonsOption,
        C2: properties::ContentsOption,
        S2: properties::StringsOption,
        N2: properties::LabelNamesOption,
    >(
        self,
        loader: impl Fn(
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

impl<G: UnderlyingLabeling> SwhUnidirectionalGraph<(), G> {
    /// Prerequisite for `load_properties`
    pub fn init_properties(
        self,
    ) -> SwhUnidirectionalGraph<properties::SwhGraphProperties<(), (), (), (), (), ()>, G> {
        SwhUnidirectionalGraph {
            properties: properties::SwhGraphProperties::new(&self.basepath, self.graph.num_nodes()),
            basepath: self.basepath,
            graph: self.graph,
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
    /// swh_graph::graph::load_unidirectional(PathBuf::from("./graph"))
    ///     .expect("Could not load graph")
    ///     .load_all_properties::<GOVMPH>()
    ///     .expect("Could not load properties");
    /// ```
    pub fn load_all_properties<MPHF: SwhidMphf>(
        self,
    ) -> Result<
        SwhUnidirectionalGraph<
            properties::SwhGraphProperties<
                properties::Maps<MPHF>,
                properties::Timestamps,
                properties::Persons,
                properties::Contents,
                properties::Strings,
                properties::LabelNames,
            >,
            G,
        >,
    > {
        self.init_properties()
            .load_properties(|properties| properties.load_all())
    }
}

impl<
        MAPS: properties::MapsOption,
        TIMESTAMPS: properties::TimestampsOption,
        PERSONS: properties::PersonsOption,
        CONTENTS: properties::ContentsOption,
        STRINGS: properties::StringsOption,
        LABELNAMES: properties::LabelNamesOption,
        G: UnderlyingLabeling,
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

impl<P, G: UnderlyingGraph> SwhUnidirectionalGraph<P, G> {
    /// Consumes this graph and returns a new one that implements [`SwhLabelledForwardGraph`]
    pub fn load_labels(self) -> Result<SwhUnidirectionalGraph<P, Zip<G, SwhGraphLabels>>> {
        Ok(SwhUnidirectionalGraph {
            graph: zip_labels(self.graph, suffix_path(&self.basepath, "-labelled"))
                .context("Could not load forward labels")?,
            properties: self.properties,
            basepath: self.basepath,
        })
    }
}

/// Class representing the compressed Software Heritage graph in both directions.
///
/// Created using [`load_bidirectional`].
///
/// Type parameters:
///
/// * `P` is either `()` or `properties::SwhGraphProperties`, manipulated using
///   [`load_properties`](SwhBidirectionalGraph::load_properties) and
///   [`load_all_properties`](SwhBidirectionalGraph::load_all_properties)
/// * FG is the forward graph (either [`BVGraph`], or `Zip<BVGraph, SwhGraphLabels>`
///   after using [`load_forward_labels`](SwhBidirectionalGraph::load_forward_labels)
/// * BG is the backward graph (either [`BVGraph`], or `Zip<BVGraph, SwhGraphLabels>`
///   after using [`load_backward_labels`](SwhBidirectionalGraph::load_backward_labels)
pub struct SwhBidirectionalGraph<
    P,
    FG: UnderlyingLabeling = DefaultUnderlyingGraph,
    BG: UnderlyingLabeling = DefaultUnderlyingGraph,
> {
    basepath: PathBuf,
    forward_graph: FG,
    backward_graph: BG,
    properties: P,
}

impl<P, FG: UnderlyingLabeling, BG: UnderlyingLabeling> SwhGraph
    for SwhBidirectionalGraph<P, FG, BG>
{
    fn path(&self) -> &Path {
        self.basepath.as_path()
    }

    fn num_nodes(&self) -> usize {
        self.forward_graph.underlying_graph().num_nodes()
    }

    fn num_arcs(&self) -> u64 {
        self.forward_graph.underlying_graph().num_arcs()
    }

    fn has_arc(&self, src_node_id: NodeId, dst_node_id: NodeId) -> bool {
        self.forward_graph
            .underlying_graph()
            .has_arc(src_node_id, dst_node_id)
    }
}

impl<P, FG: UnderlyingLabeling, BG: UnderlyingLabeling> SwhForwardGraph
    for SwhBidirectionalGraph<P, FG, BG>
{
    type Successors<'succ> = <<FG as UnderlyingLabeling>::Graph as RandomAccessLabeling>::Labels<'succ> where Self: 'succ;
    fn successors(&self, node_id: NodeId) -> Self::Successors<'_> {
        self.forward_graph.underlying_graph().successors(node_id)
    }
    fn outdegree(&self, node_id: NodeId) -> usize {
        self.forward_graph.underlying_graph().outdegree(node_id)
    }
}

impl<P, FG: UnderlyingGraph, BG: UnderlyingLabeling> SwhLabelledForwardGraph
    for SwhBidirectionalGraph<P, Zip<FG, SwhGraphLabels>, BG>
{
    type LabelledArcs<'arc> = LabelledArcIterator where Self: 'arc;
    type LabelledSuccessors<'succ> = LabelledSuccessorIterator<'succ, FG> where Self: 'succ;

    fn labelled_successors(&self, node_id: NodeId) -> Self::LabelledSuccessors<'_> {
        LabelledSuccessorIterator {
            successors: self.forward_graph.successors(node_id),
        }
    }
}

impl<P, FG: UnderlyingLabeling, BG: UnderlyingLabeling> SwhBackwardGraph
    for SwhBidirectionalGraph<P, FG, BG>
{
    type Predecessors<'succ> = <<BG as UnderlyingLabeling>::Graph as RandomAccessLabeling>::Labels<'succ> where Self: 'succ;

    fn predecessors(&self, node_id: NodeId) -> Self::Predecessors<'_> {
        self.backward_graph.underlying_graph().successors(node_id)
    }

    fn indegree(&self, node_id: NodeId) -> usize {
        self.backward_graph.underlying_graph().outdegree(node_id)
    }
}

impl<P, FG: UnderlyingLabeling, BG: UnderlyingGraph> SwhLabelledBackwardGraph
    for SwhBidirectionalGraph<P, FG, Zip<BG, SwhGraphLabels>>
{
    type LabelledArcs<'arc> = LabelledArcIterator where Self: 'arc;
    type LabelledPredecessors<'succ> = LabelledSuccessorIterator<'succ, BG> where Self: 'succ;

    fn labelled_predecessors(&self, node_id: NodeId) -> Self::LabelledPredecessors<'_> {
        LabelledSuccessorIterator {
            successors: self.backward_graph.successors(node_id),
        }
    }
}

impl<
        M: properties::MapsOption,
        T: properties::TimestampsOption,
        P: properties::PersonsOption,
        C: properties::ContentsOption,
        S: properties::StringsOption,
        N: properties::LabelNamesOption,
        BG: UnderlyingLabeling,
        FG: UnderlyingLabeling,
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
    /// swh_graph::graph::load_bidirectional(PathBuf::from("./graph"))
    ///     .expect("Could not load graph")
    ///     .init_properties()
    ///     .load_properties(SwhGraphProperties::load_maps::<GOVMPH>)
    ///     .expect("Could not load SWHID maps")
    ///     .load_properties(SwhGraphProperties::load_timestamps)
    ///     .expect("Could not load timestamps");
    /// ```
    pub fn load_properties<
        M2: properties::MapsOption,
        T2: properties::TimestampsOption,
        P2: properties::PersonsOption,
        C2: properties::ContentsOption,
        S2: properties::StringsOption,
        N2: properties::LabelNamesOption,
    >(
        self,
        loader: impl Fn(
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

impl<FG: UnderlyingLabeling, BG: UnderlyingLabeling> SwhBidirectionalGraph<(), FG, BG> {
    /// Prerequisite for `load_properties`
    pub fn init_properties(
        self,
    ) -> SwhBidirectionalGraph<properties::SwhGraphProperties<(), (), (), (), (), ()>, FG, BG> {
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
    /// swh_graph::graph::load_bidirectional(PathBuf::from("./graph"))
    ///     .expect("Could not load graph")
    ///     .load_all_properties::<GOVMPH>()
    ///     .expect("Could not load properties");
    /// ```
    pub fn load_all_properties<MPHF: SwhidMphf>(
        self,
    ) -> Result<
        SwhBidirectionalGraph<
            properties::SwhGraphProperties<
                properties::Maps<MPHF>,
                properties::Timestamps,
                properties::Persons,
                properties::Contents,
                properties::Strings,
                properties::LabelNames,
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
        MAPS: properties::MapsOption,
        TIMESTAMPS: properties::TimestampsOption,
        PERSONS: properties::PersonsOption,
        CONTENTS: properties::ContentsOption,
        STRINGS: properties::StringsOption,
        LABELNAMES: properties::LabelNamesOption,
        BG: UnderlyingLabeling,
        FG: UnderlyingLabeling,
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

impl<P, FG: UnderlyingGraph, BG: UnderlyingLabeling> SwhBidirectionalGraph<P, FG, BG> {
    /// Consumes this graph and returns a new one that implements [`SwhLabelledForwardGraph`]
    pub fn load_forward_labels(
        self,
    ) -> Result<SwhBidirectionalGraph<P, Zip<FG, SwhGraphLabels>, BG>> {
        Ok(SwhBidirectionalGraph {
            forward_graph: zip_labels(self.forward_graph, suffix_path(&self.basepath, "-labelled"))
                .context("Could not load forward labels")?,
            backward_graph: self.backward_graph,
            properties: self.properties,
            basepath: self.basepath,
        })
    }
}

impl<P, FG: UnderlyingLabeling, BG: UnderlyingGraph> SwhBidirectionalGraph<P, FG, BG> {
    /// Consumes this graph and returns a new one that implements [`SwhLabelledBackwardGraph`]
    pub fn load_backward_labels(
        self,
    ) -> Result<SwhBidirectionalGraph<P, FG, Zip<BG, SwhGraphLabels>>> {
        Ok(SwhBidirectionalGraph {
            forward_graph: self.forward_graph,
            backward_graph: zip_labels(
                self.backward_graph,
                suffix_path(&self.basepath, "-transposed-labelled"),
            )
            .context("Could not load backward labels")?,
            properties: self.properties,
            basepath: self.basepath,
        })
    }
}

impl<P, FG: UnderlyingGraph, BG: UnderlyingGraph> SwhBidirectionalGraph<P, FG, BG> {
    /// Equivalent to calling both
    /// [`load_forward_labels`](SwhBidirectionalGraph::load_forward_labels) and
    /// [`load_backward_labels`](SwhBidirectionalGraph::load_backward_labels)
    pub fn load_labels(
        self,
    ) -> Result<SwhBidirectionalGraph<P, Zip<FG, SwhGraphLabels>, Zip<BG, SwhGraphLabels>>> {
        self.load_forward_labels()
            .context("Could not load forward labels")?
            .load_backward_labels()
            .context("Could not load backward labels")
    }
}

pub struct LabelledSuccessorIterator<'a, G: RandomAccessGraph + 'a> {
    successors: <Zip<G, SwhGraphLabels> as webgraph::traits::RandomAccessLabeling>::Labels<'a>,
}

impl<'a, G: RandomAccessGraph> Iterator for LabelledSuccessorIterator<'a, G> {
    type Item = (NodeId, LabelledArcIterator);

    fn next(&mut self) -> Option<Self::Item> {
        self.successors.next().map(|(successor, arc_labels)| {
            (
                successor,
                LabelledArcIterator {
                    arc_label_ids: arc_labels,
                    label_index: 0,
                },
            )
        })
    }
}

pub struct LabelledArcIterator {
    arc_label_ids: Vec<u64>,
    label_index: usize,
}

impl Iterator for LabelledArcIterator {
    type Item = DirEntry;

    fn next(&mut self) -> Option<Self::Item> {
        self.arc_label_ids.get(self.label_index).map(|label| {
            self.label_index += 1;
            DirEntry::from(*label)
        })
    }
}

/// Returns a new [`SwhUnidirectionalGraph`]
pub fn load_unidirectional(basepath: impl AsRef<Path>) -> Result<SwhUnidirectionalGraph<()>> {
    let basepath = basepath.as_ref().to_owned();
    let graph = BVGraph::with_basename(&basepath)
        .endianness::<BE>()
        .flags(MemoryFlags::TRANSPARENT_HUGE_PAGES | MemoryFlags::RANDOM_ACCESS)
        .load()?;
    Ok(SwhUnidirectionalGraph::from_underlying_graph(
        basepath, graph,
    ))
}

/// Returns a new [`SwhBidirectionalGraph`]
pub fn load_bidirectional(basepath: impl AsRef<Path>) -> Result<SwhBidirectionalGraph<()>> {
    let basepath = basepath.as_ref().to_owned();
    let forward_graph = BVGraph::with_basename(&basepath)
        .endianness::<BE>()
        .flags(MemoryFlags::TRANSPARENT_HUGE_PAGES | MemoryFlags::RANDOM_ACCESS)
        .load()?;
    let backward_graph = BVGraph::with_basename(suffix_path(&basepath, "-transposed"))
        .endianness::<BE>()
        .flags(MemoryFlags::TRANSPARENT_HUGE_PAGES | MemoryFlags::RANDOM_ACCESS)
        .load()?;
    Ok(SwhBidirectionalGraph {
        basepath,
        forward_graph,
        backward_graph,
        properties: (),
    })
}

fn zip_labels<G: UnderlyingGraph, P: AsRef<Path>>(
    graph: G,
    base_path: P,
) -> Result<Zip<G, SwhGraphLabels>> {
    let properties_path = suffix_path(&base_path, ".properties");
    let f = std::fs::File::open(&properties_path)
        .with_context(|| format!("Could not open {}", properties_path.display()))?;
    let map = java_properties::read(std::io::BufReader::new(f))?;

    let graphclass = map
        .get("graphclass")
        .with_context(|| format!("Missing 'graphclass' from {}", properties_path.display()))?;
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
        .and_then(|labelspec| labelspec.strip_suffix(")"))
        .and_then(|labelspec| labelspec.parse::<usize>().ok());
    let width = match width {
        None =>
        bail!("Expected labelspec in {} to be \"org.softwareheritage.graph.labels.SwhLabel(DirEntry,<integer>)\" (where <integer> is a small integer, usually under 30), got {:?}", properties_path.display(), labelspec),
        Some(width) => width
    };

    let labels = SwhLabels::load_from_file(width, &base_path).with_context(|| {
        format!(
            "Could not load labeling from {}",
            base_path.as_ref().display()
        )
    })?;
    debug_assert!(webgraph::prelude::Zip(&graph, &labels).verify());
    Ok(Zip(graph, labels))
}
