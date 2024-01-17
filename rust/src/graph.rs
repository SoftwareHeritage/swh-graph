// Copyright (C) 2023  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

//! Structures to manipulate the Software Heritage graph

#![allow(clippy::type_complexity)]

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use webgraph::prelude::*;
//use webgraph::traits::{RandomAccessGraph, SequentialGraph};
use webgraph::label::swh_labels::{MmapReaderBuilder, SwhLabels};
use webgraph::EF;

use crate::labels::DirEntry;
use crate::mph::SwhidMphf;
use crate::properties;
use crate::utils::suffix_path;

/// Alias for [`usize`], which may become a newtype in a future version.
pub type NodeId = usize;

type DefaultUnderlyingGraph = BVGraph<
    DynamicCodesReaderBuilder<dsi_bitstream::prelude::BE, MmapBackend<u32>>,
    webgraph::EF<&'static [usize], &'static [u64]>,
>;

/// Wrapper for [`RandomAccessLabelling`] with a method to return an underlying graph
/// (or itself, if it implements [`UnderlyingGraph`])
pub trait UnderlyingLabelling: RandomAccessLabelling {
    type Graph: UnderlyingGraph;

    fn underlying_graph(&self) -> &Self::Graph;
}
/// Wrapper for [`RandomAccessGraph`] which implements [`UnderlyingLabelling`] by
/// returning itself as the underlying graph
pub trait UnderlyingGraph:
    RandomAccessGraph<Label = usize> + UnderlyingLabelling<Graph = Self>
{
}
impl<CRB, OFF> UnderlyingLabelling for BVGraph<CRB, OFF>
where
    CRB: BVGraphCodesReaderBuilder,
    OFF: sux::prelude::IndexedDict<Input = usize, Output = usize>,
{
    type Graph = Self;

    fn underlying_graph(&self) -> &Self::Graph {
        self
    }
}
impl<CRB, OFF> UnderlyingGraph for BVGraph<CRB, OFF>
where
    CRB: BVGraphCodesReaderBuilder,
    OFF: sux::prelude::IndexedDict<Input = usize, Output = usize>,
{
}
impl<G: UnderlyingGraph> UnderlyingLabelling for Zip<G, SwhGraphLabels> {
    type Graph = G;

    fn underlying_graph(&self) -> &Self::Graph {
        &self.0
    }
}

type SwhGraphLabels = SwhLabels<MmapReaderBuilder, EF<&'static [usize], &'static [u64]>>;

pub trait SwhGraph {
    /// Return the base path of the graph
    fn path(&self) -> &Path;
    /// Return the number of nodes in the graph.
    fn num_nodes(&self) -> usize;
    /// Return the number of arcs in the graph.
    fn num_arcs(&self) -> usize;
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
/// Created using [`load_unidirectional`]
pub struct SwhUnidirectionalGraph<P, G: UnderlyingLabelling = DefaultUnderlyingGraph> {
    basepath: PathBuf,
    graph: G,
    properties: P,
}

impl<P, G: UnderlyingLabelling> SwhGraph for SwhUnidirectionalGraph<P, G> {
    fn path(&self) -> &Path {
        self.basepath.as_path()
    }

    fn num_nodes(&self) -> usize {
        self.graph.underlying_graph().num_nodes()
    }

    fn num_arcs(&self) -> usize {
        self.graph.underlying_graph().num_arcs()
    }

    fn has_arc(&self, src_node_id: NodeId, dst_node_id: NodeId) -> bool {
        self.graph
            .underlying_graph()
            .has_arc(src_node_id, dst_node_id)
    }
}

impl<P, G: UnderlyingLabelling> SwhForwardGraph for SwhUnidirectionalGraph<P, G> {
    type Successors<'succ> = <<G as UnderlyingLabelling>::Graph as RandomAccessLabelling>::Successors<'succ> where Self: 'succ;

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
        G: UnderlyingLabelling,
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

impl<G: UnderlyingLabelling> SwhUnidirectionalGraph<(), G> {
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
        G: UnderlyingLabelling,
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
    pub fn load_labels(self) -> Result<SwhUnidirectionalGraph<P, Zip<G, SwhGraphLabels>>> {
        let labels = SwhLabels::load_from_file(7, suffix_path(&self.basepath, "-labelled"))?;
        debug_assert!(webgraph::prelude::Zip(&self.graph, labels).verify());
        let labelling_path = suffix_path(&self.basepath, "-labelled");
        Ok(SwhUnidirectionalGraph {
            properties: self.properties,
            basepath: self.basepath,
            graph: Zip(
                self.graph,
                SwhLabels::load_from_file(7, &labelling_path).with_context(|| {
                    format!("Could not load labelling from {}", labelling_path.display())
                })?,
            ),
        })
    }
}

/// Class representing the compressed Software Heritage graph in both directions.
///
/// Created using [`load_bidirectional`]
pub struct SwhBidirectionalGraph<
    P,
    FG: UnderlyingLabelling = DefaultUnderlyingGraph,
    BG: UnderlyingLabelling = DefaultUnderlyingGraph,
> {
    basepath: PathBuf,
    forward_graph: FG,
    backward_graph: BG,
    properties: P,
}

impl<P, FG: UnderlyingLabelling, BG: UnderlyingLabelling> SwhGraph
    for SwhBidirectionalGraph<P, FG, BG>
{
    fn path(&self) -> &Path {
        self.basepath.as_path()
    }

    fn num_nodes(&self) -> usize {
        self.forward_graph.underlying_graph().num_nodes()
    }

    fn num_arcs(&self) -> usize {
        self.forward_graph.underlying_graph().num_arcs()
    }

    fn has_arc(&self, src_node_id: NodeId, dst_node_id: NodeId) -> bool {
        self.forward_graph
            .underlying_graph()
            .has_arc(src_node_id, dst_node_id)
    }
}

impl<P, FG: UnderlyingLabelling, BG: UnderlyingLabelling> SwhForwardGraph
    for SwhBidirectionalGraph<P, FG, BG>
{
    type Successors<'succ> = <<FG as UnderlyingLabelling>::Graph as RandomAccessLabelling>::Successors<'succ> where Self: 'succ;
    fn successors(&self, node_id: NodeId) -> Self::Successors<'_> {
        self.forward_graph.underlying_graph().successors(node_id)
    }
    fn outdegree(&self, node_id: NodeId) -> usize {
        self.forward_graph.underlying_graph().outdegree(node_id)
    }
}

impl<P, FG: UnderlyingGraph, BG: UnderlyingLabelling> SwhLabelledForwardGraph
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

impl<P, FG: UnderlyingLabelling, BG: UnderlyingLabelling> SwhBackwardGraph
    for SwhBidirectionalGraph<P, FG, BG>
{
    type Predecessors<'succ> = <<BG as UnderlyingLabelling>::Graph as RandomAccessLabelling>::Successors<'succ> where Self: 'succ;

    fn predecessors(&self, node_id: NodeId) -> Self::Predecessors<'_> {
        self.backward_graph.underlying_graph().successors(node_id)
    }

    fn indegree(&self, node_id: NodeId) -> usize {
        self.backward_graph.underlying_graph().outdegree(node_id)
    }
}

impl<P, FG: UnderlyingLabelling, BG: UnderlyingGraph> SwhLabelledBackwardGraph
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
        BG: UnderlyingLabelling,
        FG: UnderlyingLabelling,
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

impl<FG: UnderlyingLabelling, BG: UnderlyingLabelling> SwhBidirectionalGraph<(), FG, BG> {
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
        BG: UnderlyingLabelling,
        FG: UnderlyingLabelling,
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

impl<P, FG: UnderlyingGraph, BG: UnderlyingLabelling> SwhBidirectionalGraph<P, FG, BG> {
    pub fn load_forward_labels(
        self,
    ) -> Result<SwhBidirectionalGraph<P, Zip<FG, SwhGraphLabels>, BG>> {
        let labels = SwhLabels::load_from_file(7, suffix_path(&self.basepath, "-labelled"))?;
        debug_assert!(webgraph::prelude::Zip(&self.forward_graph, labels).verify());
        let labelling_path = suffix_path(&self.basepath, "-labelled");
        Ok(SwhBidirectionalGraph {
            properties: self.properties,
            basepath: self.basepath,
            forward_graph: Zip(
                self.forward_graph,
                SwhLabels::load_from_file(7, &labelling_path).with_context(|| {
                    format!("Could not load labelling from {}", labelling_path.display())
                })?,
            ),
            backward_graph: self.backward_graph,
        })
    }
}

impl<P, FG: UnderlyingLabelling, BG: UnderlyingGraph> SwhBidirectionalGraph<P, FG, BG> {
    pub fn load_backward_labels(
        self,
    ) -> Result<SwhBidirectionalGraph<P, FG, Zip<BG, SwhGraphLabels>>> {
        let labels = SwhLabels::load_from_file(7, suffix_path(&self.basepath, "-labelled"))?;
        debug_assert!(webgraph::prelude::Zip(&self.backward_graph, labels).verify());
        let labelling_path = suffix_path(&self.basepath, "-transposed-labelled");
        Ok(SwhBidirectionalGraph {
            properties: self.properties,
            basepath: self.basepath,
            forward_graph: self.forward_graph,
            backward_graph: Zip(
                self.backward_graph,
                SwhLabels::load_from_file(7, &labelling_path).with_context(|| {
                    format!("Could not load labelling from {}", labelling_path.display())
                })?,
            ),
        })
    }
}

impl<P, FG: UnderlyingGraph, BG: UnderlyingGraph> SwhBidirectionalGraph<P, FG, BG> {
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
    successors: <Zip<G, SwhGraphLabels> as webgraph::traits::RandomAccessLabelling>::Successors<'a>,
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
    let graph = webgraph::graph::bvgraph::load(&basepath)?;
    Ok(SwhUnidirectionalGraph {
        basepath,
        graph,
        properties: (),
    })
}

/// Returns a new [`SwhBidirectionalGraph`]
pub fn load_bidirectional(basepath: impl AsRef<Path>) -> Result<SwhBidirectionalGraph<()>> {
    let basepath = basepath.as_ref().to_owned();
    let forward_graph = webgraph::graph::bvgraph::load(&basepath)?;
    let backward_graph = webgraph::graph::bvgraph::load(suffix_path(&basepath, "-transposed"))?;
    Ok(SwhBidirectionalGraph {
        basepath,
        forward_graph,
        backward_graph,
        properties: (),
    })
}
