// Copyright (C) 2023  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

//! Structures to manipulate the Software Heritage graph

#![allow(clippy::type_complexity)]

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use mmap_rs::Mmap;
use webgraph::prelude::*;
//use webgraph::traits::{RandomAccessGraph, SequentialGraph};
use webgraph::label::swh_labels::{MmapReaderBuilder, SwhLabels};
use webgraph::EF;

use crate::java_compat::fcl::FrontCodedList;
use crate::mph::SwhidMphf;
use crate::properties;
use crate::utils::suffix_path;

/// Alias for [`usize`], which may become a newtype in a future version.
pub type NodeId = usize;

type SwhGraphLabelsInner = SwhLabels<MmapReaderBuilder, EF<&'static [usize], &'static [u64]>>;
pub struct SwhGraphLabels {
    labelling: SwhGraphLabelsInner,
    name_labels: FrontCodedList<Mmap, Mmap>,
}

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
    type LabelledArcs<'arc>: IntoIterator<Item = String>
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

pub trait SwhGraphWithProperties: SwhGraph {
    type Maps: properties::MapsOption;
    type Timestamps: properties::TimestampsOption;
    type Persons: properties::PersonsOption;
    type Contents: properties::ContentsOption;
    type Strings: properties::StringsOption;

    fn properties(
        &self,
    ) -> &properties::SwhGraphProperties<
        Self::Maps,
        Self::Timestamps,
        Self::Persons,
        Self::Contents,
        Self::Strings,
    >;
}

/// Class representing the compressed Software Heritage graph in a single direction.
///
/// Created using [`load_unidirectional`]
pub struct SwhUnidirectionalGraph<
    P,
    L,
    G: RandomAccessGraph = BVGraph<
        DynamicCodesReaderBuilder<dsi_bitstream::prelude::BE, MmapBackend<u32>>,
        webgraph::EF<&'static [usize], &'static [u64]>,
    >,
> {
    basepath: PathBuf,
    graph: G,
    properties: P,
    labels: L,
}

impl<P, G: RandomAccessGraph, L> SwhGraph for SwhUnidirectionalGraph<P, L, G> {
    fn path(&self) -> &Path {
        self.basepath.as_path()
    }

    fn num_nodes(&self) -> usize {
        self.graph.num_nodes()
    }

    fn num_arcs(&self) -> usize {
        self.graph.num_arcs()
    }

    fn has_arc(&self, src_node_id: NodeId, dst_node_id: NodeId) -> bool {
        self.graph.has_arc(src_node_id, dst_node_id)
    }
}

impl<P, G: RandomAccessGraph, L> SwhForwardGraph for SwhUnidirectionalGraph<P, L, G> {
    type Successors<'succ> = <G as RandomAccessLabelling>::Successors<'succ> where Self: 'succ;

    /// Return an [`IntoIterator`] over the successors of a node.
    fn successors(&self, node_id: NodeId) -> Self::Successors<'_> {
        self.graph.successors(node_id)
    }

    /// Return the number of successors of a node.
    fn outdegree(&self, node_id: NodeId) -> usize {
        self.graph.outdegree(node_id)
    }
}

impl<P, G: RandomAccessGraph> SwhLabelledForwardGraph
    for SwhUnidirectionalGraph<P, SwhGraphLabels, G>
{
    type LabelledArcs<'arc> = LabelledArcIterator<'arc> where Self: 'arc;
    type LabelledSuccessors<'succ> = LabelledSuccessorIterator<'succ, G> where Self: 'succ;

    fn labelled_successors(&self, node_id: NodeId) -> Self::LabelledSuccessors<'_> {
        /*
        let zipped = webgraph::prelude::Zip(&self.graph, &self.labels.labelling);
        LabelledSuccessorIterator {
            labels: &self.labels.name_labels,
            successors: zipped.successors(node_id).clone(),
        }
        */
        let zipped = core::iter::zip(
            self.graph.successors(node_id),
            self.labels.labelling.successors(node_id),
        );
        LabelledSuccessorIterator {
            labels: &self.labels.name_labels,
            successors: zipped,
        }
    }
}

pub struct LabelledSuccessorIterator<'a, G: RandomAccessGraph + 'a> {
    labels: &'a FrontCodedList<Mmap, Mmap>,
    successors:
        <Zip<G, SwhGraphLabelsInner> as webgraph::traits::RandomAccessLabelling>::Successors<'a>,
}

impl<'a, G: RandomAccessGraph> Iterator for LabelledSuccessorIterator<'a, G> {
    type Item = (NodeId, LabelledArcIterator<'a>);

    fn next(&mut self) -> Option<Self::Item> {
        self.successors.next().map(|(successor, arc_labels)| {
            (
                successor,
                LabelledArcIterator {
                    labels: self.labels,
                    arc_label_ids: arc_labels,
                    label_index: 0,
                },
            )
        })
    }
}

pub struct LabelledArcIterator<'a> {
    labels: &'a FrontCodedList<Mmap, Mmap>,
    arc_label_ids: Vec<u64>,
    label_index: usize,
}

impl<'a> Iterator for LabelledArcIterator<'a> {
    type Item = String;

    fn next(&mut self) -> Option<Self::Item> {
        self.arc_label_ids.get(self.label_index).map(|label_id| {
            log::debug!("label id: {} (0x{:x})", label_id, label_id);
            if label_id % 8 != 0 {
                log::error!("Label id {} is not a multiple of 8", label_id);
            }
            self.label_index += 1;
            let label_bytes = self
                .labels
                .get(*label_id as usize / 8)
                .unwrap_or_else(|| panic!("Label id too large: {}", label_id / 8));
            String::from_utf8(label_bytes)
                .unwrap_or_else(|e| panic!("Could not decode label {} as UTF-8: {:?}", label_id, e))
        })
    }
}

impl<
        M: properties::MapsOption,
        T: properties::TimestampsOption,
        P: properties::PersonsOption,
        C: properties::ContentsOption,
        S: properties::StringsOption,
        G: RandomAccessGraph,
        L,
    > SwhUnidirectionalGraph<properties::SwhGraphProperties<M, T, P, C, S>, L, G>
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
    >(
        self,
        loader: impl Fn(
            properties::SwhGraphProperties<M, T, P, C, S>,
        ) -> Result<properties::SwhGraphProperties<M2, T2, P2, C2, S2>>,
    ) -> Result<SwhUnidirectionalGraph<properties::SwhGraphProperties<M2, T2, P2, C2, S2>, L, G>>
    {
        Ok(SwhUnidirectionalGraph {
            properties: loader(self.properties)?,
            labels: self.labels,
            basepath: self.basepath,
            graph: self.graph,
        })
    }
}

impl<G: RandomAccessGraph, L> SwhUnidirectionalGraph<(), L, G> {
    /// Prerequisite for `load_properties`
    pub fn init_properties(
        self,
    ) -> SwhUnidirectionalGraph<properties::SwhGraphProperties<(), (), (), (), ()>, L, G> {
        SwhUnidirectionalGraph {
            properties: properties::SwhGraphProperties::new(&self.basepath, self.graph.num_nodes()),
            labels: self.labels,
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
            >,
            L,
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
        L,
        G: RandomAccessGraph,
    > SwhGraphWithProperties
    for SwhUnidirectionalGraph<
        properties::SwhGraphProperties<MAPS, TIMESTAMPS, PERSONS, CONTENTS, STRINGS>,
        L,
        G,
    >
{
    type Maps = MAPS;
    type Timestamps = TIMESTAMPS;
    type Persons = PERSONS;
    type Contents = CONTENTS;
    type Strings = STRINGS;

    fn properties(
        &self,
    ) -> &properties::SwhGraphProperties<MAPS, TIMESTAMPS, PERSONS, CONTENTS, STRINGS> {
        &self.properties
    }
}

impl<P, G: RandomAccessGraph> SwhUnidirectionalGraph<P, (), G> {
    pub fn load_labels(self) -> Result<SwhUnidirectionalGraph<P, SwhGraphLabels, G>> {
        let labels = SwhLabels::load_from_file(7, suffix_path(&self.basepath, "-labelled"))?;
        debug_assert!(webgraph::prelude::Zip(&self.graph, labels).verify());
        let labelling_path = suffix_path(&self.basepath, "-labelled");
        let labels_path = suffix_path(&self.basepath, ".labels.fcl");
        Ok(SwhUnidirectionalGraph {
            properties: self.properties,
            labels: SwhGraphLabels {
                labelling: SwhLabels::load_from_file(7, &labelling_path).with_context(|| {
                    format!("Could not load labelling from {}", labelling_path.display())
                })?,
                name_labels: FrontCodedList::load(&labels_path).with_context(|| {
                    format!("Could not load labels from {}", labels_path.display())
                })?,
            },
            basepath: self.basepath,
            graph: self.graph,
        })
    }
}

/// Class representing the compressed Software Heritage graph in both directions.
///
/// Created using [`load_bidirectional`]
pub struct SwhBidirectionalGraph<
    P,
    G: RandomAccessGraph = BVGraph<
        DynamicCodesReaderBuilder<dsi_bitstream::prelude::BE, MmapBackend<u32>>,
        webgraph::EF<&'static [usize], &'static [u64]>,
    >,
> {
    basepath: PathBuf,
    forward_graph: G,
    backward_graph: G,
    properties: P,
}

impl<P, G: RandomAccessGraph> SwhGraph for SwhBidirectionalGraph<P, G> {
    fn path(&self) -> &Path {
        self.basepath.as_path()
    }

    fn num_nodes(&self) -> usize {
        self.forward_graph.num_nodes()
    }

    fn num_arcs(&self) -> usize {
        self.forward_graph.num_arcs()
    }

    fn has_arc(&self, src_node_id: NodeId, dst_node_id: NodeId) -> bool {
        self.forward_graph.has_arc(src_node_id, dst_node_id)
    }
}

impl<P, G: RandomAccessGraph> SwhForwardGraph for SwhBidirectionalGraph<P, G> {
    type Successors<'succ> = <G as RandomAccessLabelling>::Successors<'succ> where Self: 'succ;
    fn successors(&self, node_id: NodeId) -> Self::Successors<'_> {
        self.forward_graph.successors(node_id)
    }
    fn outdegree(&self, node_id: NodeId) -> usize {
        self.forward_graph.outdegree(node_id)
    }
}

impl<P, G: RandomAccessGraph> SwhBackwardGraph for SwhBidirectionalGraph<P, G> {
    type Predecessors<'succ> = <G as RandomAccessLabelling>::Successors<'succ> where Self: 'succ;

    fn predecessors(&self, node_id: NodeId) -> Self::Predecessors<'_> {
        self.backward_graph.successors(node_id)
    }

    fn indegree(&self, node_id: NodeId) -> usize {
        self.backward_graph.outdegree(node_id)
    }
}

impl<
        M: properties::MapsOption,
        T: properties::TimestampsOption,
        P: properties::PersonsOption,
        C: properties::ContentsOption,
        S: properties::StringsOption,
        G: RandomAccessGraph,
    > SwhBidirectionalGraph<properties::SwhGraphProperties<M, T, P, C, S>, G>
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
    >(
        self,
        loader: impl Fn(
            properties::SwhGraphProperties<M, T, P, C, S>,
        ) -> Result<properties::SwhGraphProperties<M2, T2, P2, C2, S2>>,
    ) -> Result<SwhBidirectionalGraph<properties::SwhGraphProperties<M2, T2, P2, C2, S2>, G>> {
        Ok(SwhBidirectionalGraph {
            properties: loader(self.properties)?,
            basepath: self.basepath,
            forward_graph: self.forward_graph,
            backward_graph: self.backward_graph,
        })
    }
}

impl<G: RandomAccessGraph> SwhBidirectionalGraph<(), G> {
    /// Prerequisite for `load_properties`
    pub fn init_properties(
        self,
    ) -> SwhBidirectionalGraph<properties::SwhGraphProperties<(), (), (), (), ()>, G> {
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
        G: RandomAccessGraph,
    > SwhGraphWithProperties
    for SwhBidirectionalGraph<
        properties::SwhGraphProperties<MAPS, TIMESTAMPS, PERSONS, CONTENTS, STRINGS>,
        G,
    >
{
    type Maps = MAPS;
    type Timestamps = TIMESTAMPS;
    type Persons = PERSONS;
    type Contents = CONTENTS;
    type Strings = STRINGS;

    fn properties(
        &self,
    ) -> &properties::SwhGraphProperties<MAPS, TIMESTAMPS, PERSONS, CONTENTS, STRINGS> {
        &self.properties
    }
}

/// Returns a new [`SwhUnidirectionalGraph`]
pub fn load_unidirectional(basepath: impl AsRef<Path>) -> Result<SwhUnidirectionalGraph<(), ()>> {
    let basepath = basepath.as_ref().to_owned();
    let graph = webgraph::graph::bvgraph::load(&basepath)?;
    Ok(SwhUnidirectionalGraph {
        basepath,
        graph,
        properties: (),
        labels: (),
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
