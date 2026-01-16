// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

use anyhow::Result;

use crate::graph::*;
use crate::properties;
use crate::{NodeType, SWHID};

mod contents;
mod label_names;
mod maps;
mod persons;
mod strings;
mod timestamps;

/// Record of a property access operation
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PropertyAccess {
    // Maps
    Swhid(NodeId),
    NodeType(NodeId),
    NodeId(SWHID),
    NodeIdForInvalidBytes([u8; SWHID::BYTES_SIZE]),
    NodeIdForInvalidStrArray([u8; 50]),
    NodeIdForInvalidString(String),

    // Contents
    IsSkippedContent(NodeId),
    ContentLength(NodeId),

    // Persons
    AuthorId(NodeId),
    CommitterId(NodeId),

    // Strings
    Message(NodeId),
    TagName(NodeId),

    // Timestamps
    AuthorTimestamp(NodeId),
    AuthorTimestampOffset(NodeId),
    CommitterTimestamp(NodeId),
    CommitterTimestampOffset(NodeId),

    // LabelNames
    /// TODO: record exact queries (LabelNameId <-> label name)
    LabelNames,
}

/// Record of a graph access operation
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GraphAccessRecord {
    // SwhGraph
    Path,
    IsTransposed,
    NumNodes,
    HasNode(NodeId),
    NumArcs,
    NumNodesByType,
    NumArcsByType,
    HasArc(NodeId, NodeId),

    // SwhGraphWithProperties
    Property(PropertyAccess),

    // SwhForwardGraph
    Successors(NodeId),
    Outdegree(NodeId),

    // SwhLabeledForwardGraph
    LabeledSuccessors(NodeId),

    // SwhBackwardGraph
    Predecessors(NodeId),
    Indegree(NodeId),

    // SwhLabeledBackwardGraph
    LabeledPredecessors(NodeId),
}

struct GraphSpyInner<G: SwhGraph> {
    graph: G,
}

/// Wraps a graph, and records calls to its methods, useful for tests
pub struct GraphSpy<
    G: SwhGraph,
    MAPS: properties::MaybeMaps = properties::NoMaps,
    TIMESTAMPS: properties::MaybeTimestamps = properties::NoTimestamps,
    PERSONS: properties::MaybePersons = properties::NoPersons,
    CONTENTS: properties::MaybeContents = properties::NoContents,
    STRINGS: properties::MaybeStrings = properties::NoStrings,
    LABELNAMES: properties::MaybeLabelNames = properties::NoLabelNames,
> {
    inner: Arc<GraphSpyInner<G>>,
    /// History of graph accesses
    pub history: Arc<Mutex<Vec<GraphAccessRecord>>>,
    properties:
        properties::SwhGraphProperties<MAPS, TIMESTAMPS, PERSONS, CONTENTS, STRINGS, LABELNAMES>,
}

impl<G: SwhGraph>
    GraphSpy<
        G,
        properties::NoMaps,
        properties::NoTimestamps,
        properties::NoPersons,
        properties::NoContents,
        properties::NoStrings,
        properties::NoLabelNames,
    >
{
    pub fn new(graph: G) -> Self {
        let path = graph.path().to_owned();
        let num_nodes = graph.num_nodes();
        GraphSpy {
            inner: Arc::new(GraphSpyInner { graph }),
            history: Arc::new(Mutex::new(Vec::new())),
            properties: properties::SwhGraphProperties {
                path,
                num_nodes,
                maps: properties::NoMaps,
                timestamps: properties::NoTimestamps,
                persons: properties::NoPersons,
                contents: properties::NoContents,
                strings: properties::NoStrings,
                label_names: properties::NoLabelNames,
                label_names_are_in_base64_order: Default::default(),
            },
        }
    }
}

impl<G, MAPS, TIMESTAMPS, PERSONS, CONTENTS, STRINGS, LABELNAMES>
    GraphSpy<G, MAPS, TIMESTAMPS, PERSONS, CONTENTS, STRINGS, LABELNAMES>
where
    G: SwhGraph,
    MAPS: properties::MaybeMaps,
    TIMESTAMPS: properties::MaybeTimestamps,
    PERSONS: properties::MaybePersons,
    CONTENTS: properties::MaybeContents,
    STRINGS: properties::MaybeStrings,
    LABELNAMES: properties::MaybeLabelNames,
{
    fn record(&self, record: GraphAccessRecord) {
        self.history.lock().unwrap().push(record);
    }

    pub fn graph(&self) -> &G {
        &self.inner.graph
    }
}

impl<G, MAPS, TIMESTAMPS, PERSONS, CONTENTS, STRINGS, LABELNAMES> SwhGraph
    for GraphSpy<G, MAPS, TIMESTAMPS, PERSONS, CONTENTS, STRINGS, LABELNAMES>
where
    G: SwhGraph,
    MAPS: properties::MaybeMaps,
    TIMESTAMPS: properties::MaybeTimestamps,
    PERSONS: properties::MaybePersons,
    CONTENTS: properties::MaybeContents,
    STRINGS: properties::MaybeStrings,
    LABELNAMES: properties::MaybeLabelNames,
{
    fn path(&self) -> &Path {
        self.record(GraphAccessRecord::Path);
        self.inner.graph.path()
    }
    fn is_transposed(&self) -> bool {
        self.record(GraphAccessRecord::IsTransposed);
        self.inner.graph.is_transposed()
    }
    fn num_nodes(&self) -> usize {
        self.record(GraphAccessRecord::NumNodes);
        self.inner.graph.num_nodes()
    }
    fn has_node(&self, node_id: NodeId) -> bool {
        self.record(GraphAccessRecord::HasNode(node_id));
        self.inner.graph.has_node(node_id)
    }
    fn num_arcs(&self) -> u64 {
        self.record(GraphAccessRecord::NumArcs);
        self.inner.graph.num_arcs()
    }
    fn num_nodes_by_type(&self) -> Result<HashMap<NodeType, usize>> {
        self.record(GraphAccessRecord::NumNodesByType);
        self.inner.graph.num_nodes_by_type()
    }
    fn num_arcs_by_type(&self) -> Result<HashMap<(NodeType, NodeType), usize>> {
        self.record(GraphAccessRecord::NumArcsByType);
        self.inner.graph.num_arcs_by_type()
    }
    fn has_arc(&self, src_node_id: NodeId, dst_node_id: NodeId) -> bool {
        self.record(GraphAccessRecord::HasArc(src_node_id, dst_node_id));
        self.inner.graph.has_arc(src_node_id, dst_node_id)
    }
}

impl<G, MAPS, TIMESTAMPS, PERSONS, CONTENTS, STRINGS, LABELNAMES> SwhForwardGraph
    for GraphSpy<G, MAPS, TIMESTAMPS, PERSONS, CONTENTS, STRINGS, LABELNAMES>
where
    G: SwhForwardGraph,
    MAPS: properties::MaybeMaps,
    TIMESTAMPS: properties::MaybeTimestamps,
    PERSONS: properties::MaybePersons,
    CONTENTS: properties::MaybeContents,
    STRINGS: properties::MaybeStrings,
    LABELNAMES: properties::MaybeLabelNames,
{
    type Successors<'succ>
        = <G as SwhForwardGraph>::Successors<'succ>
    where
        Self: 'succ;

    fn successors(&self, node_id: NodeId) -> Self::Successors<'_> {
        self.record(GraphAccessRecord::Successors(node_id));
        self.inner.graph.successors(node_id)
    }
    fn outdegree(&self, node_id: NodeId) -> usize {
        self.record(GraphAccessRecord::Outdegree(node_id));
        self.inner.graph.outdegree(node_id)
    }
}

impl<G, MAPS, TIMESTAMPS, PERSONS, CONTENTS, STRINGS, LABELNAMES> SwhLabeledForwardGraph
    for GraphSpy<G, MAPS, TIMESTAMPS, PERSONS, CONTENTS, STRINGS, LABELNAMES>
where
    G: SwhLabeledForwardGraph,
    MAPS: properties::MaybeMaps,
    TIMESTAMPS: properties::MaybeTimestamps,
    PERSONS: properties::MaybePersons,
    CONTENTS: properties::MaybeContents,
    STRINGS: properties::MaybeStrings,
    LABELNAMES: properties::MaybeLabelNames,
{
    type LabeledArcs<'arc>
        = <G as SwhLabeledForwardGraph>::LabeledArcs<'arc>
    where
        Self: 'arc;
    type LabeledSuccessors<'succ>
        = <G as SwhLabeledForwardGraph>::LabeledSuccessors<'succ>
    where
        Self: 'succ;

    fn untyped_labeled_successors(&self, node_id: NodeId) -> Self::LabeledSuccessors<'_> {
        self.record(GraphAccessRecord::LabeledSuccessors(node_id));
        self.inner.graph.untyped_labeled_successors(node_id)
    }
}

impl<G, MAPS, TIMESTAMPS, PERSONS, CONTENTS, STRINGS, LABELNAMES> SwhBackwardGraph
    for GraphSpy<G, MAPS, TIMESTAMPS, PERSONS, CONTENTS, STRINGS, LABELNAMES>
where
    G: SwhBackwardGraph,
    MAPS: properties::MaybeMaps,
    TIMESTAMPS: properties::MaybeTimestamps,
    PERSONS: properties::MaybePersons,
    CONTENTS: properties::MaybeContents,
    STRINGS: properties::MaybeStrings,
    LABELNAMES: properties::MaybeLabelNames,
{
    type Predecessors<'succ>
        = <G as SwhBackwardGraph>::Predecessors<'succ>
    where
        Self: 'succ;

    fn predecessors(&self, node_id: NodeId) -> Self::Predecessors<'_> {
        self.record(GraphAccessRecord::Predecessors(node_id));
        self.inner.graph.predecessors(node_id)
    }
    fn indegree(&self, node_id: NodeId) -> usize {
        self.record(GraphAccessRecord::Indegree(node_id));
        self.inner.graph.indegree(node_id)
    }
}

impl<G, MAPS, TIMESTAMPS, PERSONS, CONTENTS, STRINGS, LABELNAMES> SwhLabeledBackwardGraph
    for GraphSpy<G, MAPS, TIMESTAMPS, PERSONS, CONTENTS, STRINGS, LABELNAMES>
where
    G: SwhLabeledBackwardGraph,
    MAPS: properties::MaybeMaps,
    TIMESTAMPS: properties::MaybeTimestamps,
    PERSONS: properties::MaybePersons,
    CONTENTS: properties::MaybeContents,
    STRINGS: properties::MaybeStrings,
    LABELNAMES: properties::MaybeLabelNames,
{
    type LabeledArcs<'arc>
        = <G as SwhLabeledBackwardGraph>::LabeledArcs<'arc>
    where
        Self: 'arc;
    type LabeledPredecessors<'succ>
        = <G as SwhLabeledBackwardGraph>::LabeledPredecessors<'succ>
    where
        Self: 'succ;

    fn untyped_labeled_predecessors(&self, node_id: NodeId) -> Self::LabeledPredecessors<'_> {
        self.record(GraphAccessRecord::LabeledPredecessors(node_id));
        self.inner.graph.untyped_labeled_predecessors(node_id)
    }
}

impl<G, MAPS, TIMESTAMPS, PERSONS, CONTENTS, STRINGS, LABELNAMES> SwhGraphWithProperties
    for GraphSpy<G, MAPS, TIMESTAMPS, PERSONS, CONTENTS, STRINGS, LABELNAMES>
where
    G: SwhGraphWithProperties,
    MAPS: properties::MaybeMaps,
    TIMESTAMPS: properties::MaybeTimestamps,
    PERSONS: properties::MaybePersons,
    CONTENTS: properties::MaybeContents,
    STRINGS: properties::MaybeStrings,
    LABELNAMES: properties::MaybeLabelNames,
{
    type Maps = MAPS;
    type Timestamps = TIMESTAMPS;
    type Persons = PERSONS;
    type Contents = CONTENTS;
    type Strings = STRINGS;
    type LabelNames = LABELNAMES;

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
        &self.properties
    }
}
