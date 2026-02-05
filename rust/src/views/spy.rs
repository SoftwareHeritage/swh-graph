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
use crate::NodeType;

/// Wraps a graph, and records calls to its methods, useful for tests
pub struct GraphSpy<G: SwhGraph> {
    pub graph: G,
    /// Pairs of `(method, arguments)`
    pub history: Arc<Mutex<Vec<(&'static str, String)>>>,
}

impl<G: SwhGraph> GraphSpy<G> {
    pub fn new(graph: G) -> Self {
        GraphSpy {
            graph,
            history: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn record<Args: std::fmt::Debug>(&self, method: &'static str, arguments: Args) {
        self.history
            .lock()
            .unwrap()
            .push((method, format!("{arguments:?}")));
    }
}

impl<G: SwhGraph> SwhGraph for GraphSpy<G> {
    fn path(&self) -> &Path {
        self.record("path", ());
        self.graph.path()
    }
    fn is_transposed(&self) -> bool {
        self.record("is_transposed", ());
        self.graph.is_transposed()
    }
    fn num_nodes(&self) -> usize {
        self.record("num_nodes", ());
        self.graph.num_nodes()
    }
    fn has_node(&self, node_id: NodeId) -> bool {
        self.record("has_node", (node_id,));
        self.graph.has_node(node_id)
    }
    fn num_arcs(&self) -> u64 {
        self.record("num_arcs", ());
        self.graph.num_arcs()
    }
    fn num_nodes_by_type(&self) -> Result<HashMap<NodeType, usize>> {
        self.record("num_nodes_by_type", ());
        self.graph.num_nodes_by_type()
    }
    fn num_arcs_by_type(&self) -> Result<HashMap<(NodeType, NodeType), usize>> {
        self.record("num_arcs_by_type", ());
        self.graph.num_arcs_by_type()
    }
    fn has_arc(&self, src_node_id: NodeId, dst_node_id: NodeId) -> bool {
        self.record("has_arc", (src_node_id, dst_node_id));
        self.graph.has_arc(src_node_id, dst_node_id)
    }
}

impl<G: SwhForwardGraph> SwhForwardGraph for GraphSpy<G> {
    type Successors<'succ>
        = <G as SwhForwardGraph>::Successors<'succ>
    where
        Self: 'succ;

    fn successors(&self, node_id: NodeId) -> Self::Successors<'_> {
        self.record("successors", (node_id,));
        self.graph.successors(node_id)
    }
    fn outdegree(&self, node_id: NodeId) -> usize {
        self.record("outdegree", (node_id,));
        self.graph.outdegree(node_id)
    }
}

impl<G: SwhLabeledForwardGraph> SwhLabeledForwardGraph for GraphSpy<G> {
    type LabeledArcs<'arc>
        = <G as SwhLabeledForwardGraph>::LabeledArcs<'arc>
    where
        Self: 'arc;
    type LabeledSuccessors<'succ>
        = <G as SwhLabeledForwardGraph>::LabeledSuccessors<'succ>
    where
        Self: 'succ;

    fn untyped_labeled_successors(&self, node_id: NodeId) -> Self::LabeledSuccessors<'_> {
        self.record("untyped_labeled_successors", (node_id,));
        self.graph.untyped_labeled_successors(node_id)
    }
}

impl<G: SwhBackwardGraph> SwhBackwardGraph for GraphSpy<G> {
    type Predecessors<'succ>
        = <G as SwhBackwardGraph>::Predecessors<'succ>
    where
        Self: 'succ;

    fn predecessors(&self, node_id: NodeId) -> Self::Predecessors<'_> {
        self.record("predecessors", (node_id,));
        self.graph.predecessors(node_id)
    }
    fn indegree(&self, node_id: NodeId) -> usize {
        self.record("indegree", (node_id,));
        self.graph.indegree(node_id)
    }
}

impl<G: SwhLabeledBackwardGraph> SwhLabeledBackwardGraph for GraphSpy<G> {
    type LabeledArcs<'arc>
        = <G as SwhLabeledBackwardGraph>::LabeledArcs<'arc>
    where
        Self: 'arc;
    type LabeledPredecessors<'succ>
        = <G as SwhLabeledBackwardGraph>::LabeledPredecessors<'succ>
    where
        Self: 'succ;

    fn untyped_labeled_predecessors(&self, node_id: NodeId) -> Self::LabeledPredecessors<'_> {
        self.record("untyped_labeled_predecessors", (node_id,));
        self.graph.untyped_labeled_predecessors(node_id)
    }
}

impl<G: SwhGraphWithProperties> SwhGraphWithProperties for GraphSpy<G> {
    type Maps = <G as SwhGraphWithProperties>::Maps;
    type Timestamps = <G as SwhGraphWithProperties>::Timestamps;
    type Persons = <G as SwhGraphWithProperties>::Persons;
    type Contents = <G as SwhGraphWithProperties>::Contents;
    type Strings = <G as SwhGraphWithProperties>::Strings;
    type LabelNames = <G as SwhGraphWithProperties>::LabelNames;

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
        self.record("properties", ());
        self.graph.properties()
    }
}

/* TODO:
impl<G: SwhGraphWithProperties> SwhGraphWithProperties for GraphSpy<G>
where
    <G as SwhGraphWithProperties>::Maps: Maps,
    <G as SwhGraphWithProperties>::Timestamps: Timestamps,
    <G as SwhGraphWithProperties>::Persons: Persons,
    <G as SwhGraphWithProperties>::Contents: Contents,
    <G as SwhGraphWithProperties>::Strings: Strings,
    <G as SwhGraphWithProperties>::LabelNames: LabelNames,
{
    type Maps = PropertiesSpy<<G as SwhGraphWithProperties>::Maps>;
    type Timestamps = PropertiesSpy<<G as SwhGraphWithProperties>::Timestamps>;
    type Persons = PropertiesSpy<<G as SwhGraphWithProperties>::Persons>;
    type Contents = PropertiesSpy<<G as SwhGraphWithProperties>::Contents>;
    type Strings = PropertiesSpy<<G as SwhGraphWithProperties>::Strings>;
    type LabelNames = PropertiesSpy<<G as SwhGraphWithProperties>::LabelNames>;

    fn properties(&self) -> ??? {
        self.record("properties", ());
        let SwhGraphProperties {
            path,
            num_nodes,
            maps,
            timestamps,
            persons,
            contents,
            strings,
            label_names,
        } = *self.graph.properties();
        Arc::new(SwhGraphProperties {
            path,
            num_nodes,
            maps: PropertiesSpy {
                inner: maps,
                history: self.history.clone(),
            },
            timestamps: PropertiesSpy {
                inner: timestamps,
                history: self.history.clone(),
            },
            persons: PropertiesSpy {
                inner: persons,
                history: self.history.clone(),
            },
            contents: PropertiesSpy {
                inner: contents,
                history: self.history.clone(),
            },
            strings: PropertiesSpy {
                inner: strings,
                history: self.history.clone(),
            },
            label_names: PropertiesSpy {
                inner: label_names,
                history: self.history.clone(),
            },
        })
    }
}

pub struct PropertiesSpy<P> {
    pub inner: P,
    /// Pairs of `(method, arguments)`
    pub history: Arc<Mutex<Vec<(&'static str, String)>>>,
}

impl<P> PropertiesSpy<P> {
    fn record<Args: std::fmt::Debug>(&self, method: &'static str, arguments: Args) {
        self.history
            .lock()
            .unwrap()
            .push((method, format!("{:?}", arguments)));
    }
}

impl<C: Contents> Contents for PropertiesSpy<C> {
    type Data<'a> = <C as Contents>::Data<'a> where C: 'a;

    fn is_skipped_content(&self) -> Self::Data<'_> {
        self.record("is_skipped_content", ());
        self.inner.is_skipped_content()
    }
    fn content_length(&self) -> Self::Data<'_> {
        self.record("content_length", ());
        self.inner.content_length()
    }
}

impl<L: LabelNames> LabelNames for PropertiesSpy<L> {
    type LabelNames<'a> = <L as LabelNames>::LabelNames<'a> where L: 'a;

    fn label_names(&self) -> Self::LabelNames<'_> {
        self.record("label_names", ());
        self.inner.label_names()
    }
}

impl<M: Maps> Maps for PropertiesSpy<M> {
    type MPHF = <M as Maps>::MPHF;
    type Perm = <M as Maps>::Perm;
    type Memory = <M as Maps>::Memory;

    fn mphf(&self) -> &Self::MPHF {
        self.record("mphf", ());
        self.inner.mphf()
    }
    fn order(&self) -> &Self::Perm {
        self.record("order", ());
        self.inner.order()
    }
    fn node2swhid(&self) -> &Node2SWHID<Self::Memory> {
        self.record("node2swhid", ());
        self.inner.node2swhid()
    }
    fn node2type(&self) -> &Node2Type<UsizeMmap<Self::Memory>> {
        self.record("node2type", ());
        self.inner.node2type()
    }
}

impl<P: Persons> Persons for PropertiesSpy<P> {
    type PersonIds<'a> = <P as Persons>::PersonIds<'a> where P: 'a;

    fn author_id(&self) -> Self::PersonIds<'_> {
        self.record("author_id", ());
        self.inner.author_id()
    }
    fn committer_id(&self) -> Self::PersonIds<'_> {
        self.record("committer_id", ());
        self.inner.committer_id()
    }
}

impl<S: Strings> Strings for PropertiesSpy<S> {
    type Offsets<'a> = <S as Strings>::Offsets<'a> where S: 'a;

    fn message(&self) -> &[u8] {
        self.record("message", ());
        self.inner.message()
    }
    fn message_offset(&self) -> Self::Offsets<'_> {
        self.record("message_offset", ());
        self.inner.message_offset()
    }
    fn tag_name(&self) -> &[u8] {
        self.record("tag_name", ());
        self.inner.tag_name()
    }
    fn tag_name_offset(&self) -> Self::Offsets<'_> {
        self.record("tag_name_offset", ());
        self.inner.tag_name_offset()
    }
}

impl<T: Timestamps> Timestamps for PropertiesSpy<T> {
    type Timestamps<'a> = <T as Timestamps>::Timestamps<'a> where T: 'a;
    type Offsets<'a> = <T as Timestamps>::Offsets<'a> where T: 'a;

    fn author_timestamp(&self) -> Self::Timestamps<'_> {
        self.record("author_timestamp", ());
        self.inner.author_timestamp()
    }
    fn author_timestamp_offset(&self) -> Self::Offsets<'_> {
        self.record("author_timestamp_offset", ());
        self.inner.author_timestamp_offset()
    }
    fn committer_timestamp(&self) -> Self::Timestamps<'_> {
        self.record("committer_timestamp", ());
        self.inner.committer_timestamp()
    }
    fn committer_timestamp_offset(&self) -> Self::Offsets<'_> {
        self.record("committer_timestamp_offset", ());
        self.inner.committer_timestamp_offset()
    }
}
*/
