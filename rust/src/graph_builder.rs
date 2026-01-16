/*
 * Copyright (C) 2024-2025  The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

//! Utility to dynamically build a small graph in memory

use std::collections::{HashMap, HashSet};
use std::io::Write;

use anyhow::{bail, ensure, Context, Result};
use itertools::Itertools;
use sux::traits::bit_vec_ops::{BitVecOps, BitVecOpsMut};
use webgraph::graphs::vec_graph::LabeledVecGraph;

use crate::graph::*;
use crate::labels::{
    Branch, DirEntry, EdgeLabel, LabelNameId, Permission, UntypedEdgeLabel, Visit, VisitStatus,
};
use crate::properties;
use crate::views::{GraphAccessRecord, GraphSpy, PropertyAccess};
use crate::SwhGraphProperties;
use crate::{NodeType, SWHID};

/// How to sort label names in the produced graph. Defaults to `Lexicographic`.
#[derive(Default, Clone, Copy, Debug)]
pub enum LabelNamesOrder {
    #[default]
    /// Sort label names in their lexicographic order, as for graphs compressed with Rust
    /// (2023-09-06 and newer)
    Lexicographic,
    /// Sort label names in the lexicographic order of their base64-encoding, as for graphs
    /// compressed with Java (2022-12-07 and older)
    LexicographicBase64,
}

// Type (alias) of the graph built by the graph builder
pub type BuiltGraph = SwhBidirectionalGraph<
    SwhGraphProperties<
        properties::VecMaps,
        properties::VecTimestamps,
        properties::VecPersons,
        properties::VecContents,
        properties::VecStrings,
        properties::VecLabelNames,
    >,
    LabeledVecGraph<Vec<u64>>,
    LabeledVecGraph<Vec<u64>>,
>;

/// Dynamically builds a small graph in memory
///
/// # Examples
///
/// ## Directories and contents
///
/// ```
/// use swh_graph::swhid;
/// use swh_graph::graph_builder::GraphBuilder;
/// use swh_graph::labels::Permission;
///
/// let mut builder = GraphBuilder::default();
/// let a = builder
///     .node(swhid!(swh:1:dir:0000000000000000000000000000000000000010)).unwrap()
///     .done();
/// let b = builder
///     .node(swhid!(swh:1:dir:0000000000000000000000000000000000000020)).unwrap()
///     .done();
/// let c = builder
///     .node(swhid!(swh:1:cnt:0000000000000000000000000000000000000030)).unwrap()
///     .done();
/// builder.dir_arc(a, b, Permission::Directory, b"tests");
/// builder.dir_arc(a, c, Permission::ExecutableContent, b"run.sh");
/// builder.dir_arc(b, c, Permission::Content, b"test.c");
/// let _ = builder.done().unwrap();
/// ```
///
/// # Revisions and releases
///
/// ```
/// use swh_graph::swhid;
///
/// use swh_graph::graph_builder::GraphBuilder;
/// let mut builder = GraphBuilder::default();
/// let a = builder
///     .node(swhid!(swh:1:rev:0000000000000000000000000000000000000010)).unwrap()
///     .author_timestamp(1708950743, 60)
///     .done();
/// let b = builder
///     .node(swhid!(swh:1:rev:0000000000000000000000000000000000000020)).unwrap()
///     .committer_timestamp(1708950821, 120)
///     .done();
/// builder.arc(a, b);
/// let _ = builder.done().unwrap();
/// ```
#[derive(Clone, Debug, Default)]
pub struct GraphBuilder {
    pub label_names_order: LabelNamesOrder,

    name_to_id: HashMap<Vec<u8>, u64>,
    persons: HashMap<Vec<u8>, u32>,

    arcs: Vec<((NodeId, NodeId), Option<EdgeLabel>)>,

    swhids: Vec<SWHID>,
    is_skipped_content: Vec<bool>,
    content_lengths: Vec<Option<u64>>,
    author_ids: Vec<Option<u32>>,
    committer_ids: Vec<Option<u32>>,
    messages: Vec<Option<Vec<u8>>>,
    tag_names: Vec<Option<Vec<u8>>>,
    author_timestamps: Vec<Option<i64>>,
    author_timestamp_offsets: Vec<Option<i16>>,
    committer_timestamps: Vec<Option<i64>>,
    committer_timestamp_offsets: Vec<Option<i16>>,
    label_names: Vec<Vec<u8>>,
}

impl GraphBuilder {
    /// Adds a node to the graph.
    ///
    /// Returns `Err` if there is already a node with this SWHID.
    pub fn node(&mut self, swhid: SWHID) -> Result<NodeBuilder<'_>> {
        ensure!(!self.swhids.contains(&swhid), "Duplicate SWHID {swhid}");
        let node_id = self.swhids.len();
        self.swhids.push(swhid);
        self.is_skipped_content.push(false);
        self.content_lengths.push(None);
        self.author_ids.push(None);
        self.committer_ids.push(None);
        self.messages.push(None);
        self.tag_names.push(None);
        self.author_timestamps.push(None);
        self.author_timestamp_offsets.push(None);
        self.committer_timestamps.push(None);
        self.committer_timestamp_offsets.push(None);
        Ok(NodeBuilder {
            node_id,
            graph_builder: self,
        })
    }

    /// Returns `NodeId` that represents this SWHID in the graph, if it exists
    pub fn node_id(&self, swhid: SWHID) -> Option<NodeId> {
        self.swhids.iter().position(|x| *x == swhid)
    }

    /// Adds an unlabeled arc to the graph
    pub fn arc(&mut self, src: NodeId, dst: NodeId) {
        self.arcs.push(((src, dst), None));
    }

    /// Adds a labeled dir->{cnt,dir,rev} arc to the graph
    pub fn dir_arc<P: Into<Permission>, N: Into<Vec<u8>>>(
        &mut self,
        src: NodeId,
        dst: NodeId,
        permission: P,
        name: N,
    ) {
        let permission = permission.into();
        let name = name.into();
        let name_id = *self.name_to_id.entry(name.clone()).or_insert_with(|| {
            self.label_names.push(name);
            (self.label_names.len() - 1)
                .try_into()
                .expect("label_names length overflowed u64")
        });
        self.l_arc(
            src,
            dst,
            DirEntry::new(permission, LabelNameId(name_id))
                .expect("label_names is larger than 2^61 items"),
        );
    }

    /// Adds a labeled snp->{cnt,dir,rev,rel} arc to the graph
    pub fn snp_arc<N: Into<Vec<u8>>>(&mut self, src: NodeId, dst: NodeId, name: N) {
        let name = name.into();
        let name_id = *self.name_to_id.entry(name.clone()).or_insert_with(|| {
            self.label_names.push(name);
            (self.label_names.len() - 1)
                .try_into()
                .expect("label_names length overflowed u64")
        });
        self.l_arc(
            src,
            dst,
            Branch::new(LabelNameId(name_id)).expect("label_names is larger than 2^61 items"),
        );
    }

    /// Adds a labeled ori->snp arc to the graph
    pub fn ori_arc(&mut self, src: NodeId, dst: NodeId, status: VisitStatus, timestamp: u64) {
        self.l_arc(
            src,
            dst,
            Visit::new(status, timestamp).expect("invalid timestamp"),
        );
    }

    /// Adds a labeled arc to the graph
    pub fn l_arc<L: Into<EdgeLabel>>(&mut self, src: NodeId, dst: NodeId, label: L) {
        self.arcs.push(((src, dst), Some(label.into())));
    }

    #[allow(clippy::type_complexity)]
    pub fn done(&self) -> Result<BuiltGraph> {
        let num_nodes = self.swhids.len();
        let mut seen = sux::prelude::BitVec::new(num_nodes);
        for ((src, dst), _) in self.arcs.iter() {
            seen.set(*src, true);
            seen.set(*dst, true);
        }
        for node in 0..num_nodes {
            ensure!(
                seen.get(node),
                "VecGraph requires every node to have at least one arc, and {} does not have any",
                node
            );
        }

        let mut label_names_with_index: Vec<_> =
            self.label_names.iter().cloned().enumerate().collect();
        match self.label_names_order {
            LabelNamesOrder::Lexicographic => label_names_with_index
                .sort_unstable_by_key(|(_index, label_name)| label_name.clone()),
            LabelNamesOrder::LexicographicBase64 => {
                let base64 = base64_simd::STANDARD;
                label_names_with_index.sort_unstable_by_key(|(_index, label_name)| {
                    base64.encode_to_string(label_name).into_bytes()
                });
            }
        }
        let mut label_permutation = vec![0; label_names_with_index.len()];
        for (new_index, (old_index, _)) in label_names_with_index.iter().enumerate() {
            label_permutation[*old_index] = new_index;
        }
        let label_names: Vec<_> = label_names_with_index
            .into_iter()
            .map(|(_index, label_name)| label_name)
            .collect();

        let mut arcs = self.arcs.clone();
        arcs.sort_by_key(|((src, dst), _label)| (*src, *dst)); // stable sort, it makes tests easier to write

        let arcs: Vec<((NodeId, NodeId), Vec<u64>)> = arcs
            .into_iter()
            .group_by(|((src, dst), _label)| (*src, *dst))
            .into_iter()
            .map(|((src, dst), arcs)| -> ((NodeId, NodeId), Vec<u64>) {
                let labels = arcs
                    .flat_map(|((_src, _dst), labels)| {
                        labels.map(|label| {
                            UntypedEdgeLabel::from(match label {
                                EdgeLabel::Branch(branch) => EdgeLabel::Branch(
                                    Branch::new(LabelNameId(
                                        label_permutation[branch.label_name_id().0 as usize] as u64,
                                    ))
                                    .expect("Label name permutation overflowed"),
                                ),
                                EdgeLabel::DirEntry(entry) => EdgeLabel::DirEntry(
                                    DirEntry::new(
                                        entry.permission().expect("invalid permission"),
                                        LabelNameId(
                                            label_permutation[entry.label_name_id().0 as usize]
                                                as u64,
                                        ),
                                    )
                                    .expect("Label name permutation overflowed"),
                                ),
                                EdgeLabel::Visit(visit) => EdgeLabel::Visit(visit),
                            })
                            .0
                        })
                    })
                    .collect();
                ((src, dst), labels)
            })
            .collect();

        let backward_arcs: Vec<((NodeId, NodeId), Vec<u64>)> = arcs
            .iter()
            .map(|((src, dst), labels)| ((*dst, *src), labels.clone()))
            .collect();

        SwhBidirectionalGraph::from_underlying_graphs(
            std::path::PathBuf::default(),
            LabeledVecGraph::from_arcs(arcs),
            LabeledVecGraph::from_arcs(backward_arcs),
        )
        .init_properties()
        .load_properties(|properties| {
            properties
                .with_maps(properties::VecMaps::new(self.swhids.clone()))
                .context("Could not join maps")?
                .with_contents(
                    properties::VecContents::new(
                        self.is_skipped_content
                            .iter()
                            .copied()
                            .zip(self.content_lengths.iter().copied())
                            .collect(),
                    )
                    .context("Could not build VecContents")?,
                )
                .context("Could not join VecContents")?
                .with_label_names(
                    properties::VecLabelNames::new(label_names.clone())
                        .context("Could not build VecLabelNames")?,
                )
                .context("Could not join maps")?
                .with_persons(
                    properties::VecPersons::new(
                        self.author_ids
                            .iter()
                            .copied()
                            .zip(self.committer_ids.iter().copied())
                            .collect(),
                    )
                    .context("Could not build VecPersons")?,
                )
                .context("Could not join persons")?
                .with_strings(
                    properties::VecStrings::new(
                        self.messages
                            .iter()
                            .cloned()
                            .zip(self.tag_names.iter().cloned())
                            .collect(),
                    )
                    .context("Could not build VecStrings")?,
                )
                .context("Could not join strings")?
                .with_timestamps(
                    properties::VecTimestamps::new(
                        self.author_timestamps
                            .iter()
                            .copied()
                            .zip(self.author_timestamp_offsets.iter().copied())
                            .zip(self.committer_timestamps.iter().copied())
                            .zip(self.committer_timestamp_offsets.iter().copied())
                            .map(|(((a_ts, a_ts_o), c_ts), c_ts_o)| (a_ts, a_ts_o, c_ts, c_ts_o))
                            .collect(),
                    )
                    .context("Could not build VecTimestamps")?,
                )
                .context("Could not join timestamps")
        })
    }
}

pub struct NodeBuilder<'builder> {
    node_id: NodeId,
    graph_builder: &'builder mut GraphBuilder,
}

impl NodeBuilder<'_> {
    pub fn done(&self) -> NodeId {
        self.node_id
    }

    pub fn content_length(&mut self, content_length: u64) -> &mut Self {
        self.graph_builder.content_lengths[self.node_id] = Some(content_length);
        self
    }

    pub fn is_skipped_content(&mut self, is_skipped_content: bool) -> &mut Self {
        self.graph_builder.is_skipped_content[self.node_id] = is_skipped_content;
        self
    }

    pub fn author(&mut self, author: Vec<u8>) -> &mut Self {
        let next_author_id = self
            .graph_builder
            .persons
            .len()
            .try_into()
            .expect("person names overflowed u32");
        let author_id = self
            .graph_builder
            .persons
            .entry(author)
            .or_insert(next_author_id);
        self.graph_builder.author_ids[self.node_id] = Some(*author_id);
        self
    }

    pub fn committer(&mut self, committer: Vec<u8>) -> &mut Self {
        let next_committer_id = self
            .graph_builder
            .persons
            .len()
            .try_into()
            .expect("person names overflowed u32");
        let committer_id = self
            .graph_builder
            .persons
            .entry(committer)
            .or_insert(next_committer_id);
        self.graph_builder.committer_ids[self.node_id] = Some(*committer_id);
        self
    }

    pub fn message(&mut self, message: Vec<u8>) -> &mut Self {
        self.graph_builder.messages[self.node_id] = Some(message);
        self
    }

    pub fn tag_name(&mut self, tag_name: Vec<u8>) -> &mut Self {
        self.graph_builder.tag_names[self.node_id] = Some(tag_name);
        self
    }

    pub fn author_timestamp(&mut self, ts: i64, offset: i16) -> &mut Self {
        self.graph_builder.author_timestamps[self.node_id] = Some(ts);
        self.graph_builder.author_timestamp_offsets[self.node_id] = Some(offset);
        self
    }

    pub fn committer_timestamp(&mut self, ts: i64, offset: i16) -> &mut Self {
        self.graph_builder.committer_timestamps[self.node_id] = Some(ts);
        self.graph_builder.committer_timestamp_offsets[self.node_id] = Some(offset);
        self
    }
}

pub fn codegen_from_full_graph<
    G: SwhLabeledForwardGraph
        + SwhGraphWithProperties<
            Maps: properties::Maps,
            Timestamps: properties::Timestamps,
            Persons: properties::Persons,
            Contents: properties::Contents,
            Strings: properties::Strings,
            LabelNames: properties::LabelNames,
        >,
    W: Write,
>(
    graph: &G,
    mut writer: W,
) -> Result<(), std::io::Error> {
    // Turns a Vec<u8> into an escaped bytestring
    let bytestring = |b: Vec<u8>| b.into_iter().map(std::ascii::escape_default).join("");

    writer.write_all(b"use swh_graph::swhid;\nuse swh_graph::graph_builder::GraphBuilder;\n")?;
    writer.write_all(b"use swh_graph::labels::{Permission, VisitStatus};\n")?;
    writer.write_all(b"\n")?;
    writer.write_all(b"let mut builder = GraphBuilder::default();\n")?;
    for node in 0..graph.num_nodes() {
        writer.write_all(
            format!(
                "builder\n    .node(swhid!({}))\n    .unwrap()\n",
                graph.properties().swhid(node)
            )
            .as_bytes(),
        )?;
        let mut write_line = |s: String| writer.write_all(format!("    {s}\n").as_bytes());
        match graph.properties().node_type(node) {
            NodeType::Content => {
                write_line(format!(
                    ".is_skipped_content({})",
                    graph.properties().is_skipped_content(node),
                ))?;
            }
            NodeType::Revision
            | NodeType::Release
            | NodeType::Directory
            | NodeType::Snapshot
            | NodeType::Origin => {}
        }
        if let Some(v) = graph.properties().content_length(node) {
            write_line(format!(".content_length({v})"))?;
        }
        if let Some(v) = graph.properties().message(node) {
            write_line(format!(".message(b\"{}\".to_vec())", bytestring(v)))?;
        }
        if let Some(v) = graph.properties().tag_name(node) {
            write_line(format!(".tag_name(b\"{}\".to_vec())", bytestring(v)))?;
        }
        if let Some(v) = graph.properties().author_id(node) {
            write_line(format!(".author(b\"{v}\".to_vec())"))?;
        }
        if let Some(ts) = graph.properties().author_timestamp(node) {
            let offset = graph
                .properties()
                .author_timestamp_offset(node)
                .expect("Node has author_timestamp but no author_timestamp_offset");
            write_line(format!(".author_timestamp({ts}, {offset})"))?;
        }
        if let Some(v) = graph.properties().committer_id(node) {
            write_line(format!(".committer(b\"{v}\".to_vec())"))?;
        }
        if let Some(ts) = graph.properties().committer_timestamp(node) {
            let offset = graph
                .properties()
                .committer_timestamp_offset(node)
                .expect("Node has committer_timestamp but no committer_timestamp_offset");
            write_line(format!(".committer_timestamp({ts}, {offset})"))?;
        }
        writer.write_all(b"    .done();\n")?;
    }

    writer.write_all(b"\n")?;

    for node in 0..graph.num_nodes() {
        for (succ, labels) in graph.labeled_successors(node) {
            let mut has_labels = false;
            for label in labels {
                writer.write_all(
                    match label {
                        EdgeLabel::DirEntry(label) => format!(
                            "builder.dir_arc({}, {}, Permission::{:?}, b\"{}\".to_vec());\n",
                            node,
                            succ,
                            label.permission().expect("Invalid permission"),
                            bytestring(graph.properties().label_name(label.label_name_id())),
                        ),
                        EdgeLabel::Branch(label) => format!(
                            "builder.snp_arc({}, {}, b\"{}\".to_vec());\n",
                            node,
                            succ,
                            bytestring(graph.properties().label_name(label.label_name_id())),
                        ),
                        EdgeLabel::Visit(label) => format!(
                            "builder.ori_arc({}, {}, VisitStatus::{:?}, {});\n",
                            node,
                            succ,
                            label.status(),
                            label.timestamp(),
                        ),
                    }
                    .as_bytes(),
                )?;
                has_labels = true;
            }
            if !has_labels {
                writer.write_all(format!("builder.arc({node}, {succ});\n").as_bytes())?;
            }
        }
    }

    writer.write_all(b"\nbuilder.done().expect(\"Could not build graph\")\n")?;

    Ok(())
}

/// Builds a [`BuiltGraph`] from a [`GraphSpy`]'s recorded history
///
/// This analyzes which nodes, arcs, and properties were accessed during execution
/// and builds a minimal graph containing just those elements for bug reproduction.
pub fn build_graph_from_spy<G>(
    spy: &GraphSpy<
        G,
        impl properties::MaybeMaps,
        impl properties::MaybeTimestamps,
        impl properties::MaybePersons,
        impl properties::MaybeContents,
        impl properties::MaybeStrings,
        impl properties::MaybeLabelNames,
    >,
) -> Result<BuiltGraph>
where
    G: SwhLabeledForwardGraph
        + SwhLabeledBackwardGraph
        + SwhGraphWithProperties<
            Maps: properties::Maps,
            Timestamps: properties::Timestamps,
            Persons: properties::Persons,
            Contents: properties::Contents,
            Strings: properties::Strings,
            LabelNames: properties::LabelNames,
        >,
{
    let history = spy.history.lock().unwrap();

    // all accessed nodes and arcs from history
    let mut nodes = HashSet::<NodeId>::new();
    let mut arcs = HashMap::<(NodeId, NodeId), Vec<EdgeLabel>>::new();

    for record in history.iter() {
        match record {
            // SwhGraph
            GraphAccessRecord::HasNode(node) => {
                nodes.insert(*node);
            }
            GraphAccessRecord::HasArc(src, dst) => {
                nodes.insert(*src);
                nodes.insert(*dst);
                arcs.entry((*src, *dst)).or_default();
            }
            GraphAccessRecord::Path => {
                // GraphBuilder does not allow configuring this, and it probably doesn't matter
                // anyway
            }
            GraphAccessRecord::NumNodes
            | GraphAccessRecord::NumArcs
            | GraphAccessRecord::NumNodesByType
            | GraphAccessRecord::NumArcsByType => {
                // not much we can do about this one short of copying every node/arc, but it would
                // make the codegened graph huge, so it would be useless.
                // so we have to assume it's fine not to preserve the number of nodes.
                // perhaps in the future we can find a way to add "ghost" nodes in the built graph.
            }
            GraphAccessRecord::IsTransposed => {
                if spy.graph().is_transposed() {
                    // TODO
                    bail!("build_graph_from_spy does not support building transposed graphs yet");
                }
            }

            // SwhForwardGraph
            GraphAccessRecord::Successors(node) | GraphAccessRecord::Outdegree(node) => {
                nodes.insert(*node);
                for succ in spy.graph().successors(*node) {
                    nodes.insert(succ);
                    arcs.entry((*node, succ)).or_default();
                }
            }

            // SwhBackwardGraph
            GraphAccessRecord::Predecessors(node) | GraphAccessRecord::Indegree(node) => {
                nodes.insert(*node);
                for pred in spy.graph().predecessors(*node) {
                    nodes.insert(pred);
                    arcs.entry((pred, *node)).or_default();
                }
            }

            // SwhLabeledForwardGraph
            GraphAccessRecord::LabeledSuccessors(node) => {
                nodes.insert(*node);
                for (succ, labels) in spy.graph().labeled_successors(*node) {
                    nodes.insert(succ);
                    arcs.entry((*node, succ))
                        .or_insert_with(|| labels.into_iter().collect());
                }
            }

            // SwhLabeledBackwardGraph
            GraphAccessRecord::LabeledPredecessors(node) => {
                nodes.insert(*node);
                for (pred, labels) in spy.graph().labeled_predecessors(*node) {
                    nodes.insert(pred);
                    arcs.entry((pred, *node))
                        .or_insert_with(|| labels.into_iter().collect());
                }
            }

            // SwhGraphWithProperties
            GraphAccessRecord::Property(PropertyAccess::Swhid(node))
            | GraphAccessRecord::Property(PropertyAccess::NodeType(node))
            | GraphAccessRecord::Property(PropertyAccess::IsSkippedContent(node))
            | GraphAccessRecord::Property(PropertyAccess::ContentLength(node))
            | GraphAccessRecord::Property(PropertyAccess::AuthorId(node))
            | GraphAccessRecord::Property(PropertyAccess::CommitterId(node))
            | GraphAccessRecord::Property(PropertyAccess::Message(node))
            | GraphAccessRecord::Property(PropertyAccess::TagName(node))
            | GraphAccessRecord::Property(PropertyAccess::AuthorTimestamp(node))
            | GraphAccessRecord::Property(PropertyAccess::AuthorTimestampOffset(node))
            | GraphAccessRecord::Property(PropertyAccess::CommitterTimestamp(node))
            | GraphAccessRecord::Property(PropertyAccess::CommitterTimestampOffset(node)) => {
                nodes.insert(*node);
            }
            GraphAccessRecord::Property(PropertyAccess::NodeId(swhid)) => {
                if let Ok(node) = spy.graph().properties().node_id(*swhid) {
                    nodes.insert(node);
                }
            }
            GraphAccessRecord::Property(PropertyAccess::NodeIdForInvalidBytes(_))
            | GraphAccessRecord::Property(PropertyAccess::NodeIdForInvalidString(_))
            | GraphAccessRecord::Property(PropertyAccess::NodeIdForInvalidStrArray(_)) => {
                // nothing to do about those; they come from strings/bytes that can't be parsed as
                // SWHID, independently of what nodes are in te graph
            }
            GraphAccessRecord::Property(PropertyAccess::LabelNames) => {
                // we'll catch the needed ones from the arc label accesses
            }
        }
    }

    let mut builder = GraphBuilder::default();

    let mut new2old: Vec<_> = nodes.iter().copied().collect();
    new2old.sort();

    let old2new: HashMap<_, _> = new2old
        .iter()
        .enumerate()
        .map(|(new, &old)| (old, new))
        .collect();

    // Build nodes
    for &old_id in &new2old {
        let swhid = spy.graph().properties().swhid(old_id);
        let mut node_builder = builder
            .node(swhid)
            .with_context(|| format!("Could add node {swhid}"))?;

        node_builder.is_skipped_content(spy.graph().properties().is_skipped_content(old_id));

        if let Some(v) = spy.graph().properties().content_length(old_id) {
            node_builder.content_length(v);
        }

        if let Some(v) = spy.graph().properties().message(old_id) {
            node_builder.message(v);
        }

        if let Some(v) = spy.graph().properties().tag_name(old_id) {
            node_builder.tag_name(v);
        }

        if let Some(v) = spy.graph().properties().author_id(old_id) {
            // Convert PersonId to bytes (placeholder, since we don't have access to actual names)
            node_builder.author(format!("{v}").into_bytes());
        }

        if let Some(ts) = spy.graph().properties().author_timestamp(old_id) {
            let offset = spy
                .graph()
                .properties()
                .author_timestamp_offset(old_id)
                .expect("Node has author_timestamp but no author_timestamp_offset");
            node_builder.author_timestamp(ts, offset);
        }

        if let Some(v) = spy.graph().properties().committer_id(old_id) {
            // Convert PersonId to bytes (placeholder, since we don't have access to actual names)
            node_builder.committer(format!("{v}").into_bytes());
        }

        if let Some(ts) = spy.graph().properties().committer_timestamp(old_id) {
            let offset = spy
                .graph()
                .properties()
                .committer_timestamp_offset(old_id)
                .expect("Node has committer_timestamp but no committer_timestamp_offset");
            node_builder.committer_timestamp(ts, offset);
        }

        let new_id = node_builder.done();
        assert_eq!(new2old[new_id], old_id);
        assert_eq!(old2new.get(&old_id), Some(&new_id));
    }

    // Build arcs
    for ((old_src, old_dst), labels) in arcs {
        let new_src = old2new[&old_src];
        let new_dst = old2new[&old_dst];

        if labels.is_empty() {
            // unlabeled node, or node whose labels we never tried to read
            builder.arc(new_src, new_dst)
        } else {
            for label in labels {
                match label {
                    EdgeLabel::DirEntry(label) => {
                        builder.dir_arc(
                            new_src,
                            new_dst,
                            label.permission().expect("Invalid permission"),
                            spy.graph().properties().label_name(label.label_name_id()),
                        );
                    }
                    EdgeLabel::Branch(label) => {
                        builder.snp_arc(
                            new_src,
                            new_dst,
                            spy.graph().properties().label_name(label.label_name_id()),
                        );
                    }
                    EdgeLabel::Visit(label) => {
                        builder.ori_arc(new_src, new_dst, label.status(), label.timestamp());
                    }
                }
            }
        }
    }

    builder.done()
}
