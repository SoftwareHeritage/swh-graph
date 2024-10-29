/*
 * Copyright (C) 2024  The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

//! Utility to dynamically build a small graph in memory

use std::collections::HashMap;

use anyhow::{ensure, Context, Result};
use itertools::Itertools;
use webgraph::prelude::{Left, Right, Zip};

use crate::graph::{NodeId, SwhBidirectionalGraph};
use crate::labels::{
    Branch, DirEntry, EdgeLabel, FilenameId, Permission, UntypedEdgeLabel, Visit, VisitStatus,
};
use crate::properties;
use crate::webgraph::graphs::vec_graph::VecGraph;
use crate::SwhGraphProperties;
use crate::SWHID;

// Type (alias) of the graph built by the graph builder
#[allow(clippy::type_complexity)]
pub type BuiltGraph = SwhBidirectionalGraph<
    SwhGraphProperties<
        properties::VecMaps,
        properties::VecTimestamps,
        properties::VecPersons,
        properties::VecContents,
        properties::VecStrings,
        properties::VecLabelNames,
    >,
    Zip<Left<VecGraph>, Right<VecGraph<Vec<u64>>>>,
    Zip<Left<VecGraph>, Right<VecGraph<Vec<u64>>>>,
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
    name_to_id: HashMap<Vec<u8>, u64>,
    persons: HashMap<Vec<u8>, u32>,

    arcs: Vec<(NodeId, NodeId, Option<EdgeLabel>)>,

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

    /// Adds an unlabeled arc to the graph
    pub fn arc(&mut self, src: NodeId, dst: NodeId) {
        self.arcs.push((src, dst, None));
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
            DirEntry::new(permission, FilenameId(name_id))
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
            Branch::new(FilenameId(name_id)).expect("label_names is larger than 2^61 items"),
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
        self.arcs.push((src, dst, Some(label.into())));
    }

    #[allow(clippy::type_complexity)]
    pub fn done(&self) -> Result<BuiltGraph> {
        let num_nodes = self.swhids.len();
        let mut seen = sux::prelude::BitVec::new(num_nodes);
        for (src, dst, _) in self.arcs.iter() {
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
        label_names_with_index.sort_unstable_by_key(|(_index, label_name)| label_name.clone());
        let mut label_permutation = vec![0; label_names_with_index.len()];
        for (new_index, (old_index, _)) in label_names_with_index.iter().enumerate() {
            label_permutation[*old_index] = new_index;
        }
        let label_names: Vec<_> = label_names_with_index
            .into_iter()
            .map(|(_index, label_name)| label_name)
            .collect();

        let mut arcs = self.arcs.clone();
        arcs.sort_by_key(|(src, dst, _label)| (*src, *dst)); // stable sort, it makes tests easier to write

        let arcs: Vec<(NodeId, NodeId, Vec<u64>)> = arcs
            .into_iter()
            .group_by(|(src, dst, _label)| (*src, *dst))
            .into_iter()
            .map(|((src, dst), arcs)| -> (NodeId, NodeId, Vec<u64>) {
                let labels = arcs
                    .flat_map(|(_src, _dst, labels)| {
                        labels.map(|label| {
                            UntypedEdgeLabel::from(match label {
                                EdgeLabel::Branch(branch) => EdgeLabel::Branch(
                                    Branch::new(FilenameId(
                                        label_permutation[branch.filename_id().0 as usize] as u64,
                                    ))
                                    .expect("Label name permutation overflowed"),
                                ),
                                EdgeLabel::DirEntry(entry) => EdgeLabel::DirEntry(
                                    DirEntry::new(
                                        entry.permission().expect("invalid permission"),
                                        FilenameId(
                                            label_permutation[entry.filename_id().0 as usize]
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
                (src, dst, labels)
            })
            .collect();

        let backward_arcs: Vec<(NodeId, NodeId, Vec<u64>)> = arcs
            .iter()
            .map(|(src, dst, labels)| (*dst, *src, labels.clone()))
            .collect();

        SwhBidirectionalGraph::from_underlying_graphs(
            std::path::PathBuf::default(),
            // Equivalent to VecGraph::from_labeled_arc_list(arcs), but bypasses the
            // constraint that the left side of the Zip must have Copy-able labels
            Zip(
                Left(VecGraph::from_arc_list(
                    arcs.iter().map(|(src, dst, _labels)| (*src, *dst)),
                )),
                Right(VecGraph::from_labeled_arc_list(arcs)),
            ),
            // Equivalent to VecGraph::from_labeled_arc_list(backward_arcs), but bypasses the
            // constraint that the left side of the Zip must have Copy-able labels
            Zip(
                Left(VecGraph::from_arc_list(
                    backward_arcs.iter().map(|(src, dst, _labels)| (*src, *dst)),
                )),
                Right(VecGraph::from_labeled_arc_list(backward_arcs)),
            ),
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

impl<'builder> NodeBuilder<'builder> {
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
