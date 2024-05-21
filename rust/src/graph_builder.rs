/*
 * Copyright (C) 2024  The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

use std::collections::HashMap;

use anyhow::{ensure, Context, Result};

use crate::graph::{NodeId, SwhBidirectionalGraph};
use crate::labels::{DirEntry, FilenameId, Permission};
use crate::properties;
use crate::webgraph::graphs::vec_graph::VecGraph;
use crate::SwhGraphProperties;
use crate::SWHID;

#[derive(Clone, Debug, Default)]
pub struct GraphBuilder {
    name_to_id: HashMap<Vec<u8>, u64>,
    persons: HashMap<Vec<u8>, u32>,

    arcs: Vec<(NodeId, NodeId, Option<u64>)>,

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

    /// Adds an unlabelled arc to the graph
    pub fn arc(&mut self, src: NodeId, dst: NodeId) {
        self.arcs.push((src, dst, None));
    }

    /// Adds a labelled arc to the graph
    pub fn l_arc<P: Into<Permission>, N: Into<Vec<u8>>>(
        &mut self,
        src: NodeId,
        dst: NodeId,
        permission: P,
        name: N,
    ) {
        let permission = permission.into();
        let name = name.into();
        let name_id = self.name_to_id.entry(name.clone()).or_insert_with(|| {
            self.label_names.push(name);
            (self.label_names.len() - 1)
                .try_into()
                .expect("label_names length overflowed u64")
        });
        let label = Some(
            DirEntry::new(permission, FilenameId(*name_id))
                .expect("label_names is larger than 2^61 items")
                .0,
        );
        self.arcs.push((src, dst, label));
    }

    #[allow(clippy::type_complexity)]
    pub fn done(
        &self,
    ) -> Result<
        SwhBidirectionalGraph<
            SwhGraphProperties<
                properties::VecMaps,
                properties::VecTimestamps,
                properties::VecPersons,
                properties::VecContents,
                properties::VecStrings,
                properties::VecLabelNames,
            >,
            VecGraph<Option<u64>>,
            VecGraph<Option<u64>>,
        >,
    > {
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

        let arcs: Vec<_> = self
            .arcs
            .iter()
            .map(|(src, dst, label)| (*src, *dst, *label))
            .collect();

        let backward_arcs: Vec<(NodeId, NodeId, Option<u64>)> = arcs
            .iter()
            .map(|(src, dst, label)| (*dst, *src, *label))
            .collect();

        SwhBidirectionalGraph::from_underlying_graphs(
            std::path::PathBuf::default(),
            VecGraph::from_labeled_arc_list(arcs),
            VecGraph::from_labeled_arc_list(backward_arcs),
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
                    properties::VecLabelNames::new(self.label_names.clone())
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
