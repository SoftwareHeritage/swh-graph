// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

//! Serialization and deserialization of (small) graphs using [`serde`]

use serde::de::*;
use serde::ser::*;
use serde::*;
use webgraph::prelude::{Left, Right, VecGraph, Zip};

use crate::graph::*;
use crate::properties;
use crate::SWHID;

#[derive(Serialize, Deserialize)]
struct SerializedGraph<Contents, LabelNames, Persons, Strings, Timestamps> {
    swhids: Vec<SWHID>,
    contents: Contents,
    label_names: LabelNames,
    persons: Persons,
    strings: Strings,
    timestamps: Timestamps,
    /// `node_id -> (node_id, vec![label])`
    arcs: Vec<Vec<(usize, Vec<u64>)>>,
}

/// Serializes a (small) graph using [`serde`] instead of the normal serialization
pub fn serialize_with_labels_and_maps<
    S: Serializer,
    G: SwhLabeledForwardGraph + SwhGraphWithProperties,
>(
    serializer: S,
    graph: &G,
) -> Result<S::Ok, S::Error>
where
    <G as SwhGraphWithProperties>::Maps: properties::Maps,
    <G as SwhGraphWithProperties>::Contents: serde::Serialize,
    <G as SwhGraphWithProperties>::LabelNames: serde::Serialize,
    <G as SwhGraphWithProperties>::Persons: serde::Serialize,
    <G as SwhGraphWithProperties>::Strings: serde::Serialize,
    <G as SwhGraphWithProperties>::Timestamps: serde::Serialize,
{
    SerializedGraph {
        swhids: (0..graph.num_nodes())
            .map(|node| graph.properties().swhid(node))
            .collect(),
        contents: &graph.properties().contents,
        label_names: &graph.properties().label_names,
        persons: &graph.properties().persons,
        strings: &graph.properties().strings,
        timestamps: &graph.properties().timestamps,
        arcs: (0..graph.num_nodes())
            .map(|node| {
                graph
                    .untyped_labeled_successors(node)
                    .into_iter()
                    .map(|(succ, labels)| (succ, labels.into_iter().map(|label| label.0).collect()))
                    .collect()
            })
            .collect(),
    }
    .serialize(serializer)
}

/// Deserializes a (small) graph using [`serde`] instead of the normal deserialization, and
/// returns a fully in-memory graph, as if built by
/// [`GraphBuilder`](crate::graph_builder::GraphBuilder)
pub fn deserialize<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<crate::graph_builder::BuiltGraph, D::Error> {
    let graph: SerializedGraph<
        properties::VecContents,
        properties::VecLabelNames,
        properties::VecPersons,
        properties::VecStrings,
        properties::VecTimestamps,
    > = SerializedGraph::deserialize(deserializer)?;
    let forward_arcs: Vec<(NodeId, NodeId, Vec<u64>)> = graph
        .arcs
        .iter()
        .enumerate()
        .flat_map(|(src, arcs)| {
            arcs.iter()
                .map(move |(dst, labels)| (src, *dst, labels.clone()))
        })
        .collect();
    let backward_arcs: Vec<(NodeId, NodeId, Vec<u64>)> = graph
        .arcs
        .iter()
        .enumerate()
        .flat_map(|(src, arcs)| {
            arcs.iter()
                .map(move |(dst, labels)| (*dst, src, labels.clone()))
        })
        .collect();
    Ok(SwhBidirectionalGraph::from_underlying_graphs(
        std::path::PathBuf::default(),
        // Equivalent to VecGraph::from_labeled_arc_list(arcs), but bypasses the
        // constraint that the left side of the Zip must have Copy-able labels
        Zip(
            Left(VecGraph::from_arc_list(
                forward_arcs.iter().map(|(src, dst, _labels)| (*src, *dst)),
            )),
            Right(VecGraph::from_labeled_arc_list(forward_arcs)),
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
    .load_properties(move |properties| {
        Ok(properties
            .with_maps(properties::VecMaps::new(graph.swhids))
            .expect("Could not join maps")
            .with_contents(graph.contents)
            .expect("Could not join VecContents")
            .with_label_names(graph.label_names)
            .expect("Could not join maps")
            .with_persons(graph.persons)
            .expect("Could not join persons")
            .with_strings(graph.strings)
            .expect("Could not join strings")
            .with_timestamps(graph.timestamps)
            .expect("Could not join timestamps"))
    })
    .expect("Could not load properties"))
}
