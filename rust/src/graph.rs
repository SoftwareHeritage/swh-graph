// Copyright (C) 2023  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

//! Structures to manipulate the Software Heritage graph

use std::path::{Path, PathBuf};

use anyhow::Result;
use webgraph::prelude::*;
//use webgraph::traits::{RandomAccessGraph, SequentialGraph};

use crate::properties::{SwhGraphProperties, SwhidMphf};
use crate::utils::suffix_path;

/// Alias for [`usize`], which may become a newtype in a future version.
pub type NodeId = usize;

/// Class representing the compressed Software Heritage graph in a single direction.
pub struct SwhUnidirectionalGraph<G: RandomAccessGraph, P> {
    basepath: PathBuf,
    graph: G,
    properties: P,
}

impl<G: RandomAccessGraph, P> SwhUnidirectionalGraph<G, P> {
    /// Return the number of nodes in the graph.
    pub fn num_nodes(&self) -> usize {
        self.graph.num_nodes()
    }

    /// Return the number of arcs in the graph.
    pub fn num_arcs(&self) -> usize {
        self.graph.num_arcs()
    }

    /// Return an [`IntoIterator`] over the successors of a node.
    pub fn successors(&self, node_id: NodeId) -> <G as RandomAccessGraph>::Successors<'_> {
        self.graph.successors(node_id)
    }

    /// Return the number of successors of a node.
    pub fn outdegree(&self, node_id: NodeId) -> usize {
        self.graph.outdegree(node_id)
    }

    /// Return whether there is an arc going from `src_node_id` to `dst_node_id`.
    pub fn has_arc(&self, src_node_id: NodeId, dst_node_id: NodeId) -> bool {
        self.graph.has_arc(src_node_id, dst_node_id)
    }
}

impl<G: RandomAccessGraph> SwhUnidirectionalGraph<G, ()> {
    pub fn load_properties<MPHF: SwhidMphf>(
        self,
    ) -> Result<SwhUnidirectionalGraph<G, SwhGraphProperties<MPHF>>> {
        Ok(SwhUnidirectionalGraph {
            properties: SwhGraphProperties::new(&self.basepath, self.graph.num_nodes())?,
            basepath: self.basepath,
            graph: self.graph,
        })
    }
}

impl<G: RandomAccessGraph, MPHF: SwhidMphf> SwhUnidirectionalGraph<G, SwhGraphProperties<MPHF>> {
    pub fn properties(&self) -> &SwhGraphProperties<MPHF> {
        &self.properties
    }
}

/// Class representing the compressed Software Heritage graph in both directions.
pub struct SwhBidirectionalGraph<G: RandomAccessGraph, P> {
    basepath: PathBuf,
    forward_graph: G,
    backward_graph: G,
    properties: P,
}

impl<G: RandomAccessGraph, P> SwhBidirectionalGraph<G, P> {
    /// Return the number of nodes in the graph.
    pub fn num_nodes(&self) -> usize {
        self.forward_graph.num_nodes()
    }

    /// Return the number of arcs in the graph.
    pub fn num_arcs(&self) -> usize {
        self.forward_graph.num_arcs()
    }

    /// Return an [`IntoIterator`] over the successors of a node.
    pub fn successors(&self, node_id: NodeId) -> <G as RandomAccessGraph>::Successors<'_> {
        self.forward_graph.successors(node_id)
    }

    /// Return an [`IntoIterator`] over the ancestors of a node.
    pub fn ancestors(&self, node_id: NodeId) -> <G as RandomAccessGraph>::Successors<'_> {
        self.backward_graph.successors(node_id)
    }

    /// Return the number of successors of a node.
    pub fn outdegree(&self, node_id: NodeId) -> usize {
        self.forward_graph.outdegree(node_id)
    }

    /// Return the number of ancestors of a node.
    pub fn indegree(&self, node_id: NodeId) -> usize {
        self.backward_graph.outdegree(node_id)
    }

    /// Return whether there is an arc going from `src_node_id` to `dst_node_id`.
    pub fn has_arc(&self, src_node_id: NodeId, dst_node_id: NodeId) -> bool {
        self.forward_graph.has_arc(src_node_id, dst_node_id)
    }
}

impl<G: RandomAccessGraph> SwhBidirectionalGraph<G, ()> {
    pub fn load_properties<MPHF: SwhidMphf>(
        self,
    ) -> Result<SwhBidirectionalGraph<G, SwhGraphProperties<MPHF>>> {
        Ok(SwhBidirectionalGraph {
            properties: SwhGraphProperties::new(&self.basepath, self.forward_graph.num_nodes())?,
            basepath: self.basepath,
            forward_graph: self.forward_graph,
            backward_graph: self.backward_graph,
        })
    }
}

impl<G: RandomAccessGraph, MPHF: SwhidMphf> SwhBidirectionalGraph<G, SwhGraphProperties<MPHF>> {
    pub fn properties(&self) -> &SwhGraphProperties<MPHF> {
        &self.properties
    }
}

pub fn load_unidirectional(
    basepath: impl AsRef<Path>,
) -> Result<
    SwhUnidirectionalGraph<
        BVGraph<
            DynamicCodesReaderBuilder<dsi_bitstream::prelude::BE, MmapBackend<u32>>,
            webgraph::EF<&'static [usize]>,
        >,
        (),
    >,
> {
    let basepath = basepath.as_ref().to_owned();
    let graph = webgraph::graph::bvgraph::load(&basepath)?;
    Ok(SwhUnidirectionalGraph {
        basepath,
        graph,
        properties: (),
    })
}

pub fn load_bidirectional(
    basepath: impl AsRef<Path>,
) -> Result<
    SwhBidirectionalGraph<
        BVGraph<
            DynamicCodesReaderBuilder<dsi_bitstream::prelude::BE, MmapBackend<u32>>,
            webgraph::EF<&'static [usize]>,
        >,
        (),
    >,
> {
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
