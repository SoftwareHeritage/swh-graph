// Copyright (C) 2023  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::path::PathBuf;

use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;

use swh_graph::graph::*;
use swh_graph::mph::DynMphf;

create_exception!(
    swh_graph,
    SwhGraphError,
    PyException,
    "Exception raised by swh-graph-rs"
);

#[pyclass]
struct BidirectionalGraph(SwhBidirectionalGraph<swh_graph::AllSwhGraphProperties<DynMphf>>);

#[pymethods]
impl BidirectionalGraph {
    #[new]
    fn new(path: PathBuf) -> PyResult<BidirectionalGraph> {
        let g = load_bidirectional(&path)
            .map_err(|e| {
                SwhGraphError::new_err(format!(
                    "Could not initialize BidirectionalGraph properties: {:?}",
                    e
                ))
            })?
            .load_all_properties::<DynMphf>()
            .map_err(|e| {
                SwhGraphError::new_err(format!(
                    "Could not load BidirectionalGraph properties: {:?}",
                    e
                ))
            })?;
        Ok(BidirectionalGraph(g))
    }

    #[getter]
    fn num_nodes(&self) -> usize {
        self.0.num_nodes()
    }

    #[getter]
    fn num_arcs(&self) -> u64 {
        self.0.num_arcs()
    }

    fn successors(&self, node_id: usize) -> Vec<usize> {
        self.0.successors(node_id).collect()
    }

    fn outdegree(&self, node_id: usize) -> usize {
        self.0.outdegree(node_id)
    }

    fn has_arc(&self, src_node_id: usize, dst_node_id: usize) -> bool {
        self.0.has_arc(src_node_id, dst_node_id)
    }

    fn predecessors(&self, node_id: usize) -> Vec<usize> {
        self.0.predecessors(node_id).collect()
    }

    fn indegree(&self, node_id: usize) -> usize {
        self.0.indegree(node_id)
    }

    fn node_id(&self, swhid: &str) -> Option<usize> {
        self.0.properties().node_id(swhid).ok()
    }

    fn swhid(&self, node_id: usize) -> String {
        self.0.properties().swhid(node_id).to_string()
    }
}

/// A Python module implemented in Rust.
#[pymodule]
fn swh_graph_pyo3(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<BidirectionalGraph>()?;
    Ok(())
}
