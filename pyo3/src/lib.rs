// Copyright (C) 2023  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::path::PathBuf;

use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use pyo3::Bound;

use swh_graph::graph::*;
use swh_graph::mph::{DynMphf, LoadableSwhidMphf, SwhidMphf};

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
        let g = SwhBidirectionalGraph::new(path)
            .map_err(|e| {
                SwhGraphError::new_err(format!(
                    "Could not initialize BidirectionalGraph properties: {e:?}"
                ))
            })?
            .load_all_properties::<DynMphf>()
            .map_err(|e| {
                SwhGraphError::new_err(format!(
                    "Could not load BidirectionalGraph properties: {e:?}"
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

#[pyclass]
/// Minimal Perfect-Hash function on SWHIDs in a graph
struct Mphf(swh_graph::mph::DynMphf);

#[pymethods]
impl Mphf {
    #[new]
    fn new(path: PathBuf) -> PyResult<Mphf> {
        match DynMphf::load(path) {
            Ok(mphf) => Ok(Mphf(mphf)),
            Err(e) => Err(SwhGraphError::new_err(format!("{e:?}"))),
        }
    }

    /// Hash the given SWHID with this function
    ///
    /// The given SWHID must be in the list of SWHIDs used to build this MPH, or
    /// the return value may be `None` or non-unique.
    ///
    /// If the SWHID was provided by a user, the recommended pattern is to first hash
    /// it with this function, then check in an array of SWHIDs that the SWHID at the
    /// indexed returned by this function is the same as the user-provided SWHID.
    ///
    /// This allows efficiently checking the SWHID is in the input dataset as well
    /// as getting a unique hash for the SWHID if it is.
    fn hash_swhid(&self, swhid: &str) -> Option<usize> {
        self.0.hash_str(swhid)
    }
}

/// A Python module implemented in Rust.
#[pymodule]
fn swh_graph_pyo3(_py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    m.add_class::<BidirectionalGraph>()?;
    m.add_class::<Mphf>()?;
    Ok(())
}
