// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

pub mod dataset_writer;
pub mod earliest_revision;
pub mod filters;
pub mod frontier;
pub mod frontier_set;
pub mod node_dataset;

/// The current version of swh-graph-provenance.
pub const VERSION: &'static str = env!("CARGO_PKG_VERSION");

/// Returns metadata to write in the header of produced parquet files
pub fn parquet_metadata<G: swh_graph::graph::SwhGraph>(
    graph: &G,
) -> Vec<parquet::file::metadata::KeyValue> {
    use parquet::format::KeyValue;
    vec![
        KeyValue {
            key: "swh_graph_version".into(),
            value: Some(swh_graph::VERSION.into()),
        },
        KeyValue {
            key: "swh_graph_provenance_version".into(),
            value: Some(crate::VERSION.into()),
        },
        KeyValue {
            key: "swh_graph_path".into(),
            value: Some(graph.path().display().to_string()),
        },
        KeyValue {
            key: "creation_date".into(),
            value: Some(chrono::Local::now().to_rfc3339()),
        },
    ]
}
