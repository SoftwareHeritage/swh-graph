// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::sync::Arc;

use anyhow::Result;
use arrow::array::*;
use arrow::datatypes::DataType::*;
use arrow::datatypes::{Field, Schema};
use parquet::basic::{Compression, Encoding, ZstdLevel};
use parquet::file::properties::EnabledStatistics;
use parquet::file::properties::{WriterProperties, WriterPropertiesBuilder};

use swh_graph::graph::{NodeId, SwhGraph, SwhGraphWithProperties};
use swh_graph::NodeType;

use dataset_writer::StructArrayBuilder;

pub fn schema() -> Schema {
    Schema::new(vec![
        Field::new("id", UInt64, false),
        Field::new("type", Dictionary(Int8.into(), Utf8.into()), false),
        Field::new("sha1_git", FixedSizeBinary(20), false),
    ])
}

pub fn writer_properties<G: SwhGraph>(graph: &G) -> WriterPropertiesBuilder {
    WriterProperties::builder()
        // Compresses well because there are long monotonic sequences, and integers
        // don't occupy the whole UInt64 range.
        // Saves 2GB on the 2023-09-06 dataset with no visible performance penalty.
        .set_column_encoding("id".into(), Encoding::DELTA_BINARY_PACKED)
        // ZSTD has a 35% compression ratio over the delta-encoding alone, which saves
        // 2GB on the 2023-09-06 dataset; and has negligeable penalty on access
        // (SELECT * WHERE id = ?).
        .set_column_compression(
            "id".into(),
            Compression::ZSTD(ZstdLevel::try_new(3).unwrap()),
        )
        // Not worth enabling ZSTD compresson on "type" column, only 65% compression
        // ratio and it is already very succinct.
        // Make sure we have both chunk-level and page-level statistics,
        // these are very valuable.
        // (it's the default, but better safe than sorry)
        .set_column_statistics_enabled("id".into(), EnabledStatistics::Page)
        // Enable somewhat-efficient SWHID lookup
        .set_column_bloom_filter_enabled("sha1_git".into(), true)
        // SWHIDs are partitioned into files by their high bits. We can't have only
        // file statistics, but this is the next best thing.
        .set_column_statistics_enabled("sha1_git".into(), EnabledStatistics::Chunk)
        // Use dictionaries for node types
        .set_dictionary_enabled(true)
        .set_key_value_metadata(Some(crate::parquet_metadata(graph)))
}

#[derive(Debug)]
pub struct NodeTableBuilder {
    ids: UInt64Builder,
    types: Int8Builder,
    sha1_gits: FixedSizeBinaryBuilder,
}

impl NodeTableBuilder {
    pub fn add_node<G>(&mut self, graph: &G, node: NodeId)
    where
        G: SwhGraphWithProperties,
        <G as SwhGraphWithProperties>::Maps: swh_graph::properties::Maps,
    {
        let swhid = graph.properties().swhid(node);
        self.types.append_value(swhid.node_type as i8);
        self.sha1_gits
            .append_value(swhid.hash)
            .expect("Could not append sha1_git");
        let node: u64 = node.try_into().expect("Node id overflow u64");
        self.ids.append_value(node);
    }
}

const SHA1GIT_LEN: i32 = 20;

impl Default for NodeTableBuilder {
    fn default() -> Self {
        NodeTableBuilder {
            ids: UInt64Builder::new_from_buffer(
                Default::default(),
                None, // Values are not nullable -> validity buffer not needed
            ),
            types: Int8Builder::new_from_buffer(
                Default::default(),
                None, // ditto
            ),
            sha1_gits: FixedSizeBinaryBuilder::new(SHA1GIT_LEN), // can't disable the useless validity buffer :(
        }
    }
}

impl StructArrayBuilder for NodeTableBuilder {
    fn len(&self) -> usize {
        self.ids.len()
    }

    fn buffer_size(&self) -> usize {
        self.len() * (8 + 1 + 20) // u64 + u8 + [u8; 20]
                                  // TODO(arrow >= 52) + self.sha1_gits.validity_slice().map(|s| s.len()).unwrap_or(0)
    }

    fn finish(&mut self) -> Result<StructArray> {
        let types = self.types.finish();
        let types_dictionary = StringArray::from(
            NodeType::all()
                .into_iter()
                .map(|type_| type_.to_str())
                .collect::<Vec<_>>(),
        );

        // Turn the numeric ids into strings from the reader's point of view.
        let types = Arc::new(DictionaryArray::new(types, Arc::new(types_dictionary)));

        let ids = Arc::new(self.ids.finish());
        let sha1_gits = Arc::new(self.sha1_gits.finish());

        let columns: Vec<Arc<dyn Array>> = vec![ids, types, sha1_gits];

        Ok(StructArray::new(
            schema().fields().clone(),
            columns,
            None, // nulls
        ))
    }
}
