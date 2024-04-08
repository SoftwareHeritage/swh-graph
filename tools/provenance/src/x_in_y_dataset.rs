// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::sync::Arc;

use anyhow::Result;
use arrow::array::*;
use arrow::datatypes::DataType::*;
use arrow::datatypes::{Field, Schema, TimeUnit};
use parquet::basic::{Compression, Encoding, ZstdLevel};
use parquet::file::properties::EnabledStatistics;
use parquet::file::properties::{WriterProperties, WriterPropertiesBuilder};

use swh_graph::graph::SwhGraph;

use swh_graph::utils::dataset_writer::StructArrayBuilder;

#[derive(Debug)]
pub struct UtcTimestampSecondBuilder(pub TimestampSecondBuilder);

impl Default for UtcTimestampSecondBuilder {
    fn default() -> UtcTimestampSecondBuilder {
        UtcTimestampSecondBuilder(TimestampSecondBuilder::default().with_timezone("UTC"))
    }
}

impl std::ops::Deref for UtcTimestampSecondBuilder {
    type Target = TimestampSecondBuilder;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for UtcTimestampSecondBuilder {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

pub fn cnt_in_revrel_schema() -> Schema {
    Schema::new(vec![
        Field::new("cnt", UInt64, false),
        Field::new("revrel", UInt64, false),
        Field::new(
            "revrel_author_date",
            Timestamp(TimeUnit::Second, Some("UTC".into())),
            false,
        ),
        Field::new("path", Binary, false),
    ])
}

pub fn dir_in_revrel_schema() -> Schema {
    Schema::new(vec![
        Field::new("dir", UInt64, false),
        Field::new(
            "dir_max_author_date",
            Timestamp(TimeUnit::Second, Some("UTC".into())),
            false,
        ),
        Field::new("revrel", UInt64, false),
        Field::new(
            "revrel_author_date",
            Timestamp(TimeUnit::Second, Some("UTC".into())),
            false,
        ),
        Field::new("path", Binary, false),
    ])
}

pub fn cnt_in_dir_schema() -> Schema {
    Schema::new(vec![
        Field::new("cnt", UInt64, false),
        Field::new("dir", UInt64, false),
        Field::new("path", Binary, false),
    ])
}

pub fn cnt_in_revrel_writer_properties<G: SwhGraph>(graph: &G) -> WriterPropertiesBuilder {
    WriterProperties::builder()
        // Main request key
        .set_column_statistics_enabled("cnt".into(), EnabledStatistics::Page)
        .set_column_bloom_filter_enabled("cnt".into(), true)
        .set_column_compression(
            "cnt".into(),
            Compression::ZSTD(ZstdLevel::try_new(3).unwrap()),
        )
        // Monotonic, and with long sequences of equal values
        .set_column_encoding("revrel".into(), Encoding::DELTA_BINARY_PACKED)
        .set_column_compression(
            "revrel".into(),
            Compression::ZSTD(ZstdLevel::try_new(3).unwrap()),
        )
        .set_column_statistics_enabled("revrel".into(), EnabledStatistics::Page)
        .set_column_bloom_filter_enabled("revrel".into(), true)
        // Long sequences of equal value
        .set_column_compression(
            "revrel_author_date".into(),
            Compression::ZSTD(ZstdLevel::try_new(3).unwrap()),
        )
        // Consecutive entries often share the same path prefix
        .set_column_encoding("path".into(), Encoding::DELTA_BYTE_ARRAY)
        .set_column_compression(
            "path".into(),
            Compression::ZSTD(ZstdLevel::try_new(3).unwrap()),
        )
        .set_key_value_metadata(Some(crate::parquet_metadata(graph)))
}

pub fn dir_in_revrel_writer_properties<G: SwhGraph>(graph: &G) -> WriterPropertiesBuilder {
    WriterProperties::builder()
        // Main request key
        .set_column_statistics_enabled("dir".into(), EnabledStatistics::Page)
        .set_column_bloom_filter_enabled("dir".into(), true)
        .set_column_compression(
            "dir".into(),
            Compression::ZSTD(ZstdLevel::try_new(3).unwrap()),
        )
        // Long sequences of equal value
        .set_column_compression(
            "dir_max_author_date".into(),
            Compression::ZSTD(ZstdLevel::try_new(3).unwrap()),
        )
        // Monotonic, and with long sequences of equal values
        .set_column_encoding("revrel".into(), Encoding::DELTA_BINARY_PACKED)
        .set_column_compression(
            "revrel".into(),
            Compression::ZSTD(ZstdLevel::try_new(3).unwrap()),
        )
        .set_column_statistics_enabled("revrel".into(), EnabledStatistics::Page)
        .set_column_bloom_filter_enabled("revrel".into(), true)
        // Long sequences of equal value
        .set_column_compression(
            "revrel_author_date".into(),
            Compression::ZSTD(ZstdLevel::try_new(3).unwrap()),
        )
        // Consecutive entries often share the same path prefix
        .set_column_encoding("path".into(), Encoding::DELTA_BYTE_ARRAY)
        .set_column_compression(
            "path".into(),
            Compression::ZSTD(ZstdLevel::try_new(3).unwrap()),
        )
        .set_key_value_metadata(Some(crate::parquet_metadata(graph)))
}

pub fn cnt_in_dir_writer_properties<G: SwhGraph>(graph: &G) -> WriterPropertiesBuilder {
    WriterProperties::builder()
        // Main request key
        .set_column_statistics_enabled("cnt".into(), EnabledStatistics::Page)
        .set_column_bloom_filter_enabled("cnt".into(), true)
        .set_column_compression(
            "cnt".into(),
            Compression::ZSTD(ZstdLevel::try_new(3).unwrap()),
        )
        // Monotonic, and with long sequences of equal values
        .set_column_encoding("dir".into(), Encoding::DELTA_BINARY_PACKED)
        .set_column_compression(
            "dir".into(),
            Compression::ZSTD(ZstdLevel::try_new(3).unwrap()),
        )
        .set_column_statistics_enabled("dir".into(), EnabledStatistics::Page)
        .set_column_bloom_filter_enabled("dir".into(), true)
        // Consecutive entries often share the same path prefix
        .set_column_encoding("path".into(), Encoding::DELTA_BYTE_ARRAY)
        .set_column_compression(
            "path".into(),
            Compression::ZSTD(ZstdLevel::try_new(3).unwrap()),
        )
        .set_key_value_metadata(Some(crate::parquet_metadata(graph)))
}

#[derive(Debug, Default)]
pub struct CntInRevrelTableBuilder {
    pub cnt: UInt64Builder,
    pub revrel: UInt64Builder,
    pub revrel_author_date: UtcTimestampSecondBuilder,
    pub path: BinaryBuilder,
}

impl StructArrayBuilder for CntInRevrelTableBuilder {
    fn len(&self) -> usize {
        self.cnt.len()
    }

    fn finish(mut self) -> Result<StructArray> {
        let columns: Vec<Arc<dyn Array>> = vec![
            Arc::new(self.cnt.finish()),
            Arc::new(self.revrel.finish()),
            Arc::new(self.revrel_author_date.finish()),
            Arc::new(self.path.finish()),
        ];

        Ok(StructArray::new(
            cnt_in_revrel_schema().fields().clone(),
            columns,
            None, // nulls
        ))
    }
}

#[derive(Debug, Default)]
pub struct DirInRevrelTableBuilder {
    pub dir: UInt64Builder,
    pub dir_max_author_date: UtcTimestampSecondBuilder,
    pub revrel: UInt64Builder,
    pub revrel_author_date: UtcTimestampSecondBuilder,
    pub path: BinaryBuilder,
}

impl StructArrayBuilder for DirInRevrelTableBuilder {
    fn len(&self) -> usize {
        self.dir.len()
    }

    fn finish(mut self) -> Result<StructArray> {
        let columns: Vec<Arc<dyn Array>> = vec![
            Arc::new(self.dir.finish()),
            Arc::new(self.dir_max_author_date.finish()),
            Arc::new(self.revrel.finish()),
            Arc::new(self.revrel_author_date.finish()),
            Arc::new(self.path.finish()),
        ];

        Ok(StructArray::new(
            dir_in_revrel_schema().fields().clone(),
            columns,
            None, // nulls
        ))
    }
}

#[derive(Debug, Default)]
pub struct CntInDirTableBuilder {
    pub cnt: UInt64Builder,
    pub dir: UInt64Builder,
    pub path: BinaryBuilder,
}

impl StructArrayBuilder for CntInDirTableBuilder {
    fn len(&self) -> usize {
        self.cnt.len()
    }

    fn finish(mut self) -> Result<StructArray> {
        let columns: Vec<Arc<dyn Array>> = vec![
            Arc::new(self.cnt.finish()),
            Arc::new(self.dir.finish()),
            Arc::new(self.path.finish()),
        ];

        Ok(StructArray::new(
            cnt_in_dir_schema().fields().clone(),
            columns,
            None, // nulls
        ))
    }
}
