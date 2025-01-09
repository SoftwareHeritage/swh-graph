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

use dataset_writer::StructArrayBuilder;

#[derive(Debug)]
pub struct UtcTimestampSecondBuilder(pub TimestampSecondBuilder);

impl Default for UtcTimestampSecondBuilder {
    fn default() -> UtcTimestampSecondBuilder {
        UtcTimestampSecondBuilder(
            TimestampSecondBuilder::new_from_buffer(
                Default::default(),
                None, // Values are not nullable -> validity buffer not needed
            )
            .with_timezone("UTC"),
        )
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

pub fn revrel_in_ori_schema() -> Schema {
    Schema::new(vec![
        Field::new("revrel", UInt64, false),
        Field::new("ori", UInt64, false),
    ])
}

pub fn cnt_in_revrel_writer_properties<G: SwhGraph>(graph: &G) -> WriterPropertiesBuilder {
    WriterProperties::builder()
        // Main request key. Monotonic, and with long sequences of equal values
        .set_column_encoding("cnt".into(), Encoding::DELTA_BINARY_PACKED)
        .set_column_statistics_enabled("cnt".into(), EnabledStatistics::Page)
        .set_column_bloom_filter_enabled("cnt".into(), true)
        .set_column_compression(
            "cnt".into(),
            Compression::ZSTD(ZstdLevel::try_new(3).unwrap()),
        )
        // May make sense to query, too
        .set_column_compression(
            "revrel".into(),
            Compression::ZSTD(ZstdLevel::try_new(3).unwrap()),
        )
        .set_column_statistics_enabled("revrel".into(), EnabledStatistics::Page)
        .set_column_bloom_filter_enabled("revrel".into(), true)
        // Maybe long sequences of equal value?
        .set_column_compression(
            "revrel_author_date".into(),
            Compression::ZSTD(ZstdLevel::try_new(3).unwrap()),
        )
        // Textual data
        .set_column_compression(
            "path".into(),
            Compression::ZSTD(ZstdLevel::try_new(3).unwrap()),
        )
        .set_key_value_metadata(Some(crate::parquet_metadata(graph)))
        // 10% of the default value.
        // Allows the page index to filter out more rows
        //.set_data_page_row_count_limit(2000)
        // 10× the default value.
        // Not needed for this particular table, but we set it nonetheless for
        // consistency with dir_in_revrel.
        .set_max_row_group_size(10 * 1024 * 1024)
}

pub fn dir_in_revrel_writer_properties<G: SwhGraph>(graph: &G) -> WriterPropertiesBuilder {
    WriterProperties::builder()
        // Main request key. Monotonic, and with long sequences of equal values
        .set_column_encoding("dir".into(), Encoding::DELTA_BINARY_PACKED)
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
        // May make sense to query, too
        .set_column_compression(
            "revrel".into(),
            Compression::ZSTD(ZstdLevel::try_new(3).unwrap()),
        )
        .set_column_statistics_enabled("revrel".into(), EnabledStatistics::Page)
        .set_column_bloom_filter_enabled("revrel".into(), true)
        // Maybe long sequences of equal value?
        .set_column_compression(
            "revrel_author_date".into(),
            Compression::ZSTD(ZstdLevel::try_new(3).unwrap()),
        )
        // Textual data
        .set_column_compression(
            "path".into(),
            Compression::ZSTD(ZstdLevel::try_new(3).unwrap()),
        )
        .set_key_value_metadata(Some(crate::parquet_metadata(graph)))
        // 10% of the default value.
        // Allows the page index to filter out more rows
        //.set_data_page_row_count_limit(2000)
        // 10× the default value.
        // with --node-filter all, we write slightly over the 32k row groups limit,
        // so we need to write more in each row group.
        .set_max_row_group_size(10 * 1024 * 1024)
}

pub fn cnt_in_dir_writer_properties<G: SwhGraph>(graph: &G) -> WriterPropertiesBuilder {
    WriterProperties::builder()
        // Main request key. Monotonic, and with long sequences of equal values
        .set_column_encoding("cnt".into(), Encoding::DELTA_BINARY_PACKED)
        .set_column_statistics_enabled("cnt".into(), EnabledStatistics::Page)
        .set_column_bloom_filter_enabled("cnt".into(), true)
        .set_column_compression(
            "cnt".into(),
            Compression::ZSTD(ZstdLevel::try_new(3).unwrap()),
        )
        // May make sense to query, too
        .set_column_compression(
            "dir".into(),
            Compression::ZSTD(ZstdLevel::try_new(3).unwrap()),
        )
        .set_column_statistics_enabled("dir".into(), EnabledStatistics::Page)
        .set_column_bloom_filter_enabled("dir".into(), true)
        // Textual data
        .set_column_compression(
            "path".into(),
            Compression::ZSTD(ZstdLevel::try_new(3).unwrap()),
        )
        .set_key_value_metadata(Some(crate::parquet_metadata(graph)))
    // 10% of the default value.
    // Allows the page index to filter out more rows
    //.set_data_page_row_count_limit(2000)
    // Not increasing max_row_group_size, as it would make arrays for the 'path'
    // column pretty large. Switching to LargeBinaryArray would work, but it
    // still means the reader needs more than 2^31 (2GB) just to store the
    // decompressed array in RAM.
    // .set_max_row_group_size(10 * 1024 * 1024)
}

pub fn revrel_in_ori_writer_properties<G: SwhGraph>(graph: &G) -> WriterPropertiesBuilder {
    WriterProperties::builder()
        // Main request key. Monotonic, and with long sequences of equal values
        .set_column_encoding("revrel".into(), Encoding::DELTA_BINARY_PACKED)
        .set_column_statistics_enabled("revrel".into(), EnabledStatistics::Page)
        .set_column_bloom_filter_enabled("revrel".into(), true)
        .set_column_compression(
            "revrel".into(),
            Compression::ZSTD(ZstdLevel::try_new(3).unwrap()),
        )
        // May make sense to query, too
        .set_column_compression(
            "ori".into(),
            Compression::ZSTD(ZstdLevel::try_new(3).unwrap()),
        )
        .set_column_statistics_enabled("ori".into(), EnabledStatistics::Page)
        .set_column_bloom_filter_enabled("ori".into(), true)
        .set_key_value_metadata(Some(crate::parquet_metadata(graph)))
        // 10% of the default value.
        // Allows the page index to filter out more rows
        //.set_data_page_row_count_limit(2000)
        // 10× the default value.
        // Not needed for this particular table, but we set it nonetheless for
        // consistency with dir_in_revrel.
        .set_max_row_group_size(10 * 1024 * 1024)
}

#[derive(Debug)]
pub struct CntInRevrelTableBuilder {
    pub cnt: UInt64Builder,
    pub revrel: UInt64Builder,
    pub revrel_author_date: UtcTimestampSecondBuilder,
    pub path: BinaryBuilder,
}

impl Default for CntInRevrelTableBuilder {
    fn default() -> Self {
        CntInRevrelTableBuilder {
            cnt: UInt64Builder::new_from_buffer(
                Default::default(),
                None, // Values are not nullable -> validity buffer not needed
            ),
            revrel: UInt64Builder::new_from_buffer(
                Default::default(),
                None, // ditto
            ),
            revrel_author_date: Default::default(),
            path: BinaryBuilder::default(), // TODO: don't use validity buffer
        }
    }
}

impl StructArrayBuilder for CntInRevrelTableBuilder {
    fn len(&self) -> usize {
        self.cnt.len()
    }

    fn buffer_size(&self) -> usize {
        self.len() * (8 + 8 + 8) // u64 + u64 + u64
         + self.path.values_slice().len()
         + self.path.offsets_slice().len() * 4 // BinaryBuilder uses i32 indices
         + self.path.validity_slice().map(|s| s.len()).unwrap_or(0)
    }

    fn finish(&mut self) -> Result<StructArray> {
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

#[derive(Debug)]
pub struct DirInRevrelTableBuilder {
    pub dir: UInt64Builder,
    pub dir_max_author_date: UtcTimestampSecondBuilder,
    pub revrel: UInt64Builder,
    pub revrel_author_date: UtcTimestampSecondBuilder,
    pub path: BinaryBuilder,
}

impl Default for DirInRevrelTableBuilder {
    fn default() -> Self {
        DirInRevrelTableBuilder {
            dir: UInt64Builder::new_from_buffer(
                Default::default(),
                None, // Values are not nullable -> validity buffer not needed
            ),
            dir_max_author_date: Default::default(),
            revrel: UInt64Builder::new_from_buffer(
                Default::default(),
                None, // ditto
            ),
            revrel_author_date: Default::default(),
            path: BinaryBuilder::default(), // TODO: don't use validity buffer
        }
    }
}

impl StructArrayBuilder for DirInRevrelTableBuilder {
    fn len(&self) -> usize {
        self.dir.len()
    }

    fn buffer_size(&self) -> usize {
        self.len() * (8 + 8 + 8 + 8) // u64 + u64 + u64 + u64
         + self.path.values_slice().len()
         + self.path.offsets_slice().len() * 4 // BinaryBuilder uses i32 indices
         + self.path.validity_slice().map(|s| s.len()).unwrap_or(0)
    }

    fn finish(&mut self) -> Result<StructArray> {
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

#[derive(Debug)]
pub struct CntInDirTableBuilder {
    pub cnt: UInt64Builder,
    pub dir: UInt64Builder,
    pub path: BinaryBuilder,
}

impl Default for CntInDirTableBuilder {
    fn default() -> Self {
        CntInDirTableBuilder {
            cnt: UInt64Builder::new_from_buffer(
                Default::default(),
                None, // Values are not nullable -> validity buffer not needed
            ),
            dir: UInt64Builder::new_from_buffer(
                Default::default(),
                None, // ditto
            ),
            path: BinaryBuilder::default(), // TODO: don't use validity buffer
        }
    }
}

impl StructArrayBuilder for CntInDirTableBuilder {
    fn len(&self) -> usize {
        self.cnt.len()
    }

    fn buffer_size(&self) -> usize {
        self.len() * (8 + 8) // u64 + u64
         + self.path.values_slice().len()
         + self.path.offsets_slice().len() * 4 // BinaryBuilder uses i32 indices
         + self.path.validity_slice().map(|s| s.len()).unwrap_or(0)
    }

    fn finish(&mut self) -> Result<StructArray> {
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

#[derive(Debug)]
pub struct RevrelInOriTableBuilder {
    pub revrel: UInt64Builder,
    pub ori: UInt64Builder,
}

impl Default for RevrelInOriTableBuilder {
    fn default() -> Self {
        RevrelInOriTableBuilder {
            revrel: UInt64Builder::new_from_buffer(
                Default::default(),
                None, // Values are not nullable -> validity buffer not needed
            ),
            ori: UInt64Builder::new_from_buffer(
                Default::default(),
                None, // ditto
            ),
        }
    }
}

impl StructArrayBuilder for RevrelInOriTableBuilder {
    fn len(&self) -> usize {
        self.revrel.len()
    }

    fn buffer_size(&self) -> usize {
        self.len() * (8 + 8) // u64 + u64
    }

    fn finish(&mut self) -> Result<StructArray> {
        let columns: Vec<Arc<dyn Array>> =
            vec![Arc::new(self.revrel.finish()), Arc::new(self.ori.finish())];

        Ok(StructArray::new(
            revrel_in_ori_schema().fields().clone(),
            columns,
            None, // nulls
        ))
    }
}
