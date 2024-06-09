// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::fs::File;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

use anyhow::{anyhow, bail, ensure, Context, Result};
use ar_row::deserialize::ArRowDeserialize;
use ar_row_derive::ArRowDeserialize;
use arrow::array::*;
use arrow::datatypes::DataType::*;
use arrow::datatypes::{Field, Schema};
use dsi_progress_logger::ProgressLog;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ProjectionMask;
use parquet::basic::{Compression, Encoding, ZstdLevel};
use parquet::file::properties::EnabledStatistics;
use parquet::file::properties::{WriterProperties, WriterPropertiesBuilder};
use rayon::prelude::*;
use sux::bits::bit_vec::{AtomicBitVec, BitVec};

use swh_graph::collections::NodeSet;
use swh_graph::graph::*;

use swh_graph::utils::dataset_writer::{
    ParallelDatasetWriter, ParquetTableWriter, StructArrayBuilder,
};

pub fn schema() -> Schema {
    Schema::new(vec![Field::new("id", UInt64, false)])
}

pub fn writer_properties<G: SwhGraph>(graph: &G) -> WriterPropertiesBuilder {
    WriterProperties::builder()
        // See node_dataset::writer_properties for a rationale on these settings
        .set_column_encoding("id".into(), Encoding::DELTA_BINARY_PACKED)
        .set_column_compression(
            "id".into(),
            Compression::ZSTD(ZstdLevel::try_new(3).unwrap()),
        )
        .set_column_statistics_enabled("id".into(), EnabledStatistics::Page)
        .set_key_value_metadata(Some(crate::parquet_metadata(graph)))
}

#[derive(Debug)]
pub struct Builder(UInt64Builder);

impl Default for Builder {
    fn default() -> Self {
        Builder(UInt64Builder::new_from_buffer(
            Default::default(),
            None, // Values are not nullable -> validity buffer not needed
        ))
    }
}

impl StructArrayBuilder for Builder {
    fn len(&self) -> usize {
        self.0.len()
    }

    fn finish(mut self) -> Result<StructArray> {
        let columns: Vec<Arc<dyn Array>> = vec![Arc::new(self.0.finish())];

        Ok(StructArray::new(
            schema().fields().clone(),
            columns,
            None, // nulls
        ))
    }
}

pub fn to_parquet<G, NS: NodeSet + Sync, PL: ProgressLog + Send>(
    graph: &G,
    frontier: NS,
    dataset_writer: ParallelDatasetWriter<ParquetTableWriter<Builder>>,
    pl: &mut PL,
) -> Result<()>
where
    G: SwhGraph + Sync,
{
    let pl = Arc::new(Mutex::new(pl));

    // Split into a small number of chunks. This causes the node ids to form long
    // monotonically increasing sequences in the output dataset, which makes them
    // easy to index using Parquet/ORC chunk statistics. And should compress better
    // with delta-encoding.
    //
    // Querying individual nodes in the table (whether present or absent) when sharded
    // over 1 or 10 files has 10% better performance than when sharded over 100 files,
    // and takes 5% less space (nio(not that the latter matters given the small size
    // of this table)
    let num_chunks = 10;
    let chunk_size = graph.num_nodes().div_ceil(num_chunks);

    rayon::ThreadPoolBuilder::new()
        .num_threads(num_chunks)
        .build()
        .context("Could not build thread pool")?
        .install(|| {
            (0..graph.num_nodes())
                .into_par_iter()
                .by_uniform_blocks(chunk_size)
                .try_for_each_init(
                    || dataset_writer.get_thread_writer().unwrap(),
                    |writer, node| -> Result<()> {
                        if frontier.contains(node) {
                            writer
                                .builder()?
                                .0
                                .append_value(node.try_into().expect("NodeId overflowed u64"));
                        }
                        if node % 32768 == 0 {
                            pl.lock().unwrap().update_with_count(32768);
                        }
                        Ok(())
                    },
                )
        })?;
    dataset_writer.close()?;

    Ok(())
}

pub fn from_parquet<G, PL: ProgressLog + Send>(
    graph: &G,
    dataset_path: PathBuf,
    pl: &mut PL,
) -> Result<BitVec>
where
    G: SwhGraph + Sync,
{
    let mut expected_rows = 0usize;

    let readers = std::fs::read_dir(&dataset_path)
        .with_context(|| format!("Could not list {}", dataset_path.display()))?
        .map(|entry| -> Result<_> {
            let file_path = entry
                .with_context(|| format!("Could not read {} entry", dataset_path.display()))?
                .path();
            let file = File::open(&file_path)
                .with_context(|| format!("Could not open {}", file_path.display()))?;
            let reader_builder = ParquetRecordBatchReaderBuilder::try_new(file)
                .with_context(|| format!("Could not read {} as Parquet", file_path.display()))?;
            let file_metadata = reader_builder.metadata().file_metadata().clone();
            let id_col_index = file_metadata
                .schema_descr()
                .columns()
                .iter()
                .position(|col| col.name() == "id")
                .ok_or_else(|| anyhow!("{} has no 'id' column", file_path.display()))?;
            let reader_builder = reader_builder.with_projection(ProjectionMask::leaves(
                file_metadata.schema_descr(),
                [id_col_index],
            ));
            let num_rows: i64 = file_metadata.num_rows();
            ensure!(
                num_rows >= 0,
                "{} has a negative number of rows ({})",
                file_path.display(),
                num_rows
            );
            let num_rows: usize = num_rows.try_into().context("num_rows overflows usize")?;
            expected_rows += num_rows;
            let reader = reader_builder.build().with_context(|| {
                format!(
                    "Could not create Parquet reader for {}",
                    file_path.display()
                )
            })?;
            Ok(reader)
        })
        .collect::<Result<Vec<_>>>()?;

    let frontiers = AtomicBitVec::new(graph.num_nodes());

    #[derive(ArRowDeserialize, Default)]
    struct Row {
        id: u64,
    }

    pl.expected_updates(Some(expected_rows));

    let pl = Arc::new(Mutex::new(pl));

    readers.into_par_iter().try_for_each(|mut reader| {
        reader.try_for_each(|batch| -> Result<()> {
            let batch = batch.unwrap_or_else(|e| panic!("Could not read chunk: {}", e));
            let batch_num_rows = batch.num_rows();
            let rows: Vec<Row> =
                Row::from_record_batch(batch).context("Could not deserialize from arrow")?;
            rows.into_iter().try_for_each(|Row { id }| -> Result<()> {
                let Ok(id) = id.try_into() else {
                    bail!("node id overflowed u64");
                };

                // Covered by the 'graph.has_node(id)' check below, but this gives
                // a better error message for this particular error.
                ensure!(
                    id < graph.num_nodes(),
                    "Got node id {} for graph with {} nodes",
                    id,
                    graph.num_nodes()
                );
                ensure!(
                    graph.has_node(id),
                    "Graph does not have a node with id {}",
                    id
                );

                frontiers.set(id, true, Ordering::Relaxed);
                Ok(())
            })?;

            pl.lock().unwrap().update_with_count(batch_num_rows);

            Ok(())
        })
    })?;

    Ok(frontiers.into())
}
