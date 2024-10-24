// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use arrow::array::*;
use arrow::datatypes::DataType::*;
use arrow::datatypes::{Field, Schema};
use clap::Parser;
use dataset_writer::{
    ParallelDatasetWriter, ParquetTableWriter, StructArrayBuilder, Utf8PartitionedTableWriter,
};
use dsi_progress_logger::{progress_logger, ProgressLog};
use parquet::basic::{Compression, Encoding, ZstdLevel};
use parquet::file::properties::EnabledStatistics;
use parquet::file::properties::{WriterProperties, WriterPropertiesBuilder};
use rayon::prelude::*;

use swh_graph::graph::*;
use swh_graph::mph::DynMphf;
use swh_graph::utils::progress_logger::{BufferedProgressLogger, MinimalProgressLog};
use swh_graph::NodeType;

use swh_graph_aggregate::parquet_metadata;

#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

pub fn schema() -> Schema {
    Schema::new(vec![
        Field::new("id", UInt64, false),
        Field::new("swhid", Utf8, true),
        Field::new("url", Utf8, true),
    ])
}

#[derive(Debug)]
pub struct TableBuilder {
    pub id: UInt64Builder,
    pub swhid: StringBuilder,
    pub url: StringBuilder,
}

impl Default for TableBuilder {
    fn default() -> Self {
        TableBuilder {
            id: UInt64Builder::new_from_buffer(
                Default::default(),
                None, // Values are not nullable -> validity buffer not needed
            ),
            swhid: StringBuilder::new(),
            url: StringBuilder::new(),
        }
    }
}

impl StructArrayBuilder for TableBuilder {
    fn len(&self) -> usize {
        self.id.len()
    }

    fn buffer_size(&self) -> usize {
        self.len() * 8 // u64
         + self.swhid.values_slice().len()
         + self.swhid.offsets_slice().len() * 4 // StringBuilder uses i32 indices
         + self.swhid.validity_slice().map(|s| s.len()).unwrap_or(0)
         + self.url.values_slice().len()
         + self.url.offsets_slice().len() * 4 // StringBuilder uses i32 indices
         + self.url.validity_slice().map(|s| s.len()).unwrap_or(0)
    }

    fn finish(&mut self) -> Result<StructArray> {
        let columns: Vec<Arc<dyn Array>> = vec![
            Arc::new(self.id.finish()),
            Arc::new(self.swhid.finish()),
            Arc::new(self.url.finish()),
        ];

        Ok(StructArray::new(
            schema().fields().clone(),
            columns,
            None, // nulls
        ))
    }
}

fn writer_properties<G: SwhGraph>(graph: &G, args: &Args) -> WriterPropertiesBuilder {
    WriterProperties::builder()
        // Main request key. Monotonic, and with long sequences of equal values
        .set_column_encoding("cnt".into(), Encoding::DELTA_BINARY_PACKED)
        .set_column_statistics_enabled("cnt".into(), EnabledStatistics::Page)
        .set_column_bloom_filter_enabled("cnt".into(), true)
        .set_column_compression(
            "cnt".into(),
            Compression::ZSTD(ZstdLevel::try_new(3).unwrap()),
        )
        // Secondary request key (equality).
        // Hashes are uniformly distributed and we are already sharding by object type,
        // so it does not make sense to have page-level statistics. We keep chunk-level statistics
        // because it is cheaps, and helps readers who don't want to (or can't) configure
        // partitioning.
        // Has to be zstd-compressed, because there is the redundant prefix + a hex digest
        .set_column_statistics_enabled("swhid".into(), EnabledStatistics::Chunk)
        .set_column_bloom_filter_enabled("swhid".into(), true)
        .set_column_bloom_filter_fpp("swhid".into(), args.bloom_fpp)
        .set_column_bloom_filter_ndv("swhid".into(), args.bloom_ndv)
        .set_column_compression(
            "swhid".into(),
            Compression::ZSTD(ZstdLevel::try_new(6).unwrap()),
        )
        // Secondary request key (equality).
        // ZSTD-compressed because it's textual
        .set_column_statistics_enabled("url".into(), EnabledStatistics::Chunk)
        .set_column_bloom_filter_enabled("url".into(), true)
        .set_column_bloom_filter_fpp("url".into(), args.bloom_fpp)
        .set_column_bloom_filter_ndv("url".into(), args.bloom_ndv)
        .set_column_compression(
            "url".into(),
            Compression::ZSTD(ZstdLevel::try_new(6).unwrap()),
        )
        .set_key_value_metadata(Some(parquet_metadata(graph)))
}

#[derive(Parser, Debug)]
/** Writes the list of nodes reachable from a 'head' revision or a release.
 */
struct Args {
    graph_path: PathBuf,
    #[arg(long, default_value_t = 1_000_000)] // Parquet's default max_row_group_size
    /// Number of rows written at once, forming a row group on disk.
    ///
    /// Higher values reduce index scans, but lower values reduce time wasted per
    /// false positive in the probabilitistic filters.
    row_group_size: usize,
    #[arg(long, default_value_t = 0.05)] // Parquet's default
    /// false positive probability for 'sha1_git' Bloom filter
    bloom_fpp: f64,
    #[arg(long, default_value_t = 1_000_000)] // Parquet's default
    /// number of distinct values for 'sha1_git' Bloom filter
    bloom_ndv: u64,
    #[arg(long)]
    /// Directory to write the list of nodes to
    nodes_out: PathBuf,
}

pub fn main() -> Result<()> {
    let args = Args::parse();

    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    log::info!("Loading graph");
    let graph = swh_graph::graph::SwhBidirectionalGraph::new(&args.graph_path)
        .context("Could not load graph")?
        .init_properties()
        .load_properties(|props| props.load_maps::<DynMphf>())
        .context("Could not load maps")?
        .load_properties(|props| props.load_strings())
        .context("Could not load string properties")?;
    log::info!("Graph loaded.");

    let mut dataset_writer =
        ParallelDatasetWriter::<Utf8PartitionedTableWriter<ParquetTableWriter<_>>>::with_schema(
            args.nodes_out.clone(),
            (
                "node_type".into(),
                (Arc::new(schema()), writer_properties(&graph, &args).build()),
            ),
        )?;

    // We write at most one row per call to `dataset_writer.builder()`, so every row
    // group will be exactly this size.
    dataset_writer.config.autoflush_row_group_len = Some(args.row_group_size);

    write_nodes(&graph, dataset_writer)?;

    Ok(())
}

fn write_nodes<G>(
    graph: &G,
    dataset_writer: ParallelDatasetWriter<
        Utf8PartitionedTableWriter<ParquetTableWriter<TableBuilder>>,
    >,
) -> Result<()>
where
    G: SwhGraphWithProperties + Sync,
    <G as SwhGraphWithProperties>::Maps: swh_graph::properties::Maps,
    <G as SwhGraphWithProperties>::Strings: swh_graph::properties::Strings,
{
    let mut pl = progress_logger!(
        item_name = "node",
        display_memory = true,
        local_speed = true,
        expected_updates = Some(graph.num_nodes()),
    );
    pl.start("Writing list of reachable nodes...");

    // Split into a small number of chunks. This causes the node ids to form long
    // monotonically increasing sequences in the output dataset, which makes them
    // easy to index using Parquet/ORC chunk statistics. And should compress better
    // with delta-encoding.
    // However, we still want this loop (and readers) to parallelize nicely,
    // so the number of chunks cannot be too low either.
    let num_chunks = 96;
    let chunk_size = graph.num_nodes().div_ceil(num_chunks);

    let shared_pl = Arc::new(Mutex::new(&mut pl));
    (0..graph.num_nodes())
        .into_par_iter()
        .by_uniform_blocks(chunk_size)
        .try_for_each_init(
            || {
                (
                    dataset_writer.get_thread_writer().unwrap(),
                    BufferedProgressLogger::new(shared_pl.clone()),
                )
            },
            |(writer, thread_pl), node| -> Result<()> {
                let node_type = graph.properties().node_type(node);
                let builder = writer.partition(node_type.to_str().into())?.builder()?;
                builder
                    .id
                    .append_value(node.try_into().expect("node id overflowed u64"));
                if node_type == NodeType::Origin {
                    builder.swhid.append_null(); // Don't expose swh:1:ori:, it's an impl detail
                    builder.url.append_option(
                        graph
                            .properties()
                            .message(node)
                            .and_then(|url| String::from_utf8(url).ok()),
                    );
                } else {
                    let swhid = graph.properties().swhid(node);
                    builder.swhid.append_value(swhid.to_string());
                    builder.url.append_null();
                }
                thread_pl.light_update();
                Ok(())
            },
        )?;

    log::info!("Flushing writers...");
    dataset_writer.close()?;

    pl.done();

    Ok(())
}
