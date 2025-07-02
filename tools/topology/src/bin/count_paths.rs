// Copyright (C) 2023-2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::io;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use anyhow::{ensure, Context, Result};
use arrow::array::*;
use arrow::datatypes::{DataType, Field, Schema};
use clap::{Parser, ValueEnum};
use dsi_progress_logger::{progress_logger, ProgressLog};
use itertools::Itertools;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::EnabledStatistics;
use parquet::file::properties::WriterProperties;
use rayon::prelude::*;
use serde::Deserialize;

use dataset_writer::{
    ParquetTableWriter, ParquetTableWriterConfig, StructArrayBuilder, TableWriter,
};
use swh_graph::graph::*;
use swh_graph::mph::DynMphf;
use swh_graph::views::Transposed;

#[derive(Debug, Clone, Copy, ValueEnum)]
enum Direction {
    Forward,
    Backward,
}

#[derive(Parser, Debug)]
#[command(about = "Counts the number of (non-singleton) paths reaching each node, from all other nodes.", long_about = Some("
Counts in the output may be large enough to overflow long integers, so they are computed with double-precision floating point number and written as such.

This requires a topological order as input (as returned by the toposort command).
"))]
struct Args {
    graph_path: PathBuf,
    #[arg(short, long)]
    direction: Direction,
    #[arg(long, default_value_t = 1_000_000)]
    /// Number of input records to deserialize at once in parallel
    batch_size: usize,
    #[arg(long, default_value_t = 96)]
    /// Number of Parquet files to write to
    num_shards: usize,
    #[arg(long)]
    /// Directory where to write the output, as Parquet files
    out: PathBuf,
}

#[derive(Debug, Deserialize)]
struct InputRecord {
    #[serde(rename = "SWHID")]
    swhid: String,
}

#[derive(Debug)]
struct OutputRecord {
    swhid: String,
    paths_from_roots: f64,
    all_paths: f64,
}

#[derive(Debug)]
pub struct OutputBuilder {
    swhid: StringBuilder,
    paths_from_roots: Float64Builder,
    all_paths: Float64Builder,
}

fn schema() -> Schema {
    Schema::new(vec![
        Field::new("swhid", DataType::Utf8, false),
        Field::new("paths_from_roots", DataType::Float64, false),
        Field::new("all_paths", DataType::Float64, false),
    ])
}

impl Default for OutputBuilder {
    fn default() -> Self {
        Self {
            swhid: StringBuilder::default(), // TODO: don't allocate validity buffer
            paths_from_roots: Float64Builder::new_from_buffer(
                Default::default(),
                None, // Values are not nullable -> validity buffer not needed
            ),
            all_paths: Float64Builder::new_from_buffer(
                Default::default(),
                None, // ditto
            ),
        }
    }
}

impl StructArrayBuilder for OutputBuilder {
    fn len(&self) -> usize {
        self.swhid.len()
    }

    fn buffer_size(&self) -> usize {
        // swhid + f64 + f64
        self.len() * (50 + 8 + 8)
    }

    fn finish(&mut self) -> Result<StructArray> {
        let columns: Vec<Arc<dyn Array>> = vec![
            Arc::new(self.swhid.finish()),
            Arc::new(self.paths_from_roots.finish()),
            Arc::new(self.all_paths.finish()),
        ];

        Ok(StructArray::new(
            schema().fields().clone(),
            columns,
            None, // nulls
        ))
    }
}

pub fn main() -> Result<()> {
    let args = Args::parse();

    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    log::info!("Loading graph");
    let graph = swh_graph::graph::SwhBidirectionalGraph::new(args.graph_path)
        .context("Could not load graph")?
        .init_properties()
        .load_properties(|props| props.load_maps::<DynMphf>())
        .context("Could not load maps")?;

    let writer_properties = WriterProperties::builder()
        // Allows filtering on node type
        .set_column_statistics_enabled("swhid".into(), EnabledStatistics::Page)
        // Allows looking for specific nodes
        .set_column_bloom_filter_enabled("swhid".into(), true)
        // Compress, as it's mostly an hexadecimal string
        .set_column_compression(
            "swhid".into(),
            Compression::ZSTD(ZstdLevel::try_new(3).unwrap()),
        )
        // Efficient encoding+compression combination for floating-point
        // Not supported yet
        //.set_column_encoding("paths_from_roots".into(), Encoding::BYTE_STREAM_SPLIT)
        .set_column_compression(
            "paths_from_roots".into(),
            Compression::ZSTD(ZstdLevel::try_new(3).unwrap()),
        )
        // Not supported yet
        // .set_column_encoding("all_paths".into(), Encoding::BYTE_STREAM_SPLIT)
        .set_column_compression(
            "all_paths".into(),
            Compression::ZSTD(ZstdLevel::try_new(3).unwrap()),
        )
        // Allows filtering only nodes with large/small values
        .set_column_statistics_enabled("paths_from_roots".into(), EnabledStatistics::Page)
        .set_column_statistics_enabled("all_paths".into(), EnabledStatistics::Page)
        .build();

    std::fs::create_dir_all(&args.out)
        .with_context(|| format!("Could not create {}", args.out.display()))?;
    let dataset_writers = (0..args.num_shards)
        .map(|i| {
            ParquetTableWriter::new(
                args.out.join(format!("{i}.parquet")),
                (Arc::new(schema()), writer_properties.clone()),
                ParquetTableWriterConfig::default(),
            )
        })
        .collect::<Result<Vec<_>>>()?;

    match args.direction {
        Direction::Forward => {
            count_paths(std::sync::Arc::new(graph), dataset_writers, args.batch_size)
        }
        Direction::Backward => count_paths(
            std::sync::Arc::new(Transposed(std::sync::Arc::new(graph))),
            dataset_writers,
            args.batch_size,
        ),
    }
}

fn count_paths<G>(
    graph: G,
    dataset_writers: Vec<ParquetTableWriter<OutputBuilder>>,
    batch_size: usize,
) -> Result<()>
where
    G: SwhForwardGraph + SwhBackwardGraph + SwhGraphWithProperties + Sync + Send + Clone + 'static,
    <G as SwhGraphWithProperties>::Maps: swh_graph::properties::Maps,
{
    // Arbitrary constant; ensures the deserialization thread is unlikely to block
    // unless the main thread is actually busy
    let (nodes_tx, nodes_rx) = std::sync::mpsc::sync_channel(4);
    let (counts_tx, counts_rx) = std::sync::mpsc::sync_channel(4);

    let graph2 = graph.clone();
    let deser_thread = std::thread::spawn(move || queue_nodes(graph2, nodes_tx, batch_size));
    let ser_thread = std::thread::spawn(|| write_path_counts(dataset_writers, counts_rx));

    count_paths_from_node_ids(graph, nodes_rx, counts_tx)?;

    log::info!("Cleaning up...");
    for (name, thread) in [
        ("deserialization", deser_thread),
        ("serialization", ser_thread),
    ] {
        thread
            .join()
            .unwrap_or_else(|e| panic!("Could not join {name} thread: {e:?}"))
            .with_context(|| format!("Error in {name} thread"))?;
    }

    Ok(())
}

fn count_paths_from_node_ids<G>(
    graph: G,
    nodes_rx: std::sync::mpsc::Receiver<Box<[(String, NodeId)]>>,
    counts_tx: std::sync::mpsc::SyncSender<Box<[OutputRecord]>>,
) -> Result<()>
where
    G: SwhForwardGraph + SwhBackwardGraph + SwhGraphWithProperties + Sync + Send + Clone + 'static,
    <G as SwhGraphWithProperties>::Maps: swh_graph::properties::Maps,
{
    log::info!("Initializing counts...");
    let mut paths_from_roots_vec = vec![0f64; graph.num_nodes()];
    let mut all_paths_vec = vec![0f64; graph.num_nodes()];

    let mut pl = progress_logger!(
        display_memory = true,
        local_speed = true,
        item_name = "node",
        expected_updates = Some(graph.num_nodes()),
    );
    pl.start("Listing nodes...");

    // Read sequentially, *in order*
    while let Ok(chunk) = nodes_rx.recv() {
        let counts = Vec::from(chunk)
            .into_iter()
            .map(|(swhid, node)| {
                pl.light_update();
                let mut paths_from_roots = paths_from_roots_vec[node];
                let mut all_paths = all_paths_vec[node];

                let counts = OutputRecord {
                    swhid,
                    paths_from_roots,
                    all_paths,
                };

                // Add counts of paths coming from this node to all successors
                all_paths += 1.;
                if paths_from_roots == 0. {
                    paths_from_roots += 1.;
                }
                for succ in graph.successors(node) {
                    paths_from_roots_vec[succ] += paths_from_roots;
                    all_paths_vec[succ] += all_paths;
                }

                counts
            })
            .collect();
        counts_tx
            .send(counts)
            .context("Could not send OutputRecord")?;
    }

    pl.done();

    Ok(())
}

fn write_path_counts(
    dataset_writers: Vec<ParquetTableWriter<OutputBuilder>>,
    counts_rx: std::sync::mpsc::Receiver<Box<[OutputRecord]>>,
) -> Result<()> {
    // Write consecutive records in consecutively in the same writer,
    // as they are likely to have similar path counts, and therefore compress better
    let counts_rx = Arc::new(Mutex::new(counts_rx));

    std::thread::scope(|scope| -> Result<()> {
        let handles: Vec<_> = dataset_writers
            .into_iter()
            .map(|mut writer| {
                scope.spawn(|| -> Result<()> {
                    while let Ok(chunk) = counts_rx.lock().unwrap().recv() {
                        for record in Vec::from(chunk).drain(0..) {
                            let OutputRecord {
                                swhid,
                                paths_from_roots,
                                all_paths,
                            } = record;

                            let builder = writer.builder()?;
                            builder.swhid.append_value(swhid);
                            builder.paths_from_roots.append_value(paths_from_roots);
                            builder.all_paths.append_value(all_paths);
                        }
                    }

                    writer.close()?;

                    Ok(())
                })
            })
            .collect();

        for handle in handles {
            handle
                .join()
                .expect("Could not join serialization thread")?;
        }

        Ok(())
    })
}

/// Reads CSV records from stdin, and queues their SWHIDs and node ids to `tx`,
/// preserving the order.
///
/// This is equivalent to:
///
/// ```
/// std::thread::spawn(move || -> Result<()> {
///     let mut reader = csv::ReaderBuilder::new()
///         .has_headers(true)
///         .from_reader(io::stdin());
///
///     for record in reader.deserialize() {
///         let InputRecord { swhid, .. } =
///             record.with_context(|| format!("Could not deserialize record"))?;
///         let node = graph
///             .properties()
///             .node_id_from_string_swhid(swhid)
///             .with_context(|| format!("Unknown SWHID: {}", swhid))?;
///
///         tx.send((swhid, node))
///     }
/// });
/// ```
///
/// but uses inner parallelism as `node_id()` could otherwise be a bottleneck on systems
/// where accessing `graph.order` has high latency (network and/or compressed filesystem).
/// This reduces the runtime from a couple of weeks to less than a day on the 2023-09-06
/// graph on a ZSTD-compressed ZFS.
fn queue_nodes<G>(
    graph: G,
    tx: std::sync::mpsc::SyncSender<Box<[(String, NodeId)]>>,
    batch_size: usize,
) -> Result<()>
where
    G: SwhGraphWithProperties + Sync + Send + Clone + 'static,
    <G as SwhGraphWithProperties>::Maps: swh_graph::properties::Maps,
{
    // Workers in this function block until they get data from a queue.
    // as that queue is filled by other Rayon workers, using a shared thread pool
    // risks deadlocks, as this function can block all the threads, leaving no thread
    // to fill the queue.
    let pool = rayon::ThreadPoolBuilder::new()
        .build()
        .context("Could not build thread pool")?;
    pool.install(|| {
        let mut reader = csv::ReaderBuilder::new()
            .has_headers(true)
            .from_reader(io::stdin());

        // Makes sure the input at least has a header, even when there is no payload
        ensure!(
            reader
                .headers()
                .context("Invalid header in input")?
                .iter()
                .any(|item| item == "SWHID"),
            "Input has no 'swhid' header"
        );

        reader
            .deserialize()
            .chunks(batch_size)
            .into_iter()
            .try_for_each(|chunk| {
                // Process entries of this chunk in parallel
                let results: Result<Box<[_]>> = chunk
                    .collect::<Vec<Result<InputRecord, _>>>()
                    .into_par_iter()
                    .map(|record| {
                        let InputRecord { swhid, .. } =
                            record.with_context(|| "Could not deserialize record".to_string())?;

                        let node = graph.properties().node_id_from_string_swhid(&swhid)?;
                        Ok((swhid, node))
                    })
                    .collect();

                let results = results?;

                // Then collect them **IN ORDER** before pushing to 'tx'.
                tx.send(results).expect("Could not send (swhid, node_id)");

                Ok::<_, anyhow::Error>(())
            })?;

        Ok(())
    })
}
