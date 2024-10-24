// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::collections::hash_map::{Entry, HashMap};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use anyhow::{bail, ensure, Context, Result};
use arrow::array::*;
use arrow::datatypes::DataType::*;
use arrow::datatypes::{Field, Schema, TimeUnit};
use clap::Parser;
use dataset_writer::{ParallelDatasetWriter, ParquetTableWriter, StructArrayBuilder};
use dsi_progress_logger::{progress_logger, ProgressLog};
use parquet::basic::{Compression, Encoding, ZstdLevel};
use parquet::file::properties::EnabledStatistics;
use parquet::file::properties::{WriterProperties, WriterPropertiesBuilder};
use rayon::prelude::*;

use swh_graph::collections::{AdaptiveNodeSet, NodeSet};
use swh_graph::graph::*;
use swh_graph::mph::DynMphf;
use swh_graph::utils::mmap::NumberMmap;
use swh_graph::utils::progress_logger::{BufferedProgressLogger, MinimalProgressLog};
use swh_graph::utils::GetIndex;
use swh_graph::NodeType;

use swh_graph_aggregate::parquet_metadata;

#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

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

#[derive(Parser, Debug)]
/** Taking as argument paths to datasets on content objects, aggregates these datasets into a
 * single Parquet dataset, with a column for each of:
 *
 * * the content id
 * * the content's length
 * * the most popular name of each content
 * * number of occurrences of that name for the content
 * * its date of first occurrence in a revision or release, if any
 * * said revision or release, if any
 * * an origin containing said revision or release, if any
 */
struct Args {
    graph_path: PathBuf,
    #[arg(long)]
    /// Path to a directory containing CSV files with the most popular file name
    file_names: PathBuf,
    #[arg(long)]
    /// Path to read the array of timestamps from
    earliest_timestamps: PathBuf,
    #[arg(long)]
    /// Path to write the array of max timestamps to
    out: PathBuf,
}

pub fn schema() -> Schema {
    Schema::new(vec![
        Field::new("id", UInt64, false),
        Field::new("length", UInt64, true),
        Field::new("filename", Binary, true),
        Field::new("filename_occurrences", UInt64, true),
        Field::new(
            "first_occurrence_timestamp",
            Timestamp(TimeUnit::Second, Some("UTC".into())),
            true,
        ),
        Field::new("first_occurrence_revrel", UInt64, true),
        Field::new("first_occurrence_origin", UInt64, true),
    ])
}

#[derive(Debug, Default)]
pub struct TableBuilder {
    pub id: UInt64Builder,
    pub length: UInt64Builder,
    pub filename: BinaryBuilder,
    pub filename_occurrences: UInt64Builder,
    pub first_occurrence_timestamp: UtcTimestampSecondBuilder,
    pub first_occurrence_revrel: UInt64Builder,
    pub first_occurrence_origin: UInt64Builder,
}

impl StructArrayBuilder for TableBuilder {
    fn len(&self) -> usize {
        self.id.len()
    }

    fn buffer_size(&self) -> usize {
        self.len() * (8 + 8 + 8 + 8 + 8 + 8) // u64 + u64 + u64 + utc timestamp + u64 + u64
         + self.len() * 7 / 8 // 6 validity buffers
         + self.filename.values_slice().len()
         + self.filename.offsets_slice().len() * 4 // BinaryBuilder uses i32 indices
         + self.filename.validity_slice().map(|s| s.len()).unwrap_or(0)
    }

    fn finish(&mut self) -> Result<StructArray> {
        let columns: Vec<Arc<dyn Array>> = vec![
            Arc::new(self.id.finish()),
            Arc::new(self.length.finish()),
            Arc::new(self.filename.finish()),
            Arc::new(self.filename_occurrences.finish()),
            Arc::new(self.first_occurrence_timestamp.finish()),
            Arc::new(self.first_occurrence_revrel.finish()),
            Arc::new(self.first_occurrence_origin.finish()),
        ];

        Ok(StructArray::new(
            schema().fields().clone(),
            columns,
            None, // nulls
        ))
    }
}

pub fn writer_properties<G: SwhGraph>(graph: &G) -> WriterPropertiesBuilder {
    WriterProperties::builder()
        // Main request key. Monotonic, and with long sequences of equal values
        .set_column_encoding("id".into(), Encoding::DELTA_BINARY_PACKED)
        .set_column_statistics_enabled("id".into(), EnabledStatistics::Page)
        .set_column_bloom_filter_enabled("id".into(), true)
        .set_column_compression(
            "id".into(),
            Compression::ZSTD(ZstdLevel::try_new(3).unwrap()),
        )
        // Too wildly distributed to query by range
        .set_column_compression(
            "length".into(),
            Compression::ZSTD(ZstdLevel::try_new(3).unwrap()),
        )
        .set_column_statistics_enabled("length".into(), EnabledStatistics::Chunk)
        // Textual data. makes sense to query by exact match
        .set_column_compression(
            "filename".into(),
            Compression::ZSTD(ZstdLevel::try_new(3).unwrap()),
        )
        .set_column_statistics_enabled("filename".into(), EnabledStatistics::Page)
        .set_column_bloom_filter_enabled("filename".into(), true)
        // Too wildly distributed to query by range
        .set_column_compression(
            "filename_occurrences".into(),
            Compression::ZSTD(ZstdLevel::try_new(3).unwrap()),
        )
        .set_column_statistics_enabled("filename_occurrences".into(), EnabledStatistics::Chunk)
        // May make sense to query by range. Maybe long sequences of equal value?
        .set_column_encoding(
            "first_occurrence_timestamp".into(),
            Encoding::DELTA_BINARY_PACKED,
        )
        .set_column_compression(
            "first_occurrence_timestamp".into(),
            Compression::ZSTD(ZstdLevel::try_new(3).unwrap()),
        )
        .set_column_statistics_enabled("first_occurrence_timestamp".into(), EnabledStatistics::Page)
        // Maybe long sequences of equal value?
        .set_column_encoding(
            "first_occurrence_revrel".into(),
            Encoding::DELTA_BINARY_PACKED,
        )
        .set_column_compression(
            "first_occurrence_revrel".into(),
            Compression::ZSTD(ZstdLevel::try_new(3).unwrap()),
        )
        .set_key_value_metadata(Some(crate::parquet_metadata(graph)))
        // Maybe long sequences of equal value?
        .set_column_encoding(
            "first_occurrence_origin".into(),
            Encoding::DELTA_BINARY_PACKED,
        )
        .set_column_compression(
            "first_occurrence_origin".into(),
            Compression::ZSTD(ZstdLevel::try_new(3).unwrap()),
        )
}

pub fn main() -> Result<()> {
    let args = Args::parse();

    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    log::info!("Loading graph");
    let graph = swh_graph::graph::SwhBidirectionalGraph::new(args.graph_path)
        .context("Could not load graph")?
        .load_backward_labels()
        .context("Could not load labels")?
        .init_properties()
        .load_properties(|props| props.load_maps::<DynMphf>())
        .context("Could not load maps")?
        .load_properties(|props| props.load_contents())
        .context("Could not load content properties")?
        .load_properties(|props| props.load_timestamps())
        .context("Could not load timestamp properties")?
        .load_properties(|props| props.load_label_names())
        .context("Could not load label names")?;
    log::info!("Graph loaded.");

    let earliest_timestamps =
        NumberMmap::<byteorder::BE, i64, _>::new(&args.earliest_timestamps, graph.num_nodes())
            .with_context(|| format!("Could not mmap {}", args.earliest_timestamps.display()))?;
    let earliest_timestamps = &earliest_timestamps;

    let dataset_writer = ParallelDatasetWriter::with_schema(
        args.out,
        (Arc::new(schema()), writer_properties(&graph).build()),
    )?;

    let mut pl = progress_logger!(
        item_name = "node",
        display_memory = true,
        local_speed = true,
        expected_updates = Some(graph.num_nodes()),
    );
    pl.start("Aggregating datasets");
    let shared_pl = Arc::new(Mutex::new(&mut pl));

    (0..graph.num_nodes()).into_par_iter().try_for_each_init(
        || {
            (
                dataset_writer.get_thread_writer().unwrap(),
                HashMap::new(), // Thread-specific cache so we don't have to pay the cost of
                // locking. Threads are unlikely to share much anyway, because the
                // content partitioning assigns similar contents to a single (or
                // few) threads.
                BufferedProgressLogger::new(shared_pl.clone()),
            )
        },
        |(writer, revrel2ori_cache, thread_pl), node| -> Result<()> {
            if graph.properties().node_type(node) == NodeType::Content {
                let length = graph.properties().content_length(node);
                let names = swh_graph_file_names::count_file_names(&graph, node)?;
                let (filename, occurrences) = match names
                    .into_iter()
                    .max_by_key(|(_, occurrences)| *occurrences)
                {
                    Some((filename_id, occurrences)) => (
                        Some(graph.properties().label_name(filename_id)),
                        Some(occurrences),
                    ),
                    None => (None, None),
                };
                write_content(
                    &graph,
                    writer,
                    earliest_timestamps,
                    revrel2ori_cache,
                    node,
                    length,
                    filename,
                    occurrences,
                )
                .map_err(|e| {
                    // Log early; errors occurring between two writes to Arrow can cause an
                    // inconsistent state of the Arrow builders that panics before we see this
                    // error.
                    log::error!(
                        "Failed to process {}: {:#?}",
                        graph.properties().swhid(node),
                        e
                    );
                    e
                })?;
            }
            thread_pl.light_update();
            Ok(())
        },
    )?;

    log::info!("Done, flushing output");

    dataset_writer.close()?;

    pl.done();

    Ok(())
}

#[allow(clippy::too_many_arguments)]
#[inline(always)]
fn write_content<G>(
    graph: &G,
    writer: &mut ParquetTableWriter<TableBuilder>,
    earliest_timestamps: impl GetIndex<Output = i64>,
    revrel2ori_cache: &mut HashMap<NodeId, Option<NodeId>>,
    node: NodeId,
    length: Option<u64>,
    filename: Option<Vec<u8>>,
    filename_occurrences: Option<u64>,
) -> Result<()>
where
    G: SwhGraphWithProperties + SwhBackwardGraph,
    <G as SwhGraphWithProperties>::Contents: swh_graph::properties::Contents,
    <G as SwhGraphWithProperties>::Maps: swh_graph::properties::Maps,
    <G as SwhGraphWithProperties>::Timestamps: swh_graph::properties::Timestamps,
{
    let builder = writer.builder()?;
    builder
        .id
        .append_value(node.try_into().expect("NodeId overflowed u64"));
    builder
        .length
        .append_option(length.or_else(|| graph.properties().content_length(node)));
    builder.filename.append_option(filename);
    builder
        .filename_occurrences
        .append_option(filename_occurrences);
    match earliest_timestamps.get(node) {
        None => bail!("Node {} is out of earliest_timestamps range", node),
        Some(i64::MIN) => {
            // content is not in any timestamped revrel
            builder.first_occurrence_timestamp.append_null();
            builder.first_occurrence_revrel.append_null();
            builder.first_occurrence_origin.append_null();
        }
        Some(first_occurrence_timestamp) => {
            let revrel = find_earliest_revrel_with_content(
                graph,
                &earliest_timestamps,
                node,
                first_occurrence_timestamp,
            )?;
            let origin = match revrel2ori_cache.entry(revrel) {
                Entry::Vacant(entry) => *entry.insert(find_origin_for_revrel(graph, revrel)?),
                Entry::Occupied(entry) => *entry.get(),
            };
            builder
                .first_occurrence_timestamp
                .append_value(first_occurrence_timestamp);
            builder
                .first_occurrence_revrel
                .append_value(revrel.try_into().expect("Node id overflows u64"));
            builder.first_occurrence_origin.append_option(
                origin.map(|origin| origin.try_into().expect("Node id overflows u64")),
            );
        }
    }

    Ok(())
}

/// Given a content and the known timestamp of its first occurrence in a revision/release,
/// find that revision/release
///
/// Implemented as a DFS
fn find_earliest_revrel_with_content<G>(
    graph: &G,
    timestamps: &impl GetIndex<Output = i64>,
    cnt: NodeId,
    timestamp: i64,
) -> Result<NodeId>
where
    G: SwhGraphWithProperties + SwhBackwardGraph,
    <G as SwhGraphWithProperties>::Maps: swh_graph::properties::Maps,
    <G as SwhGraphWithProperties>::Timestamps: swh_graph::properties::Timestamps,
{
    let mut stack = vec![cnt];
    let mut visited = AdaptiveNodeSet::new(graph.num_nodes());

    while let Some(node) = stack.pop() {
        let mut found_way_forward = false;
        for pred in graph.predecessors(node) {
            match graph.properties().node_type(pred) {
                NodeType::Content | NodeType::Origin => bail!(
                    "Unexpected predecessor type of {}: {}",
                    graph.properties().swhid(node),
                    graph.properties().swhid(pred)
                ),
                NodeType::Directory => {
                    use std::cmp::Ordering;
                    match timestamps.get(pred) {
                        None => bail!("Directory not in range"),
                        Some(i64::MIN) => {
                            // directory is not reachable from any timestamped revrel, so
                            // it cannot lead us to the revrel we are looking for
                        },
                        Some(dir_timestamp) => match dir_timestamp.cmp(&timestamp) {
                            Ordering::Less => bail!(
                                "{} has first occurrence at ts={}, but it contains {} which has first occurrence at ts={}",
                                graph.properties().swhid(pred),
                                dir_timestamp,
                                graph.properties().swhid(node),
                                timestamp,
                            ),
                            Ordering::Greater => {
                                // The directory is not reachable from the revision we are looking for,
                                // so recursing would be a waste of time.
                                // If it was, it would have a lower timestamp than it does.
                            }
                            Ordering::Equal => {
                                // The directory is reachable from the revision we are looking for,
                                // let's recurse
                                found_way_forward = true;
                                if visited.contains(pred) {
                                    continue;
                                }
                                visited.insert(pred);
                                stack.push(pred)
                            }
                        }
                    }
                }
                NodeType::Revision | NodeType::Release => {
                    if graph.properties().author_timestamp(pred) == Some(timestamp) {
                        return Ok(pred);
                    }
                    // don't recurse to parent revisions
                }
                NodeType::Snapshot => {} // ignore
            }
        }
        ensure!(
            found_way_forward,
            "Found inconsistent timestamps while traversing from {}: {} has first occurrence timestamp {}, but none of its predecessors do.",
            graph.properties().swhid(cnt),
            graph.properties().swhid(node),
            timestamp,
        )
    }

    bail!(
        "Could not find earliest revision for {}, even though there is a known first occurrence timestamp",
        graph.properties().swhid(cnt)
    );
}

/// Given a revision/release id, returns the id of an origin that contains it
///
/// Implemented as a DFS
fn find_origin_for_revrel<G>(graph: &G, revrel: NodeId) -> Result<Option<NodeId>>
where
    G: SwhGraphWithProperties + SwhBackwardGraph,
    <G as SwhGraphWithProperties>::Maps: swh_graph::properties::Maps,
{
    let mut stack = vec![revrel];
    let mut visited = AdaptiveNodeSet::new(graph.num_nodes());

    while let Some(node) = stack.pop() {
        for pred in graph.predecessors(node) {
            if visited.contains(pred) {
                continue;
            }
            visited.insert(pred);
            match graph.properties().node_type(pred) {
                NodeType::Origin => return Ok(Some(pred)),
                NodeType::Content => bail!(
                    "Unexpected predecessor type of {}: {}",
                    graph.properties().swhid(node),
                    graph.properties().swhid(pred)
                ),
                NodeType::Snapshot | NodeType::Revision | NodeType::Release => stack.push(pred),
                NodeType::Directory => {} // ignore
            }
        }
    }

    Ok(None)
}
