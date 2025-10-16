/*
 * Copyright (C) 2023-2024  The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

use anyhow::{Context, Result};
use dsi_bitstream::prelude::*;
use rayon::prelude::*;
use webgraph::prelude::sort_pairs::{BitReader, BitWriter};
use webgraph::prelude::*;

use swh_graph::utils::sort::*;

#[test]
#[cfg_attr(miri, ignore)] // miri does not support file-backed mmap
fn test_par_sort_arcs() -> Result<()> {
    let tempdir = tempfile::tempdir().context("temp dir")?;
    assert_eq!(
        par_sort_arcs(
            tempdir.path(),
            100, // sort_batch_size
            vec![(1, 10), (2, 1), (1, 5), (2, 10)].into_par_iter(),
            10, // num_partition
            (),
            (),
            |buf, (src, dst)| {
                let partition_id = src / 10;
                buf.insert(partition_id, src, dst)
            }
        )
        .context("par_sort_arcs")?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>(),
        vec![(1, 5, ()), (1, 10, ()), (2, 1, ()), (2, 10, ())],
    );

    Ok(())
}

#[test]
#[cfg_attr(miri, ignore)] // miri does not support file-backed mmap
fn test_par_sort_arcs_empty_buffer() -> Result<()> {
    let tempdir = tempfile::tempdir().context("temp dir")?;
    assert_eq!(
        par_sort_arcs(
            tempdir.path(),
            0, // Causes buffers to be flushed immediately, so the last batch will be empty
            vec![(1, 10), (2, 1), (1, 5), (2, 10)].into_par_iter(),
            1, // num_partitions
            (),
            (),
            |buf, (src, dst)| {
                let partition_id = 0;
                buf.insert(partition_id, src, dst)
            }
        )
        .context("par_sort_arcs")?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>(),
        vec![(1, 5, ()), (1, 10, ()), (2, 1, ()), (2, 10, ())],
    );

    Ok(())
}

#[derive(Clone, Copy)]
struct TestLabelDeserializer;
#[derive(Clone, Copy)]
struct TestLabelSerializer;

impl BitDeserializer<NE, BitReader> for TestLabelDeserializer {
    type DeserType = i32;
    fn deserialize(
        &self,
        bitstream: &mut BitReader,
    ) -> Result<Self::DeserType, <BitReader as BitRead<NE>>::Error> {
        bitstream.read_delta().map(|x| x as i32)
    }
}

impl BitSerializer<NE, BitWriter> for TestLabelSerializer {
    type SerType = i32;
    fn serialize(
        &self,
        value: &Self::SerType,
        bitstream: &mut BitWriter,
    ) -> Result<usize, <BitWriter as BitWrite<NE>>::Error> {
        bitstream.write_delta(*value as u64)
    }
}

#[test]
#[cfg_attr(miri, ignore)] // miri does not support file-backed mmap
fn test_par_sort_arcs_labeled() -> Result<()> {
    let tempdir = tempfile::tempdir().context("temp dir")?;
    let res = par_sort_arcs(
        tempdir.path(),
        100, // sort_batch_size
        vec![
            (1, 10, 123),
            (2, 1, 456),
            (1, 10, 789),
            (1, 5, -123),
            (2, 10, -456),
        ]
        .into_par_iter(),
        10, // num_partition
        TestLabelSerializer,
        TestLabelDeserializer,
        |buf: &mut PartitionedBuffer<i32, TestLabelSerializer, TestLabelDeserializer>,
         (src, dst, label)| {
            let partition_id = src / 10;
            buf.insert_labeled(partition_id, src, dst, label)
        },
    )
    .context("par_sort_arcs")?
    .into_iter()
    .flatten()
    .collect::<Vec<_>>();

    // Two options, as labels are not sorted
    if res
        != vec![
            (1, 5, -123),
            (1, 10, 123),
            (1, 10, 789),
            (2, 1, 456),
            (2, 10, -456),
        ]
    {
        assert_eq!(
            res,
            vec![
                (1, 5, -123),
                (1, 10, 789),
                (1, 10, 123),
                (2, 1, 456),
                (2, 10, -456)
            ]
        )
    }

    Ok(())
}
