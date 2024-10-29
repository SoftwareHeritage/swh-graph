// Copyright (C) 2023-2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use anyhow::{ensure, Context, Result};
use mmap_rs::Mmap;

use super::suffixes::*;
use super::*;
use crate::graph::NodeId;
use crate::utils::suffix_path;

/// Trait implemented by both [`NoTimestamps`] and all implementors of [`Timestamps`],
/// to allow loading timestamp properties only if needed.
pub trait MaybeTimestamps {}

pub struct MappedTimestamps {
    author_timestamp: NumberMmap<BigEndian, i64, Mmap>,
    author_timestamp_offset: NumberMmap<BigEndian, i16, Mmap>,
    committer_timestamp: NumberMmap<BigEndian, i64, Mmap>,
    committer_timestamp_offset: NumberMmap<BigEndian, i16, Mmap>,
}
impl<T: Timestamps> MaybeTimestamps for T {}

#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct NoTimestamps;
impl MaybeTimestamps for NoTimestamps {}

#[diagnostic::on_unimplemented(
    label = "does not have Timestamp properties loaded",
    note = "Use `let graph = graph.load_properties(|props| props.load_timestamps()).unwrap()` to load them",
    note = "Or replace `graph.init_properties()` with `graph.load_all_properties::<DynMphf>().unwrap()` to load all properties"
)]
/// Trait for backend storage of timestamp properties (either in-memory or memory-mapped)
pub trait Timestamps {
    type Timestamps<'a>: GetIndex<Output = i64> + 'a
    where
        Self: 'a;
    type Offsets<'a>: GetIndex<Output = i16> + 'a
    where
        Self: 'a;

    fn author_timestamp(&self) -> Self::Timestamps<'_>;
    fn author_timestamp_offset(&self) -> Self::Offsets<'_>;
    fn committer_timestamp(&self) -> Self::Timestamps<'_>;
    fn committer_timestamp_offset(&self) -> Self::Offsets<'_>;
}

impl Timestamps for MappedTimestamps {
    type Timestamps<'a> = &'a NumberMmap<BigEndian, i64, Mmap>;
    type Offsets<'a> = &'a NumberMmap<BigEndian, i16, Mmap>;

    #[inline(always)]
    fn author_timestamp(&self) -> Self::Timestamps<'_> {
        &self.author_timestamp
    }
    #[inline(always)]
    fn author_timestamp_offset(&self) -> Self::Offsets<'_> {
        &self.author_timestamp_offset
    }
    #[inline(always)]
    fn committer_timestamp(&self) -> Self::Timestamps<'_> {
        &self.committer_timestamp
    }
    #[inline(always)]
    fn committer_timestamp_offset(&self) -> Self::Offsets<'_> {
        &self.committer_timestamp_offset
    }
}

#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct VecTimestamps {
    author_timestamp: Vec<i64>,
    author_timestamp_offset: Vec<i16>,
    committer_timestamp: Vec<i64>,
    committer_timestamp_offset: Vec<i16>,
}

impl VecTimestamps {
    /// Builds [`VecTimestamps`] from 4-tuples of `(author_timestamp, author_timestamp_offset,
    /// committer_timestamp, committer_timestamp_offset)`
    #[allow(clippy::type_complexity)]
    pub fn new(
        timestamps: Vec<(Option<i64>, Option<i16>, Option<i64>, Option<i16>)>,
    ) -> Result<Self> {
        let mut author_timestamp = Vec::with_capacity(timestamps.len());
        let mut author_timestamp_offset = Vec::with_capacity(timestamps.len());
        let mut committer_timestamp = Vec::with_capacity(timestamps.len());
        let mut committer_timestamp_offset = Vec::with_capacity(timestamps.len());
        for (a_ts, a_ts_o, c_ts, c_ts_o) in timestamps {
            ensure!(
                a_ts != Some(i64::MIN),
                "author timestamp may not be {}",
                i64::MIN
            );
            ensure!(
                a_ts_o != Some(i16::MIN),
                "author timestamp offset may not be {}",
                i16::MIN
            );
            ensure!(
                c_ts != Some(i64::MIN),
                "committer timestamp may not be {}",
                i64::MIN
            );
            ensure!(
                c_ts_o != Some(i16::MIN),
                "committer timestamp offset may not be {}",
                i16::MIN
            );
            author_timestamp.push(a_ts.unwrap_or(i64::MIN));
            author_timestamp_offset.push(a_ts_o.unwrap_or(i16::MIN));
            committer_timestamp.push(c_ts.unwrap_or(i64::MIN));
            committer_timestamp_offset.push(c_ts_o.unwrap_or(i16::MIN));
        }
        Ok(VecTimestamps {
            author_timestamp,
            author_timestamp_offset,
            committer_timestamp,
            committer_timestamp_offset,
        })
    }
}

impl Timestamps for VecTimestamps {
    type Timestamps<'a> = &'a [i64];
    type Offsets<'a> = &'a [i16];

    #[inline(always)]
    fn author_timestamp(&self) -> Self::Timestamps<'_> {
        self.author_timestamp.as_slice()
    }
    #[inline(always)]
    fn author_timestamp_offset(&self) -> Self::Offsets<'_> {
        self.author_timestamp_offset.as_slice()
    }
    #[inline(always)]
    fn committer_timestamp(&self) -> Self::Timestamps<'_> {
        self.committer_timestamp.as_slice()
    }
    #[inline(always)]
    fn committer_timestamp_offset(&self) -> Self::Offsets<'_> {
        self.committer_timestamp_offset.as_slice()
    }
}

impl<
        MAPS: MaybeMaps,
        PERSONS: MaybePersons,
        CONTENTS: MaybeContents,
        STRINGS: MaybeStrings,
        LABELNAMES: MaybeLabelNames,
    > SwhGraphProperties<MAPS, NoTimestamps, PERSONS, CONTENTS, STRINGS, LABELNAMES>
{
    /// Consumes a [`SwhGraphProperties`] and returns a new one with these methods
    /// available:
    ///
    /// * [`SwhGraphProperties::author_timestamp`]
    /// * [`SwhGraphProperties::author_timestamp_offset`]
    /// * [`SwhGraphProperties::committer_timestamp`]
    /// * [`SwhGraphProperties::committer_timestamp_offset`]
    pub fn load_timestamps(
        self,
    ) -> Result<SwhGraphProperties<MAPS, MappedTimestamps, PERSONS, CONTENTS, STRINGS, LABELNAMES>>
    {
        let timestamps = MappedTimestamps {
            author_timestamp: NumberMmap::new(
                suffix_path(&self.path, AUTHOR_TIMESTAMP),
                self.num_nodes,
            )
            .context("Could not load author_timestamp")?,
            author_timestamp_offset: NumberMmap::new(
                suffix_path(&self.path, AUTHOR_TIMESTAMP_OFFSET),
                self.num_nodes,
            )
            .context("Could not load author_timestamp_offset")?,
            committer_timestamp: NumberMmap::new(
                suffix_path(&self.path, COMMITTER_TIMESTAMP),
                self.num_nodes,
            )
            .context("Could not load committer_timestamp")?,
            committer_timestamp_offset: NumberMmap::new(
                suffix_path(&self.path, COMMITTER_TIMESTAMP_OFFSET),
                self.num_nodes,
            )
            .context("Could not load committer_timestamp_offset")?,
        };
        self.with_timestamps(timestamps)
    }

    /// Alternative to [`load_timestamps`](Self::load_timestamps) that allows using arbitrary
    /// timestamps implementations
    pub fn with_timestamps<TIMESTAMPS: Timestamps>(
        self,
        timestamps: TIMESTAMPS,
    ) -> Result<SwhGraphProperties<MAPS, TIMESTAMPS, PERSONS, CONTENTS, STRINGS, LABELNAMES>> {
        Ok(SwhGraphProperties {
            maps: self.maps,
            timestamps,
            persons: self.persons,
            contents: self.contents,
            strings: self.strings,
            label_names: self.label_names,
            path: self.path,
            num_nodes: self.num_nodes,
        })
    }
}

/// Functions to access timestamps of `revision` and `release` nodes
///
/// Only available after calling [`load_timestamps`](SwhGraphProperties::load_timestamps)
/// or [`load_all_properties`](crate::graph::SwhBidirectionalGraph::load_all_properties)
impl<
        MAPS: MaybeMaps,
        TIMESTAMPS: Timestamps,
        PERSONS: MaybePersons,
        CONTENTS: MaybeContents,
        STRINGS: MaybeStrings,
        LABELNAMES: MaybeLabelNames,
    > SwhGraphProperties<MAPS, TIMESTAMPS, PERSONS, CONTENTS, STRINGS, LABELNAMES>
{
    /// Returns the number of seconds since Epoch that a release or revision was
    /// authored at
    ///
    /// # Panics
    ///
    /// If the node id does not exist
    #[inline]
    pub fn author_timestamp(&self, node_id: NodeId) -> Option<i64> {
        self.try_author_timestamp(node_id)
            .unwrap_or_else(|e| panic!("Cannot get author timestamp: {}", e))
    }

    /// Returns the number of seconds since Epoch that a release or revision was
    /// authored at
    ///
    /// Returns `Err` if the node id is unknown, and `Ok(None)` if the node has
    /// no author timestamp
    #[inline]
    pub fn try_author_timestamp(&self, node_id: NodeId) -> Result<Option<i64>, OutOfBoundError> {
        match self.timestamps.author_timestamp().get(node_id) {
            None => Err(OutOfBoundError {
                index: node_id,
                len: self.timestamps.author_timestamp().len(),
            }),
            Some(i64::MIN) => Ok(None),
            Some(ts) => Ok(Some(ts)),
        }
    }

    /// Returns the UTC offset in minutes of a release or revision's authorship date
    ///
    /// # Panics
    ///
    /// If the node id does not exist
    #[inline]
    pub fn author_timestamp_offset(&self, node_id: NodeId) -> Option<i16> {
        self.try_author_timestamp_offset(node_id)
            .unwrap_or_else(|e| panic!("Cannot get author timestamp offset: {}", e))
    }

    /// Returns the UTC offset in minutes of a release or revision's authorship date
    ///
    /// Returns `Err` if the node id is unknown, and `Ok(None)` if the node has
    /// no author timestamp
    #[inline]
    pub fn try_author_timestamp_offset(
        &self,
        node_id: NodeId,
    ) -> Result<Option<i16>, OutOfBoundError> {
        match self.timestamps.author_timestamp_offset().get(node_id) {
            None => Err(OutOfBoundError {
                index: node_id,
                len: self.timestamps.author_timestamp_offset().len(),
            }),
            Some(i16::MIN) => Ok(None),
            Some(offset) => Ok(Some(offset)),
        }
    }

    /// Returns the number of seconds since Epoch that a revision was committed at
    ///
    /// # Panics
    ///
    /// If the node id does not exist
    #[inline]
    pub fn committer_timestamp(&self, node_id: NodeId) -> Option<i64> {
        self.try_committer_timestamp(node_id)
            .unwrap_or_else(|e| panic!("Cannot get committer timestamp: {}", e))
    }

    /// Returns the number of seconds since Epoch that a revision was committed at
    ///
    /// Returns `Err` if the node id is unknown, and `Ok(None)` if the node has
    /// no committer timestamp
    #[inline]
    pub fn try_committer_timestamp(&self, node_id: NodeId) -> Result<Option<i64>, OutOfBoundError> {
        match self.timestamps.committer_timestamp().get(node_id) {
            None => Err(OutOfBoundError {
                index: node_id,
                len: self.timestamps.committer_timestamp().len(),
            }),
            Some(i64::MIN) => Ok(None),
            Some(ts) => Ok(Some(ts)),
        }
    }

    /// Returns the UTC offset in minutes of a revision's committer date
    ///
    /// # Panics
    ///
    /// If the node id does not exist
    #[inline]
    pub fn committer_timestamp_offset(&self, node_id: NodeId) -> Option<i16> {
        self.try_committer_timestamp_offset(node_id)
            .unwrap_or_else(|e| panic!("Cannot get committer timestamp: {}", e))
    }

    /// Returns the UTC offset in minutes of a revision's committer date
    ///
    /// Returns `Err` if the node id is unknown, and `Ok(None)` if the node has
    /// no committer timestamp
    #[inline]
    pub fn try_committer_timestamp_offset(
        &self,
        node_id: NodeId,
    ) -> Result<Option<i16>, OutOfBoundError> {
        match self.timestamps.committer_timestamp_offset().get(node_id) {
            None => Err(OutOfBoundError {
                index: node_id,
                len: self.timestamps.committer_timestamp_offset().len(),
            }),
            Some(i16::MIN) => Ok(None),
            Some(offset) => Ok(Some(offset)),
        }
    }
}
