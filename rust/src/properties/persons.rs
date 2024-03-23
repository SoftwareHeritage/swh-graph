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

/// Trait implemented by both [`NoPersons`] and all implementors of [`Persons`],
/// to allow loading person ids only if needed.
pub trait MaybePersons {}

pub struct MappedPersons {
    author_id: NumberMmap<BigEndian, u32, Mmap>,
    committer_id: NumberMmap<BigEndian, u32, Mmap>,
}
impl<P: Persons> MaybePersons for P {}

/// Placeholder for when person ids are not loaded
pub struct NoPersons;
impl MaybePersons for NoPersons {}

/// Trait for backend storage of person properties (either in-memory or memory-mapped)
pub trait Persons {
    type PersonIds<'a>: GetIndex<Output = u32>
    where
        Self: 'a;

    fn author_id(&self) -> Self::PersonIds<'_>;
    fn committer_id(&self) -> Self::PersonIds<'_>;
}

impl Persons for MappedPersons {
    type PersonIds<'a> = &'a NumberMmap<BigEndian, u32, Mmap> where Self: 'a;

    #[inline(always)]
    fn author_id(&self) -> Self::PersonIds<'_> {
        &self.author_id
    }
    #[inline(always)]
    fn committer_id(&self) -> Self::PersonIds<'_> {
        &self.committer_id
    }
}

pub struct VecPersons {
    author_id: Vec<u32>,
    committer_id: Vec<u32>,
}

impl VecPersons {
    /// Returns a [`VecPersons`] from pairs of `(author_id, committer_id)`
    pub fn new(data: Vec<(Option<u32>, Option<u32>)>) -> Result<Self> {
        let mut author_id = Vec::with_capacity(data.len());
        let mut committer_id = Vec::with_capacity(data.len());
        for (a, c) in data.into_iter() {
            ensure!(a != Some(u32::MAX), "author_id may not be {}", u32::MAX);
            ensure!(c != Some(u32::MAX), "author_id may not be {}", u32::MAX);
            author_id.push(a.unwrap_or(u32::MAX));
            committer_id.push(c.unwrap_or(u32::MAX));
        }
        Ok(VecPersons {
            author_id,
            committer_id,
        })
    }
}

impl Persons for VecPersons {
    type PersonIds<'a> = &'a [u32] where Self: 'a;

    #[inline(always)]
    fn author_id(&self) -> Self::PersonIds<'_> {
        self.author_id.as_slice()
    }
    #[inline(always)]
    fn committer_id(&self) -> Self::PersonIds<'_> {
        self.committer_id.as_slice()
    }
}

impl<
        MAPS: MaybeMaps,
        TIMESTAMPS: MaybeTimestamps,
        CONTENTS: MaybeContents,
        STRINGS: MaybeStrings,
        LABELNAMES: MaybeLabelNames,
    > SwhGraphProperties<MAPS, TIMESTAMPS, NoPersons, CONTENTS, STRINGS, LABELNAMES>
{
    /// Consumes a [`SwhGraphProperties`] and returns a new one with these methods
    /// available:
    ///
    /// * [`SwhGraphProperties::author_id`]
    /// * [`SwhGraphProperties::committer_id`]
    pub fn load_persons(
        self,
    ) -> Result<SwhGraphProperties<MAPS, TIMESTAMPS, MappedPersons, CONTENTS, STRINGS, LABELNAMES>>
    {
        let persons = MappedPersons {
            author_id: NumberMmap::new(suffix_path(&self.path, AUTHOR_ID), self.num_nodes)
                .context("Could not load author_id")?,
            committer_id: NumberMmap::new(suffix_path(&self.path, COMMITTER_ID), self.num_nodes)
                .context("Could not load committer_id")?,
        };
        self.with_persons(persons)
    }

    /// Alternative to [`load_persons`] that allows using arbitrary persons implementations
    pub fn with_persons<PERSONS: Persons>(
        self,
        persons: PERSONS,
    ) -> Result<SwhGraphProperties<MAPS, TIMESTAMPS, PERSONS, CONTENTS, STRINGS, LABELNAMES>> {
        Ok(SwhGraphProperties {
            maps: self.maps,
            timestamps: self.timestamps,
            persons,
            contents: self.contents,
            strings: self.strings,
            label_names: self.label_names,
            path: self.path,
            num_nodes: self.num_nodes,
        })
    }
}

/// Functions to access the id of the author or committer of `revision`/`release` nodes.
///
/// Only available after calling [`load_persons`](SwhGraphProperties::load_persons)
/// or [`load_all_properties`](SwhGraph::load_all_properties)
impl<
        MAPS: MaybeMaps,
        TIMESTAMPS: MaybeTimestamps,
        PERSONS: Persons,
        CONTENTS: MaybeContents,
        STRINGS: MaybeStrings,
        LABELNAMES: MaybeLabelNames,
    > SwhGraphProperties<MAPS, TIMESTAMPS, PERSONS, CONTENTS, STRINGS, LABELNAMES>
{
    /// Returns the id of the author of a revision or release, if any
    ///
    /// # Panics
    ///
    /// If the node id does not exist
    #[inline]
    pub fn author_id(&self, node_id: NodeId) -> Option<u32> {
        self.try_author_id(node_id)
            .unwrap_or_else(|e| panic!("Cannot get node author: {}", e))
    }

    /// Returns the id of the author of a revision or release, if any
    ///
    /// Returns `Err` if the node id does not exist, and `Ok(Node)` if the node
    /// has no author
    #[inline]
    pub fn try_author_id(&self, node_id: NodeId) -> Result<Option<u32>, OutOfBoundError> {
        match self.persons.author_id().get(node_id) {
            None => Err(OutOfBoundError {
                // Invalid node id
                index: node_id,
                len: self.persons.author_id().len(),
            }),
            Some(u32::MAX) => Ok(None), // No author
            Some(id) => Ok(Some(id)),
        }
    }

    /// Returns the id of the committer of a revision, if any
    ///
    /// # Panics
    ///
    /// If the node id does not exist
    #[inline]
    pub fn committer_id(&self, node_id: NodeId) -> Option<u32> {
        self.try_committer_id(node_id)
            .unwrap_or_else(|e| panic!("Cannot get node committer: {}", e))
    }

    /// Returns the id of the committer of a revision, if any
    ///
    /// Returns `None` if the node id does not exist, and `Some(Node)` if the node
    /// has no author
    #[inline]
    pub fn try_committer_id(&self, node_id: NodeId) -> Result<Option<u32>, OutOfBoundError> {
        match self.persons.committer_id().get(node_id) {
            None => Err(OutOfBoundError {
                // Invalid node id
                index: node_id,
                len: self.persons.committer_id().len(),
            }),
            Some(u32::MAX) => Ok(None), // No committer
            Some(id) => Ok(Some(id)),
        }
    }
}
