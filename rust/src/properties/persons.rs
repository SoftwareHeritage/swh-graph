// Copyright (C) 2023-2025  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use anyhow::{ensure, Context, Result};
use mmap_rs::Mmap;

use super::suffixes::*;
use super::*;
use crate::graph::NodeId;

/// Trait implemented by both [`NoPersons`] and all implementors of [`Persons`],
/// to allow loading person ids only if needed.
pub trait MaybePersons {}
impl<P: OptPersons> MaybePersons for P {}

/// Placeholder for when person ids are not loaded
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct NoPersons;
impl MaybePersons for NoPersons {}

#[diagnostic::on_unimplemented(
    label = "does not have Person properties loaded",
    note = "Use `let graph = graph.load_properties(|props| props.load_persons()).unwrap()` to load them",
    note = "Or replace `graph.init_properties()` with `graph.load_all_properties::<DynMphf>().unwrap()` to load all properties"
)]
/// Trait for backend storage of person properties (either in-memory or memory-mapped)
pub trait OptPersons: MaybePersons + PropertiesBackend {
    /// Returns `None` if out of bounds, `Some(u32::MAX)` if the node has no author
    fn author_id(&self, node: NodeId) -> PropertiesResult<'_, Option<u32>, Self>;
    /// Returns `None` if out of bounds, `Some(u32::MAX)` if the node has no committer
    fn committer_id(&self, node: NodeId) -> PropertiesResult<'_, Option<u32>, Self>;
}

#[diagnostic::on_unimplemented(
    label = "does not have Person properties loaded",
    note = "Use `let graph = graph.load_properties(|props| props.load_persons()).unwrap()` to load them",
    note = "Or replace `graph.init_properties()` with `graph.load_all_properties::<DynMphf>().unwrap()` to load all properties"
)]
/// Trait for backend storage of person properties (either in-memory or memory-mapped)
pub trait Persons: OptPersons<DataFilesAvailability = GuaranteedDataFiles> {}
impl<P: OptPersons<DataFilesAvailability = GuaranteedDataFiles>> Persons for P {}

pub struct OptMappedPersons {
    author_id: Result<NumberMmap<BigEndian, u32, Mmap>, UnavailableProperty>,
    committer_id: Result<NumberMmap<BigEndian, u32, Mmap>, UnavailableProperty>,
}
impl PropertiesBackend for OptMappedPersons {
    type DataFilesAvailability = OptionalDataFiles;
}
impl OptPersons for OptMappedPersons {
    #[inline(always)]
    fn author_id(&self, node: NodeId) -> PropertiesResult<'_, Option<u32>, Self> {
        self.author_id
            .as_ref()
            .map(|author_ids| author_ids.get(node))
    }
    #[inline(always)]
    fn committer_id(&self, node: NodeId) -> PropertiesResult<'_, Option<u32>, Self> {
        self.committer_id
            .as_ref()
            .map(|committer_ids| committer_ids.get(node))
    }
}

pub struct MappedPersons {
    author_id: NumberMmap<BigEndian, u32, Mmap>,
    committer_id: NumberMmap<BigEndian, u32, Mmap>,
}
impl PropertiesBackend for MappedPersons {
    type DataFilesAvailability = GuaranteedDataFiles;
}
impl OptPersons for MappedPersons {
    /// Returns `None` if author ids are not loaded, `Some(u32::MAX)` if they are
    /// loaded and the node has no author, or `Some(Some(_))` if they are loaded
    /// and the node has an author
    #[inline(always)]
    fn author_id(&self, node: NodeId) -> Option<u32> {
        (&self.author_id).get(node)
    }
    /// See [`Self::author_id`]
    #[inline(always)]
    fn committer_id(&self, node: NodeId) -> Option<u32> {
        (&self.committer_id).get(node)
    }
}

#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
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

impl PropertiesBackend for VecPersons {
    type DataFilesAvailability = GuaranteedDataFiles;
}
impl OptPersons for VecPersons {
    #[inline(always)]
    fn author_id(&self, node: NodeId) -> Option<u32> {
        self.author_id.get(node)
    }
    #[inline(always)]
    fn committer_id(&self, node: NodeId) -> Option<u32> {
        self.committer_id.get(node)
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
        let OptMappedPersons {
            author_id,
            committer_id,
        } = self.get_persons()?;
        let persons = MappedPersons {
            author_id: author_id?,
            committer_id: committer_id?,
        };
        self.with_persons(persons)
    }

    /// Equivalent to [`Self::load_persons`] that does not require all files to be present
    pub fn opt_load_persons(
        self,
    ) -> Result<SwhGraphProperties<MAPS, TIMESTAMPS, OptMappedPersons, CONTENTS, STRINGS, LABELNAMES>>
    {
        let persons = self.get_persons()?;
        self.with_persons(persons)
    }

    fn get_persons(&self) -> Result<OptMappedPersons> {
        Ok(OptMappedPersons {
            author_id: load_if_exists(&self.path, AUTHOR_ID, |path| {
                NumberMmap::new(path, self.num_nodes).context("Could not load author_id")
            })?,
            committer_id: load_if_exists(&self.path, COMMITTER_ID, |path| {
                NumberMmap::new(path, self.num_nodes).context("Could not load committer_id")
            })?,
        })
    }

    /// Alternative to [`load_persons`](Self::load_persons) that allows using arbitrary
    /// persons implementations
    pub fn with_persons<PERSONS: MaybePersons>(
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
            label_names_are_in_base64_order: self.label_names_are_in_base64_order,
        })
    }
}

/// Functions to access the id of the author or committer of `revision`/`release` nodes.
///
/// Only available after calling [`load_persons`](SwhGraphProperties::load_persons)
/// or [`load_all_properties`](crate::graph::SwhBidirectionalGraph::load_all_properties)
impl<
        MAPS: MaybeMaps,
        TIMESTAMPS: MaybeTimestamps,
        PERSONS: OptPersons,
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
    pub fn author_id(&self, node_id: NodeId) -> PropertiesResult<'_, Option<u32>, PERSONS> {
        PERSONS::map_if_available(self.try_author_id(node_id), |author_id| {
            author_id.unwrap_or_else(|e| panic!("Cannot get node author: {e}"))
        })
    }

    /// Returns the id of the author of a revision or release, if any
    ///
    /// Returns `Err` if the node id does not exist, and `Ok(None)` if the node
    /// has no author
    #[inline]
    pub fn try_author_id(
        &self,
        node_id: NodeId,
    ) -> PropertiesResult<'_, Result<Option<u32>, OutOfBoundError>, PERSONS> {
        PERSONS::map_if_available(self.persons.author_id(node_id), |author_id| {
            match author_id {
                None => Err(OutOfBoundError {
                    // Invalid node id
                    index: node_id,
                    len: self.num_nodes,
                }),
                Some(u32::MAX) => Ok(None), // No author
                Some(id) => Ok(Some(id)),
            }
        })
    }

    /// Returns the id of the committer of a revision, if any
    ///
    /// # Panics
    ///
    /// If the node id does not exist
    #[inline]
    pub fn committer_id(&self, node_id: NodeId) -> PropertiesResult<'_, Option<u32>, PERSONS> {
        PERSONS::map_if_available(self.try_committer_id(node_id), |committer_id| {
            committer_id.unwrap_or_else(|e| panic!("Cannot get node committer: {e}"))
        })
    }

    /// Returns the id of the committer of a revision, if any
    ///
    /// Returns `None` if the node id does not exist, and `Ok(None)` if the node
    /// has no author
    #[inline]
    pub fn try_committer_id(
        &self,
        node_id: NodeId,
    ) -> PropertiesResult<'_, Result<Option<u32>, OutOfBoundError>, PERSONS> {
        PERSONS::map_if_available(self.persons.committer_id(node_id), |committer_id| {
            match committer_id {
                None => Err(OutOfBoundError {
                    // Invalid node id
                    index: node_id,
                    len: self.num_nodes,
                }),
                Some(u32::MAX) => Ok(None), // No committer
                Some(id) => Ok(Some(id)),
            }
        })
    }
}
