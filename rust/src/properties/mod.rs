// Copyright (C) 2023-2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

//! Node labels
//!
//! [`SwhGraphProperties`] is populated by the `load_properties` and `load_all_properties`
//! of [`SwhUnidirectionalGraph`](crate::graph::SwhUnidirectionalGraph) and
//! [`SwhBidirectionalGraph`](crate::graph::SwhBidirectionalGraph) and returned by
//! their `properties` method.
//!
//! ```no_run
//! # use std::path::PathBuf;
//! use swh_graph::graph::SwhGraphWithProperties;
//! use swh_graph::mph::DynMphf;
//! use swh_graph::SwhGraphProperties;
//!
//! let properties: &SwhGraphProperties<_, _, _, _, _, _> =
//!     swh_graph::graph::SwhUnidirectionalGraph::new(PathBuf::from("./graph"))
//!     .expect("Could not load graph")
//!     .load_all_properties::<DynMphf>()
//!     .expect("Could not load properties")
//!     .properties();
//! ```

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use byteorder::BigEndian;
use mmap_rs::Mmap;

use crate::mph::LoadableSwhidMphf;
use crate::utils::mmap::NumberMmap;
use crate::utils::GetIndex;
use crate::OutOfBoundError;

pub(crate) mod suffixes {
    pub const NODE2SWHID: &str = ".node2swhid.bin";
    pub const NODE2TYPE: &str = ".node2type.bin";
    pub const AUTHOR_TIMESTAMP: &str = ".property.author_timestamp.bin";
    pub const AUTHOR_TIMESTAMP_OFFSET: &str = ".property.author_timestamp_offset.bin";
    pub const COMMITTER_TIMESTAMP: &str = ".property.committer_timestamp.bin";
    pub const COMMITTER_TIMESTAMP_OFFSET: &str = ".property.committer_timestamp_offset.bin";
    pub const AUTHOR_ID: &str = ".property.author_id.bin";
    pub const COMMITTER_ID: &str = ".property.committer_id.bin";
    pub const CONTENT_IS_SKIPPED: &str = ".property.content.is_skipped.bits";
    pub const CONTENT_LENGTH: &str = ".property.content.length.bin";
    pub const MESSAGE: &str = ".property.message.bin";
    pub const MESSAGE_OFFSET: &str = ".property.message.offset.bin";
    pub const TAG_NAME: &str = ".property.tag_name.bin";
    pub const TAG_NAME_OFFSET: &str = ".property.tag_name.offset.bin";
    pub const LABEL_NAME: &str = ".labels.fcl";
}

#[derive(thiserror::Error, Debug)]
#[error("{path} cannot be loaded: {source}")]
pub struct UnavailableProperty {
    path: PathBuf,
    #[source]
    source: std::io::Error,
}

/// Wrapper for the return type of [`SwhGraphProperties`] methods.
///
/// When `B` implements `GuaranteedDataFiles` (the most common case), `PropertiesResult<'err, T, B>`
/// is exactly the same type as `T`.
///
/// aWhen `B` implements `OptionalDataFiles` (which is the case when using
/// `opt_load_*` instead of `load_*` or [`load_all`](SwhGraphProperties::load_all) for example),
/// then `PropertiesResult<'err, T, B>` is exactly the same type as `Result<T, &'err UnavailableProperty>`.
pub type PropertiesResult<'err, T, B> =
    <<B as PropertiesBackend>::DataFilesAvailability as DataFilesAvailability>::Result<'err, T>;

/// Common trait for type parameters of [`SwhGraphProperties`]
pub trait PropertiesBackend {
    type DataFilesAvailability: DataFilesAvailability;

    /// Applies the given function `f` to the value `v` if the value is available
    ///
    /// This is an alias for `<Self::DataFilesAvailability as DataFilesAvailability>::map(v, f)`,
    /// meaning that:
    ///
    /// 1. if `Self::DataFilesAvailability` is `GuaranteedDataFiles`, then `map_if_available(v, f)`
    ///    is equivalent to `f(v)` and has type `U`
    /// 2. if `Self::DataFilesAvailability` is `OptionalDataFiles`, then `map_if_available(v, f)`
    ///    is equivalent to `v.map(f)` and has type `Result<U, &'err UnavailableProperty>`
    fn map_if_available<T, U>(
        v: <Self::DataFilesAvailability as DataFilesAvailability>::Result<'_, T>,
        f: impl FnOnce(T) -> U,
    ) -> <Self::DataFilesAvailability as DataFilesAvailability>::Result<'_, U> {
        <Self::DataFilesAvailability as DataFilesAvailability>::map(v, f)
    }

    /// Returns `(v1, v2)` if both are available, or an error otherwise
    ///
    /// This is an alias for `<Self::DataFilesAvailability as DataFilesAvailability>::zip(v, f)`,
    /// meaning that:
    ///
    /// 1. if `Self::DataFilesAvailability` is `GuaranteedDataFiles`, then `zip_if_available(v1, v2)`
    ///    is equivalent to `(v1, v2)` and has type `(T1, T2)`
    /// 2. if `Self::DataFilesAvailability` is `OptionalDataFiles`, then `zip_if_available(v1, v2)`
    ///    is equivalent to `v1.and_then(|v1| v2.map(|v2| (v1, v2)))` and has type
    ///    `Result<(T1, T2), &'err UnavailableProperty>`
    fn zip_if_available<'err, T1, T2>(
        v1: <Self::DataFilesAvailability as DataFilesAvailability>::Result<'err, T1>,
        v2: <Self::DataFilesAvailability as DataFilesAvailability>::Result<'err, T2>,
    ) -> <Self::DataFilesAvailability as DataFilesAvailability>::Result<'err, (T1, T2)> {
        <Self::DataFilesAvailability as DataFilesAvailability>::zip(v1, v2)
    }
}

/// Helper trait to work with [`PropertiesResult`]
///
/// It is implemented by:
/// * [`GuaranteedDataFiles`]: the common case, where data files are guaranteed to exist
///   once a graph is loaded, in which case `Self::Result<'err, T>` is the same type as `T`
/// * [`OptionalDataFiles`]: when they are not, in which case `Self::Result<T>`
///   is the same type as `Result<T, &'err UnavailableProperty>`.
pub trait DataFilesAvailability {
    type Result<'err, T>;

    fn map<T, U>(v: Self::Result<'_, T>, f: impl FnOnce(T) -> U) -> Self::Result<'_, U>;
    fn zip<'err, T1, T2>(
        v1: Self::Result<'err, T1>,
        v2: Self::Result<'err, T2>,
    ) -> Self::Result<'err, (T1, T2)>;
    fn make_result<T>(value: Self::Result<'_, T>) -> Result<T, &UnavailableProperty>;
}

/// Helper type that implements [`DataFilesAvailability`] to signal underlying data files
/// may be missing at runtime
pub struct OptionalDataFiles {
    _marker: (), // Prevents users from instantiating
}

/// Helper type that implements [`DataFilesAvailability`] to signal underlying data files
/// are guaranteed to be available once the graph is loaded
pub struct GuaranteedDataFiles {
    _marker: (), // Prevents users from instantiating
}

impl DataFilesAvailability for OptionalDataFiles {
    type Result<'err, T> = Result<T, &'err UnavailableProperty>;

    #[inline(always)]
    fn map<T, U>(v: Self::Result<'_, T>, f: impl FnOnce(T) -> U) -> Self::Result<'_, U> {
        v.map(f)
    }

    #[inline(always)]
    fn zip<'err, T1, T2>(
        v1: Self::Result<'err, T1>,
        v2: Self::Result<'err, T2>,
    ) -> Self::Result<'err, (T1, T2)> {
        v1.and_then(|v1| v2.map(|v2| (v1, v2)))
    }

    #[inline(always)]
    fn make_result<T>(value: Self::Result<'_, T>) -> Result<T, &UnavailableProperty> {
        value
    }
}

impl DataFilesAvailability for GuaranteedDataFiles {
    type Result<'err, T> = T;

    #[inline(always)]
    fn map<T, U>(v: Self::Result<'_, T>, f: impl FnOnce(T) -> U) -> Self::Result<'_, U> {
        f(v)
    }

    #[inline(always)]
    fn zip<'err, T1, T2>(
        v1: Self::Result<'err, T1>,
        v2: Self::Result<'err, T2>,
    ) -> Self::Result<'err, (T1, T2)> {
        (v1, v2)
    }

    #[inline(always)]
    fn make_result<T>(value: Self::Result<'_, T>) -> Result<T, &UnavailableProperty> {
        Ok(value)
    }
}

/// Properties on graph nodes
///
/// This structures has many type parameters, to allow loading only some properties,
/// and checking at compile time that only loaded properties are accessed.
///
/// Extra properties can be loaded, following the builder pattern on the owning graph.
/// For example, this does not compile:
///
/// ```compile_fail
/// # use std::path::PathBuf;
/// use swh_graph::graph::SwhGraphWithProperties;
/// use swh_graph::mph::DynMphf;
/// use swh_graph::SwhGraphProperties;
///
/// swh_graph::graph::SwhUnidirectionalGraph::new(PathBuf::from("./graph"))
///     .expect("Could not load graph")
///     .init_properties()
///     .properties()
///     .author_timestamp(42);
/// ```
///
/// but this does:
///
/// ```no_run
/// # use std::path::PathBuf;
/// use swh_graph::graph::SwhGraphWithProperties;
/// use swh_graph::mph::DynMphf;
/// use swh_graph::SwhGraphProperties;
///
/// swh_graph::graph::SwhUnidirectionalGraph::new(PathBuf::from("./graph"))
///     .expect("Could not load graph")
///     .init_properties()
///     .load_properties(SwhGraphProperties::load_timestamps)
///     .expect("Could not load timestamp properties")
///     .properties()
///     .author_timestamp(42);
/// ```
pub struct SwhGraphProperties<
    MAPS: MaybeMaps,
    TIMESTAMPS: MaybeTimestamps,
    PERSONS: MaybePersons,
    CONTENTS: MaybeContents,
    STRINGS: MaybeStrings,
    LABELNAMES: MaybeLabelNames,
> {
    pub(crate) path: PathBuf,
    pub(crate) num_nodes: usize,
    pub(crate) maps: MAPS,
    pub(crate) timestamps: TIMESTAMPS,
    pub(crate) persons: PERSONS,
    pub(crate) contents: CONTENTS,
    pub(crate) strings: STRINGS,
    pub(crate) label_names: LABELNAMES,
    /// Hack: `Some(false)` if the graph was compressed with Rust (2023-09-06 and newer),
    /// `Some(true)` if the graph was compressed with Java (2022-12-07 and older),
    /// `None` if we don't know yet (as we compute this lazily)
    pub(crate) label_names_are_in_base64_order: once_cell::race::OnceBool,
}

pub type AllSwhGraphProperties<MPHF> = SwhGraphProperties<
    MappedMaps<MPHF>,
    MappedTimestamps,
    MappedPersons,
    MappedContents,
    MappedStrings,
    MappedLabelNames,
>;

pub type AllSwhGraphDynProperties<MPHF> = SwhGraphProperties<
    MappedMaps<MPHF>,
    OptMappedTimestamps,
    OptMappedPersons,
    OptMappedContents,
    OptMappedStrings,
    MappedLabelNames,
>;

impl SwhGraphProperties<NoMaps, NoTimestamps, NoPersons, NoContents, NoStrings, NoLabelNames> {
    /// Creates an empty [`SwhGraphProperties`] instance, which will load properties
    /// from the given path prefix.
    pub fn new(path: impl AsRef<Path>, num_nodes: usize) -> Self {
        SwhGraphProperties {
            path: path.as_ref().to_owned(),
            num_nodes,
            maps: NoMaps,
            timestamps: NoTimestamps,
            persons: NoPersons,
            contents: NoContents,
            strings: NoStrings,
            label_names: NoLabelNames,
            label_names_are_in_base64_order: Default::default(),
        }
    }

    /// Consumes an empty [`SwhGraphProperties`] instance and returns a new one
    /// with all properties loaded and all methods available.
    ///
    /// ```no_run
    /// # use std::path::PathBuf;
    ///  use swh_graph::graph::SwhGraphWithProperties;
    /// use swh_graph::mph::DynMphf;
    /// use swh_graph::SwhGraphProperties;
    ///
    /// SwhGraphProperties::new(PathBuf::from("./graph"), 123)
    ///     .load_all::<DynMphf>()
    ///     .expect("Could not load properties");
    /// ```
    ///
    /// is equivalent to:
    ///
    /// ```no_run
    /// # use std::path::PathBuf;
    /// use swh_graph::mph::DynMphf;
    /// use swh_graph::SwhGraphProperties;
    ///
    /// SwhGraphProperties::new(PathBuf::from("./graph"), 123)
    ///     .load_maps::<DynMphf>()
    ///     .expect("Could not load node2swhid/swhid2node")
    ///     .load_timestamps()
    ///     .expect("Could not load timestamp properties")
    ///     .load_persons()
    ///     .expect("Could not load person properties")
    ///     .load_contents()
    ///     .expect("Could not load content properties")
    ///     .load_strings()
    ///     .expect("Could not load string properties");
    /// ```
    pub fn load_all<MPHF: LoadableSwhidMphf>(self) -> Result<AllSwhGraphProperties<MPHF>> {
        self.load_maps()?
            .load_timestamps()?
            .load_persons()?
            .load_contents()?
            .load_strings()?
            .load_label_names()
    }
}

mod maps;
pub use maps::{MappedMaps, Maps, MaybeMaps, NoMaps, NodeIdFromSwhidError, VecMaps};

mod timestamps;
pub use timestamps::{
    MappedTimestamps, MaybeTimestamps, NoTimestamps, OptMappedTimestamps, OptTimestamps,
    Timestamps, VecTimestamps,
};

mod persons;
pub use persons::{
    MappedPersons, MaybePersons, NoPersons, OptMappedPersons, OptPersons, Persons, VecPersons,
};

mod contents;
pub use contents::{
    Contents, MappedContents, MaybeContents, NoContents, OptContents, OptMappedContents,
    VecContents,
};

mod strings;
pub use strings::{
    MappedStrings, MaybeStrings, NoStrings, OptMappedStrings, OptStrings, Strings, VecStrings,
};

mod label_names;
pub use label_names::{
    LabelIdFromNameError, LabelNames, MappedLabelNames, MaybeLabelNames, NoLabelNames,
    VecLabelNames,
};

mod utils;
use utils::*;
