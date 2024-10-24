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

use crate::mph::SwhidMphf;
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
    path: PathBuf,
    num_nodes: usize,
    maps: MAPS,
    timestamps: TIMESTAMPS,
    persons: PERSONS,
    contents: CONTENTS,
    strings: STRINGS,
    label_names: LABELNAMES,
}

pub type AllSwhGraphProperties<MPHF> = SwhGraphProperties<
    MappedMaps<MPHF>,
    MappedTimestamps,
    MappedPersons,
    MappedContents,
    MappedStrings,
    MappedLabelNames,
>;

fn mmap(path: &Path) -> Result<Mmap> {
    let file_len = path
        .metadata()
        .with_context(|| format!("Could not stat {}", path.display()))?
        .len();
    let file =
        std::fs::File::open(path).with_context(|| format!("Could not open {}", path.display()))?;
    let data = unsafe {
        mmap_rs::MmapOptions::new(file_len as _)
            .with_context(|| format!("Could not initialize mmap of size {}", file_len))?
            .with_flags(
                mmap_rs::MmapFlags::TRANSPARENT_HUGE_PAGES | mmap_rs::MmapFlags::RANDOM_ACCESS,
            )
            .with_file(&file, 0)
            .map()
            .with_context(|| format!("Could not mmap {}", path.display()))?
    };
    Ok(data)
}

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
    pub fn load_all<MPHF: SwhidMphf>(self) -> Result<AllSwhGraphProperties<MPHF>> {
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
pub use timestamps::{MappedTimestamps, MaybeTimestamps, NoTimestamps, Timestamps, VecTimestamps};

mod persons;
pub use persons::{MappedPersons, MaybePersons, NoPersons, Persons, VecPersons};

mod contents;
pub use contents::{Contents, MappedContents, MaybeContents, NoContents, VecContents};

mod strings;
pub use strings::{MappedStrings, MaybeStrings, NoStrings, Strings, VecStrings};

mod label_names;
pub use label_names::{
    LabelIdFromNameError, LabelNames, MappedLabelNames, MaybeLabelNames, NoLabelNames,
    VecLabelNames,
};
