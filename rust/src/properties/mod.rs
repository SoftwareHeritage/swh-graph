// Copyright (C) 2023  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

//! Node labels

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use byteorder::{BigEndian, LittleEndian};
use mmap_rs::Mmap;

use crate::java_compat::bit_vector::LongArrayBitVector;
use crate::mph::SwhidMphf;
use crate::utils::mmap::NumberMmap;

pub(crate) mod suffixes {
    pub const NODE2SWHID: &str = ".node2swhid.bin";
    pub const NODE2TYPE: &str = ".node2type.bin";
    pub const AUTHOR_TIMESTAMP: &str = ".property.author_timestamp.bin";
    pub const AUTHOR_TIMESTAMP_OFFSET: &str = ".property.author_timestamp_offset.bin";
    pub const COMMITTER_TIMESTAMP: &str = ".property.committer_timestamp.bin";
    pub const COMMITTER_TIMESTAMP_OFFSET: &str = ".property.committer_timestamp_offset.bin";
    pub const AUTHOR_ID: &str = ".property.author_id.bin";
    pub const COMMITTER_ID: &str = ".property.committer_id.bin";
    pub const CONTENT_IS_SKIPPED: &str = ".property.content.is_skipped.bin";
    pub const CONTENT_LENGTH: &str = ".property.content.length.bin";
    pub const MESSAGE: &str = ".property.message.bin";
    pub const MESSAGE_OFFSET: &str = ".property.message.offset.bin";
    pub const TAG_NAME: &str = ".property.tag_name.bin";
    pub const TAG_NAME_OFFSET: &str = ".property.tag_name.offset.bin";
}

/// Properties on graph nodes
///
/// This structures has many type parameters, to allow loading only some properties,
/// and checking at compile time that only loaded properties are accessed.
///
/// Extra properties can be loaded, following the builder pattern.
pub struct SwhGraphProperties<
    MAPS: MapsOption,
    TIMESTAMPS: TimestampsOption,
    PERSONS: PersonsOption,
    CONTENTS: ContentsOption,
    STRINGS: StringsOption,
> {
    path: PathBuf,
    num_nodes: usize,
    maps: MAPS,
    timestamps: TIMESTAMPS,
    persons: PERSONS,
    contents: CONTENTS,
    strings: STRINGS,
}

pub type AllSwhGraphProperties<MPHF> =
    SwhGraphProperties<Maps<MPHF>, Timestamps, Persons, Contents, Strings>;

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
            .with_flags(mmap_rs::MmapFlags::TRANSPARENT_HUGE_PAGES)
            .with_file(file, 0)
            .map()
            .with_context(|| format!("Could not mmap {}", path.display()))?
    };
    #[cfg(target_os = "linux")]
    unsafe {
        libc::madvise(data.as_ptr() as *mut _, data.len(), libc::MADV_RANDOM)
    };
    Ok(data)
}

impl SwhGraphProperties<(), (), (), (), ()> {
    pub fn new(path: impl AsRef<Path>, num_nodes: usize) -> Self {
        SwhGraphProperties {
            path: path.as_ref().to_owned(),
            num_nodes,
            maps: (),
            timestamps: (),
            persons: (),
            contents: (),
            strings: (),
        }
    }
    pub fn load_all<MPHF: SwhidMphf>(self) -> Result<AllSwhGraphProperties<MPHF>> {
        Ok(self
            .load_maps()?
            .load_timestamps()?
            .load_persons()?
            .load_contents()?
            .load_strings()?)
    }
}

mod maps;
pub use maps::{Maps, MapsOption};

mod timestamps;
pub use timestamps::{Timestamps, TimestampsOption};

mod persons;
pub use persons::{Persons, PersonsOption};

mod contents;
pub use contents::{Contents, ContentsOption};

mod strings;
pub use strings::{Strings, StringsOption};
