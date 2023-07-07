// Copyright (C) 2023  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use anyhow::{bail, Result};

#[repr(u8)]
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
/// Object type of an SWHID
///
/// # Reference
/// - <https://docs.softwareheritage.org/devel/swh-model/data-model.html>
pub enum SWHType {
    Content = 0,
    /// a list of named directory entries, each of which pointing to other
    /// artifacts, usually file contents or sub-directories. Directory entries
    /// are also associated to some metadata stored as permission bits.
    Directory = 1,
    /// code “hosting places” as previously described are usually large
    /// platforms that host several unrelated software projects. For software
    /// provenance purposes it is important to be more specific than that.
    ///
    /// Software origins are fine grained references to where source code
    /// artifacts archived by Software Heritage have been retrieved from. They
    /// take the form of `(type, url)` pairs, where url is a canonical URL
    /// (e.g., the address at which one can `git clone` a repository or download
    /// a source tarball) and `type` the kind of software origin (e.g., git,
    /// svn, or dsc for Debian source packages).
    Origin = 2,
    ///AKA “tags”
    ///
    /// some revisions are more equals than others and get selected by
    /// developers as denoting important project milestones known as “releases”.
    /// Each release points to the last commit in project history corresponding
    /// to the release and carries metadata: release name and version, release
    /// message, cryptographic signatures, etc.
    Release = 3,
    /// AKA commits
    ///
    /// Software development within a specific project is
    /// essentially a time-indexed series of copies of a single “root” directory
    /// that contains the entire project source code. Software evolves when a d
    /// eveloper modifies the content of one or more files in that directory
    /// and record their changes.
    ///
    /// Each recorded copy of the root directory is known as a “revision”. It
    /// points to a fully-determined directory and is equipped with arbitrary
    /// metadata. Some of those are added manually by the developer
    /// (e.g., commit message), others are automatically synthesized
    /// (timestamps, preceding commit(s), etc).
    Revision = 4,
    /// any kind of software origin offers multiple pointers to the “current”
    /// state of a development project. In the case of VCS this is reflected by
    /// branches (e.g., master, development, but also so called feature branches
    /// dedicated to extending the software in a specific direction); in the
    /// case of package distributions by notions such as suites that correspond
    /// to different maturity levels of individual packages (e.g., stable,
    /// development, etc.).
    ///
    /// A “snapshot” of a given software origin records all entry points found
    /// there and where each of them was pointing at the time. For example, a
    /// snapshot object might track the commit where the master branch was
    /// pointing to at any given time, as well as the most recent release of a
    /// given package in the stable suite of a FOSS distribution.
    Snapshot = 5,
}

impl TryFrom<u8> for SWHType {
    type Error = anyhow::Error;
    fn try_from(value: u8) -> Result<Self> {
        Ok(match value {
            0 => Self::Content,
            1 => Self::Directory,
            2 => Self::Origin,
            3 => Self::Release,
            4 => Self::Revision,
            5 => Self::Snapshot,
            _ => bail!("Invalid SWHType {}.", value),
        })
    }
}

impl SWHType {
    /// Get the number of possible types.
    ///
    /// To avoid having to update this when adding a new type
    /// we can use the unstable function `std::mem::variant_count`
    /// or the `variant_count` crate.
    /// But for now we just hardcode it while we decide how to
    /// deal with this.
    pub const NUMBER_OF_TYPES: usize = 6;

    /// The number of bits needed to store the node type as integers
    /// This is `ceil(log2(NUMBER_OF_TYPES))`  which can be arithmetized into
    /// `floor(log2(NUMBER_OF_TYPES))` plus one if it's not a power of two.
    pub const BITWIDTH: usize = Self::NUMBER_OF_TYPES.ilog2() as usize
        + (!Self::NUMBER_OF_TYPES.is_power_of_two()) as usize;

    /// Convert a type to the str used in the SWHID
    pub fn to_str(&self) -> &'static str {
        match self {
            Self::Content => "cnt",
            Self::Directory => "dir",
            Self::Origin => "ori",
            Self::Release => "rel",
            Self::Revision => "rev",
            Self::Snapshot => "snp",
        }
    }
}

impl core::fmt::Display for SWHType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.to_str())
    }
}
