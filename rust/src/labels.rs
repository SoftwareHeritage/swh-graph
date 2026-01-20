// Copyright (C) 2023-2026  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

//! Labels on graph arcs

use thiserror::Error;

use crate::NodeType;

/// Intermediary type that needs to be casted into one of the [`EdgeLabel`] variants
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct UntypedEdgeLabel(pub(crate) u64);

impl From<u64> for UntypedEdgeLabel {
    fn from(n: u64) -> UntypedEdgeLabel {
        UntypedEdgeLabel(n)
    }
}

#[derive(Error, Debug, Clone, PartialEq, Eq, Hash)]
pub enum EdgeTypingError {
    #[error("{src} -> {dst} arcs cannot have labels")]
    NodeTypes { src: NodeType, dst: NodeType },
}

impl UntypedEdgeLabel {
    pub fn for_edge_type(
        &self,
        src: NodeType,
        dst: NodeType,
        transpose_graph: bool,
    ) -> Result<EdgeLabel, EdgeTypingError> {
        use crate::NodeType::*;

        let (src, dst) = if transpose_graph {
            (dst, src)
        } else {
            (src, dst)
        };

        match (src, dst) {
            (Snapshot, _) => Ok(EdgeLabel::Branch(self.0.into())),
            (Directory, _) => Ok(EdgeLabel::DirEntry(self.0.into())),
            (Origin, Snapshot) => Ok(EdgeLabel::Visit(self.0.into())),
            _ => Err(EdgeTypingError::NodeTypes { src, dst }),
        }
    }
}

impl From<EdgeLabel> for UntypedEdgeLabel {
    fn from(label: EdgeLabel) -> Self {
        UntypedEdgeLabel(match label {
            EdgeLabel::Branch(branch) => branch.0,
            EdgeLabel::DirEntry(dir_entry) => dir_entry.0,
            EdgeLabel::Visit(visit) => visit.0,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum EdgeLabel {
    /// `snp -> *` branches (or `* -> snp` on the transposed graph)
    Branch(Branch),
    /// `dir -> *` branches (or `* -> dir` on the transposed graph)
    DirEntry(DirEntry),
    /// `ori -> snp` branches (or `snp -> ori` on the transposed graph)
    Visit(Visit),
}

macro_rules! impl_edgelabel_convert {
    ( $variant:ident ( $inner:ty ) ) => {
        impl From<$inner> for EdgeLabel {
            fn from(v: $inner) -> EdgeLabel {
                EdgeLabel::$variant(v)
            }
        }

        impl TryFrom<EdgeLabel> for $inner {
            type Error = ();

            fn try_from(label: EdgeLabel) -> Result<$inner, Self::Error> {
                match label {
                    EdgeLabel::$variant(v) => Ok(v),
                    _ => Err(()),
                }
            }
        }

        impl From<UntypedEdgeLabel> for $inner {
            fn from(label: UntypedEdgeLabel) -> $inner {
                <$inner>::from(label.0)
            }
        }
    };
}

impl_edgelabel_convert!(Branch(Branch));
impl_edgelabel_convert!(DirEntry(DirEntry));
impl_edgelabel_convert!(Visit(Visit));

/// Lossy representation of visit types in the
/// [SWH data model](https://docs.softwareheritage.org/devel/swh-model/data-model.html)
///
/// While the SWH data model precisely identifies the type of loader used to load content of an
/// origin, swh-graph has to group them in order to fit in the compressed graph representation.
///
/// To get the exact visit type, you need to cross-reference with the [columnar
/// export](https://docs.softwareheritage.org/devel/swh-export/graph/dataset.html)
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum VisitType {
    /// tar, zip, ...
    Archive,
    /// single content, patch, ...
    Misc,
    /// npm, pypi, nixguix, ...
    PackageManager,
    /// partial archiving of a repository (git-checkout, hg-checkout, ...)
    Partial,
    /// deposit, possibly coar-notify in the future
    Push,
    /// git, hg, cvs, ...
    Vcs,
    /// Either a type not categorized above, or graph is too old to support visit types)
    Unknown,
}

impl VisitType {
    #[rustfmt::skip]
    pub fn from_swh_type(swh_type: &str) -> Option<Self> {
        Some(match swh_type {
            "ftp" |
            "tar" |
            "tarball-directory"
            => VisitType::Archive,

            "content"
            => VisitType::Misc,

            "cran" |
            "deb" |
            "golang" |
            "opam" |
            "nixguix" |
            "npm" |
            "maven" |
            "pypi" |
            "pubdev"
            => VisitType::PackageManager,

            "hg-checkout" |
            "git-checkout" |
            "svn-export"
            => VisitType::Partial,

            "deposit"
            => VisitType::Push,


            "bzr" |
            "cvs" |
            "git" |
            "hg" |
            "svn"
            => VisitType::Vcs,

            _ => return None,
        })
    }

    pub fn to_bits(self) -> u8 {
        use VisitType::*;

        match self {
            Unknown => 0b000,
            Archive => 0b001,
            Misc => 0b010,
            PackageManager => 0b011,
            Partial => 0b100,
            Push => 0b101,
            Vcs => 0b110,
        }
    }

    /// Returns `None` if any
    pub fn from_bits(bits: u8) -> Self {
        use VisitType::*;

        match bits {
            0b000 => Unknown, // used by graphs 2024-08-23 to 2025-10-08
            0b001 => Archive,
            0b010 => Misc,
            0b011 => PackageManager,
            0b100 => Partial,
            0b101 => Push,
            0b110 => Vcs,
            0b111 => Unknown, // used by graph 2024-05-16
            0b1000..=u8::MAX => {
                panic!("VisitType::from_bits got value greater than 0b111");
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum VisitStatus {
    Full,
    Partial,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Visit(pub(crate) u64);

impl From<u64> for Visit {
    fn from(n: u64) -> Visit {
        Visit(n)
    }
}

impl Visit {
    /// Returns a new [`Visit`]
    ///
    /// or `None` if `timestamp` is 2^59 or greater
    pub fn new(status: VisitStatus, timestamp: u64, visit_type: VisitType) -> Option<Visit> {
        let is_full = match status {
            VisitStatus::Full => 1u64,
            VisitStatus::Partial => 0,
        };
        let reserved_bits = 0b1000u64;
        let visit_type = u64::from(visit_type.to_bits());
        assert!(visit_type <= 0b111);
        timestamp.checked_shl(5).map(|shifted_timestamp| {
            Visit(shifted_timestamp | (is_full << 4) | reserved_bits | visit_type)
        })
    }

    pub fn timestamp(&self) -> u64 {
        self.0 >> 5
    }

    pub fn status(&self) -> VisitStatus {
        if self.0 & 0b10000 != 0 {
            VisitStatus::Full
        } else {
            VisitStatus::Partial
        }
    }

    pub fn visit_type(&self) -> VisitType {
        VisitType::from_bits((self.0 & 0b111) as u8)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Branch(pub(crate) u64);

impl From<u64> for Branch {
    fn from(n: u64) -> Branch {
        Branch(n)
    }
}

impl Branch {
    /// Returns a new [`Branch`]
    ///
    /// or `None` if `label_name_id` is 2^61 or greater
    pub fn new(label_name_id: LabelNameId) -> Option<Branch> {
        label_name_id.0.checked_shl(3).map(Branch)
    }

    #[deprecated(since = "7.0.0", note = "filename_id was renamed label_name_id")]
    /// Deprecated alias for [`label_name_id`](Self::label_name_id)
    pub fn filename_id(self) -> LabelNameId {
        self.label_name_id()
    }

    /// Returns an id of the label name of the entry.
    ///
    /// The id can be resolved to the label name through graph properties.
    pub fn label_name_id(self) -> LabelNameId {
        LabelNameId(self.0 >> 3)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct DirEntry(pub(crate) u64);

impl From<u64> for DirEntry {
    fn from(n: u64) -> DirEntry {
        DirEntry(n)
    }
}

impl DirEntry {
    /// Returns a new [`DirEntry`]
    ///
    /// or `None` if `label_name_id` is 2^61 or greater
    pub fn new(permission: Permission, label_name_id: LabelNameId) -> Option<DirEntry> {
        label_name_id
            .0
            .checked_shl(3)
            .map(|shifted_label_name_id| DirEntry(shifted_label_name_id | (permission as u64)))
    }

    #[deprecated(since = "7.0.0", note = "filename_id was renamed label_name_id")]
    /// Deprecated alias for [`label_name_id`](Self::label_name_id)
    pub fn filename_id(self) -> LabelNameId {
        self.label_name_id()
    }

    /// Returns an id of the filename of the entry.
    ///
    /// The id can be resolved to the label name through graph properties.
    pub fn label_name_id(self) -> LabelNameId {
        LabelNameId(self.0 >> 3)
    }

    /// Returns the file permission of the given directory entry
    ///
    /// Returns `None` when the labeled graph is corrupt or generated by a newer swh-graph
    /// version with more [`Permission`] variants
    pub fn permission(self) -> Option<Permission> {
        use Permission::*;
        match self.0 & 0b111 {
            0 => Some(None),
            1 => Some(Content),
            2 => Some(ExecutableContent),
            3 => Some(Symlink),
            4 => Some(Directory),
            5 => Some(Revision),
            _ => Option::None,
        }
    }

    /// Returns the file permission of the given directory entry
    ///
    /// # Safety
    ///
    /// May return an invalid [`Permission`] variant if the labeled graph is corrupt
    /// or generated by a newer swh-graph version with more variants
    pub unsafe fn permission_unchecked(self) -> Permission {
        use Permission::*;
        match self.0 & 0b111 {
            0 => None,
            1 => Content,
            2 => ExecutableContent,
            3 => Symlink,
            4 => Directory,
            5 => Revision,
            n => unreachable!("{} mode", n),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct LabelNameId(pub u64);

#[deprecated(since = "7.0.0", note = "FilenameId was renamed to LabelNameId")]
pub type FilenameId = LabelNameId;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum Permission {
    None = 0,
    Content = 1,
    ExecutableContent = 2,
    Symlink = 3,
    Directory = 4,
    Revision = 5,
}

impl Permission {
    /// Returns a UNIX-like mode matching the permission
    ///
    /// `0100644` for contents, `0100755` for executable contents, `0120000` for symbolic
    /// links, `0040000` for directories, and `0160000` for revisions (git submodules);
    /// or `0` if the [`DirEntry`] has no associated permission.
    pub fn to_git(self) -> u16 {
        use Permission::*;
        match self {
            None => 0,
            Content => 0o100644,
            ExecutableContent => 0o100755,
            Symlink => 0o120000,
            Directory => 0o040000,
            Revision => 0o160000,
        }
    }

    /// Returns a permission from a subset of UNIX-like modes.
    ///
    /// This is the inverse of [`Permission::to_git`].
    pub fn from_git(mode: u16) -> Option<Permission> {
        use Permission::*;
        match mode {
            0 => Some(None),
            0o100644 => Some(Content),
            0o100755 => Some(ExecutableContent),
            0o120000 => Some(Symlink),
            0o040000 => Some(Directory),
            0o160000 => Some(Revision),
            _ => Option::None,
        }
    }

    /// Returns a permission from a subset of UNIX-like modes.
    ///
    /// This is the inverse of [`Permission::to_git`].
    ///
    /// # Safety
    ///
    /// Undefined behavior if the given mode is not one of the values returned by [`Permission::to_git`]
    pub unsafe fn from_git_unchecked(mode: u16) -> Permission {
        use Permission::*;
        match mode {
            0 => None,
            0o100644 => Content,
            0o100755 => ExecutableContent,
            0o120000 => Symlink,
            0o040000 => Directory,
            0o160000 => Revision,
            _ => unreachable!("{} mode", mode),
        }
    }
}
