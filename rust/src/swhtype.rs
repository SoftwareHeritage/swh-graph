// Copyright (C) 2023-2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::str::FromStr;

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
/// Object type of an SWHID
///
/// # Reference
/// - <https://docs.softwareheritage.org/devel/swh-model/data-model.html>
pub enum NodeType {
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

impl<'a> TryFrom<&'a [u8]> for NodeType {
    type Error = &'a [u8];
    fn try_from(value: &'a [u8]) -> Result<Self, Self::Error> {
        Ok(match value {
            b"cnt" => Self::Content,
            b"dir" => Self::Directory,
            b"ori" => Self::Origin,
            b"rel" => Self::Release,
            b"rev" => Self::Revision,
            b"snp" => Self::Snapshot,
            _ => return Err(value),
        })
    }
}

impl FromStr for NodeType {
    type Err = String;

    /// # Examples
    ///
    /// ```
    /// # use swh_graph::NodeType;
    ///
    /// assert_eq!("dir".parse::<NodeType>(), Ok(NodeType::Directory));
    /// assert!(matches!("xyz".parse::<NodeType>(), Err(_)));
    /// ```
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "cnt" => Self::Content,
            "dir" => Self::Directory,
            "ori" => Self::Origin,
            "rel" => Self::Release,
            "rev" => Self::Revision,
            "snp" => Self::Snapshot,
            _ => return Err(s.to_owned()),
        })
    }
}

impl TryFrom<u8> for NodeType {
    type Error = u8;
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        Ok(match value {
            0 => Self::Content,
            1 => Self::Directory,
            2 => Self::Origin,
            3 => Self::Release,
            4 => Self::Revision,
            5 => Self::Snapshot,
            _ => return Err(value),
        })
    }
}

impl NodeType {
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

    /// Convert a type to its enum discriminant value.
    ///
    /// In all cases using this method is both safer and more concise than
    /// `(node_type as isize).try_into().unwrap()`.
    pub fn to_u8(&self) -> u8 {
        match self {
            Self::Content => 0,
            Self::Directory => 1,
            Self::Origin => 2,
            Self::Release => 3,
            Self::Revision => 4,
            Self::Snapshot => 5,
        }
    }

    /// Returns a vector containing all possible `NodeType` values.
    // TODO make this return an HashSet instead, as the order does not matter
    pub fn all() -> Vec<Self> {
        vec![
            NodeType::Content,
            NodeType::Directory,
            NodeType::Origin,
            NodeType::Release,
            NodeType::Revision,
            NodeType::Snapshot,
        ]
    }
}

impl core::fmt::Display for NodeType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.to_str())
    }
}

/// Compact representation of a set of [NodeType]-s, as a bit array.
type NodeTypeSet = u64;

/// Constraint on allowed node types, as a set of node types.
#[derive(Debug, PartialEq, Clone, Copy)]
pub struct NodeConstraint(pub NodeTypeSet);

impl Default for NodeConstraint {
    fn default() -> Self {
        Self(0b111111)
    }
}

impl NodeConstraint {
    /// # Examples
    ///
    /// ```
    /// # use std::collections::HashSet;
    /// # use swh_graph::{NodeConstraint, NodeType};
    ///
    /// let only_dirs: NodeConstraint = "dir".parse().unwrap();
    /// let history_nodes: NodeConstraint = "rel,rev".parse().unwrap();
    /// let all_nodes: NodeConstraint = "*".parse().unwrap();
    ///
    /// assert!(only_dirs.matches(NodeType::Directory));
    /// assert!(!only_dirs.matches(NodeType::Content));
    /// assert!(history_nodes.matches(NodeType::Release));
    /// assert!(history_nodes.matches(NodeType::Revision));
    /// assert!(!history_nodes.matches(NodeType::Origin));
    /// for node_type in NodeType::all() {
    ///     assert!(all_nodes.matches(node_type));
    /// }
    /// ```
    pub fn matches(&self, node_type: NodeType) -> bool {
        self.0 & (1 << node_type.to_u8()) != 0
    }

    pub fn to_vec(&self) -> Vec<NodeType> {
        (0..NodeType::NUMBER_OF_TYPES as u8)
            .filter(|type_idx| self.0 & (1 << type_idx) != 0)
            .map(|type_idx| type_idx.try_into().unwrap())
            .collect()
    }
}

impl FromStr for NodeConstraint {
    type Err = String;

    /// # Examples
    ///
    /// ```
    /// # use std::collections::HashSet;
    /// # use swh_graph::{NodeConstraint, NodeType};
    ///
    /// assert_eq!("*".parse::<NodeConstraint>(), Ok(NodeConstraint(0b111111)));
    /// assert_eq!("rel".parse::<NodeConstraint>(), Ok(NodeConstraint(0b001000)));
    /// assert_eq!("dir,cnt".parse::<NodeConstraint>(), Ok(NodeConstraint(0b000011)));
    /// assert!(matches!("xyz".parse::<NodeConstraint>(), Err(_)));
    /// ```
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s == "*" {
            Ok(NodeConstraint::default())
        } else {
            let mut node_types = 0;
            for s in s.split(',') {
                node_types |= 1 << s.parse::<NodeType>()?.to_u8();
            }
            Ok(NodeConstraint(node_types))
        }
    }
}

impl core::fmt::Display for NodeConstraint {
    /// ```
    /// # use std::collections::HashSet;
    /// # use swh_graph::{NodeConstraint, NodeType};
    ///
    /// assert_eq!(format!("{}", NodeConstraint::default()), "*");
    /// assert_eq!(
    ///     format!("{}", NodeConstraint(0b000011)),
    ///     "cnt,dir"
    /// );
    /// assert_eq!(
    ///     format!("{}", NodeConstraint(0b111100)),
    ///     "ori,rel,rev,snp"
    /// );
    /// ```
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if *self == Self::default() {
            write!(f, "*")?;
        } else {
            let mut type_strings: Vec<&str> = self.to_vec().iter().map(|t| t.to_str()).collect();
            type_strings.sort();
            write!(f, "{}", type_strings.join(","))?;
        }
        Ok(())
    }
}

/// Type of an arc between two nodes in the Software Heritage graph, as a pair
/// of type constraints on the source and destination arc. When one of the two
/// is None, it means "any node type accepted".
// TODO remove Options from ArcType and create a (more  expressive, similar to
// NodeConstraint) type called ArcConstraint
pub struct ArcType {
    pub src: Option<NodeType>,
    pub dst: Option<NodeType>,
}
