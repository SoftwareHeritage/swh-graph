// Copyright (C) 2023  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

//! Structures to load file created with legacy swh-graph implementation
//! written in Java.
//!
//! `mph`, and `sf` are imported [from sux-rs](https://archive.softwareheritage.org/swh:1:dir:5aef0244039204e3cbe1424f645c9eadcc80956f;origin=https://github.com/vigna/sux-rs;visit=swh:1:snp:855180f9102fd3d7451e98f293cdd90cff7f17d9;anchor=swh:1:rev:9cafac06c95c2d916b76dc374a6f9d976bf65456)
pub mod mph;
pub mod sf;

/// Deprecated alias of [`crate::front_coded_list`]
#[deprecated(
    since = "5.2.0",
    note = "please use `swh_graph::front_coded_list` instead"
)]
pub mod fcl {
    /// Deprecated alias of [`crate::front_coded_list::FrontCodedList`]
    #[deprecated(
        since = "5.2.0",
        note = "please use `swh_graph::front_coded_list::FrontCodedList` instead"
    )]
    pub use crate::front_coded_list::FrontCodedList;
}
