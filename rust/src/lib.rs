// Copyright (C) 2023-2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

#![doc = include_str!("../README.md")]

use thiserror::Error;

pub mod _crash_course;
pub mod _tutorial;

mod swhid;
#[cfg(feature = "macros")]
pub use swhid::__parse_swhid;
pub use swhid::{StrSWHIDDeserializationError, SWHID};

mod swhtype;
pub use swhtype::{ArcType, NodeConstraint, NodeType};

pub mod algos;
pub mod arc_iterators;
pub mod front_coded_list;
pub mod graph;
pub mod graph_builder;
mod r#impl;
mod labeling;
pub mod labels;
pub mod map;
pub mod mph;
pub mod properties;
pub use properties::{AllSwhGraphProperties, SwhGraphProperties};
#[cfg(feature = "serde")]
pub mod serde;
pub mod stats;
mod underlying_graph;

#[cfg(feature = "compression")]
pub mod compress;
pub mod views;

pub mod approximate_bfs;
pub mod java_compat;

pub mod utils;

pub mod person;

pub use webgraph;

/// Returned by a `try_` method when the given index is past the number of nodes
/// (or number of label names for [`LabelNames`](properties::LabelNames) properties)
#[derive(Error, Debug, PartialEq, Eq, Hash, Clone)]
#[error("Accessed property index {index} out of {len}")]
pub struct OutOfBoundError {
    /// Indexed that was accessed
    pub index: usize,
    /// Length of the underlying collection (maximum index + 1)
    pub len: usize,
}

/// The current version of swh-graph.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
