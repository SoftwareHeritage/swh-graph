// Copyright (C) 2023  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

#![doc = include_str!("../README.md")]

mod swhid;
#[cfg(feature = "macros")]
pub use swhid::__parse_swhid;
pub use swhid::SWHID;

mod swhtype;
pub use swhtype::SWHType;

pub mod collections;
pub mod graph;
pub mod graph_builder;
pub mod labels;
pub mod map;
pub mod mph;
pub mod properties;
pub use properties::{AllSwhGraphProperties, SwhGraphProperties};

#[cfg(feature = "compression")]
pub mod compress;
#[cfg(feature = "grpc-server")]
pub mod server;
pub mod views;

pub mod approximate_bfs;
pub mod java_compat;

pub mod utils;

pub use webgraph;
