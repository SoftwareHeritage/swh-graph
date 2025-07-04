// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

//! Helpful data structures to efficiently traverse the graph

mod node_set;
pub use node_set::*;

mod paths;
pub use paths::*;

mod small_node_set;
pub use small_node_set::*;
