// Copyright (C) 2023  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

//! Wrappers for [`SwhGraph`](crate::graph::SwhGraph) that filter or change the nodes and arcs it returns.

mod spy;
pub use spy::GraphSpy;
mod subgraph;
pub use subgraph::Subgraph;
mod transposed;
pub use transposed::Transposed;
