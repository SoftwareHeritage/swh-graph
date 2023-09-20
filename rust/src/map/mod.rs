// Copyright (C) 2023  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

//! This module contains the data structures used to map a node id to a SWHID
//! and vice versa and retrieve the labels for each node.

mod node2swhid;
pub use node2swhid::Node2SWHID;

mod node2type;
pub use node2type::Node2Type;

mod order;
pub use order::Order;
