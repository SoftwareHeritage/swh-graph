//! This module contains the data structures used to map a node id to a SWHID
//! and vice versa and retreive the labels for each node.

mod node2swhid;
pub use node2swhid::Node2SWHID;

mod order;
pub use order::Order;
