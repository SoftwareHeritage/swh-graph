// Copyright (C) 2023-2026  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

pub mod bv;
mod iter_arcs;

pub use iter_arcs::iter_arcs;

mod iter_labeled_arcs;

mod iter_labels;

pub use iter_labels::iter_labels;

mod iter_origins;
pub use iter_origins::iter_origins;

mod iter_persons;
pub use iter_persons::iter_persons;

mod iter_fullnames;
pub use iter_fullnames::iter_fullnames;

mod iter_swhids;
pub use iter_swhids::iter_swhids;

pub mod label_names;

pub mod maps;

pub mod mph;

#[cfg(feature = "orc")]
pub mod orc;

#[cfg(feature = "parquet")]
pub mod parquet;

pub mod properties;

pub mod persons;

mod export_readers;
pub use export_readers::*;

pub mod stats;

pub mod transform;

pub mod zst_dir;

const SWHID_TXT_SIZE: usize = 50;
type TextSwhid = [u8; SWHID_TXT_SIZE];
