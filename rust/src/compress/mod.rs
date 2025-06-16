// Copyright (C) 2023  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

#[cfg(feature = "orc")]
pub mod bv;

#[cfg(feature = "orc")]
mod iter_arcs;

#[cfg(feature = "orc")]
pub use iter_arcs::iter_arcs;

#[cfg(feature = "orc")]
mod iter_labeled_arcs;

#[cfg(feature = "orc")]
mod iter_labels;

#[cfg(feature = "orc")]
pub use iter_labels::iter_labels;

#[cfg(feature = "orc")]
mod iter_origins;
#[cfg(feature = "orc")]
pub use iter_origins::iter_origins;

#[cfg(feature = "orc")]
mod iter_persons;
#[cfg(feature = "orc")]
pub use iter_persons::iter_persons;

#[cfg(feature = "orc")]
mod iter_fullnames;
#[cfg(feature = "orc")]
pub use iter_fullnames::iter_fullnames;

#[cfg(feature = "orc")]
mod iter_swhids;
#[cfg(feature = "orc")]
pub use iter_swhids::iter_swhids;

pub mod label_names;

pub mod maps;

pub mod mph;

#[cfg(feature = "orc")]
pub mod orc;

#[cfg(feature = "orc")]
pub mod properties;

pub mod persons;

#[cfg(feature = "orc")]
pub mod stats;

pub mod transform;

pub mod zst_dir;

const SWHID_TXT_SIZE: usize = 50;
type TextSwhid = [u8; SWHID_TXT_SIZE];
