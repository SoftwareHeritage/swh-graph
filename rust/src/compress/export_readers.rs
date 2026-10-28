// Copyright (C) 2023-2026  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::path::Path;

use anyhow::Result;
use ar_row::deserialize::{ArRowDeserialize, ArRowStruct};
use rayon::prelude::*;

/// Provides iteration on rows in a ORC/Parquet/... file
pub trait ExportTableReader: Send + Sized {
    /// Opens a directory and returns one instance for each file
    fn new<P: AsRef<Path>>(dataset_dir: P, subdirectory: &str) -> Result<Vec<Self>>;

    fn iter<T, IntoIterU, U, F>(self, f: F) -> impl Iterator<Item = U>
    where
        F: FnMut(T) -> IntoIterU,
        IntoIterU: IntoIterator<Item = U>,
        T: ArRowDeserialize + ArRowStruct;

    fn par_iter<T, IntoIterU, U: Send, F>(self, f: F) -> impl ParallelIterator<Item = U>
    where
        F: Fn(T) -> IntoIterU + Send + Sync,
        IntoIterU: IntoIterator<Item = U> + Send + Sync,
        T: ArRowDeserialize + ArRowStruct + Send;

    fn count_rows(self) -> u64;
}
