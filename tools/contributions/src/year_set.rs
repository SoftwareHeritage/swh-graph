// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use chrono::prelude::*;
use lazy_static::lazy_static;

lazy_static! {
    static ref MAX_YEAR: i32 = Utc::now().year();
}

/// Compact representation of a set of years
///
/// Only the current year and the last 63 are supported, others are ignored
///
/// ```
/// # use std::collections::HashSet;
/// # use swh_graph_contributions::YearSet;
///
/// let mut set = YearSet::default();
/// set.insert_timestamp_year(1713967146); // 2024-04-24
/// set.insert_timestamp_year(1666019057); // 2022-10-17
/// set.insert_timestamp_year(-1713967146); // 1915-09-09
///
/// assert_eq!(set.iter().collect::<HashSet<_>>(), vec![2022, 2024].into_iter().collect());
/// ```
#[derive(Clone, Default, Eq, PartialEq)]
pub struct YearSet(
    u64, // Bitfield, current year is the lowest bit, 63 years ago is the highest
);

impl YearSet {
    pub fn insert_timestamp_year(&mut self, timestamp: i64) {
        self.0 |= Self::mask_for_timestamp(timestamp)
    }

    pub fn max_year() -> i32 {
        *MAX_YEAR
    }

    pub fn min_year() -> i32 {
        *MAX_YEAR - 63
    }

    fn mask_for_timestamp(timestamp: i64) -> u64 {
        let Some(date) = DateTime::<Utc>::from_timestamp(timestamp, 0) else {
            return 0;
        };
        let year = date.year();

        if year < Self::min_year() || year > Self::max_year() {
            0
        } else {
            1 << (Self::max_year() - year)
        }
    }

    /// Adds all entries from another `YearSet`.
    pub fn merge(&mut self, other: &Self) {
        self.0 |= other.0
    }

    /// Returns years (as Gregorian calendar years) contained in this set
    pub fn iter(&self) -> impl Iterator<Item = i32> + '_ {
        (0..64)
            .rev()
            .filter(|offset| (self.0 & (1 << offset)) != 0)
            .map(|offset| Self::max_year() - offset)
    }
}
