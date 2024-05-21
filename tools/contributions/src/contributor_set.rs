// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::collections::HashMap;

use super::YearSet;

pub type PersonId = u32;

#[derive(Clone, Default, Eq, PartialEq)]
pub struct ContributorSet(HashMap<PersonId, YearSet>);

impl ContributorSet {
    pub fn insert(&mut self, person: PersonId, timestamp: i64) {
        self.0
            .entry(person)
            .or_default()
            .insert_timestamp_year(timestamp)
    }

    /// Adds all entries from another `ContributorSet`.
    pub fn merge(&mut self, other: &Self) {
        for (person, years) in &other.0 {
            self.0.entry(*person).or_default().merge(years)
        }
    }
}

impl IntoIterator for ContributorSet {
    type Item = (PersonId, YearSet);
    type IntoIter = <HashMap<PersonId, YearSet> as IntoIterator>::IntoIter;

    /// Returns all contributors and the years they contributed
    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}
