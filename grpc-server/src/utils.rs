// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::ops::{Deref, DerefMut};

/// Wraps a value, and calls a custom destructor closure when being dropped
pub(crate) struct OnDrop<T, D: FnMut(&mut T)> {
    value: T,
    destructor: D,
}

impl<T, D: FnMut(&mut T)> OnDrop<T, D> {
    pub fn new(value: T, destructor: D) -> Self {
        OnDrop { value, destructor }
    }
}

impl<T, D: FnMut(&mut T)> Deref for OnDrop<T, D> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<T, D: FnMut(&mut T)> DerefMut for OnDrop<T, D> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}

impl<T, D: FnMut(&mut T)> Drop for OnDrop<T, D> {
    fn drop(&mut self) {
        (self.destructor)(&mut self.value)
    }
}
