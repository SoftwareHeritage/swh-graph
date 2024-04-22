// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use crate::labels::FilenameId;

/// Value in the path_stack between two lists of path parts
const PATH_SEPARATOR: FilenameId = FilenameId(u64::MAX);

/// A stack that stores sequences of [`FilenameId`] as a single array
///
/// Internally, it is a flattened array of paths. Each sequence is made of parts represented
/// by an id, and sequences are separated by PATH_SEPARATOR.
///
/// Parts are in the reverse order of insertion, to avoid lazily popping without peeking.
///
/// ```
/// use swh_graph::collections::PathStack;
/// use swh_graph::labels::FilenameId;
///
/// let path1 = [FilenameId(0), FilenameId(10)];
/// let path2 = [FilenameId(1)];
///
/// let mut stack = PathStack::new();
/// stack.push(path1);
/// stack.push(path2);
/// assert_eq!(stack.pop().unwrap().collect::<Vec<_>>(), path2);
/// assert_eq!(stack.pop().unwrap().collect::<Vec<_>>(), path1);
/// assert!(stack.pop().is_none());
/// ```
#[derive(Debug, Clone, Eq, PartialEq, Default)]
pub struct PathStack(Vec<FilenameId>);

impl PathStack {
    pub fn new() -> PathStack {
        PathStack(Vec::new())
    }

    /// Creates a new empty [`PathStack`] pre-allocated for this many paths plus path parts.
    pub fn with_capacity(capacity: usize) -> PathStack {
        PathStack(Vec::with_capacity(capacity))
    }

    /// Adds a path to this stack
    ///
    /// # Panics
    ///
    /// If any of the [`FilenameId`] path parts was manually built from `u64::MAX`.
    #[inline]
    pub fn push<Iter: IntoIterator<Item = FilenameId>>(&mut self, path: Iter)
    where
        <Iter as IntoIterator>::IntoIter: DoubleEndedIterator,
    {
        self.0.push(PATH_SEPARATOR);
        for item in path.into_iter().rev() {
            assert_ne!(
                item, PATH_SEPARATOR,
                "u64::MAX may not be used as path part"
            );
            self.0.push(item);
        }
    }

    /// Adds a path part to the last path on this stack
    ///
    /// # Panics
    ///
    /// If the [`FilenameId`] path parts was manually built from `u64::MAX`.
    #[inline]
    pub fn push_filename(&mut self, item: FilenameId) {
        assert_ne!(
            item, PATH_SEPARATOR,
            "u64::MAX may not be used as path part"
        );
        self.0.push(item);
    }

    /// Removes the last path of this stack
    ///
    /// Returns an iterator that pops parts from the part as the iterator is consumed.
    /// It is safe to drop the iterator without consuming it.
    #[inline]
    pub fn pop(&mut self) -> Option<PopPathStack<'_>> {
        if self.0.is_empty() {
            None
        } else {
            Some(PopPathStack(self))
        }
    }
}

/// Returned by [`PathStack::pop`]
pub struct PopPathStack<'a>(&'a mut PathStack);

impl<'a> Iterator for PopPathStack<'a> {
    type Item = FilenameId;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if *self
            .0
             .0
            .last()
            .expect("PopPathStack reached the bottom of the stack")
            == PATH_SEPARATOR
        {
            None
        } else {
            Some(self.0 .0.pop().unwrap())
        }
    }
}

impl<'a> Drop for PopPathStack<'a> {
    fn drop(&mut self) {
        // Finish popping from the stack so a partial path does not remain on top of it
        while self
            .0
             .0
            .pop()
            .expect("PopPathStack reached the bottom of the stack")
            != PATH_SEPARATOR
        {}
    }
}

#[test]
fn test_path_stack_empty() {
    let mut stack = PathStack::new();
    assert!(stack.pop().is_none());
    assert!(stack.pop().is_none());
}

#[test]
/// Checks that dropping PopPathStack does pop the rest of the path from the stack
fn test_path_stack_drop() {
    let path1 = [FilenameId(0), FilenameId(10)];
    let path2 = [FilenameId(1)];

    let mut stack = PathStack::new();
    stack.push(path1);
    stack.push(path2);
    stack.pop().unwrap(); // not consumed
    assert_eq!(stack.pop().unwrap().collect::<Vec<_>>(), path1);
    assert!(stack.pop().is_none());
}
