// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::ops::DerefMut;
use std::sync::{Arc, Mutex};

use dsi_progress_logger::ProgressLog;

pub const DEFAULT_THRESHOLD: u16 = 32768;

/// Like [`ProgressLog`], but only allows updates (ie. no start/stop/done/configuration)
pub trait MinimalProgressLog {
    fn update(&mut self);
    fn update_with_count(&mut self, count: usize);
    fn light_update(&mut self);
    fn update_and_display(&mut self);
}

impl<T: ProgressLog> MinimalProgressLog for T {
    fn update(&mut self) {
        ProgressLog::update(self)
    }
    fn update_with_count(&mut self, count: usize) {
        ProgressLog::update_with_count(self, count)
    }
    fn light_update(&mut self) {
        ProgressLog::light_update(self)
    }
    fn update_and_display(&mut self) {
        ProgressLog::update_and_display(self)
    }
}

#[derive(Debug)]
/// A wrapper for `Arc<Mutex<ProgressLog>>` that buffers writes to the underlying
/// [`ProgressLog`] to avoid taking the lock too often.
///
/// This is useful when writing to a common [`ProgressLog`] from multiple threads.
pub struct BufferedProgressLogger<T: DerefMut<Target: MinimalProgressLog>> {
    inner: Arc<Mutex<T>>,
    count: u16,
    threshold: u16,
}

impl<T: DerefMut<Target: MinimalProgressLog>> BufferedProgressLogger<T> {
    pub fn new(pl: Arc<Mutex<T>>) -> Self {
        Self::with_threshold(pl, DEFAULT_THRESHOLD)
    }

    /// Creates a new `BufferedProgressLogger` that buffers its writes up to `threshold`,
    /// before flushing to the underlying [`MinimalProgressLog`].
    pub fn with_threshold(pl: Arc<Mutex<T>>, threshold: u16) -> Self {
        Self {
            inner: pl,
            count: 0,
            threshold,
        }
    }

    pub fn inner(&self) -> &Arc<Mutex<T>> {
        &self.inner
    }

    pub fn flush(&mut self) {
        if self.count > 0 {
            self.inner
                .lock()
                .unwrap()
                .update_with_count(self.count.into());
            self.count = 0;
        }
    }
}

impl<T: DerefMut<Target: MinimalProgressLog>> MinimalProgressLog for BufferedProgressLogger<T> {
    fn update(&mut self) {
        self.update_with_count(1)
    }
    fn update_with_count(&mut self, count: usize) {
        match usize::from(self.count).checked_add(count) {
            None => {
                // Sum overflows, update in two steps
                let mut inner = self.inner.lock().unwrap();
                inner.update_with_count(self.count.into());
                inner.update_with_count(count);
                self.count = 0;
            }
            Some(total_count) => {
                if total_count >= usize::from(self.threshold) {
                    // Threshold reached, time to flush to the inner ProgressLog
                    let mut inner = self.inner.lock().unwrap();
                    inner.update_with_count(total_count);
                    self.count = 0;
                } else {
                    // total_count is lower than self.threshold, which is a u16;
                    // so total_count fits in u16.
                    self.count = total_count as u16;
                }
            }
        }
    }
    fn light_update(&mut self) {
        self.update_with_count(1)
    }
    fn update_and_display(&mut self) {
        let mut inner = self.inner.lock().unwrap();
        inner.update_with_count(self.count.into());
        self.count = 0;
        inner.update_and_display()
    }
}

/// Flush count buffer to the inner [`MinimalProgressLog`]
impl<T: DerefMut<Target: MinimalProgressLog>> Drop for BufferedProgressLogger<T> {
    fn drop(&mut self) {
        if self.count > 0 {
            self.inner
                .lock()
                .unwrap()
                .update_with_count(self.count.into());
            self.count = 0;
        }
    }
}

impl<T: DerefMut<Target: MinimalProgressLog>> Clone for BufferedProgressLogger<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            count: 0, // Not copied or it would cause double-counting!
            threshold: self.threshold,
        }
    }
}

#[derive(Debug)]
/// A wrapper for [`ProgressLog`] that provides access to the total counter.
pub struct CountingProgressLogger<T: DerefMut<Target: ProgressLog>> {
    inner: T,
    count: usize,
}

impl<T: DerefMut<Target: ProgressLog>> CountingProgressLogger<T> {
    pub fn new(pl: T) -> Self {
        CountingProgressLogger {
            inner: pl,
            count: 0,
        }
    }

    pub fn inner(&self) -> &T {
        &self.inner
    }

    pub fn inner_mut(&mut self) -> &mut T {
        &mut self.inner
    }

    pub fn into_inner(self) -> T {
        self.inner
    }

    /// Returns the current counter value
    pub fn total(&self) -> usize {
        self.count
    }
}

impl<T: DerefMut<Target: ProgressLog>> MinimalProgressLog for CountingProgressLogger<T> {
    fn update(&mut self) {
        self.count += 1;
        self.inner.update()
    }
    fn update_with_count(&mut self, count: usize) {
        self.count += count;
        self.inner.update_with_count(count)
    }
    fn light_update(&mut self) {
        self.count += 1;
        self.inner.light_update()
    }
    fn update_and_display(&mut self) {
        self.count += 1;
        self.inner.update_and_display()
    }
}
