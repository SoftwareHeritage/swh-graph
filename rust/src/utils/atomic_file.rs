/*
 * Copyright (C) 2026  The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

use std::fs::File;
use std::io::IoSlice;
use std::io::Result;
use std::path::{Path, PathBuf};

/// Wrapper around [`File`] that writes to a temporary file and
/// moves it to the path when closed
pub struct AtomicFile {
    tmp_path: PathBuf,
    path: PathBuf,
    /// `None` if already closed
    file: Option<File>,
}

impl AtomicFile {
    pub fn create_new<P: AsRef<Path>>(path: P) -> Result<Self> {
        use rand::distr::{Alphanumeric, SampleString};

        let path = path.as_ref().to_path_buf();
        let file_name = path.file_name().unwrap_or_default().to_string_lossy();
        let random_string = Alphanumeric.sample_string(&mut rand::rng(), 16);
        let tmp_path = path.with_file_name(format!(".tmp.{file_name}.{random_string}"));
        Ok(Self {
            file: Some(File::create_new(&tmp_path)?),
            tmp_path,
            path,
        })
    }

    pub fn commit(mut self) -> Result<()> {
        if self.file.is_some() {
            std::fs::rename(&self.tmp_path, &self.path)?;
            self.file = None;
        }
        Ok(())
    }

    pub fn rollback(mut self) -> Result<()> {
        if self.file.is_some() {
            std::fs::remove_file(&self.tmp_path)?;
            self.file = None;
        }
        Ok(())
    }

    fn file(&mut self) -> Result<&mut File> {
        self.file.as_mut().ok_or_else(|| {
            std::io::Error::other(anyhow::anyhow!(
                "File {} was already committed to {} and closed",
                self.tmp_path.display(),
                self.path.display()
            ))
        })
    }
}

impl Drop for AtomicFile {
    fn drop(&mut self) {
        if self.file.is_some() {
            let mut other = AtomicFile {
                tmp_path: PathBuf::from("/"),
                path: PathBuf::from("/"),
                file: None,
            };
            std::mem::swap(&mut other, self);
            other
                .rollback()
                .expect("Error while removing dropped AtomicFile");
        }
    }
}

impl std::io::Write for AtomicFile {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        self.file()?.write(buf)
    }
    fn flush(&mut self) -> Result<()> {
        self.file()?.flush()
    }

    fn write_vectored(&mut self, bufs: &[IoSlice<'_>]) -> Result<usize> {
        self.file()?.write_vectored(bufs)
    }
    fn write_all(&mut self, buf: &[u8]) -> Result<()> {
        self.file()?.write_all(buf)
    }
    fn write_fmt(&mut self, args: std::fmt::Arguments<'_>) -> Result<()> {
        self.file()?.write_fmt(args)
    }
    fn by_ref(&mut self) -> &mut Self
    where
        Self: Sized,
    {
        self
    }
}
