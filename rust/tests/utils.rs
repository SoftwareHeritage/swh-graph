/*
 * Copyright (C) 2026  The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

use std::io::Write;
use std::path::Path;

use swh_graph::utils::AtomicFile;

fn list_entries(path: &Path) -> Vec<String> {
    std::fs::read_dir(path)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_str().unwrap().to_owned())
        .collect()
}

#[test]
fn atomic_file_commit() {
    let tmp_dir = tempfile::TempDir::new().unwrap();
    let path = tmp_dir.path().join("my_file.txt");
    {
        let mut file = AtomicFile::create_new(&path).unwrap();

        let entries = list_entries(tmp_dir.path());
        assert_eq!(entries.len(), 1, "{entries:?}");
        assert_ne!(entries.first().unwrap(), "my_file.txt");

        file.write_all(b"foo\n").unwrap();

        let entries = list_entries(tmp_dir.path());
        assert_eq!(entries.len(), 1, "{entries:?}");
        assert_ne!(entries.first().unwrap(), "my_file.txt");

        file.commit().unwrap();

        let entries = list_entries(tmp_dir.path());
        assert_eq!(entries.len(), 1, "{entries:?}");
        assert_eq!(entries.first().unwrap(), "my_file.txt"); // renamed
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "foo\n");
    }

    let entries = list_entries(tmp_dir.path());
    assert_eq!(entries.len(), 1, "{entries:?}");
    assert_eq!(entries.first().unwrap(), "my_file.txt");
    assert_eq!(std::fs::read_to_string(&path).unwrap(), "foo\n");
}

#[test]
fn atomic_file_rollback() {
    let tmp_dir = tempfile::TempDir::new().unwrap();
    let path = tmp_dir.path().join("my_file.txt");
    {
        let mut file = AtomicFile::create_new(&path).unwrap();

        let entries = list_entries(tmp_dir.path());
        assert_eq!(entries.len(), 1, "{entries:?}");
        assert_ne!(entries.first().unwrap(), "my_file.txt");

        file.write_all(b"foo\n").unwrap();

        let entries = list_entries(tmp_dir.path());
        assert_eq!(entries.len(), 1, "{entries:?}");
        assert_ne!(entries.first().unwrap(), "my_file.txt");

        file.rollback().unwrap();

        let entries = list_entries(tmp_dir.path());
        assert_eq!(entries, Vec::<&str>::new());
    }

    let entries = list_entries(tmp_dir.path());
    assert_eq!(entries, Vec::<&str>::new());
}

#[test]
fn atomic_file_drop() {
    let tmp_dir = tempfile::TempDir::new().unwrap();
    let path = tmp_dir.path().join("my_file.txt");
    {
        let mut file = AtomicFile::create_new(&path).unwrap();

        let entries = list_entries(tmp_dir.path());
        assert_eq!(entries.len(), 1, "{entries:?}");
        assert_ne!(entries.first().unwrap(), "my_file.txt");

        file.write_all(b"foo\n").unwrap();

        let entries = list_entries(tmp_dir.path());
        assert_eq!(entries.len(), 1, "{entries:?}");
        assert_ne!(entries.first().unwrap(), "my_file.txt");
    } // rolled back

    let entries = list_entries(tmp_dir.path());
    assert_eq!(entries, Vec::<&str>::new());
}
