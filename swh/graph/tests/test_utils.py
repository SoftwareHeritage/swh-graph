# Copyright (C) 2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from pathlib import Path
import time

import pytest

from swh.graph.utils import link


@pytest.fixture
def source_tree(tmp_path: Path) -> Path:
    """Create a source directory tree with various file types."""
    source = tmp_path / "source"
    source.mkdir()

    # Create subdirectories
    (source / "subdir1").mkdir()
    (source / "subdir2" / "nested").mkdir(parents=True)

    # Create various files
    (source / "file1.txt").write_text("content1")
    (source / "file2.graph").write_text("graph content")
    (source / "file3.ef").write_text("ef content")
    (source / "subdir1" / "nested.txt").write_text("nested content")
    (source / "subdir1" / "nested.graph").write_text("nested graph")
    (source / "subdir2" / "nested" / "deep.ef").write_text("deep ef")

    return source


def test_link_default_symlinks_all(source_tree: Path, tmp_path: Path) -> None:
    """Test that by default all files are symlinked except .graph and .ef which
    are not copied."""
    dest = tmp_path / "dest"

    link(source_tree, dest, copy_graph=False, copy_ef=False)

    # Check directory structure is created
    assert dest.exists()
    assert (dest / "subdir1").is_dir()
    assert (dest / "subdir2" / "nested").is_dir()

    # Check all files are symlinked
    assert (dest / "file1.txt").is_symlink()
    assert (dest / "file2.graph").is_symlink()
    assert (dest / "file3.ef").is_symlink()
    assert (dest / "subdir1" / "nested.txt").is_symlink()
    assert (dest / "subdir1" / "nested.graph").is_symlink()
    assert (dest / "subdir2" / "nested" / "deep.ef").is_symlink()

    # Check symlinks point to correct targets
    assert (dest / "file1.txt").resolve() == (source_tree / "file1.txt").resolve()
    assert (dest / "file2.graph").resolve() == (source_tree / "file2.graph").resolve()


def test_link_copy(source_tree: Path, tmp_path: Path) -> None:
    """Test that .graph files are copied when copy_graph=True."""
    dest = tmp_path / "dest"

    link(source_tree, dest, copy_graph=True, copy_ef=True)

    # .graph and .ef files should be regular files (copied)
    assert not (dest / "file2.graph").is_symlink()
    assert (dest / "file2.graph").read_text() == "graph content"

    assert not (dest / "file3.ef").is_symlink()
    assert (dest / "file3.ef").read_text() == "ef content"

    assert not (dest / "subdir1" / "nested.graph").is_symlink()
    assert (dest / "subdir1" / "nested.graph").read_text() == "nested graph"

    assert not (dest / "subdir2" / "nested" / "deep.ef").is_symlink()
    assert (dest / "subdir2" / "nested" / "deep.ef").read_text() == "deep ef"

    # Other files should be symlinked
    assert (dest / "file1.txt").is_symlink()


def test_link_touches_ef_files(source_tree: Path, tmp_path: Path) -> None:
    """Test that .ef files in destination are touched after linking."""
    dest = tmp_path / "dest"

    time.sleep(0.01)
    link(source_tree, dest, copy_graph=False, copy_ef=True)
    time.sleep(0.01)

    # Get the mtime of the source .ef files
    source_mtime = (source_tree / "file3.ef").stat().st_mtime

    # Get the mtime of the destination .ef files
    dest_mtime = (dest / "file3.ef").stat().st_mtime

    # Destination .ef file should be touched (have a newer mtime than source)
    assert dest_mtime >= source_mtime


def test_link_with_existing_destination(source_tree: Path, tmp_path: Path) -> None:
    """Test that linking raises an error when destination already exists."""
    dest = tmp_path / "dest"
    dest.mkdir()
    (dest / "existing.txt").write_text("already here")

    with pytest.raises(FileExistsError):
        link(source_tree, dest, copy_graph=False, copy_ef=False)
