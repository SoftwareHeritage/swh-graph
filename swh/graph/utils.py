# Copyright (C) 2025-2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
from pathlib import Path
import shutil

from tqdm.contrib.concurrent import thread_map


def link(
    source_path: Path,
    destination_path: Path,
    copy_graph: bool,
    copy_ef: bool,
) -> None:
    """
    Symlink (or copy) an existing graph to the desired location.

    By default, all files but *.graph and *.ef are symlinked, but files and directories can be
    specified to be copied instead.

    This functionality is intended for internal use, and is there to ease the
    process of sharing an existing graph between multiple users on the same
    machine.
    """
    logger = logging.getLogger(f"{__name__}.link")

    destination_path.mkdir(parents=True, exist_ok=True)

    copy_paths = []

    for dirpath, dirnames, filenames in source_path.walk():
        destination_dirpath = destination_path / dirpath.relative_to(source_path)
        for dirname in dirnames:
            (destination_dirpath / dirname).mkdir(parents=True, exist_ok=True)
        for filename in filenames:
            source_file_path = dirpath / filename
            destination_file_path = destination_dirpath / filename
            if copy_graph and source_file_path.suffix == ".graph":
                copy_paths.append(source_file_path)
            elif copy_ef and source_file_path.suffix == ".ef":
                copy_paths.append(source_file_path)
            else:
                destination_file_path.symlink_to(source_file_path)
                logger.info(
                    "Creating symlink from %s to %s",
                    destination_file_path,
                    source_file_path,
                )

    def _copy(source_item: Path):
        logger.info(f"Copying {source_item} to {destination_path}")
        if source_item.is_file():
            shutil.copy(
                source_item, destination_path / source_item.relative_to(source_path)
            )
        else:
            shutil.copytree(
                source_item, destination_path / source_item.relative_to(source_path)
            )

    if len(copy_paths) > 0:
        thread_map(_copy, copy_paths, desc="Copying files", max_workers=len(copy_paths))
    else:
        logger.warning("No .graph or .ef files found in %s", source_path)

    for path in destination_path.iterdir():
        if path.suffix == ".ef":
            # silence
            # https://github.com/vigna/webgraph-rs/commit/b494048f787e3f0a021f6f289d66400bdfb5d5f3
            path.touch()
