#!/usr/bin/env python3

# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

# type: ignore

import argparse
import datetime
import logging
from pathlib import Path
import shutil

from swh.dataset.exporters.edges import GraphEdgesExporter
from swh.dataset.exporters.orc import ORCExporter
from swh.graph.webgraph import compress
from swh.model.model import (
    Content,
    Directory,
    DirectoryEntry,
    ObjectType,
    Origin,
    OriginVisit,
    OriginVisitStatus,
    Person,
    Release,
    Revision,
    RevisionType,
    SkippedContent,
    Snapshot,
    SnapshotBranch,
    TargetType,
    Timestamp,
    TimestampWithTimezone,
)


def h(id: int, width=40) -> bytes:
    return bytes.fromhex(f"{id:0{width}}")


PERSONS = [
    Person(fullname=b"foo", name=b"foo", email=b""),
    Person(fullname=b"bar", name=b"bar", email=b""),
    Person(fullname=b"baz", name=b"baz", email=b""),
]

TEST_DATASET = [
    Content(sha1_git=h(1), sha1=h(1), sha256=h(1, 64), blake2s256=h(1, 64), length=42),
    Content(sha1_git=h(4), sha1=h(4), sha256=h(4, 64), blake2s256=h(4, 64), length=404),
    Content(
        sha1_git=h(5), sha1=h(5), sha256=h(5, 64), blake2s256=h(5, 64), length=1337
    ),
    Content(sha1_git=h(7), sha1=h(7), sha256=h(7, 64), blake2s256=h(7, 64), length=666),
    Content(
        sha1_git=h(11), sha1=h(11), sha256=h(11, 64), blake2s256=h(11, 64), length=313
    ),
    Content(
        sha1_git=h(14), sha1=h(14), sha256=h(14, 64), blake2s256=h(14, 64), length=14
    ),
    SkippedContent(
        sha1_git=h(15),
        sha1=h(15),
        sha256=h(15, 64),
        blake2s256=h(15, 64),
        length=404,
        status="absent",
        reason="Not found",
    ),
    Directory(
        id=h(2),
        entries=(
            DirectoryEntry(
                name=b"README.md",
                perms=0o100644,
                type="file",
                target=h(1),
            ),
        ),
    ),
    Directory(
        id=h(6),
        entries=(
            DirectoryEntry(
                name=b"README.md",
                perms=0o100644,
                type="file",
                target=h(4),
            ),
            DirectoryEntry(
                name=b"parser.c",
                perms=0o100644,
                type="file",
                target=h(5),
            ),
        ),
    ),
    Directory(
        id=h(8),
        entries=(
            DirectoryEntry(
                name=b"README.md",
                perms=0o100644,
                type="file",
                target=h(1),
            ),
            DirectoryEntry(
                name=b"parser.c",
                perms=0o100644,
                type="file",
                target=h(7),
            ),
            DirectoryEntry(
                name=b"tests",
                perms=0o100755,
                type="dir",
                target=h(6),
            ),
        ),
    ),
    Directory(
        id=h(12),
        entries=(
            DirectoryEntry(
                name=b"README.md",
                perms=0o100644,
                type="file",
                target=h(11),
            ),
            DirectoryEntry(
                name=b"oldproject",
                perms=0o100755,
                type="dir",
                target=h(8),
            ),
        ),
    ),
    Directory(
        id=h(16),
        entries=(
            DirectoryEntry(
                name=b"TODO.txt",
                perms=0o100644,
                type="file",
                target=h(15),
            ),
        ),
    ),
    Directory(
        id=h(17),
        entries=(
            DirectoryEntry(
                name=b"TODO.txt",
                perms=0o100644,
                type="file",
                target=h(14),
            ),
            DirectoryEntry(
                name=b"old",
                perms=0o100755,
                type="dir",
                target=h(16),
            ),
        ),
    ),
    Revision(
        id=h(3),
        message=b"Initial commit",
        date=TimestampWithTimezone(
            timestamp=Timestamp(
                seconds=1111122220,
                microseconds=0,
            ),
            offset_bytes=b"+0200",
        ),
        committer=PERSONS[0],
        author=PERSONS[0],
        committer_date=TimestampWithTimezone(
            timestamp=Timestamp(
                seconds=1111122220,
                microseconds=0,
            ),
            offset_bytes=b"+0200",
        ),
        type=RevisionType.GIT,
        directory=h(2),
        synthetic=False,
        metadata=None,
        parents=(),
    ),
    Revision(
        id=h(9),
        message=b"Add parser",
        date=TimestampWithTimezone(
            timestamp=Timestamp(
                seconds=1111144440,
                microseconds=0,
            ),
            offset_bytes=b"+0200",
        ),
        committer=PERSONS[1],
        author=PERSONS[1],
        committer_date=TimestampWithTimezone(
            timestamp=Timestamp(
                seconds=1111155550,
                microseconds=0,
            ),
            offset_bytes=b"+0200",
        ),
        type=RevisionType.GIT,
        directory=h(8),
        synthetic=False,
        metadata=None,
        parents=(h(3),),
    ),
    Revision(
        id=h(13),
        message=b"Add tests",
        date=TimestampWithTimezone(
            timestamp=Timestamp(
                seconds=1111166660,
                microseconds=0,
            ),
            offset_bytes=b"+0200",
        ),
        committer=PERSONS[1],
        author=PERSONS[0],
        committer_date=TimestampWithTimezone(
            timestamp=Timestamp(
                seconds=1111166660,
                microseconds=0,
            ),
            offset_bytes=b"+0200",
        ),
        type=RevisionType.GIT,
        directory=h(12),
        synthetic=False,
        metadata=None,
        parents=(h(9),),
    ),
    Revision(
        id=h(18),
        message=b"Refactor codebase",
        date=TimestampWithTimezone(
            timestamp=Timestamp(
                seconds=1111177770,
                microseconds=0,
            ),
            offset_bytes=b"+0000",
        ),
        committer=PERSONS[0],
        author=PERSONS[2],
        committer_date=TimestampWithTimezone(
            timestamp=Timestamp(
                seconds=1111177770,
                microseconds=0,
            ),
            offset_bytes=b"+0000",
        ),
        type=RevisionType.GIT,
        directory=h(17),
        synthetic=False,
        metadata=None,
        parents=(h(13),),
    ),
    Release(
        id=h(10),
        name=b"v1.0",
        date=TimestampWithTimezone(
            timestamp=Timestamp(
                seconds=1234567890,
                microseconds=0,
            ),
            offset_bytes=b"+0200",
        ),
        author=PERSONS[0],
        target_type=ObjectType.REVISION,
        target=h(9),
        message=b"Version 1.0",
        synthetic=False,
    ),
    Release(
        id=h(19),
        name=b"v2.0",
        date=None,
        author=PERSONS[1],
        target_type=ObjectType.REVISION,
        target=h(18),
        message=b"Version 2.0",
        synthetic=False,
    ),
    Snapshot(
        id=h(20),
        branches={
            b"refs/heads/master": SnapshotBranch(
                target=h(9), target_type=TargetType.REVISION
            ),
            b"refs/tags/v1.0": SnapshotBranch(
                target=h(10), target_type=TargetType.RELEASE
            ),
        },
    ),
    OriginVisit(
        origin="https://example.com/swh/graph",
        date=datetime.datetime(
            2013, 5, 7, 4, 20, 39, 369271, tzinfo=datetime.timezone.utc
        ),
        visit=1,
        type="git",
    ),
    OriginVisitStatus(
        origin="https://example.com/swh/graph",
        date=datetime.datetime(
            2013, 5, 7, 4, 20, 41, 369271, tzinfo=datetime.timezone.utc
        ),
        visit=1,
        type="git",
        status="full",
        snapshot=h(20),
        metadata=None,
    ),
    Origin(url="https://example.com/swh/graph"),
]


def main():
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(description="Generate a test dataset")
    parser.add_argument(
        "--compress",
        action="store_true",
        default=False,
        help="Also compress the dataset",
    )
    parser.add_argument("output", help="output directory", nargs="?", default=".")
    args = parser.parse_args()

    exporters = {"edges": GraphEdgesExporter, "orc": ORCExporter}
    config = {"test_unique_file_id": "all"}
    output_path = Path(args.output)
    for name, exporter in exporters.items():
        if (output_path / name).exists():
            shutil.rmtree(output_path / name)
        with exporter(config, output_path / name) as e:
            for obj in TEST_DATASET:
                e.process_object(obj.object_type, obj.to_dict())

    if args.compress:
        if (output_path / "compressed").exists():
            shutil.rmtree(output_path / "compressed")
        compress("example", output_path / "orc", output_path / "compressed")


if __name__ == "__main__":
    main()
