# Copyright (C) 2021-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""
This module defines the various objects of the
:ref:`swh.graph example dataset <swh-graph-example-dataset>`.
"""

import datetime
from pathlib import Path
from typing import List, Union

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
    """
    :meta private:
    """
    return bytes.fromhex(f"{id:0{width}}")


PERSONS: List[Person] = [
    Person(fullname=b"foo", name=b"foo", email=b""),
    Person(fullname=b"bar", name=b"bar", email=b""),
    Person(fullname=b"baz", name=b"baz", email=b""),
]
"""Example :py:class:`swh.model.model.Person` instances

  Each Person is respectively named “foo“, “bar”, and “baz”.
  They are used as authors in :py:const:`REVISIONS` and :py:const:`RELEASES`.

  :meta hide-value:
"""  # pylint: disable=W0105

CONTENTS: List[Content] = [
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
]
"""Example :py:class:`swh.model.model.Content` instances

  :meta hide-value:

  Objects have the following SWHIDs:

  - ``swh:1:cnt:0000000000000000000000000000000000000001``
  - ``swh:1:cnt:0000000000000000000000000000000000000004``
  - ``swh:1:cnt:0000000000000000000000000000000000000005``
  - ``swh:1:cnt:0000000000000000000000000000000000000007``
  - ``swh:1:cnt:0000000000000000000000000000000000000011``
  - ``swh:1:cnt:0000000000000000000000000000000000000014``
"""  # pylint: disable=W0105
# "\n".join([f"  - {c.swhid()}" for c in CONTENTS])


SKIPPED_CONTENTS: List[SkippedContent] = [
    SkippedContent(
        sha1_git=h(15),
        sha1=h(15),
        sha256=h(15, 64),
        blake2s256=h(15, 64),
        length=404,
        status="absent",
        reason="Not found",
    ),
]
"""Example :py:class:`swh.model.model.SkippedContent` instances

  :meta hide-value:

  Object has the following SWHIDs:

  - ``swh:1:cnt:0000000000000000000000000000000000000015``
"""  # pylint: disable=W0105
# "\n".join([f"  - {c.swhid()}" for c in SKIPPED_CONTENTS])


DIRECTORIES: List[Directory] = [
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
]
""" Example :py:class:`swh.model.model.Directory` instances

  Directories have the following SWHIDs and content:

  * ``swh:1:dir:0000000000000000000000000000000000000002``

    - ``README.md`` → ``swh:1:cnt:0000000000000000000000000000000000000001``

  * ``swh:1:dir:0000000000000000000000000000000000000006``

    - ``README.md`` → ``swh:1:cnt:0000000000000000000000000000000000000004``
    - ``parser.c`` → ``swh:1:cnt:0000000000000000000000000000000000000005``

  * ``swh:1:dir:0000000000000000000000000000000000000008``

    - ``README.md`` → ``swh:1:cnt:0000000000000000000000000000000000000001``
    - ``parser.c`` → ``swh:1:cnt:0000000000000000000000000000000000000007``
    - ``tests`` → ``swh:1:dir:0000000000000000000000000000000000000006``

  * ``swh:1:dir:0000000000000000000000000000000000000012``

    - ``README.md`` → ``swh:1:cnt:0000000000000000000000000000000000000011``
    - ``oldproject`` → ``swh:1:dir:0000000000000000000000000000000000000008``

  * ``swh:1:dir:0000000000000000000000000000000000000016``

    - ``TODO.txt`` → ``swh:1:cnt:0000000000000000000000000000000000000015``

  * ``swh:1:dir:0000000000000000000000000000000000000017``

    - ``TODO.txt`` → ``swh:1:cnt:0000000000000000000000000000000000000014``
    - ``old`` → ``swh:1:dir:0000000000000000000000000000000000000016``

  :meta hide-value:
"""  # pylint: disable=W0105
# "\n".join(
#     itertools.chain(
#         *[
#             [
#                 f"  * ``{d.swhid()}``",
#                 "",
#                 *[
#                     f"    - ``{e.name.decode('us-ascii')}`` → ``{e.swhid()}``"
#                     for e in d.entries
#                 ],
#                 "",
#             ]
#             for d in DIRECTORIES
#         ]
#     )
# )


REVISIONS: List[Revision] = [
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
]
"""Example :py:class:`swh.model.model.Revision` instances

  :meta hide-value:

  Objects have the following SWHIDs:

  - ``swh:1:rev:0000000000000000000000000000000000000003``
  - ``swh:1:rev:0000000000000000000000000000000000000009``
  - ``swh:1:rev:0000000000000000000000000000000000000013``
  - ``swh:1:rev:0000000000000000000000000000000000000018``
"""  # pylint: disable=W0105
# "\n".join([f"  - {r.swhid()}" for r in REVISIONS])


RELEASES: List[Release] = [
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
    Release(
        id=h(21),
        name=b"v2.0-anonymous",
        date=None,
        author=None,
        target_type=ObjectType.REVISION,
        target=h(18),
        message=b"Version 2.0 but with no author",
        synthetic=False,
    ),
]
"""Example :py:class:`swh.model.model.Release` instances

  :meta hide-value:

  Objects have the following SWHIDs:

  - ``swh:1:rel:0000000000000000000000000000000000000010``
  - ``swh:1:rel:0000000000000000000000000000000000000019``
  - ``swh:1:rel:0000000000000000000000000000000000000021``
"""  # pylint: disable=W0105
# "\n".join([f"  - {r.swhid()}" for r in RELEASES])


SNAPSHOTS: List[Snapshot] = [
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
    Snapshot(
        id=h(22),
        branches={
            b"refs/heads/master": SnapshotBranch(
                target=h(9), target_type=TargetType.REVISION
            ),
            b"refs/tags/v1.0": SnapshotBranch(
                target=h(10), target_type=TargetType.RELEASE
            ),
            b"refs/tags/v2.0-anonymous": SnapshotBranch(
                target=h(21), target_type=TargetType.RELEASE
            ),
        },
    ),
]
"""Example :py:class:`swh.model.model.Release` instances

  :meta hide-value:

  Objects have the following SWHIDs and branches:

  * ``swh:1:snp:0000000000000000000000000000000000000020``

    - ``refs/heads/master`` →
      ``swh:1:rev:0000000000000000000000000000000000000009``
    - ``refs/tags/v1.0`` →
      ``swh:1:rel:0000000000000000000000000000000000000010``

  * ``swh:1:snp:0000000000000000000000000000000000000022``

    - ``refs/heads/master`` →
      ``swh:1:rev:0000000000000000000000000000000000000009``
    - ``refs/tags/v1.0`` →
      ``swh:1:rel:0000000000000000000000000000000000000010``
    - ``refs/tags/v2.0-anonymous`` →
      ``swh:1:rel:0000000000000000000000000000000000000021``
"""  # pylint: disable=W0105
# "\n".join(
#     itertools.chain(
#         *[
#             [
#                 "",
#                 f"  * ``{d.swhid()}``",
#                 "",
#                 *[f"    - ``{name.decode('us-ascii')}`` →\n"
#                    "      ``{b.swhid()}``"
#                    for name, b in d.branches.items()],
#             ]
#             for d in SNAPSHOTS
#         ]
#     )
# )

ORIGIN_VISITS: List[OriginVisit] = [
    OriginVisit(
        origin="https://example.com/swh/graph",
        date=datetime.datetime(
            2013, 5, 7, 4, 20, 39, 369271, tzinfo=datetime.timezone.utc
        ),
        visit=1,
        type="git",
    ),
    OriginVisit(
        origin="https://example.com/swh/graph2",
        date=datetime.datetime(
            2013, 5, 7, 4, 20, 39, 369271, tzinfo=datetime.timezone.utc
        ),
        visit=1,
        type="git",
    ),
]
"""Example :py:class:`swh.model.model.OriginVisit` instances

  :meta hide-value:

  Objects have the following origins:

  - ``https://example.com/swh/graph`` (``swh:1:ori:83404f995118bd25774f4ac14422a8f175e7a054``)
  - ``https://example.com/swh/graph2`` (``swh:1:ori:8f50d3f60eae370ddbf85c86219c55108a350165``)
"""  # pylint: disable=W0105


ORIGIN_VISIT_STATUSES: List[OriginVisitStatus] = [
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
    OriginVisitStatus(
        origin="https://example.com/swh/graph2",
        date=datetime.datetime(
            2013, 5, 7, 4, 20, 41, 369271, tzinfo=datetime.timezone.utc
        ),
        visit=1,
        type="git",
        status="full",
        snapshot=h(22),
        metadata=None,
    ),
]
"""Example :py:class:`swh.model.model.OriginVisitStatus` instances

  :meta hide-value:

  Objects have the following origins and snapshots:

  - ``swh:1:ori:83404f995118bd25774f4ac14422a8f175e7a054`` →
    ``swh:1:snp:0000000000000000000000000000000000000020``
  - ``swh:1:ori:8f50d3f60eae370ddbf85c86219c55108a350165`` →
    ``swh:1:snp:0000000000000000000000000000000000000022``
"""  # pylint: disable=W0105


INITIAL_ORIGIN: Origin = Origin(url="https://example.com/swh/graph")
"""Origin where the development of the tiny project represented by this
example dataset was initially made.

:meta hide-value:

- URL: ``https://example.com/swh/graph``
- SWHID: ``swh:1:ori:83404f995118bd25774f4ac14422a8f175e7a054``
"""  # pylint: disable=W0105


FORKED_ORIGIN: Origin = Origin(url="https://example.com/swh/graph2")
"""Origin that picked up (forked) the development of the tiny project
represented by this example dataset.

:meta hide-value:

- URL: ``https://example.com/swh/graph2``
- SWHID: ``swh:1:ori:8f50d3f60eae370ddbf85c86219c55108a350165``
"""  # pylint: disable=W0105


ORIGINS: List[Origin] = [INITIAL_ORIGIN, FORKED_ORIGIN]
"""Example :py:class:`swh.model.model.Origin` instances

  :meta hide-value:

  Objects have the following SWHIDs:

  - ``https://example.com/swh/graph`` (``swh:1:ori:83404f995118bd25774f4ac14422a8f175e7a054``)
  - ``https://example.com/swh/graph2`` (``swh:1:ori:8f50d3f60eae370ddbf85c86219c55108a350165``)
"""  # pylint: disable=W0105
# "\n".join([f"  - ``{o.url}`` (``{o.swhid()}``)" for o in ORIGINS]


DATASET: List[
    Union[
        Content,
        SkippedContent,
        Directory,
        Revision,
        Release,
        Snapshot,
        OriginVisit,
        OriginVisitStatus,
        Origin,
    ]
] = [
    *CONTENTS,
    *SKIPPED_CONTENTS,
    *DIRECTORIES,
    *REVISIONS,
    *RELEASES,
    *SNAPSHOTS,
    *ORIGIN_VISITS,
    *ORIGIN_VISIT_STATUSES,
    *ORIGINS,
]
"""Full dataset comprised with all the objects defined above.

  :meta hide-value:
"""  # pylint: disable=W0105


DATASET_DIR: Path = Path(__file__).parent
"""Path to the dataset directory

  :meta hide-value:
"""  # pylint: disable=W0105
