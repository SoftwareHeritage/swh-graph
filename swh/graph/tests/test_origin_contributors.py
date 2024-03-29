# Copyright (C) 2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import base64
import datetime
from pathlib import Path
import subprocess

from swh.graph.luigi.origin_contributors import (
    DeanonymizeOriginContributors,
    ExportDeanonymizationTable,
    ListOriginContributors,
)
from swh.model.model import (
    ObjectType,
    Person,
    Release,
    Revision,
    RevisionType,
    TimestampWithTimezone,
)

from .test_toposort import EXPECTED as TOPOLOGICAL_ORDER

DATA_DIR = Path(__file__).parents[0] / "dataset"


# FIXME: do not hardcode ids here; they should be dynamically loaded
# from the test graph
ORIGIN_CONTRIBUTORS = """\
origin_id,contributor_id
2,0
2,2
0,0
0,1
0,2
"""

assert (
    base64.b64decode("aHR0cHM6Ly9leGFtcGxlLmNvbS9zd2gvZ3JhcGg=")
    == b"https://example.com/swh/graph"
)
assert (
    base64.b64decode("aHR0cHM6Ly9leGFtcGxlLmNvbS9zd2gvZ3JhcGgy")
    == b"https://example.com/swh/graph2"
)

ORIGIN_URLS = """\
origin_id,origin_url_base64
2,aHR0cHM6Ly9leGFtcGxlLmNvbS9zd2gvZ3JhcGg=
0,aHR0cHM6Ly9leGFtcGxlLmNvbS9zd2gvZ3JhcGgy
"""

DEANONYMIZATION_TABLE = """\
sha256_base64,base64,escaped
8qhF7WQ2bmeoRbZipAaqtNw6QdOCDcpggLWCQLzITsI=,Sm9obiBEb2UgPGpkb2VAZXhhbXBsZS5vcmc+,John Doe <jdoe@example.org>
aZA9TeLhVzqVDQHQOd53UABAZYyek0tY3vTo6VSlA4U=,SmFuZSBEb2UgPGpkb2VAZXhhbXBsZS5jb20+,Jane Doe <jdoe@example.com>
UaCrgAZBvn1LBd2sAinmdNvAX/G4sjo1aJA9GDd9UUs=,SmFuZSBEb2UgPGpkb2VAZXhhbXBsZS5uZXQ+,Jane Doe <jdoe@example.net>
"""  # noqa

PERSONS = """\
aZA9TeLhVzqVDQHQOd53UABAZYyek0tY3vTo6VSlA4U=
UaCrgAZBvn1LBd2sAinmdNvAX/G4sjo1aJA9GDd9UUs=
8qhF7WQ2bmeoRbZipAaqtNw6QdOCDcpggLWCQLzITsI=
"""

DEANONYMIZED_ORIGIN_CONTRIBUTORS = """\
origin_id,contributor_base64,contributor_escaped
2,SmFuZSBEb2UgPGpkb2VAZXhhbXBsZS5jb20+,Jane Doe <jdoe@example.com>
2,Sm9obiBEb2UgPGpkb2VAZXhhbXBsZS5vcmc+,John Doe <jdoe@example.org>
0,SmFuZSBEb2UgPGpkb2VAZXhhbXBsZS5jb20+,Jane Doe <jdoe@example.com>
0,SmFuZSBEb2UgPGpkb2VAZXhhbXBsZS5uZXQ+,Jane Doe <jdoe@example.net>
0,Sm9obiBEb2UgPGpkb2VAZXhhbXBsZS5vcmc+,John Doe <jdoe@example.org>
"""  # noqa


def test_list_origin_contributors(tmpdir):
    tmpdir = Path(tmpdir)

    topological_order_path = tmpdir / "topo_order.csv.zst"
    origin_contributors_path = tmpdir / "origin_contributors.csv.zst"
    origin_urls_path = tmpdir / "origin_urls.csv.zst"

    subprocess.run(
        ["zstdmt", "-o", topological_order_path],
        input=TOPOLOGICAL_ORDER.encode(),
        check=True,
    )

    task = ListOriginContributors(
        local_graph_path=DATA_DIR / "compressed",
        topological_order_path=topological_order_path,
        origin_contributors_path=origin_contributors_path,
        origin_urls_path=origin_urls_path,
        graph_name="example",
    )

    task.run()

    csv_text = subprocess.check_output(["zstdcat", origin_contributors_path]).decode()
    assert csv_text == ORIGIN_CONTRIBUTORS

    urls_text = subprocess.check_output(["zstdcat", origin_urls_path]).decode()
    assert urls_text == ORIGIN_URLS


def test_export_deanonymization_table(tmpdir, swh_storage_postgresql, swh_storage):
    tmpdir = Path(tmpdir)

    tstz = TimestampWithTimezone.from_datetime(
        datetime.datetime.now(tz=datetime.timezone.utc)
    )
    swh_storage.release_add(
        [
            Release(
                name=b"v1.0",
                message=b"first release",
                author=Person.from_fullname(b"John Doe <jdoe@example.org>"),
                target=b"\x00" * 20,
                target_type=ObjectType.REVISION,
                synthetic=True,
            )
        ]
    )
    swh_storage.revision_add(
        [
            Revision(
                message=b"first commit",
                author=Person.from_fullname(b"Jane Doe <jdoe@example.com>"),
                committer=Person.from_fullname(b"Jane Doe <jdoe@example.net>"),
                date=tstz,
                committer_date=tstz,
                directory=b"\x00" * 20,
                type=RevisionType.GIT,
                synthetic=True,
            )
        ]
    )

    deanonymization_table_path = tmpdir / "contributor_sha256_to_names.csv.zst"

    task = ExportDeanonymizationTable(
        storage_dsn=swh_storage_postgresql.dsn,
        deanonymization_table_path=deanonymization_table_path,
    )

    task.run()

    csv_text = subprocess.check_output(["zstdcat", deanonymization_table_path]).decode()

    (header, *rows) = csv_text.split("\n")
    (expected_header, *expected_rows) = DEANONYMIZATION_TABLE.split("\n")

    assert header == expected_header

    assert rows.pop() == "", "Missing trailing newline"
    expected_rows.pop()

    assert set(rows) == set(expected_rows)


def test_deanonymize_origin_contributors(tmpdir):
    tmpdir = Path(tmpdir)

    persons_path = tmpdir / "example.persons.csv.zst"
    origin_contributors_path = tmpdir / "origin_contributors.csv.zst"
    deanonymization_table_path = tmpdir / "contributor_sha256_to_names.csv.zst"
    deanonymized_origin_contributors_path = (
        tmpdir / "sensitive" / "origin_contributors.deanonymized.csv.zst"
    )

    subprocess.run(
        ["zstdmt", "-o", origin_contributors_path],
        input=ORIGIN_CONTRIBUTORS.encode(),
        check=True,
    )

    subprocess.run(
        ["zstdmt", "-o", persons_path],
        input=PERSONS.encode(),
        check=True,
    )

    subprocess.run(
        ["zstdmt", "-o", deanonymization_table_path],
        input=DEANONYMIZATION_TABLE.encode(),
        check=True,
    )

    task = DeanonymizeOriginContributors(
        local_graph_path=tmpdir,
        origin_contributors_path=origin_contributors_path,
        deanonymization_table_path=deanonymization_table_path,
        deanonymized_origin_contributors_path=deanonymized_origin_contributors_path,
        graph_name="example",
    )

    task.run()

    csv_text = subprocess.check_output(
        ["zstdcat", deanonymized_origin_contributors_path]
    ).decode()

    assert csv_text == DEANONYMIZED_ORIGIN_CONTRIBUTORS
