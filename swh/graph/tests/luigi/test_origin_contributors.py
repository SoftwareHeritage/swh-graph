# Copyright (C) 2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import base64
import datetime
from pathlib import Path
import shutil
import subprocess

from swh.graph.example_dataset import DATASET_DIR
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

from .test_topology import TOPO_ORDER_BACKWARD as TOPOLOGICAL_ORDER

# FIXME: do not hardcode ids here; they should be dynamically loaded
# from the test graph
ORIGIN_CONTRIBUTORS = """\
origin_id,contributor_id,years
2,0,2005 2009
2,2,2005
0,0,2005 2009
0,1,2005
0,2,2005
""".replace(
    "\n", "\r\n"
)

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
""".replace(
    "\n", "\r\n"
)

DEANONYMIZATION_TABLE = """\
sha256_base64,base64,escaped
YmFy,Sm9obiBEb2UgPGpkb2VAZXhhbXBsZS5vcmc+,John Doe <jdoe@example.org>
YmF6,SmFuZSBEb2UgPGpkb2VAZXhhbXBsZS5jb20+,Jane Doe <jdoe@example.com>
Zm9v,SmFuZSBEb2UgPGpkb2VAZXhhbXBsZS5uZXQ+,Jane Doe <jdoe@example.net>
"""  # noqa

PERSONS = """\
YmF6
YmFy
Zm9v
""".replace(
    "\n", "\r\n"
)

DEANONYMIZED_ORIGIN_CONTRIBUTORS = """\
contributor_id,contributor_base64,contributor_escaped
0,SmFuZSBEb2UgPGpkb2VAZXhhbXBsZS5uZXQ+,Jane Doe <jdoe@example.net>
1,SmFuZSBEb2UgPGpkb2VAZXhhbXBsZS5jb20+,Jane Doe <jdoe@example.com>
2,Sm9obiBEb2UgPGpkb2VAZXhhbXBsZS5vcmc+,John Doe <jdoe@example.org>
""".replace(
    "\n", "\r\n"
)  # noqa


def test_list_origin_contributors(tmpdir):
    tmpdir = Path(tmpdir)

    topological_order_dir = tmpdir
    topological_order_path = (
        topological_order_dir / "topological_order_dfs_backward_rev,rel,snp,ori.csv.zst"
    )
    origin_contributors_path = tmpdir / "origin_contributors.csv.zst"
    origin_urls_path = tmpdir / "origin_urls.csv.zst"

    subprocess.run(
        ["zstdmt", "-o", topological_order_path],
        input=TOPOLOGICAL_ORDER.encode(),
        check=True,
    )

    task = ListOriginContributors(
        local_graph_path=DATASET_DIR / "compressed",
        topological_order_dir=topological_order_dir,
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
    assert header == "sha256_base64,base64,escaped"
    expected_rows = {
        "8qhF7WQ2bmeoRbZipAaqtNw6QdOCDcpggLWCQLzITsI=,"
        "Sm9obiBEb2UgPGpkb2VAZXhhbXBsZS5vcmc+,John Doe <jdoe@example.org>",
        "aZA9TeLhVzqVDQHQOd53UABAZYyek0tY3vTo6VSlA4U=,"
        "SmFuZSBEb2UgPGpkb2VAZXhhbXBsZS5jb20+,Jane Doe <jdoe@example.com>",
        "UaCrgAZBvn1LBd2sAinmdNvAX/G4sjo1aJA9GDd9UUs=,"
        "SmFuZSBEb2UgPGpkb2VAZXhhbXBsZS5uZXQ+,Jane Doe <jdoe@example.net>",
    }

    assert rows.pop() == "", "Missing trailing newline"

    assert set(rows) == expected_rows


def test_deanonymize_origin_contributors(tmpdir):
    tmpdir = Path(tmpdir)

    shutil.copyfile(
        DATASET_DIR / "compressed" / "example.persons.mph",
        tmpdir / "example.persons.mph",
    )

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
