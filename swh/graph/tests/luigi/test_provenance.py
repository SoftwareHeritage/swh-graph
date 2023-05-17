# Copyright (C) 2023  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from pathlib import Path
import subprocess

import pytest

from swh.graph.example_dataset import DATASET_DIR
from swh.graph.luigi.provenance import ListEarliestRevisions, SortRevrelByDate
from swh.graph.luigi.shell import CommandException

SORTED_REVRELS = """\
author_date,SWHID
2005-03-18T05:03:40,swh:1:rev:0000000000000000000000000000000000000003
2005-03-18T11:14:00,swh:1:rev:0000000000000000000000000000000000000009
2005-03-18T17:24:20,swh:1:rev:0000000000000000000000000000000000000013
2005-03-18T20:29:30,swh:1:rev:0000000000000000000000000000000000000018
2009-02-13T23:31:30,swh:1:rel:0000000000000000000000000000000000000010
""".replace(
    "\n", "\r\n"
)

EARLIEST_REVREL_FOR_CNTDIR = """\
author_date,revrel_SWHID,cntdir_SWHID
2005-03-18T05:03:40,swh:1:rev:0000000000000000000000000000000000000003,swh:1:dir:0000000000000000000000000000000000000002
2005-03-18T05:03:40,swh:1:rev:0000000000000000000000000000000000000003,swh:1:cnt:0000000000000000000000000000000000000001
2005-03-18T11:14:00,swh:1:rev:0000000000000000000000000000000000000009,swh:1:dir:0000000000000000000000000000000000000008
2005-03-18T11:14:00,swh:1:rev:0000000000000000000000000000000000000009,swh:1:dir:0000000000000000000000000000000000000006
2005-03-18T11:14:00,swh:1:rev:0000000000000000000000000000000000000009,swh:1:cnt:0000000000000000000000000000000000000007
2005-03-18T11:14:00,swh:1:rev:0000000000000000000000000000000000000009,swh:1:cnt:0000000000000000000000000000000000000005
2005-03-18T11:14:00,swh:1:rev:0000000000000000000000000000000000000009,swh:1:cnt:0000000000000000000000000000000000000004
2005-03-18T17:24:20,swh:1:rev:0000000000000000000000000000000000000013,swh:1:dir:0000000000000000000000000000000000000012
2005-03-18T17:24:20,swh:1:rev:0000000000000000000000000000000000000013,swh:1:cnt:0000000000000000000000000000000000000011
2005-03-18T20:29:30,swh:1:rev:0000000000000000000000000000000000000018,swh:1:dir:0000000000000000000000000000000000000017
2005-03-18T20:29:30,swh:1:rev:0000000000000000000000000000000000000018,swh:1:dir:0000000000000000000000000000000000000016
2005-03-18T20:29:30,swh:1:rev:0000000000000000000000000000000000000018,swh:1:cnt:0000000000000000000000000000000000000014
2005-03-18T20:29:30,swh:1:rev:0000000000000000000000000000000000000018,swh:1:cnt:0000000000000000000000000000000000000015
""".replace(
    "\n", "\r\n"
)  # noqa


def test_sort(tmpdir):
    tmpdir = Path(tmpdir)
    provenance_dir = tmpdir / "provenance"

    task = SortRevrelByDate(
        local_export_path=DATASET_DIR,
        local_graph_path=DATASET_DIR / "compressed",
        graph_name="example",
        provenance_dir=provenance_dir,
    )

    task.run()

    csv_text = subprocess.check_output(
        ["zstdcat", provenance_dir / "revrel_by_author_date.csv.zst"]
    ).decode()

    assert csv_text == SORTED_REVRELS


def test_listearliestrevisions(tmpdir):
    tmpdir = Path(tmpdir)
    provenance_dir = tmpdir / "provenance"
    provenance_dir.mkdir()

    (provenance_dir / "revrel_by_author_date.csv.zst").write_text(SORTED_REVRELS)

    task = ListEarliestRevisions(
        local_export_path=DATASET_DIR,
        local_graph_path=DATASET_DIR / "compressed",
        graph_name="example",
        provenance_dir=provenance_dir,
    )

    task.run()

    csv_text = subprocess.check_output(
        ["zstdcat", provenance_dir / "earliest_revrel_for_cntdir.csv.zst"]
    ).decode()

    assert csv_text == EARLIEST_REVREL_FOR_CNTDIR


def test_listearliestrevisions_disordered(tmpdir):
    tmpdir = Path(tmpdir)
    provenance_dir = tmpdir / "provenance"
    provenance_dir.mkdir()

    (header, *rows) = SORTED_REVRELS.split("\r\n")
    (rows[1], rows[2]) = (rows[2], rows[1])  # swap second and third rows
    nonsorted_revrels = "\r\n".join([header, *rows])

    (provenance_dir / "revrel_by_author_date.csv.zst").write_text(nonsorted_revrels)

    task = ListEarliestRevisions(
        local_export_path=DATASET_DIR,
        local_graph_path=DATASET_DIR / "compressed",
        graph_name="example",
        provenance_dir=provenance_dir,
    )

    with pytest.raises(CommandException, match="java returned: 3"):
        task.run()
