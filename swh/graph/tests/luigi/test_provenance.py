# Copyright (C) 2023  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime
from pathlib import Path
import subprocess
import textwrap
from typing import List

import pytest

from swh.graph.example_dataset import DATASET_DIR
from swh.graph.luigi.provenance import (
    ComputeDirectoryFrontier,
    DeduplicateFrontierDirectories,
    ListContentsInFrontierDirectories,
    ListContentsInRevisionsWithoutFrontier,
    ListDirectoryMaxLeafTimestamp,
    ListEarliestRevisions,
    SortRevrelByDate,
)
from swh.graph.luigi.shell import CommandException

from .test_topology import TOPO_ORDER_BACKWARD

HEADS_ONLY = True

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

if HEADS_ONLY:
    EARLIEST_REVREL_FOR_CNTDIR = """\
author_date,revrel_SWHID,cntdir_SWHID
2005-03-18T11:14:00,swh:1:rev:0000000000000000000000000000000000000009,swh:1:dir:0000000000000000000000000000000000000008
2005-03-18T11:14:00,swh:1:rev:0000000000000000000000000000000000000009,swh:1:dir:0000000000000000000000000000000000000006
2005-03-18T11:14:00,swh:1:rev:0000000000000000000000000000000000000009,swh:1:cnt:0000000000000000000000000000000000000001
2005-03-18T11:14:00,swh:1:rev:0000000000000000000000000000000000000009,swh:1:cnt:0000000000000000000000000000000000000007
2005-03-18T11:14:00,swh:1:rev:0000000000000000000000000000000000000009,swh:1:cnt:0000000000000000000000000000000000000005
2005-03-18T11:14:00,swh:1:rev:0000000000000000000000000000000000000009,swh:1:cnt:0000000000000000000000000000000000000004
2005-03-18T20:29:30,swh:1:rev:0000000000000000000000000000000000000018,swh:1:dir:0000000000000000000000000000000000000017
2005-03-18T20:29:30,swh:1:rev:0000000000000000000000000000000000000018,swh:1:dir:0000000000000000000000000000000000000016
2005-03-18T20:29:30,swh:1:rev:0000000000000000000000000000000000000018,swh:1:cnt:0000000000000000000000000000000000000014
2005-03-18T20:29:30,swh:1:rev:0000000000000000000000000000000000000018,swh:1:cnt:0000000000000000000000000000000000000015
""".replace(
        "\n", "\r\n"
    )  # noqa
else:
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


if HEADS_ONLY:
    DIRECTORY_MAX_LEAF_TIMESTAMPS = """\
    max_author_date,dir_SWHID
2005-03-18T11:14:00,swh:1:dir:0000000000000000000000000000000000000002
2005-03-18T11:14:00,swh:1:dir:0000000000000000000000000000000000000008
2005-03-18T11:14:00,swh:1:dir:0000000000000000000000000000000000000006
2005-03-18T20:29:30,swh:1:dir:0000000000000000000000000000000000000017
2005-03-18T20:29:30,swh:1:dir:0000000000000000000000000000000000000016
    """.replace(
        "\n", "\r\n"
    )
else:
    DIRECTORY_MAX_LEAF_TIMESTAMPS = """\
max_author_date,dir_SWHID
2005-03-18T05:03:40,swh:1:dir:0000000000000000000000000000000000000002
2005-03-18T11:14:00,swh:1:dir:0000000000000000000000000000000000000008
2005-03-18T11:14:00,swh:1:dir:0000000000000000000000000000000000000006
2005-03-18T17:24:20,swh:1:dir:0000000000000000000000000000000000000012
2005-03-18T20:29:30,swh:1:dir:0000000000000000000000000000000000000017
2005-03-18T20:29:30,swh:1:dir:0000000000000000000000000000000000000016
""".replace(
        "\n", "\r\n"
    )

CONTENT_TIMESTAMPS = """\
2005-03-18T05:03:40,swh:1:cnt:0000000000000000000000000000000000000001
2005-03-18T11:14:00,swh:1:cnt:0000000000000000000000000000000000000004
2005-03-18T11:14:00,swh:1:cnt:0000000000000000000000000000000000000005
2005-03-18T11:14:00,swh:1:cnt:0000000000000000000000000000000000000007
2005-03-18T17:24:20,swh:1:cnt:0000000000000000000000000000000000000011
2005-03-18T20:29:30,swh:1:cnt:0000000000000000000000000000000000000014
2005-03-18T20:29:30,swh:1:cnt:0000000000000000000000000000000000000015
""".replace(
    "\n", "\r\n"
)

if HEADS_ONLY:
    FRONTIER_DIRECTORIES = """\
max_author_date,frontier_dir_SWHID,rev_author_date,rev_SWHID,path
1111144440,swh:1:dir:0000000000000000000000000000000000000006,2009-02-13T23:31:30Z,swh:1:rel:0000000000000000000000000000000000000010,tests/
""".replace(
        "\n", "\r\n"
    )
    DEDUPLICATED_FRONTIER_DIRECTORIES = """\
frontier_dir_SWHID
swh:1:dir:0000000000000000000000000000000000000006
"""
else:
    FRONTIER_DIRECTORIES = """\
max_author_date,frontier_dir_SWHID,rev_author_date,rev_SWHID,path
1111144440,swh:1:dir:0000000000000000000000000000000000000008,2005-03-18T17:24:20Z,swh:1:rev:0000000000000000000000000000000000000013,oldproject/
""".replace(
        "\n", "\r\n"
    )
    DEDUPLICATED_FRONTIER_DIRECTORIES = """\
frontier_dir_SWHID
swh:1:dir:0000000000000000000000000000000000000008
"""

if HEADS_ONLY:
    CONTENTS_IN_REVISIONS_WITHOUT_FRONTIERS = """\
cnt_SWHID,rev_author_date,rev_SWHID,path
swh:1:cnt:0000000000000000000000000000000000000001,2005-03-18T11:14:00Z,swh:1:rev:0000000000000000000000000000000000000009,README.md
swh:1:cnt:0000000000000000000000000000000000000007,2005-03-18T11:14:00Z,swh:1:rev:0000000000000000000000000000000000000009,parser.c
swh:1:cnt:0000000000000000000000000000000000000014,2005-03-18T20:29:30Z,swh:1:rev:0000000000000000000000000000000000000018,TODO.txt
swh:1:cnt:0000000000000000000000000000000000000015,2005-03-18T20:29:30Z,swh:1:rev:0000000000000000000000000000000000000018,old/TODO.txt
""".replace(
        "\n", "\r\n"
    )
else:
    CONTENTS_IN_REVISIONS_WITHOUT_FRONTIERS = """\
cnt_SWHID,rev_author_date,rev_SWHID,path
swh:1:cnt:0000000000000000000000000000000000000001,2005-03-18T05:03:40Z,swh:1:rev:0000000000000000000000000000000000000003,README.md
swh:1:cnt:0000000000000000000000000000000000000011,2005-03-18T17:24:20Z,swh:1:rev:0000000000000000000000000000000000000013,README.md
swh:1:cnt:0000000000000000000000000000000000000014,2005-03-18T20:29:30Z,swh:1:rev:0000000000000000000000000000000000000018,TODO.txt
swh:1:cnt:0000000000000000000000000000000000000015,2005-03-18T20:29:30Z,swh:1:rev:0000000000000000000000000000000000000018,old/TODO.txt
""".replace(
        "\n", "\r\n"
    )

if HEADS_ONLY:
    CONTENTS_IN_FRONTIER_DIRECTORIES = """\
cnt_SWHID,dir_SWHID,path
swh:1:cnt:0000000000000000000000000000000000000004,swh:1:dir:0000000000000000000000000000000000000006,README.md
swh:1:cnt:0000000000000000000000000000000000000005,swh:1:dir:0000000000000000000000000000000000000006,parser.c
""".replace(
        "\n", "\r\n"
    )
else:
    CONTENTS_IN_FRONTIER_DIRECTORIES = """\
cnt_SWHID,dir_SWHID,path
swh:1:cnt:0000000000000000000000000000000000000001,swh:1:dir:0000000000000000000000000000000000000008,README.md
swh:1:cnt:0000000000000000000000000000000000000007,swh:1:dir:0000000000000000000000000000000000000008,parser.c
swh:1:cnt:0000000000000000000000000000000000000005,swh:1:dir:0000000000000000000000000000000000000008,tests/parser.c
swh:1:cnt:0000000000000000000000000000000000000004,swh:1:dir:0000000000000000000000000000000000000008,tests/README.md
""".replace(
        "\n", "\r\n"
    )


def timestamps_bin_to_csv(bin_timestamps_path: Path) -> List[str]:
    """Given a .bin file containing timestamps, zips it with the graph's
    .node2swhid.bin and convert timestamps to date, in order to get a CSV
    containing equivalent data.
    """

    bin_timestamps = bin_timestamps_path.read_bytes()

    assert (
        len(bin_timestamps) % 8 == 0
    ), f"{bin_timestamps_path}'s size is not a multiple of sizeof(long)"

    bin_swhids = (DATASET_DIR / "compressed" / "example.node2swhid.bin").read_bytes()

    assert (
        len(bin_swhids) % 22 == 0
    ), "example.node2swhid.bin's size is not a multiple of 22"

    # See java/src/main/java/org/softwareheritage/graph/SwhType.java
    type_map = {0: "cnt", 1: "dir", 2: "ori", 3: "rev", 4: "rel", 5: "snp"}
    swhids = [
        f"swh:{bin_swhids[i]}:{type_map[bin_swhids[i+1]]}:{bin_swhids[i+2:i+22].hex()}"
        for i in range(0, len(bin_swhids), 22)
    ]

    dates = [
        datetime.datetime.fromtimestamp(
            int.from_bytes(bin_timestamps[i : i + 8], byteorder="big")
        )
        .astimezone(datetime.timezone.utc)
        .isoformat()
        .split("+")[0]
        if bin_timestamps[i : i + 8] != b"\x80" + b"\x00" * 7  # min long, meaning no TS
        else None
        for i in range(0, len(bin_timestamps), 8)
    ]

    assert len(swhids) == len(dates)

    return [
        f"{date},{swhid}" for (date, swhid) in zip(dates, swhids) if date is not None
    ]


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

    rows = set(timestamps_bin_to_csv(provenance_dir / "earliest_timestamps.bin"))
    (header, *expected_rows) = [
        f"{author_date},{cntdir_SWHID}"
        for (author_date, revrel_SWHID, cntdir_SWHID) in (
            row.split(",") for row in EARLIEST_REVREL_FOR_CNTDIR.rstrip().split("\r\n")
        )
    ]
    assert rows == set(expected_rows)


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


def test_listdirectorymaxleaftimestamp(tmpdir):
    tmpdir = Path(tmpdir)
    topology_dir = tmpdir / "topology"
    provenance_dir = tmpdir / "provenance"

    topology_dir.mkdir()
    toposort_path = (
        topology_dir / "topological_order_dfs_backward_dir,rev,rel,snp,ori.csv.zst"
    )
    toposort_path.write_text(TOPO_ORDER_BACKWARD)

    # Generate the binary file, used as input by ComputeDirectoryFrontier
    test_listearliestrevisions(tmpdir)

    (provenance_dir / "earliest_revrel_for_cntdir.csv.zst").write_text(
        EARLIEST_REVREL_FOR_CNTDIR
    )

    task = ListDirectoryMaxLeafTimestamp(
        local_export_path=DATASET_DIR,
        local_graph_path=DATASET_DIR / "compressed",
        graph_name="example",
        provenance_dir=provenance_dir,
        topological_order_dir=topology_dir,
    )

    task.run()

    rows = set(timestamps_bin_to_csv(provenance_dir / "max_leaf_timestamps.bin"))
    (header, *expected_rows) = DIRECTORY_MAX_LEAF_TIMESTAMPS.rstrip().split("\r\n")
    assert rows == set(expected_rows)


def test_computedirectoryfrontier(tmpdir):
    tmpdir = Path(tmpdir)
    topology_dir = tmpdir / "topology"
    provenance_dir = tmpdir / "provenance"

    # Generate the binary file, used as input by ComputeDirectoryFrontier
    test_listdirectorymaxleaftimestamp(tmpdir)

    task = ComputeDirectoryFrontier(
        local_export_path=DATASET_DIR,
        local_graph_path=DATASET_DIR / "compressed",
        graph_name="example",
        provenance_dir=provenance_dir,
        topological_order_dir=topology_dir,
        batch_size=100,  # faster
    )

    task.run()

    csv_text = subprocess.check_output(
        ["zstdcat", provenance_dir / "directory_frontier.csv.zst"]
    ).decode()

    assert csv_text == FRONTIER_DIRECTORIES


@pytest.mark.parametrize("duplicates_in_input", [True, False])
def test_deduplicatefrontierdirectories(tmpdir, duplicates_in_input):
    tmpdir = Path(tmpdir)

    provenance_dir = tmpdir / "provenance"
    topology_dir = tmpdir / "topology"

    provenance_dir.mkdir()

    (provenance_dir / "directory_frontier.csv.zst").write_text(
        FRONTIER_DIRECTORIES * (3 if duplicates_in_input else 1)
    )

    task = DeduplicateFrontierDirectories(
        local_export_path=DATASET_DIR,
        local_graph_path=DATASET_DIR / "compressed",
        graph_name="example",
        provenance_dir=provenance_dir,
        topological_order_dir=topology_dir,
    )

    task.run()

    csv_text = subprocess.check_output(
        ["zstdcat", provenance_dir / "directory_frontier.deduplicated.csv.zst"]
    ).decode()

    assert csv_text == DEDUPLICATED_FRONTIER_DIRECTORIES


def test_listcontentsinrevisionswithoutfrontier(tmpdir):
    tmpdir = Path(tmpdir)

    provenance_dir = tmpdir / "provenance"
    topology_dir = tmpdir / "topology"

    provenance_dir.mkdir()

    (provenance_dir / "directory_frontier.deduplicated.csv.zst").write_text(
        DEDUPLICATED_FRONTIER_DIRECTORIES
    )

    task = ListContentsInRevisionsWithoutFrontier(
        local_export_path=DATASET_DIR,
        local_graph_path=DATASET_DIR / "compressed",
        graph_name="example",
        provenance_dir=provenance_dir,
        topological_order_dir=topology_dir,
        batch_size=100,  # faster
    )

    task.run()

    csv_text = subprocess.check_output(
        ["zstdcat", provenance_dir / "contents_in_revisions_without_frontiers.csv.zst"]
    ).decode()

    (header, *rows) = csv_text.split("\r\n")
    (expected_header, *expected_rows) = CONTENTS_IN_REVISIONS_WITHOUT_FRONTIERS.split(
        "\r\n"
    )
    assert header == expected_header
    assert sorted(rows) == sorted(expected_rows)


def test_listcontentsindirectories(tmpdir):
    tmpdir = Path(tmpdir)

    provenance_dir = tmpdir / "provenance"
    topology_dir = tmpdir / "topology"

    provenance_dir.mkdir()

    (provenance_dir / "directory_frontier.deduplicated.csv.zst").write_text(
        DEDUPLICATED_FRONTIER_DIRECTORIES
    )

    task = ListContentsInFrontierDirectories(
        local_export_path=DATASET_DIR,
        local_graph_path=DATASET_DIR / "compressed",
        graph_name="example",
        provenance_dir=provenance_dir,
        topological_order_dir=topology_dir,
    )

    task.run()

    csv_text = subprocess.check_output(
        ["zstdcat", provenance_dir / "contents_in_frontier_directories.csv.zst"]
    ).decode()

    (header, *rows) = csv_text.split("\r\n")
    (expected_header, *expected_rows) = CONTENTS_IN_FRONTIER_DIRECTORIES.split("\r\n")
    assert header == expected_header
    assert sorted(rows) == sorted(expected_rows)


@pytest.mark.parametrize("duplicates", [True, False])
def test_listcontentsindirectories_root(tmpdir, duplicates):
    """Tests ListContentsInFrontierDirectories but on root directories instead
    of frontier directories"""
    tmpdir = Path(tmpdir)

    provenance_dir = tmpdir / "provenance"
    topology_dir = tmpdir / "topology"

    provenance_dir.mkdir()

    (provenance_dir / "directory_frontier.deduplicated.csv.zst").write_text(
        "dir_SWHID\r\n"
        + (
            "swh:1:dir:0000000000000000000000000000000000000012\n"
            "swh:1:dir:0000000000000000000000000000000000000017\n"
        )
        * (2 if duplicates else 1)
    )

    task = ListContentsInFrontierDirectories(
        local_export_path=DATASET_DIR,
        local_graph_path=DATASET_DIR / "compressed",
        graph_name="example",
        provenance_dir=provenance_dir,
        topological_order_dir=topology_dir,
    )

    task.run()

    csv_text = subprocess.check_output(
        ["zstdcat", provenance_dir / "contents_in_frontier_directories.csv.zst"]
    ).decode()

    (header, *rows) = csv_text.split("\r\n")

    assert header == "cnt_SWHID,dir_SWHID,path"
    assert sorted(rows) == sorted(
        textwrap.dedent(
            """\
            swh:1:cnt:0000000000000000000000000000000000000011,swh:1:dir:0000000000000000000000000000000000000012,README.md
            swh:1:cnt:0000000000000000000000000000000000000001,swh:1:dir:0000000000000000000000000000000000000012,oldproject/README.md
            swh:1:cnt:0000000000000000000000000000000000000007,swh:1:dir:0000000000000000000000000000000000000012,oldproject/parser.c
            swh:1:cnt:0000000000000000000000000000000000000005,swh:1:dir:0000000000000000000000000000000000000012,oldproject/tests/parser.c
            swh:1:cnt:0000000000000000000000000000000000000004,swh:1:dir:0000000000000000000000000000000000000012,oldproject/tests/README.md
            swh:1:cnt:0000000000000000000000000000000000000014,swh:1:dir:0000000000000000000000000000000000000017,TODO.txt
            swh:1:cnt:0000000000000000000000000000000000000015,swh:1:dir:0000000000000000000000000000000000000017,old/TODO.txt
            """  # noqa
        ).split("\n")
    )
