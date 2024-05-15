# Copyright (C) 2023-2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import csv
import datetime
import io
from pathlib import Path
import textwrap
from typing import List

import datafusion
import pyarrow.dataset

from swh.graph.example_dataset import DATASET, DATASET_DIR
from swh.graph.luigi.provenance import (
    ComputeDirectoryFrontier,
    ComputeEarliestTimestamps,
    ListContentsInFrontierDirectories,
    ListContentsInRevisionsWithoutFrontier,
    ListDirectoryMaxLeafTimestamp,
    ListFrontierDirectoriesInRevisions,
    ListProvenanceNodes,
)

HEADS_ONLY = True

ALL_NODES = [str(node.swhid()) for node in DATASET if hasattr(node, "swhid")]

if HEADS_ONLY:
    PROVENANCE_NODES = [
        "swh:1:cnt:0000000000000000000000000000000000000001",
        "swh:1:cnt:0000000000000000000000000000000000000004",
        "swh:1:cnt:0000000000000000000000000000000000000005",
        "swh:1:cnt:0000000000000000000000000000000000000007",
        "swh:1:cnt:0000000000000000000000000000000000000014",
        "swh:1:cnt:0000000000000000000000000000000000000015",
        "swh:1:dir:0000000000000000000000000000000000000006",
        "swh:1:dir:0000000000000000000000000000000000000008",
        "swh:1:dir:0000000000000000000000000000000000000016",
        "swh:1:dir:0000000000000000000000000000000000000017",
        "swh:1:rel:0000000000000000000000000000000000000010",
        "swh:1:rel:0000000000000000000000000000000000000019",
        "swh:1:rel:0000000000000000000000000000000000000021",
        "swh:1:rev:0000000000000000000000000000000000000009",
        "swh:1:rev:0000000000000000000000000000000000000018",
    ]
else:
    PROVENANCE_NODES = ALL_NODES

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
    FRONTIER_DIRECTORIES = ["swh:1:dir:0000000000000000000000000000000000000006"]
    FRONTIER_DIRECTORIES_IN_REVISIONS = """\
dir_max_author_date,dir_swhid,revrel_author_date,revrel_swhid,path
1111144440,swh:1:dir:0000000000000000000000000000000000000006,2005-03-18T11:14:00Z,swh:1:rev:0000000000000000000000000000000000000009,tests/
1111144440,swh:1:dir:0000000000000000000000000000000000000006,2009-02-13T23:31:30Z,swh:1:rel:0000000000000000000000000000000000000010,tests/
""".replace(
        "\n", "\r\n"
    )
else:
    FRONTIER_DIRECTORIES = ["swh:1:dir:0000000000000000000000000000000000000008"]

if HEADS_ONLY:
    CONTENTS_IN_REVISIONS_WITHOUT_FRONTIERS = """\
cnt_swhid,revrel_author_date,revrel_swhid,path
swh:1:cnt:0000000000000000000000000000000000000001,2005-03-18T11:14:00Z,swh:1:rev:0000000000000000000000000000000000000009,README.md
swh:1:cnt:0000000000000000000000000000000000000001,2009-02-13T23:31:30Z,swh:1:rel:0000000000000000000000000000000000000010,README.md
swh:1:cnt:0000000000000000000000000000000000000007,2005-03-18T11:14:00Z,swh:1:rev:0000000000000000000000000000000000000009,parser.c
swh:1:cnt:0000000000000000000000000000000000000007,2009-02-13T23:31:30Z,swh:1:rel:0000000000000000000000000000000000000010,parser.c
swh:1:cnt:0000000000000000000000000000000000000014,2005-03-18T20:29:30Z,swh:1:rev:0000000000000000000000000000000000000018,TODO.txt
swh:1:cnt:0000000000000000000000000000000000000015,2005-03-18T20:29:30Z,swh:1:rev:0000000000000000000000000000000000000018,old/TODO.txt
""".replace(
        "\n", "\r\n"
    )
else:
    CONTENTS_IN_REVISIONS_WITHOUT_FRONTIERS = """\
cnt_swhid,revrel_author_date,revrel_swhid,path
swh:1:cnt:0000000000000000000000000000000000000001,2005-03-18T05:03:40Z,swh:1:rev:0000000000000000000000000000000000000003,README.md
swh:1:cnt:0000000000000000000000000000000000000011,2005-03-18T17:24:20Z,swh:1:rev:0000000000000000000000000000000000000013,README.md
swh:1:cnt:0000000000000000000000000000000000000014,2005-03-18T20:29:30Z,swh:1:rev:0000000000000000000000000000000000000018,TODO.txt
swh:1:cnt:0000000000000000000000000000000000000015,2005-03-18T20:29:30Z,swh:1:rev:0000000000000000000000000000000000000018,old/TODO.txt
""".replace(
        "\n", "\r\n"
    )

if HEADS_ONLY:
    CONTENTS_IN_FRONTIER_DIRECTORIES = """\
cnt_swhid,dir_swhid,path
swh:1:cnt:0000000000000000000000000000000000000004,swh:1:dir:0000000000000000000000000000000000000006,README.md
swh:1:cnt:0000000000000000000000000000000000000005,swh:1:dir:0000000000000000000000000000000000000006,parser.c
""".replace(
        "\n", "\r\n"
    )
else:
    CONTENTS_IN_FRONTIER_DIRECTORIES = """\
cnt_swhid,dir_swhid,path
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


def write_directory_frontier(provenance_dir, swhids=FRONTIER_DIRECTORIES):
    ctx = datafusion.SessionContext()
    ctx.register_dataset(
        "nodes", pyarrow.dataset.dataset(provenance_dir / "nodes", format="parquet")
    )
    ctx.from_pydict(
        {"swhid": swhids},
        name="directory_frontier_swhids",
    )

    target_path = provenance_dir / "directory_frontier"
    # needed for datafusion >=36, or it creates a file.
    # https://github.com/apache/arrow-datafusion/issues/9684
    target_path.mkdir()

    ctx.sql(
        """
        SELECT id
        FROM nodes
        INNER JOIN directory_frontier_swhids ON (
            directory_frontier_swhids.swhid = concat(
                'swh:1:',
                nodes.type,
                ':',
                encode(CAST(nodes.sha1_git AS bytea), 'hex')
            )
        )
        """
    ).write_parquet(str(target_path))


def test_listprovenancenodes(tmpdir, provenance_node_filter="heads"):
    tmpdir = Path(tmpdir)
    provenance_dir = tmpdir / "provenance"
    provenance_dir.mkdir(exist_ok=True)

    task = ListProvenanceNodes(
        local_export_path=DATASET_DIR,
        local_graph_path=DATASET_DIR / "compressed",
        graph_name="example",
        provenance_dir=provenance_dir,
        provenance_node_filter=provenance_node_filter,
    )

    task.run()

    dataset = pyarrow.dataset.dataset(provenance_dir / "nodes", format="parquet")
    rows = dataset.to_table().to_pylist()
    node_ids = [row["id"] for row in rows]
    assert sorted(node_ids) == sorted(set(node_ids)), "node ids are not unique"
    swhids = sorted(f"swh:1:{row['type']}:{row['sha1_git'].hex()}" for row in rows)
    if provenance_node_filter == "heads":
        assert swhids == PROVENANCE_NODES
    elif provenance_node_filter == "all":
        assert swhids == sorted(ALL_NODES)


def test_computeearliesttimestamps(tmpdir):
    tmpdir = Path(tmpdir)
    provenance_dir = tmpdir / "provenance"
    provenance_dir.mkdir()

    task = ComputeEarliestTimestamps(
        local_export_path=DATASET_DIR,
        local_graph_path=DATASET_DIR / "compressed",
        graph_name="example",
        provenance_dir=provenance_dir,
    )

    task.run()

    (
        expected_header,
        *expected_rows,
        trailing,
    ) = EARLIEST_REVREL_FOR_CNTDIR.split("\r\n")
    assert trailing == ""

    rows = set(timestamps_bin_to_csv(provenance_dir / "earliest_timestamps.bin"))
    (header, *expected_rows) = [
        f"{author_date},{cntdir_SWHID}"
        for (author_date, revrel_SWHID, cntdir_SWHID) in (
            row.split(",") for row in EARLIEST_REVREL_FOR_CNTDIR.rstrip().split("\r\n")
        )
    ]
    assert rows == set(expected_rows)


def test_listdirectorymaxleaftimestamp(tmpdir):
    tmpdir = Path(tmpdir)
    provenance_dir = tmpdir / "provenance"

    # Generate the binary file, used as input by ComputeDirectoryFrontier
    test_computeearliesttimestamps(tmpdir)

    task = ListDirectoryMaxLeafTimestamp(
        local_export_path=DATASET_DIR,
        local_graph_path=DATASET_DIR / "compressed",
        graph_name="example",
        provenance_dir=provenance_dir,
    )

    task.run()

    rows = set(timestamps_bin_to_csv(provenance_dir / "max_leaf_timestamps.bin"))
    (header, *expected_rows) = DIRECTORY_MAX_LEAF_TIMESTAMPS.rstrip().split("\r\n")
    assert rows == set(expected_rows)


def test_computedirectoryfrontier(tmpdir):
    tmpdir = Path(tmpdir)
    provenance_dir = tmpdir / "provenance"

    # Generate the binary file, used as input by ComputeDirectoryFrontier
    test_listdirectorymaxleaftimestamp(tmpdir)

    task = ComputeDirectoryFrontier(
        local_export_path=DATASET_DIR,
        local_graph_path=DATASET_DIR / "compressed",
        graph_name="example",
        provenance_dir=provenance_dir,
        batch_size=100,  # faster
    )

    task.run()

    # Generate the 'nodes' table
    test_listprovenancenodes(tmpdir)

    ctx = datafusion.SessionContext()

    ctx.register_dataset(
        "nodes", pyarrow.dataset.dataset(provenance_dir / "nodes", format="parquet")
    )
    ctx.register_dataset(
        "directory_frontier",
        pyarrow.dataset.dataset(
            provenance_dir / "directory_frontier", format="parquet"
        ),
    )

    swhids = ctx.sql(
        """
        SELECT concat(
            'swh:1:',
            nodes.type,
            ':',
            encode(CAST(nodes.sha1_git AS bytea), 'hex')
        ) AS swhid
        FROM directory_frontier
        LEFT JOIN nodes ON (directory_frontier.id=nodes.id)
        """
    ).to_pydict()["swhid"]

    assert swhids == FRONTIER_DIRECTORIES


def test_listfrontierdirectoriesinrevisions(tmpdir):
    tmpdir = Path(tmpdir)
    provenance_dir = tmpdir / "provenance"

    # Generate the binary file, used as input by ListFrontierDirectoriesInRevisions
    test_listdirectorymaxleaftimestamp(tmpdir)

    # Generate the 'nodes' table
    test_listprovenancenodes(tmpdir)

    write_directory_frontier(provenance_dir)

    task = ListFrontierDirectoriesInRevisions(
        local_export_path=DATASET_DIR,
        local_graph_path=DATASET_DIR / "compressed",
        graph_name="example",
        provenance_dir=provenance_dir,
        batch_size=100,  # faster
    )

    task.run()

    ctx = datafusion.SessionContext()

    ctx.register_dataset(
        "nodes", pyarrow.dataset.dataset(provenance_dir / "nodes", format="parquet")
    )
    ctx.register_dataset(
        "dir_in_revrel",
        pyarrow.dataset.dataset(
            provenance_dir / "frontier_directories_in_revisions", format="parquet"
        ),
    )

    rows = ctx.sql(
        """
        SELECT
            CAST(dir_max_author_date AS text) AS dir_max_author_date,
            concat(
                'swh:1:',
                dir_nodes.type,
                ':',
                encode(CAST(dir_nodes.sha1_git AS bytea), 'hex')
            ) AS dir_SWHID,
            concat(CAST(from_unixtime(revrel_author_date) AS text), 'Z') AS revrel_author_date,
            concat(
                'swh:1:',
                revrel_nodes.type,
                ':',
                encode(CAST(revrel_nodes.sha1_git AS bytea), 'hex')
            ) AS revrel_SWHID,
            CAST(path AS text) AS path
        FROM dir_in_revrel
        LEFT JOIN nodes AS dir_nodes ON (dir_in_revrel.dir=dir_nodes.id)
        LEFT JOIN nodes AS revrel_nodes ON (dir_in_revrel.revrel=revrel_nodes.id)
        """
    ).to_pylist()

    expected_rows = list(csv.DictReader(io.StringIO(FRONTIER_DIRECTORIES_IN_REVISIONS)))
    rows.sort(key=lambda d: tuple(sorted(d.items())))
    expected_rows.sort(key=lambda d: tuple(sorted(d.items())))

    assert rows == expected_rows


def test_listcontentsinrevisionswithoutfrontier(tmpdir):
    tmpdir = Path(tmpdir)

    provenance_dir = tmpdir / "provenance"

    provenance_dir.mkdir()

    # Generate the 'nodes' table
    test_listprovenancenodes(tmpdir)

    write_directory_frontier(provenance_dir)

    task = ListContentsInRevisionsWithoutFrontier(
        local_export_path=DATASET_DIR,
        local_graph_path=DATASET_DIR / "compressed",
        graph_name="example",
        provenance_dir=provenance_dir,
        batch_size=100,  # faster
    )

    task.run()

    ctx = datafusion.SessionContext()

    ctx.register_dataset(
        "nodes", pyarrow.dataset.dataset(provenance_dir / "nodes", format="parquet")
    )
    ctx.register_dataset(
        "cnt_in_revrel",
        pyarrow.dataset.dataset(
            provenance_dir / "contents_in_revisions_without_frontiers", format="parquet"
        ),
    )

    rows = ctx.sql(
        """
        SELECT
            concat(
                'swh:1:',
                cnt_nodes.type,
                ':',
                encode(CAST(cnt_nodes.sha1_git AS bytea), 'hex')
            ) AS cnt_SWHID,
            concat(CAST(from_unixtime(revrel_author_date) AS text), 'Z') AS revrel_author_date,
            concat(
                'swh:1:',
                revrel_nodes.type,
                ':',
                encode(CAST(revrel_nodes.sha1_git AS bytea), 'hex')
            ) AS revrel_SWHID,
            CAST(path AS text) AS path
        FROM cnt_in_revrel
        LEFT JOIN nodes AS cnt_nodes ON (cnt_in_revrel.cnt=cnt_nodes.id)
        LEFT JOIN nodes AS revrel_nodes ON (cnt_in_revrel.revrel=revrel_nodes.id)
        """
    ).to_pylist()

    expected_rows = list(
        csv.DictReader(io.StringIO(CONTENTS_IN_REVISIONS_WITHOUT_FRONTIERS))
    )
    rows.sort(key=lambda d: tuple(sorted(d.items())))
    expected_rows.sort(key=lambda d: tuple(sorted(d.items())))

    assert rows == expected_rows


def test_listcontentsindirectories(tmpdir):
    tmpdir = Path(tmpdir)

    provenance_dir = tmpdir / "provenance"

    provenance_dir.mkdir()

    # Generate the 'nodes' table
    test_listprovenancenodes(tmpdir)

    write_directory_frontier(provenance_dir)

    task = ListContentsInFrontierDirectories(
        local_export_path=DATASET_DIR,
        local_graph_path=DATASET_DIR / "compressed",
        graph_name="example",
        provenance_dir=provenance_dir,
    )

    task.run()

    ctx = datafusion.SessionContext()

    ctx.register_dataset(
        "nodes", pyarrow.dataset.dataset(provenance_dir / "nodes", format="parquet")
    )
    ctx.register_dataset(
        "cnt_in_dir",
        pyarrow.dataset.dataset(
            provenance_dir / "contents_in_frontier_directories", format="parquet"
        ),
    )

    rows = ctx.sql(
        """
        SELECT
            concat(
                'swh:1:',
                cnt_nodes.type,
                ':',
                encode(CAST(cnt_nodes.sha1_git AS bytea), 'hex')
            ) AS cnt_SWHID,
            concat(
                'swh:1:',
                dir_nodes.type,
                ':',
                encode(CAST(dir_nodes.sha1_git AS bytea), 'hex')
            ) AS dir_SWHID,
            CAST(path AS text) AS path
        FROM cnt_in_dir
        LEFT JOIN nodes AS cnt_nodes ON (cnt_in_dir.cnt=cnt_nodes.id)
        LEFT JOIN nodes AS dir_nodes ON (cnt_in_dir.dir=dir_nodes.id)
        """
    ).to_pylist()

    expected_rows = list(csv.DictReader(io.StringIO(CONTENTS_IN_FRONTIER_DIRECTORIES)))
    rows.sort(key=lambda d: tuple(sorted(d.items())))
    expected_rows.sort(key=lambda d: tuple(sorted(d.items())))

    assert rows == expected_rows


def test_listcontentsindirectories_root(tmpdir):
    """Tests ListContentsInFrontierDirectories but on root directories instead
    of frontier directories"""
    tmpdir = Path(tmpdir)

    provenance_dir = tmpdir / "provenance"

    provenance_dir.mkdir()

    # Generate the 'nodes' table
    test_listprovenancenodes(tmpdir, provenance_node_filter="all")

    write_directory_frontier(
        provenance_dir,
        swhids=[
            "swh:1:dir:0000000000000000000000000000000000000012",
            "swh:1:dir:0000000000000000000000000000000000000017",
        ],
    )

    task = ListContentsInFrontierDirectories(
        local_export_path=DATASET_DIR,
        local_graph_path=DATASET_DIR / "compressed",
        graph_name="example",
        provenance_dir=provenance_dir,
    )

    task.run()

    expected_csv = textwrap.dedent(
        """\
        cnt_swhid,dir_swhid,path
        swh:1:cnt:0000000000000000000000000000000000000011,swh:1:dir:0000000000000000000000000000000000000012,README.md
        swh:1:cnt:0000000000000000000000000000000000000001,swh:1:dir:0000000000000000000000000000000000000012,oldproject/README.md
        swh:1:cnt:0000000000000000000000000000000000000007,swh:1:dir:0000000000000000000000000000000000000012,oldproject/parser.c
        swh:1:cnt:0000000000000000000000000000000000000005,swh:1:dir:0000000000000000000000000000000000000012,oldproject/tests/parser.c
        swh:1:cnt:0000000000000000000000000000000000000004,swh:1:dir:0000000000000000000000000000000000000012,oldproject/tests/README.md
        swh:1:cnt:0000000000000000000000000000000000000014,swh:1:dir:0000000000000000000000000000000000000017,TODO.txt
        swh:1:cnt:0000000000000000000000000000000000000015,swh:1:dir:0000000000000000000000000000000000000017,old/TODO.txt"""  # noqa
    )

    ctx = datafusion.SessionContext()

    ctx.register_dataset(
        "nodes", pyarrow.dataset.dataset(provenance_dir / "nodes", format="parquet")
    )
    ctx.register_dataset(
        "cnt_in_dir",
        pyarrow.dataset.dataset(
            provenance_dir / "contents_in_frontier_directories", format="parquet"
        ),
    )

    rows = ctx.sql(
        """
        SELECT
            concat(
                'swh:1:',
                cnt_nodes.type,
                ':',
                encode(CAST(cnt_nodes.sha1_git AS bytea), 'hex')
            ) AS cnt_SWHID,
            concat(
                'swh:1:',
                dir_nodes.type,
                ':',
                encode(CAST(dir_nodes.sha1_git AS bytea), 'hex')
            ) AS dir_SWHID,
            CAST(path AS text) AS path
        FROM cnt_in_dir
        LEFT JOIN nodes AS cnt_nodes ON (cnt_in_dir.cnt=cnt_nodes.id)
        LEFT JOIN nodes AS dir_nodes ON (cnt_in_dir.dir=dir_nodes.id)
        """
    ).to_pylist()

    expected_rows = list(csv.DictReader(io.StringIO(expected_csv)))
    rows.sort(key=lambda d: tuple(sorted(d.items())))
    expected_rows.sort(key=lambda d: tuple(sorted(d.items())))

    assert rows == expected_rows
