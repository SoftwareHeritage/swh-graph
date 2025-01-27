# Copyright (C) 2023  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import itertools
from pathlib import Path
import subprocess
import textwrap

import pytest
import pyzstd

from swh.graph.example_dataset import DATASET_DIR
from swh.graph.luigi.file_names import (
    ListFilesByName,
    PopularContentNames,
    PopularContentPaths,
)

CSV_HEADER_NAMES = "SWHID,length,filename,occurrences"
CSV_HEADER_PATHS = "SWHID,length,filepath,occurrences"

EXPECTED_LINES_DEPTH1 = """\
swh:1:cnt:0000000000000000000000000000000000000005,1337,parser.c,1
swh:1:cnt:0000000000000000000000000000000000000004,404,README.md,1
swh:1:cnt:0000000000000000000000000000000000000001,42,README.md,2
swh:1:cnt:0000000000000000000000000000000000000007,666,parser.c,1
swh:1:cnt:0000000000000000000000000000000000000011,313,README.md,1
swh:1:cnt:0000000000000000000000000000000000000015,404,TODO.txt,1
swh:1:cnt:0000000000000000000000000000000000000014,14,TODO.txt,1
"""

EXPECTED_LINES_DEPTH2 = """\
swh:1:cnt:0000000000000000000000000000000000000005,1337,tests/parser.c,1
swh:1:cnt:0000000000000000000000000000000000000004,404,tests/README.md,1
swh:1:cnt:0000000000000000000000000000000000000001,42,README.md,1
swh:1:cnt:0000000000000000000000000000000000000007,666,oldproject/parser.c,1
swh:1:cnt:0000000000000000000000000000000000000011,313,README.md,1
swh:1:cnt:0000000000000000000000000000000000000015,404,old/TODO.txt,1
swh:1:cnt:0000000000000000000000000000000000000014,14,TODO.txt,1
"""


@pytest.mark.parametrize("popularity_threshold", [0, 1, 2])
def test_popularcontentnames(tmpdir, popularity_threshold):
    tmpdir = Path(tmpdir)

    popular_contents_path = tmpdir / "popcon"

    task = PopularContentNames(
        local_graph_path=DATASET_DIR / "compressed",
        popular_contents_path=popular_contents_path,
        popularity_threshold=popularity_threshold,
        max_results_per_content=0,
        graph_name="example",
    )

    task.run()

    all_rows = []
    for path in popular_contents_path.iterdir():
        csv_text = pyzstd.decompress(path.read_bytes()).decode()
        if not csv_text:
            continue
        (header, *rows, trailing) = csv_text.split("\r\n")
        assert header == CSV_HEADER_NAMES
        assert trailing == ""
        all_rows.extend(rows)

    expected_lines = set(EXPECTED_LINES_DEPTH1.rstrip().split("\n"))

    if popularity_threshold != 0:
        expected_lines = {
            line
            for line in expected_lines
            if int(line.split(",")[-1]) >= popularity_threshold
        }

    assert list(sorted(all_rows)) == list(sorted(expected_lines))


@pytest.mark.parametrize("depth,subset", itertools.product([1, 2], [None, 1, 2, 3]))
def test_popularcontentpaths(tmpdir, depth, subset):
    tmpdir = Path(tmpdir)

    input_swhids_dir = tmpdir / "inputs"

    input_swhids = [
        line.split(",")[0] for line in EXPECTED_LINES_DEPTH1.split("\n") if line
    ]

    if subset:
        # try disjoint subsets; in case PopularContentPaths fails to remove the header
        # and the MPH mistakenly hashes "SWHID" and produces a bogus content id
        input_swhids = input_swhids[subset::3]

    input_swhids_dir.mkdir()
    with pyzstd.open(input_swhids_dir / "input1.csv.zst", "wt") as f:
        f.write("SWHID\n")
        for swhid in input_swhids[0:5]:
            f.write(swhid + "\n")
    with pyzstd.open(input_swhids_dir / "input2.csv.zst", "wt") as f:
        f.write("SWHID\n")
        for swhid in input_swhids[5:]:
            f.write(swhid + "\n")

    popular_contents_path = tmpdir / "popcon"

    task = PopularContentPaths(
        local_graph_path=DATASET_DIR / "compressed",
        input_swhids=input_swhids_dir,
        popular_contents_path=popular_contents_path,
        max_depth=depth,
        graph_name="example",
    )

    task.run()

    all_rows = []
    for file in popular_contents_path.iterdir():
        csv_text = subprocess.check_output(["zstdcat", file]).decode()
        if not csv_text:
            continue
        (header, *rows, trailing) = csv_text.split("\r\n")

        assert header == CSV_HEADER_PATHS
        assert trailing == ""
        all_rows.extend(rows)

    if depth == 1:
        expected_lines = {
            line
            for line in EXPECTED_LINES_DEPTH1.rstrip().split("\n")
            if line.split(",")[0] in input_swhids
        }
    elif depth == 2:
        expected_lines = {
            line
            for line in EXPECTED_LINES_DEPTH2.rstrip().split("\n")
            if line.split(",")[0] in input_swhids
        }
    else:
        assert False, depth

    # Workaround for non-deterministic result
    all_rows = [
        (
            "swh:1:cnt:0000000000000000000000000000000000000001,42,README.md,1"
            if row
            == "swh:1:cnt:0000000000000000000000000000000000000001,42,oldproject/README.md,1"
            else row
        )
        for row in all_rows
    ]

    assert list(sorted(all_rows)) == list(sorted(expected_lines))


@pytest.mark.parametrize("file_name", ["README.md", "parser.c", "TODO.txt", "tests"])
def test_listfilesbyname(tmpdir, file_name):
    tmpdir = Path(tmpdir)

    output_path = tmpdir / "files"

    task = ListFilesByName(
        local_graph_path=DATASET_DIR / "compressed",
        graph_name="example",
        output_path=output_path,
        file_name=file_name,
    )

    task.run()

    all_rows = []
    for file in output_path.iterdir():
        csv_text = subprocess.check_output(["zstdcat", file]).decode()
        if not csv_text:
            continue

        (header, *rows) = csv_text.split("\r\n")

        assert header == "snp_SWHID,branch_name,dir_SWHID,file_name,cnt_SWHID"
        all_rows.extend(rows)

    if file_name == "README.md":
        assert set(all_rows) == set(
            textwrap.dedent(
                """\
                swh:1:snp:0000000000000000000000000000000000000022,refs/heads/master,swh:1:dir:0000000000000000000000000000000000000008,README.md,swh:1:cnt:0000000000000000000000000000000000000001
                swh:1:snp:0000000000000000000000000000000000000022,refs/heads/master,swh:1:dir:0000000000000000000000000000000000000006,README.md,swh:1:cnt:0000000000000000000000000000000000000004
                swh:1:snp:0000000000000000000000000000000000000020,refs/heads/master,swh:1:dir:0000000000000000000000000000000000000008,README.md,swh:1:cnt:0000000000000000000000000000000000000001
                swh:1:snp:0000000000000000000000000000000000000020,refs/heads/master,swh:1:dir:0000000000000000000000000000000000000006,README.md,swh:1:cnt:0000000000000000000000000000000000000004
                """
            ).split(  # noqa
                "\n"
            )
        )
    elif file_name == "parser.c":
        assert set(all_rows) == set(
            textwrap.dedent(
                """\
                swh:1:snp:0000000000000000000000000000000000000022,refs/heads/master,swh:1:dir:0000000000000000000000000000000000000008,parser.c,swh:1:cnt:0000000000000000000000000000000000000007
                swh:1:snp:0000000000000000000000000000000000000022,refs/heads/master,swh:1:dir:0000000000000000000000000000000000000006,parser.c,swh:1:cnt:0000000000000000000000000000000000000005
                swh:1:snp:0000000000000000000000000000000000000020,refs/heads/master,swh:1:dir:0000000000000000000000000000000000000008,parser.c,swh:1:cnt:0000000000000000000000000000000000000007
                swh:1:snp:0000000000000000000000000000000000000020,refs/heads/master,swh:1:dir:0000000000000000000000000000000000000006,parser.c,swh:1:cnt:0000000000000000000000000000000000000005
                """
            ).split(  # noqa
                "\n"
            )
        )
    elif file_name == "TODO.txt":
        assert all_rows == []
    else:
        assert all_rows == []
