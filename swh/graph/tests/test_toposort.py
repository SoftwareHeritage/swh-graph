# Copyright (C) 2022-2023  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import itertools
from pathlib import Path
import subprocess

import pytest

from swh.graph.luigi.misc_datasets import CountPaths, TopoSort

DATA_DIR = Path(__file__).parents[0] / "dataset"


# FIXME: the order of sample ancestors should not be hardcoded
# FIXME: swh:1:snp:0000000000000000000000000000000000000022,3,1,swh has three possible
# sample ancestors; they should not be hardecoded here
TOPO_ORDER_BACKWARD = """\
SWHID,ancestors,successors,sample_ancestor1,sample_ancestor2
swh:1:rev:0000000000000000000000000000000000000003,0,1,,
swh:1:rev:0000000000000000000000000000000000000009,1,4,swh:1:rev:0000000000000000000000000000000000000003,
swh:1:rel:0000000000000000000000000000000000000010,1,2,swh:1:rev:0000000000000000000000000000000000000009,
swh:1:snp:0000000000000000000000000000000000000020,2,1,swh:1:rev:0000000000000000000000000000000000000009,swh:1:rel:0000000000000000000000000000000000000010
swh:1:ori:83404f995118bd25774f4ac14422a8f175e7a054,1,0,swh:1:snp:0000000000000000000000000000000000000020,
swh:1:rev:0000000000000000000000000000000000000013,1,1,swh:1:rev:0000000000000000000000000000000000000009,
swh:1:rev:0000000000000000000000000000000000000018,1,2,swh:1:rev:0000000000000000000000000000000000000013,
swh:1:rel:0000000000000000000000000000000000000019,1,0,swh:1:rev:0000000000000000000000000000000000000018,
swh:1:rel:0000000000000000000000000000000000000021,1,1,swh:1:rev:0000000000000000000000000000000000000018,
swh:1:snp:0000000000000000000000000000000000000022,3,1,swh:1:rev:0000000000000000000000000000000000000009,swh:1:rel:0000000000000000000000000000000000000010
swh:1:ori:8f50d3f60eae370ddbf85c86219c55108a350165,1,0,swh:1:snp:0000000000000000000000000000000000000022,
"""

TOPO_ORDER_FORWARD = """\
SWHID,ancestors,successors,sample_ancestor1,sample_ancestor2
swh:1:rel:0000000000000000000000000000000000000019,0,1,,
swh:1:ori:83404f995118bd25774f4ac14422a8f175e7a054,0,1,,
swh:1:snp:0000000000000000000000000000000000000020,1,2,swh:1:ori:83404f995118bd25774f4ac14422a8f175e7a054,
swh:1:ori:8f50d3f60eae370ddbf85c86219c55108a350165,0,1,,
swh:1:snp:0000000000000000000000000000000000000022,1,3,swh:1:ori:8f50d3f60eae370ddbf85c86219c55108a350165,
swh:1:rel:0000000000000000000000000000000000000021,1,1,swh:1:snp:0000000000000000000000000000000000000022,
swh:1:rev:0000000000000000000000000000000000000018,2,1,swh:1:rel:0000000000000000000000000000000000000021,swh:1:rel:0000000000000000000000000000000000000019
swh:1:rev:0000000000000000000000000000000000000013,1,1,swh:1:rev:0000000000000000000000000000000000000018,
swh:1:rel:0000000000000000000000000000000000000010,2,1,swh:1:snp:0000000000000000000000000000000000000022,swh:1:snp:0000000000000000000000000000000000000020
swh:1:rev:0000000000000000000000000000000000000009,4,1,swh:1:snp:0000000000000000000000000000000000000022,swh:1:rel:0000000000000000000000000000000000000010
swh:1:rev:0000000000000000000000000000000000000003,1,0,swh:1:rev:0000000000000000000000000000000000000009,
"""

PATH_COUNTS_BACKWARD = """\
swhid,paths_from_roots,all_paths
swh:1:rev:0000000000000000000000000000000000000003,0,0
swh:1:rev:0000000000000000000000000000000000000009,1,1
swh:1:rel:0000000000000000000000000000000000000010,1,2
swh:1:snp:0000000000000000000000000000000000000020,2,5
swh:1:ori:83404f995118bd25774f4ac14422a8f175e7a054,2,6
swh:1:rev:0000000000000000000000000000000000000013,1,2
swh:1:rev:0000000000000000000000000000000000000018,1,3
swh:1:rel:0000000000000000000000000000000000000019,1,4
swh:1:rel:0000000000000000000000000000000000000021,1,4
swh:1:snp:0000000000000000000000000000000000000022,3,10
swh:1:ori:8f50d3f60eae370ddbf85c86219c55108a350165,3,11
"""

PATH_COUNTS_FORWARD = """\
swhid,paths_from_roots,all_paths
swh:1:rel:0000000000000000000000000000000000000019,0,0
swh:1:ori:83404f995118bd25774f4ac14422a8f175e7a054,0,0
swh:1:snp:0000000000000000000000000000000000000020,1,1
swh:1:ori:8f50d3f60eae370ddbf85c86219c55108a350165,0,0
swh:1:snp:0000000000000000000000000000000000000022,1,1
swh:1:rel:0000000000000000000000000000000000000021,1,2
swh:1:rev:0000000000000000000000000000000000000018,2,4
swh:1:rev:0000000000000000000000000000000000000013,2,5
swh:1:rel:0000000000000000000000000000000000000010,2,4
swh:1:rev:0000000000000000000000000000000000000009,6,15
swh:1:rev:0000000000000000000000000000000000000003,6,16
"""


@pytest.mark.parametrize(
    "direction,algorithm", itertools.product(["backward", "forward"], ["bfs", "dfs"])
)
def test_toposort(tmpdir, direction: str, algorithm: str):
    tmpdir = Path(tmpdir)

    topological_order_path = (
        tmpdir / f"topological_order_{algorithm}_{direction}_rev,rel,snp,ori.csv.zst"
    )

    task = TopoSort(
        local_graph_path=DATA_DIR / "compressed",
        topological_order_dir=tmpdir,
        direction=direction,
        algorithm=algorithm,
        object_types="rev,rel,snp,ori",
        graph_name="example",
    )

    task.run()

    csv_text = subprocess.check_output(["zstdcat", topological_order_path]).decode()

    expected = TOPO_ORDER_BACKWARD if direction == "backward" else TOPO_ORDER_FORWARD

    (header, *rows) = csv_text.split("\n")
    (expected_header, *expected_lines) = expected.split("\n")
    assert header == expected_header

    assert set(rows) == set(expected_lines)

    assert rows.pop() == "", "Missing trailing newline"

    if direction == "backward":
        # Only one possible first row
        assert rows[0] == "swh:1:rev:0000000000000000000000000000000000000003,0,1,,"

        # The only three possible last rows
        assert rows[-1] in [
            "swh:1:ori:83404f995118bd25774f4ac14422a8f175e7a054,1,0"
            ",swh:1:snp:0000000000000000000000000000000000000020,",
            "swh:1:ori:8f50d3f60eae370ddbf85c86219c55108a350165,1,0"
            ",swh:1:snp:0000000000000000000000000000000000000022,",
            "swh:1:rel:0000000000000000000000000000000000000019,1,0"
            ",swh:1:rev:0000000000000000000000000000000000000018,",
        ]
    else:
        # Three possible first rows
        assert rows[0] in [
            "swh:1:rel:0000000000000000000000000000000000000019,0,1,,",
            "swh:1:ori:83404f995118bd25774f4ac14422a8f175e7a054,0,1,,",
            "swh:1:ori:8f50d3f60eae370ddbf85c86219c55108a350165,0,1,,",
        ]

        # The only possible last row
        assert rows[-1] == (
            "swh:1:rev:0000000000000000000000000000000000000003,1,0,"
            "swh:1:rev:0000000000000000000000000000000000000009,"
        )


@pytest.mark.parametrize("direction", ["backward", "forward"])
def test_countpaths(tmpdir, direction: str):
    tmpdir = Path(tmpdir)

    topological_order_path = (
        tmpdir / f"topological_order_dfs_{direction}_rev,rel,snp,ori.csv.zst"
    )
    path_counts_path = tmpdir / f"path_counts_{direction}_rev,rel,snp,ori.csv.zst"

    topological_order_path.write_text(
        TOPO_ORDER_BACKWARD if direction == "backward" else TOPO_ORDER_FORWARD
    )

    task = CountPaths(
        local_graph_path=DATA_DIR / "compressed",
        topological_order_dir=tmpdir,
        direction=direction,
        object_types="rev,rel,snp,ori",
        graph_name="example",
    )

    task.run()

    csv_text = subprocess.check_output(["zstdcat", path_counts_path]).decode()

    expected = PATH_COUNTS_BACKWARD if direction == "backward" else PATH_COUNTS_FORWARD

    assert csv_text == expected
