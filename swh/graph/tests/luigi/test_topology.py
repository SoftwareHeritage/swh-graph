# Copyright (C) 2022-2023  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import itertools
from pathlib import Path
import subprocess

import pytest

from swh.graph.example_dataset import DATASET_DIR
from swh.graph.luigi.topology import CountPaths, TopoSort

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
""".replace(
    "\n", "\r\n"
)

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
""".replace(
    "\n", "\r\n"
)

PATH_COUNTS_BACKWARD = """\
SWHID,paths_from_roots,all_paths
swh:1:rev:0000000000000000000000000000000000000003,0.0,0.0
swh:1:rev:0000000000000000000000000000000000000009,1.0,1.0
swh:1:rel:0000000000000000000000000000000000000010,1.0,2.0
swh:1:snp:0000000000000000000000000000000000000020,2.0,5.0
swh:1:ori:83404f995118bd25774f4ac14422a8f175e7a054,2.0,6.0
swh:1:rev:0000000000000000000000000000000000000013,1.0,2.0
swh:1:rev:0000000000000000000000000000000000000018,1.0,3.0
swh:1:rel:0000000000000000000000000000000000000019,1.0,4.0
swh:1:rel:0000000000000000000000000000000000000021,1.0,4.0
swh:1:snp:0000000000000000000000000000000000000022,3.0,10.0
swh:1:ori:8f50d3f60eae370ddbf85c86219c55108a350165,3.0,11.0
""".replace(
    "\n", "\r\n"
)

PATH_COUNTS_FORWARD = """\
SWHID,paths_from_roots,all_paths
swh:1:rel:0000000000000000000000000000000000000019,0.0,0.0
swh:1:ori:83404f995118bd25774f4ac14422a8f175e7a054,0.0,0.0
swh:1:snp:0000000000000000000000000000000000000020,1.0,1.0
swh:1:ori:8f50d3f60eae370ddbf85c86219c55108a350165,0.0,0.0
swh:1:snp:0000000000000000000000000000000000000022,1.0,1.0
swh:1:rel:0000000000000000000000000000000000000021,1.0,2.0
swh:1:rev:0000000000000000000000000000000000000018,2.0,4.0
swh:1:rev:0000000000000000000000000000000000000013,2.0,5.0
swh:1:rel:0000000000000000000000000000000000000010,2.0,4.0
swh:1:rev:0000000000000000000000000000000000000009,6.0,15.0
swh:1:rev:0000000000000000000000000000000000000003,6.0,16.0
""".replace(
    "\n", "\r\n"
)


@pytest.mark.parametrize(
    "direction,algorithm", itertools.product(["backward", "forward"], ["bfs", "dfs"])
)
def test_toposort(tmpdir, direction: str, algorithm: str):
    tmpdir = Path(tmpdir)

    topological_order_path = (
        tmpdir / f"topological_order_{algorithm}_{direction}_rev,rel,snp,ori.csv.zst"
    )

    task = TopoSort(
        local_graph_path=DATASET_DIR / "compressed",
        topological_order_dir=tmpdir,
        direction=direction,
        algorithm=algorithm,
        object_types="rev,rel,snp,ori",
        graph_name="example",
    )

    task.run()

    csv_text = subprocess.check_output(["zstdcat", topological_order_path]).decode()

    expected = TOPO_ORDER_BACKWARD if direction == "backward" else TOPO_ORDER_FORWARD

    (header, *rows) = csv_text.split("\r\n")
    (expected_header, *expected_lines) = expected.split("\r\n")
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
        local_graph_path=DATASET_DIR / "compressed",
        topological_order_dir=tmpdir,
        direction=direction,
        object_types="rev,rel,snp,ori",
        graph_name="example",
    )

    task.run()

    assert path_counts_path.exists()
    csv_text = subprocess.check_output(["zstdcat", path_counts_path]).decode()

    expected = PATH_COUNTS_BACKWARD if direction == "backward" else PATH_COUNTS_FORWARD

    assert csv_text == expected


@pytest.mark.parametrize("direction", ["backward", "forward"])
def test_countpaths_contents(tmpdir, direction):
    """Tests the edge case of counting paths from a toposort which doesn't include
    contents (because they can be trivially added at the start/end of the order)
    """
    tmpdir = Path(tmpdir)

    topological_order_path = (
        tmpdir / f"topological_order_dfs_{direction}_dir,rev,rel,snp,ori.csv.zst"
    )
    path_counts_path = (
        tmpdir / f"path_counts_{direction}_cnt,dir,rev,rel,snp,ori.csv.zst"
    )

    # headers: SWHID,ancestors,successors,sample_ancestor1,sample_ancestor2
    dir_order = """\
        swh:1:dir:0000000000000000000000000000000000000012,1,2,,
        swh:1:dir:0000000000000000000000000000000000000017,1,2,,
        swh:1:dir:0000000000000000000000000000000000000002,1,1,,
        swh:1:dir:0000000000000000000000000000000000000008,2,3,,
        swh:1:dir:0000000000000000000000000000000000000016,1,1,,
        swh:1:dir:0000000000000000000000000000000000000006,1,2,,
        """.replace(
        "        ", ""
    )

    if direction == "forward":
        # teeeechnically we should concatenate them this ways, because revisions have
        # successors=0 even though they have directory successors in this concatenation.
        # but CountPaths doesn't care about that field, so it's good enough for a unit
        # test
        with topological_order_path.open("at") as fd:
            fd.write(TOPO_ORDER_FORWARD)
            fd.write(dir_order)
    else:
        # ditto (and we should also swap ancestors/successors for the directories)
        with topological_order_path.open("at") as fd:
            header, rest = TOPO_ORDER_BACKWARD.split("\r\n", 1)
            fd.write(header)
            fd.write("\n".join(reversed(dir_order.split("\n"))))
            fd.write(rest)

    task = CountPaths(
        local_graph_path=DATASET_DIR / "compressed",
        topological_order_dir=tmpdir,
        direction=direction,
        object_types="cnt,dir,rev,rel,snp,ori",
        graph_name="example",
    )

    task.run()

    assert path_counts_path.exists()
    csv_text = subprocess.check_output(["zstdcat", path_counts_path]).decode()

    if direction == "forward":
        expected = PATH_COUNTS_FORWARD
        expected += """\
            swh:1:dir:0000000000000000000000000000000000000012,2.0,6.0
            swh:1:dir:0000000000000000000000000000000000000017,2.0,5.0
            swh:1:dir:0000000000000000000000000000000000000002,6.0,17.0
            swh:1:dir:0000000000000000000000000000000000000008,8.0,23.0
            swh:1:dir:0000000000000000000000000000000000000016,2.0,6.0
            swh:1:dir:0000000000000000000000000000000000000006,8.0,24.0
            swh:1:cnt:0000000000000000000000000000000000000001,14.0,42.0
            swh:1:cnt:0000000000000000000000000000000000000004,8.0,25.0
            swh:1:cnt:0000000000000000000000000000000000000005,8.0,25.0
            swh:1:cnt:0000000000000000000000000000000000000007,8.0,24.0
            swh:1:cnt:0000000000000000000000000000000000000011,2.0,7.0
            swh:1:cnt:0000000000000000000000000000000000000014,2.0,6.0
            swh:1:cnt:0000000000000000000000000000000000000015,2.0,7.0
            """.replace(
            "            ", ""
        ).replace(
            "\n", "\r\n"
        )

    else:
        # can't reuse PATH_COUNTS_BACKWARD because directories change the count
        # of other objects in this direction
        expected = """\
            SWHID,paths_from_roots,all_paths
            swh:1:cnt:0000000000000000000000000000000000000001,0.0,0.0
            swh:1:cnt:0000000000000000000000000000000000000004,0.0,0.0
            swh:1:cnt:0000000000000000000000000000000000000005,0.0,0.0
            swh:1:cnt:0000000000000000000000000000000000000007,0.0,0.0
            swh:1:cnt:0000000000000000000000000000000000000011,0.0,0.0
            swh:1:cnt:0000000000000000000000000000000000000014,0.0,0.0
            swh:1:cnt:0000000000000000000000000000000000000015,0.0,0.0
            swh:1:dir:0000000000000000000000000000000000000006,2.0,2.0
            swh:1:dir:0000000000000000000000000000000000000016,1.0,1.0
            swh:1:dir:0000000000000000000000000000000000000008,4.0,5.0
            swh:1:dir:0000000000000000000000000000000000000002,1.0,1.0
            swh:1:dir:0000000000000000000000000000000000000017,2.0,3.0
            swh:1:dir:0000000000000000000000000000000000000012,5.0,7.0
            swh:1:rev:0000000000000000000000000000000000000009,4.0,6.0
            swh:1:rel:0000000000000000000000000000000000000010,4.0,7.0
            swh:1:snp:0000000000000000000000000000000000000020,8.0,15.0
            swh:1:ori:83404f995118bd25774f4ac14422a8f175e7a054,8.0,16.0
            swh:1:rev:0000000000000000000000000000000000000013,9.0,15.0
            swh:1:rev:0000000000000000000000000000000000000018,11.0,20.0
            swh:1:rel:0000000000000000000000000000000000000019,11.0,21.0
            swh:1:rel:0000000000000000000000000000000000000021,11.0,21.0
            swh:1:snp:0000000000000000000000000000000000000022,19.0,37.0
            swh:1:ori:8f50d3f60eae370ddbf85c86219c55108a350165,19.0,38.0
            """.replace(
            "            ", ""
        ).replace(
            "\n", "\r\n"
        )

    assert csv_text == expected
