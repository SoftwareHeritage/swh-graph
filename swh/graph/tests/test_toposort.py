# Copyright (C) 2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from pathlib import Path
import subprocess

from swh.graph.luigi import TopoSort

DATA_DIR = Path(__file__).parents[0] / "dataset"


# FIXME: the order of sample ancestors should not be hardcoded
# FIXME: swh:1:snp:0000000000000000000000000000000000000022,3,1,swh has three possible
# sample ancestors; they should not be hardecoded here
EXPECTED = """\
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


def test_toposort(tmpdir):
    tmpdir = Path(tmpdir)

    topological_order_path = tmpdir / "topo_order.csv.zst"

    task = TopoSort(
        local_graph_path=DATA_DIR / "compressed",
        topological_order_path=topological_order_path,
        graph_name="example",
    )

    task.run()

    csv_text = subprocess.check_output(["zstdcat", topological_order_path]).decode()

    (header, *rows) = csv_text.split("\n")
    (expected_header, *expected_lines) = EXPECTED.split("\n")
    assert header == expected_header

    # The only possible first line
    assert rows[0] == "swh:1:rev:0000000000000000000000000000000000000003,0,1,,"

    assert set(rows) == set(expected_lines)

    assert rows.pop() == "", "Missing trailing newline"

    # The only three possible last lines
    assert rows[-1] in [
        "swh:1:ori:83404f995118bd25774f4ac14422a8f175e7a054,1,0"
        ",swh:1:snp:0000000000000000000000000000000000000020,",
        "swh:1:ori:8f50d3f60eae370ddbf85c86219c55108a350165,1,0"
        ",swh:1:snp:0000000000000000000000000000000000000022,",
        "swh:1:rel:0000000000000000000000000000000000000019,1,0"
        ",swh:1:rev:0000000000000000000000000000000000000018,",
    ]
