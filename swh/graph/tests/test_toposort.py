# Copyright (C) 2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from pathlib import Path
import subprocess

from swh.graph.luigi import TopoSort

DATA_DIR = Path(__file__).parents[0] / "dataset"


EXPECTED_ROWS = """
swh:1:rev:0000000000000000000000000000000000000003,0,1,,
swh:1:rev:0000000000000000000000000000000000000009,1,3,swh:1:rev:0000000000000000000000000000000000000003,
swh:1:rel:0000000000000000000000000000000000000010,1,1,swh:1:rev:0000000000000000000000000000000000000009,
swh:1:snp:0000000000000000000000000000000000000020,2,1,swh:1:rev:0000000000000000000000000000000000000009,swh:1:rel:0000000000000000000000000000000000000010
swh:1:ori:83404f995118bd25774f4ac14422a8f175e7a054,1,0,swh:1:snp:0000000000000000000000000000000000000020,
swh:1:rev:0000000000000000000000000000000000000013,1,1,swh:1:rev:0000000000000000000000000000000000000009,
swh:1:rev:0000000000000000000000000000000000000018,1,1,swh:1:rev:0000000000000000000000000000000000000013,
swh:1:rel:0000000000000000000000000000000000000019,1,0,swh:1:rev:0000000000000000000000000000000000000018,
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

    lines = csv_text.split("\n")
    (header, *rows) = lines
    assert header == "SWHID,ancestors,successors,sample_ancestor1,sample_ancestor2"

    # The only possible first line
    assert rows[0] == "swh:1:rev:0000000000000000000000000000000000000003,0,1,,"

    assert set(rows) == set(EXPECTED_ROWS.split("\n"))

    assert rows.pop() == "", "Missing trailing newline"

    # The only two possible last lines
    assert rows[-1] in [
        "swh:1:ori:83404f995118bd25774f4ac14422a8f175e7a054,1,0"
        ",swh:1:snp:0000000000000000000000000000000000000020,",
        "swh:1:rel:0000000000000000000000000000000000000019,1,0"
        ",swh:1:rev:0000000000000000000000000000000000000018,",
    ]
