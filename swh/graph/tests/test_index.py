# Copyright (C) 2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from pathlib import Path
import shutil

from swh.graph.example_dataset import DATASET_DIR
from swh.graph.shell import Rust


def test_rebuild_order(tmpdir):
    graph_dir = Path(tmpdir / "graph")
    shutil.copytree(DATASET_DIR / "compressed", graph_dir)
    order_path = graph_dir / "example.fmphgo.order"
    order = order_path.read_bytes()
    order_path.unlink()

    Rust(
        "swh-graph-index",
        "rebuild-order",
        "--mph-algo",
        "fmphgo",
        "--function",
        graph_dir / "example.fmphgo",
        "--node2swhid",
        graph_dir / "example.node2swhid.bin",
        "--target-order",
        order_path,
    ).run()

    # should be exactly the same file content
    assert order == order_path.read_bytes()
