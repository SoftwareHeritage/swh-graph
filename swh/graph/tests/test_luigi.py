# Copyright (C) 2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import json
from pathlib import Path

from swh.graph.luigi.compressed_graph import CompressGraph

from .test_cli import read_properties

DATA_DIR = Path(__file__).parents[0] / "dataset"


def test_compressgraph(tmpdir):
    tmpdir = Path(tmpdir)

    task = CompressGraph(
        local_export_path=DATA_DIR,
        local_graph_path=tmpdir / "compressed_graph",
        batch_size=1000,  # go fast on the trivial dataset
    )

    task.run()

    properties = read_properties(tmpdir / "compressed_graph" / "graph.properties")

    assert int(properties["nodes"]) == 24
    assert int(properties["arcs"]) == 28

    export_meta_path = tmpdir / "compressed_graph/meta/export.json"
    assert export_meta_path.read_bytes() == (DATA_DIR / "meta/export.json").read_bytes()

    compression_meta_path = tmpdir / "compressed_graph/meta/compression.json"
    assert json.load(compression_meta_path.open())[0]["conf"] == {"batch_size": 1000}
