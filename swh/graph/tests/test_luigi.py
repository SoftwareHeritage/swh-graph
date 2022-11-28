# Copyright (C) 2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import json
from pathlib import Path

from swh.graph.luigi import CompressGraph

from .test_cli import read_properties

DATA_DIR = Path(__file__).parents[0] / "dataset"


def test_compressgraph(tmpdir):
    tmpdir = Path(tmpdir)

    task = CompressGraph(
        local_export_path=DATA_DIR / "dataset",
        output_directory=tmpdir / "compressed_graph",
        graph_name="example",
        batch_size=1000,  # go fast on the trivial dataset
    )

    task.run()

    properties = read_properties(tmpdir / "compressed_graph" / "example.properties")

    assert int(properties["nodes"]) == 21
    assert int(properties["arcs"]) == 23

    export_meta_path = tmpdir / "compressed_graph/example.meta/export.json"
    assert (
        export_meta_path.read_bytes()
        == (DATA_DIR / "dataset/meta/export.json").read_bytes()
    )

    compression_meta_path = tmpdir / "compressed_graph/example.meta/compression.json"
    assert json.load(compression_meta_path.open())[0]["conf"] == {"batch_size": 1000}
