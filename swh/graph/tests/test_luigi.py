# Copyright (C) 2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import json
from pathlib import Path

from click.testing import CliRunner
import pytest

from swh.graph.cli import graph_cli_group

from .test_cli import read_properties

DATA_DIR = Path(__file__).parents[0] / "dataset"


@pytest.mark.parametrize("workers", [None, 1, 2, 3, 4, 5, 100])
def test_compressgraph(tmpdir, workers):
    tmpdir = Path(tmpdir)

    runner = CliRunner()

    command = [
        "luigi",
        "--base-directory",
        tmpdir / "base_dir",
        "--dataset-name",
        "testdataset",
        "CompressGraph",
        "--batch-size=1000",  # small value, to go fast on the trivial dataset
        "--max-ram=70M",
        "--",
        "--local-scheduler",
        "--CompressGraph-local-export-path",
        DATA_DIR,
        "--CompressGraph-local-graph-path",
        tmpdir / "compressed_graph",
    ]

    if workers is not None:
        command.append("--workers=100")

    result = runner.invoke(graph_cli_group, command)
    assert result.exit_code == 0, result.stdout

    properties = read_properties(tmpdir / "compressed_graph" / "graph.properties")

    assert int(properties["nodes"]) == 24
    assert int(properties["arcs"]) == 28

    export_meta_path = tmpdir / "compressed_graph/meta/export.json"
    assert export_meta_path.read_bytes() == (DATA_DIR / "meta/export.json").read_bytes()

    compression_meta_path = tmpdir / "compressed_graph/meta/compression.json"
    compression_meta = json.loads(compression_meta_path.read_text())

    assert compression_meta[0]["conf"]["batch_size"] == 1000
    for step in compression_meta[0]["steps"]:
        assert step["conf"]["batch_size"] == 1000
