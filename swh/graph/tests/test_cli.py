# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Dict

from click.testing import CliRunner
import yaml

from swh.graph.cli import graph_cli_group

DATA_DIR = Path(__file__).parents[0] / "dataset"


def read_properties(properties_fname) -> Dict[str, str]:
    """read a Java .properties file"""
    with open(properties_fname) as f:
        keyvalues = (
            line.split("=", maxsplit=1)
            for line in f
            if not line.strip().startswith("#")
        )
        return dict((k.strip(), v.strip()) for (k, v) in keyvalues)


def test_pipeline():
    """run full compression pipeline"""
    # bare bone configuration, to allow testing the compression pipeline
    # with minimum RAM requirements on trivial graphs
    config = {"graph": {"compress": {"batch_size": 1000}}}
    runner = CliRunner()

    with TemporaryDirectory(suffix=".swh-graph-test") as tmpdir:
        config_path = Path(tmpdir, "config.yml")
        config_path.write_text(yaml.dump(config))

        result = runner.invoke(
            graph_cli_group,
            [
                "--config-file",
                config_path,
                "compress",
                "--graph",
                DATA_DIR / "example",
                "--outdir",
                tmpdir,
            ],
        )
        assert result.exit_code == 0, result
        properties = read_properties(Path(tmpdir) / "example.properties")

    assert int(properties["nodes"]) == 21
    assert int(properties["arcs"]) == 23
