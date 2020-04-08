# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest

from pathlib import Path
from tempfile import TemporaryDirectory, NamedTemporaryFile
from typing import Dict

from click.testing import CliRunner

from swh.core import config
from swh.graph import cli


def read_properties(properties_fname) -> Dict[str, str]:
    """read a Java .properties file"""
    properties = {}
    with open(properties_fname) as f:
        for line in f:
            if line.startswith("#"):
                continue
            (key, value) = line.rstrip().split("=", maxsplit=1)
            properties[key] = value

    return properties


class TestCompress(unittest.TestCase):

    DATA_DIR = Path(__file__).parents[0] / "dataset"

    def setUp(self):
        self.runner = CliRunner()

        tmpconf = NamedTemporaryFile(
            mode="w", delete=False, prefix="swh-graph-test", suffix=".yml"
        )
        # bare bone configuration, to allow testing the compression pipeline
        # with minimum RAM requirements on trivial graphs
        tmpconf.write(
            """
graph:
  compress:
    batch_size: 1000
"""
        )
        tmpconf.close()
        self.conffile = Path(tmpconf.name)
        self.config = config.read(self.conffile, cli.DEFAULT_CONFIG)

    def tearDown(self):
        if self.conffile.exists():
            self.conffile.unlink()

    def test_pipeline(self):
        """run full compression pipeline"""
        with TemporaryDirectory(suffix=".swh-graph-test") as tmpdir:
            result = self.runner.invoke(
                cli.compress,
                ["--graph", self.DATA_DIR / "example", "--outdir", tmpdir],
                obj={"config": self.config},
            )

            self.assertEqual(result.exit_code, 0)
            properties = read_properties(Path(tmpdir) / "example.properties")
            self.assertEqual(int(properties["nodes"]), 21)
            self.assertEqual(int(properties["arcs"]), 23)
