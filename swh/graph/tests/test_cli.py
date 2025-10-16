# Copyright (C) 2019-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
from pathlib import Path
import shutil
from tempfile import TemporaryDirectory
from typing import Dict

from click.testing import CliRunner
import pytest
import yaml

from swh.graph.cli import graph_cli_group
from swh.graph.example_dataset import DATASET_DIR


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
    config = {
        "profile": "debug",
        "graph": {
            "compress": {
                "batch_size": 1000,
                "rust_executable_dir": "./target/debug/",
                "check_flavor": "example",
            }
        },
    }
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
                "--input-dataset",
                DATASET_DIR / "orc",
                "--output-directory",
                tmpdir,
                "--sensitive-output-directory",
                tmpdir,
                "--graph-name",
                "example",
            ],
        )
        assert result.exit_code == 0, result
        properties = read_properties(Path(tmpdir) / "example.properties")

    assert int(properties["nodes"]) == 24
    assert int(properties["arcs"]) == 28


@pytest.mark.parametrize("option", ["none", "--ef", "--force"])
def test_reindex(mocker, tmpdir, option):
    config = {
        "graph": {
            "compress": {
                "rust_executable_dir": "./target/debug/",
            }
        }
    }
    runner = CliRunner()

    trailing = []
    if option != "none":
        trailing.append(option)

    with TemporaryDirectory(suffix=".swh-graph-test") as tmpdir:
        config_path = Path(tmpdir, "config.yml")
        config_path.write_text(yaml.dump(config))

        shutil.copytree(DATASET_DIR / "compressed", tmpdir, dirs_exist_ok=True)

        result = runner.invoke(
            graph_cli_group,
            [
                "--config-file",
                config_path,
                "--profile",
                "debug",
                "reindex",
                f"{tmpdir}/example",
                *trailing,
            ],
        )
        assert result.exit_code == 0, result


@pytest.mark.parametrize("exit_code", [0, 1])
def test_luigi(mocker, tmpdir, exit_code):
    """calls Luigi with the given configuration"""
    # bare bone configuration, to allow testing the compression pipeline
    # with minimum RAM requirements on trivial graphs
    runner = CliRunner()

    subprocess_run = mocker.patch("subprocess.run")
    subprocess_run.return_value.returncode = exit_code

    with TemporaryDirectory(suffix=".swh-graph-test") as tmpdir:
        result = runner.invoke(
            graph_cli_group,
            [
                "luigi",
                "--base-directory",
                f"{tmpdir}/base_dir",
                "--dataset-name",
                "2022-12-07",
                "--",
                "foo",
                "bar",
                "--baz",
                "qux",
            ],
            catch_exceptions=False,
        )
        assert result.exit_code == exit_code, result

    luigi_config_path = subprocess_run.mock_calls[0][2]["env"]["LUIGI_CONFIG_PATH"]
    subprocess_run.assert_called_once_with(
        [
            "luigi",
            "--module",
            "swh.export.luigi",
            "--module",
            "swh.graph.luigi",
            "foo",
            "bar",
            "--baz",
            "qux",
        ],
        env={"LUIGI_CONFIG_PATH": luigi_config_path, **os.environ},
    )
