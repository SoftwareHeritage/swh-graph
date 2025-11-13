# Copyright (C) 2019-2025  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
from pathlib import Path
import shutil
from typing import Dict

from click.testing import CliRunner
import pytest
import yaml

from swh.graph.cli import graph_cli_group
from swh.graph.example_dataset import DATASET_DIR


@pytest.fixture
def cli_runner():
    return CliRunner()


def read_properties(properties_fname) -> Dict[str, str]:
    """read a Java .properties file"""
    with open(properties_fname) as f:
        keyvalues = (
            line.split("=", maxsplit=1)
            for line in f
            if not line.strip().startswith("#")
        )
        return dict((k.strip(), v.strip()) for (k, v) in keyvalues)


def test_pipeline(cli_runner, tmp_path):
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

    config_path = Path(tmp_path, "config.yml")
    config_path.write_text(yaml.dump(config))

    result = cli_runner.invoke(
        graph_cli_group,
        [
            "--config-file",
            config_path,
            "compress",
            "--input-dataset",
            DATASET_DIR / "orc",
            "--output-directory",
            tmp_path,
            "--sensitive-output-directory",
            tmp_path,
            "--graph-name",
            "example",
        ],
    )
    assert result.exit_code == 0, result
    properties = read_properties(tmp_path / "example.properties")

    assert int(properties["nodes"]) == 24
    assert int(properties["arcs"]) == 28


@pytest.mark.parametrize("option", ["none", "--ef", "--force"])
def test_reindex(cli_runner, mocker, tmp_path, option):
    config = {
        "graph": {
            "compress": {
                "rust_executable_dir": "./target/debug/",
            }
        }
    }

    trailing = []
    if option != "none":
        trailing.append(option)

    config_path = Path(tmp_path, "config.yml")
    config_path.write_text(yaml.dump(config))

    shutil.copytree(DATASET_DIR / "compressed", tmp_path, dirs_exist_ok=True)

    result = cli_runner.invoke(
        graph_cli_group,
        [
            "--config-file",
            config_path,
            "--profile",
            "debug",
            "reindex",
            f"{tmp_path}/example",
            *trailing,
        ],
    )
    assert result.exit_code == 0, result


@pytest.mark.parametrize("exit_code", [0, 1])
def test_luigi(cli_runner, mocker, tmp_path, exit_code):
    """calls Luigi with the given configuration"""
    # bare bone configuration, to allow testing the compression pipeline
    # with minimum RAM requirements on trivial graphs

    subprocess_run = mocker.patch("subprocess.run")
    subprocess_run.return_value.returncode = exit_code

    result = cli_runner.invoke(
        graph_cli_group,
        [
            "luigi",
            "--base-directory",
            f"{tmp_path}/base_dir",
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


def test_download_graph_ok(cli_runner, s3_graph_dataset_name, tmp_path, mocked_aws):
    result = cli_runner.invoke(
        graph_cli_group,
        [
            "download",
            "--name",
            s3_graph_dataset_name,
            str(tmp_path),
        ],
    )
    assert result.exit_code == 0, result.output
    assert (tmp_path / "example.graph").exists()
    assert (tmp_path / "meta/compression.json").exists()

    assert (
        f"Graph dataset {s3_graph_dataset_name} successfully downloaded to path {tmp_path}."
    ) in result.output


def test_download_graph_resumption(
    cli_runner, s3_graph_dataset_name, mocker, tmp_path, mocked_aws
):
    mocker.patch("swh.graph.download.GraphDownloader.download").side_effect = [
        False,
        True,
    ]
    result = cli_runner.invoke(
        graph_cli_group,
        [
            "download",
            "--name",
            s3_graph_dataset_name,
            str(tmp_path),
        ],
    )
    assert result.exit_code == 0, result.output

    assert (
        f"Graph dataset {s3_graph_dataset_name} successfully downloaded to path {tmp_path}."
    ) in result.output
