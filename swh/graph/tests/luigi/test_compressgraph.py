# Copyright (C) 2022-2023  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import json
from pathlib import Path

from click.testing import CliRunner
import pytest

from swh.graph.cli import graph_cli_group
from swh.graph.example_dataset import (
    DATASET_DIR,
    RELEASES,
    REVISIONS,
    SENSITIVE_DATASET_DIR,
)
from swh.graph.shell import Rust, Sink

from ..test_cli import read_properties


@pytest.mark.parametrize("workers", [1, 2, 100])
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
        "--retry-luigi-delay=1",  # retry quickly
        "--",
        "--CompressGraph-check-flavor",
        "example",
        "--local-scheduler",
        "--CompressGraph-local-export-path",
        DATASET_DIR,
        "--CompressGraph-local-graph-path",
        tmpdir / "compressed_graph",
        "--CompressGraph-rust-executable-dir",
        "./target/debug/",
        f"--workers={workers}",
    ]

    result = runner.invoke(graph_cli_group, command)
    assert result.exit_code == 0, result.stdout

    properties = read_properties(tmpdir / "compressed_graph" / "graph.properties")

    assert int(properties["nodes"]) == 24
    assert int(properties["arcs"]) == 28

    export_meta_path = tmpdir / "compressed_graph/meta/export.json"
    assert (
        export_meta_path.read_bytes() == (DATASET_DIR / "meta/export.json").read_bytes()
    )

    compression_meta_path = tmpdir / "compressed_graph/meta/compression.json"
    compression_meta = json.loads(compression_meta_path.read_text())

    assert compression_meta[0]["conf"]["batch_size"] == 1000
    for step in compression_meta[0]["steps"]:
        assert step["conf"]["batch_size"] == 1000

    assert compression_meta[0]["object_types"] == "cnt,dir,rev,rel,snp,ori"

    with open(
        "swh/graph/example_dataset/compressed/example.property.author_timestamp.bin",
        "rb",
    ) as f:
        timestamps = [
            int.from_bytes(f.read(8), byteorder="big")
            for _ in range(int(properties["nodes"]))
        ]

    # remove non revision/releases
    timestamps = [timestamp for timestamp in timestamps if timestamp != 2**63]
    timestamps.sort()

    expected_timestamps = [
        rel.date.timestamp.seconds for rel in RELEASES if rel.date is not None
    ] + [rev.date.timestamp.seconds for rev in REVISIONS if rev.date is not None]
    expected_timestamps.sort()

    assert timestamps == expected_timestamps

    assert [item.name for item in tmpdir.iterdir()] == ["compressed_graph"]


def test_compressgraph_sensitive(tmpdir):
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
        "--retry-luigi-delay=1",  # retry quickly
        "--",
        "--CompressGraph-check-flavor",
        "example",
        "--local-scheduler",
        "--CompressGraph-local-export-path",
        DATASET_DIR,
        "--CompressGraph-local-sensitive-export-path",
        SENSITIVE_DATASET_DIR,
        "--CompressGraph-local-graph-path",
        tmpdir / "compressed_graph",
        "--CompressGraph-local-sensitive-graph-path",
        tmpdir / "compressed_graph_sensitive",
        "--CompressGraph-rust-executable-dir",
        "./target/debug/",
        "--LocalExport-export-task-type",
        "ExportGraph",
    ]

    result = runner.invoke(graph_cli_group, command)
    assert result.exit_code == 0, result.stdout

    assert (tmpdir / "compressed_graph_sensitive").exists()
    assert [item.name for item in tmpdir.iterdir()] == [
        "compressed_graph_sensitive",
        "compressed_graph",
    ]

    e2e_data = json.loads((tmpdir / "compressed_graph/meta/e2e-check.json").read_text())
    assert e2e_data["flavor"] == "example"
    assert e2e_data["authors_checked"]

    fullnames = sorted(
        [
            (
                Rust(
                    "swh-graph-person-id-to-name",
                    str(i),
                    (tmpdir / "compressed_graph_sensitive/graph"),
                    conf={"profile": "debug"},
                )
                > Sink()
            )
            .run()
            .stdout.removesuffix(b"\n")
            for i in range(3)
        ]
    )

    assert fullnames == [b"bar", b"baz", b"foo"]


@pytest.mark.parametrize(
    "workers,object_types",
    [
        (1, "ori,snp,rel,rev"),
        (5, "ori,snp,rel,rev"),
        (5, "dir,cnt"),
        (5, "ori,snp"),
        (5, "rel,rev"),
        (5, "rev"),
        (5, "rel"),
    ],
)
def test_compressgraph_partial(tmpdir, workers, object_types):
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
        "--retry-luigi-delay=1",  # retry quickly
        "--",
        "--CompressGraph-check-flavor",
        "none",
        "--local-scheduler",
        "--CompressGraph-local-export-path",
        DATASET_DIR,
        "--CompressGraph-local-graph-path",
        tmpdir / "compressed_graph",
        f"--CompressGraph-object-types={object_types}",
        "--CompressGraph-rust-executable-dir",
        "./target/debug/",
        f"--workers={workers}",
    ]

    result = runner.invoke(graph_cli_group, command)
    assert result.exit_code == 0, result.stdout

    properties = read_properties(tmpdir / "compressed_graph" / "graph.properties")

    if object_types == "dir,cnt":
        assert int(properties["nodes"]) == 13
        assert int(properties["arcs"]) == 11
    elif object_types == "ori,snp,rel,rev":
        assert int(properties["nodes"]) == 15
        assert int(properties["arcs"]) == 17
    elif object_types == "ori,snp":
        assert int(properties["nodes"]) == 7
        assert int(properties["arcs"]) == 7
        # FIXME: without objects pointed by snapshots, it would be this:
        # assert int(properties["nodes"]) == 4
        # assert int(properties["arcs"]) == 2
    elif object_types == "rel":
        assert int(properties["nodes"]) == 5
        assert int(properties["arcs"]) == 3
        # FIXME: without objects pointed by releases, it would be this:
        # assert int(properties["nodes"]) == 3
        # assert int(properties["arcs"]) == 0
    elif object_types == "rev":
        assert int(properties["nodes"]) == 8
        assert int(properties["arcs"]) == 7
        # FIXME: without objects pointed by revisions, it would be this:
        # assert int(properties["nodes"]) == 4
        # assert int(properties["arcs"]) == 3
    elif object_types == "rel,rev":
        assert int(properties["nodes"]) == 11
        assert int(properties["arcs"]) == 10
        # FIXME: without objects pointed by revisions, it would be this:
        # assert int(properties["nodes"]) == 7
        # assert int(properties["arcs"]) == 7
    else:
        assert False, f"Unexpected object types {object_types}"

    export_meta_path = tmpdir / "compressed_graph/meta/export.json"
    assert (
        export_meta_path.read_bytes() == (DATASET_DIR / "meta/export.json").read_bytes()
    )

    compression_meta_path = tmpdir / "compressed_graph/meta/compression.json"
    compression_meta = json.loads(compression_meta_path.read_text())

    assert compression_meta[0]["conf"]["batch_size"] == 1000
    for step in compression_meta[0]["steps"]:
        assert step["conf"]["batch_size"] == 1000

    assert compression_meta[0]["object_types"] == object_types
