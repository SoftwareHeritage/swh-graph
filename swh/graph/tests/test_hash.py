# Copyright (c) 2022-2025  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Iterable, List

import pytest

from swh.graph.example_dataset import DATASET, DATASET_DIR
from swh.graph.shell import Command, CommandException, Rust, Sink


def hash_swhids_with_node2swhid(swhids: Iterable[str]) -> List[int]:
    # fmt: off
    return (
        Command.echo("\n".join(swhids))
        | Rust(
            "swh-graph-hash",
            "swhids",
            "--num-nodes",
            (DATASET_DIR / "compressed/example.nodes.count.txt").read_text().strip(),
            "--mph-algo",
            "pthash",
            "--mph",
            DATASET_DIR / "compressed/example",
            "--permutation",
            DATASET_DIR / "compressed/example.pthash.order",
            "--node2swhid",
            DATASET_DIR / "compressed/example.node2swhid.bin"
        )
        > Sink()
    ).run().stdout.decode().strip().split("\n")
    # fmt: on


def test_hash_all_swhids() -> None:
    swhids = [obj.swhid() for obj in DATASET if hasattr(obj, "swhid")]
    hashes = hash_swhids_with_node2swhid(map(str, swhids))

    # Check there are as many output lines as input lines, and they are each a unique
    # integer
    assert sorted(map(int, hashes)) == list(range(len(swhids)))


def test_hash_unknown_swhid() -> None:
    swhids = ["swh:1:dir:9999999999999999999999999999999999999999"]

    with pytest.raises(CommandException):
        hash_swhids_with_node2swhid(map(str, swhids))
