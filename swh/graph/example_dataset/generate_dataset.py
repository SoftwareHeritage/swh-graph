#!/usr/bin/env python3

# Copyright (C) 2021-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

# type: ignore

import argparse
import logging
from pathlib import Path
import shutil

from swh.dataset.exporters.edges import GraphEdgesExporter
from swh.dataset.exporters.orc import ORCExporter
from swh.graph.example_dataset import DATASET
from swh.graph.webgraph import compress


def main():
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(description="Generate a test dataset")
    parser.add_argument(
        "--compress",
        action="store_true",
        default=False,
        help="Also compress the dataset",
    )
    parser.add_argument("output", help="output directory", nargs="?", default=".")
    args = parser.parse_args()

    exporters = {"edges": GraphEdgesExporter, "orc": ORCExporter}
    config = {"test_unique_file_id": "all"}
    output_path = Path(args.output)
    for name, exporter in exporters.items():
        if (output_path / name).exists():
            shutil.rmtree(output_path / name)
        with exporter(config, output_path / name) as e:
            for obj in DATASET:
                e.process_object(obj.object_type, obj.to_dict())

    if args.compress:
        if (output_path / "compressed").exists():
            shutil.rmtree(output_path / "compressed")
        compress("example", output_path / "orc", output_path / "compressed")


if __name__ == "__main__":
    main()
