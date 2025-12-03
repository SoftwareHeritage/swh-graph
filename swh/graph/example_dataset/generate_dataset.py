#!/usr/bin/env python3

# Copyright (C) 2021-2025  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

# type: ignore

import argparse
import csv
import logging
import os
from pathlib import Path
import shutil
import subprocess

from swh.export.exporters.edges import GraphEdgesExporter
from swh.export.exporters.orc import ORCExporter
from swh.export.fullnames import process_fullnames
from swh.export.journalprocessor import _add_person
from swh.graph.example_dataset import DATASET
from swh.graph.webgraph import compress
from swh.model.model import Release, Revision


def main():
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(description="Generate a test dataset")
    parser.add_argument(
        "--compress",
        action="store_true",
        default=False,
        help="Also compress the dataset",
    )
    parser.add_argument(
        "--profile", default="release", help="rust profile to use for compression"
    )
    parser.add_argument("output", help="output directory", nargs="?", default=".")
    parser.add_argument(
        "sensitive_output", help="sensitive output directory", nargs="?", default=None
    )
    args = parser.parse_args()

    exporters = {"edges": GraphEdgesExporter, "orc": ORCExporter}
    object_types = [
        "origin",
        "origin_visit",
        "origin_visit_status",
        "snapshot",
        "release",
        "revision",
        "directory",
        "content",
        "skipped_content",
    ]
    config = {"test_unique_file_id": "all"}
    output_path = Path(args.output)
    sensitive_output_path = (
        Path(args.sensitive_output) if args.sensitive_output is not None else None
    )

    if sensitive_output_path is not None:
        if (sensitive_output_path / "orc/person").exists():
            shutil.rmtree(sensitive_output_path / "orc/person")

        sensitive_output_path.mkdir(parents=True, exist_ok=True)

        tmp_sensitive_dir = sensitive_output_path / "tmp"
        tmp_sensitive_dir.mkdir(parents=True, exist_ok=True)

        tmp_dup_dir = tmp_sensitive_dir / "duplicated"
        tmp_dup_dir.mkdir(parents=True, exist_ok=True)

        tmp_dedup_dir = tmp_sensitive_dir / "deduplicated"
        tmp_dedup_dir.mkdir(parents=True, exist_ok=True)

        (sensitive_output_path / "orc/person").mkdir(parents=True, exist_ok=True)

    for name, exporter in exporters.items():
        if (output_path / name).exists():
            shutil.rmtree(output_path / name)
        with exporter(config, object_types, output_path / name) as e:
            for idx, obj in enumerate(DATASET):
                e.process_object(obj.object_type, obj.anonymize() or obj)
                if (
                    isinstance(obj, (Release, Revision))
                    and sensitive_output_path is not None
                ):
                    with open(f"{tmp_dup_dir}/{idx}.csv", "w") as tmp_csv:
                        writer = csv.writer(tmp_csv)
                        if obj.author is not None:
                            _add_person(writer, obj.author)
                        if isinstance(obj, Revision):
                            _add_person(writer, obj.committer)

    if sensitive_output_path is not None:
        for dup_file in tmp_dup_dir.iterdir():
            subprocess.Popen(
                # fmt: off
                [
                    "sort",
                    "-t", ",",
                    "-k", "2",
                    "-u",
                    "-o",
                    tmp_dedup_dir / dup_file.name,
                    dup_file,
                ],
                # fmt: on
                env={**os.environ, "LC_ALL": "C", "LC_COLLATE": "C", "LANG": "C"},
            )

        process_fullnames(
            sensitive_output_path / "orc/person/fullnames.orc", tmp_dedup_dir
        )
        shutil.rmtree(tmp_sensitive_dir)

    if args.compress:
        if (output_path / "compressed").exists():
            shutil.rmtree(output_path / "compressed")
        if (
            sensitive_output_path is not None
            and (sensitive_output_path / "compressed").exists()
        ):
            shutil.rmtree(sensitive_output_path / "compressed")
        sensitive_out_dir = (
            sensitive_output_path / "compressed"
            if sensitive_output_path is not None
            else None
        )
        compress(
            graph_name="example",
            in_dir=output_path / "orc",
            out_dir=output_path / "compressed",
            sensitive_in_dir=sensitive_output_path,
            sensitive_out_dir=sensitive_out_dir,
            check_flavor="example",
            conf={"profile": args.profile},
        )


if __name__ == "__main__":
    main()
