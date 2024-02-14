# Copyright (C) 2023  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from pathlib import Path
import subprocess

from swh.graph.example_dataset import DATASET_DIR
from swh.graph.luigi.blobs_datasets import SelectBlobs

README_BLOBS = """\
swhid,sha1,name
swh:1:cnt:0000000000000000000000000000000000000001,0000000000000000000000000000000000000001,readme.md
swh:1:cnt:0000000000000000000000000000000000000004,0000000000000000000000000000000000000004,readme.md
swh:1:cnt:0000000000000000000000000000000000000011,0000000000000000000000000000000000000011,readme.md
"""


def test_SelectBlobs(tmpdir):
    tmpdir = Path(tmpdir)
    derived_datasets_path = tmpdir / "derived"

    task = SelectBlobs(
        blob_filter="readme",
        local_export_path=DATASET_DIR,
        derived_datasets_path=derived_datasets_path,
    )

    task.run()

    csv_text = subprocess.check_output(
        ["zstdcat", derived_datasets_path / "readme" / "blobs.csv.zst"]
    ).decode()

    assert csv_text == README_BLOBS

    assert (
        int(
            (derived_datasets_path / "readme" / "stats" / "count.txt")
            .read_text()
            .strip()
        )
        == 3
    )
