# Copyright (C) 2023  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from pathlib import Path
import subprocess

from swh.graph.example_dataset import DATASET_DIR
from swh.graph.luigi.blobs_datasets import FindEarliestRevisions, SelectBlobs

README_BLOBS = """\
swhid,sha1,name
swh:1:cnt:0000000000000000000000000000000000000001,0000000000000000000000000000000000000001,readme.md
swh:1:cnt:0000000000000000000000000000000000000004,0000000000000000000000000000000000000004,readme.md
swh:1:cnt:0000000000000000000000000000000000000011,0000000000000000000000000000000000000011,readme.md
"""

EARLIEST_REVISIONS = """\
swhid,earliest_swhid,earliest_ts,rev_occurrences
swh:1:cnt:0000000000000000000000000000000000000001,swh:1:rev:0000000000000000000000000000000000000003,1111122220,3
swh:1:cnt:0000000000000000000000000000000000000004,swh:1:rev:0000000000000000000000000000000000000009,1111155550,2
swh:1:cnt:0000000000000000000000000000000000000011,swh:1:rev:0000000000000000000000000000000000000013,1111166660,1
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


def test_FindEarliestRevision(tmpdir):
    tmpdir = Path(tmpdir)
    derived_datasets_path = tmpdir / "derived"
    readme_dataset_path = derived_datasets_path / "readme"
    readme_dataset_path.mkdir(parents=True)

    # Write blobs.csv.zst and stats/count.txt as SelectBlobs would
    subprocess.run(
        ["zstdmt"],
        input=README_BLOBS.encode(),
        stdout=(readme_dataset_path / "blobs.csv.zst").open("wb"),
        check=True,
    )
    (readme_dataset_path / "stats").mkdir()
    (readme_dataset_path / "stats" / "count.txt").write_text("3\n")

    # Run the task
    task = FindEarliestRevisions(
        blob_filter="readme",
        derived_datasets_path=derived_datasets_path,
        local_graph_path=DATASET_DIR / "compressed",
        graph_name="example",
    )

    task.run()

    # Check output
    csv_text = subprocess.check_output(
        ["zstdcat", derived_datasets_path / "readme" / "blobs-earliest.csv.zst"]
    ).decode()

    (header, *rows) = csv_text.replace("\r\n", "\n").split("\n")
    (expected_header, *expected_rows) = EARLIEST_REVISIONS.split("\n")
    assert header == expected_header
    assert set(rows) == set(expected_rows)
