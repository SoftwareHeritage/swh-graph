# Copyright (C) 2022-2025 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from __future__ import annotations

import logging
from pathlib import Path
import subprocess
from typing import TYPE_CHECKING, List

from swh.core.s3.downloader import S3Downloader

if TYPE_CHECKING:
    from types_boto3_s3.service_resource import ObjectSummary


logger = logging.getLogger(__name__)


class GraphDownloader(S3Downloader):
    """Utility class to download a compressed Software Heritage graph dataset
    from S3 implementing a download resumption feature in case some files fail to
    be downloaded (when connection errors happen for instance).

    Example of use::

        from swh.graph.download import GraphDownloader

        # download "2025-05-18-popular-1k" graph dataset into a sub-directory of the
        # current working directory named "2025-05-18-popular-1k"

        graph_downloader = GraphDownloader(
            local_path="2025-05-18-popular-1k",
            s3_url="s3://softareheritage/graph/2025-05-18-popular-1k/compressed/"
        )

        while not graph_downloader.download():
            continue

    """

    def filter_objects(self, objects: List[ObjectSummary]) -> List[ObjectSummary]:
        # meta/compression.json file must be downloaded after all other files
        return [obj for obj in objects if not obj.key.endswith("meta/compression.json")]

    def can_download_file(self, relative_path: str, local_file_path: Path) -> bool:
        # do not download again a file compressed with zstd if it was locally uncompressed
        return (
            not relative_path.endswith(".bin.zst")
            or not Path(str(local_file_path)[:-4]).exists()
        )

    def post_download_file(self, relative_path: str, local_file_path: Path) -> None:
        if (
            relative_path.endswith(".bin.zst")
            and not Path(str(local_file_path)[:-4]).exists()
        ):
            # The file was compressed with zstd before uploading it to S3, we need it
            # to be decompressed locally
            subprocess.check_call(["unzstd", "-d", "-q", "--rm", local_file_path])

    def post_downloads(self) -> None:
        if not self.s3_url.startswith(
            tuple(
                (
                    f"s3://softwareheritage/graph/{year}-"
                    for year in (2018, 2019, 2020, 2021)
                )
            )
        ):
            # skip metadata download for old graphs, they did not have that file yet
            objects = list(self.bucket.objects.filter(Prefix=self.prefix))
            for obj in objects:
                if obj.key.endswith("meta/compression.json"):
                    # Write it last, to act as a stamp
                    self._download_file(obj.key)
                    break
            else:
                raise ValueError(
                    "did not see meta/compression.json in directory listing"
                )
