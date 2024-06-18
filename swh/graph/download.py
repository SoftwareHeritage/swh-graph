# Copyright (C) 2022-2024 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import multiprocessing.dummy
from pathlib import Path
import tempfile
from typing import Callable

import boto3
import botocore
import tqdm

from .shell import Command


class GraphDownloader:
    def __init__(
        self,
        local_graph_path: Path,
        s3_graph_path: str,
        parallelism: int,
    ) -> None:
        if not s3_graph_path.startswith("s3://"):
            raise ValueError("Unsupported S3 URL")

        self.s3 = boto3.resource("s3")
        # don't require credentials to list the bucket
        self.s3.meta.client.meta.events.register(
            "choose-signer.s3.*", botocore.handlers.disable_signing
        )
        self.client = boto3.client(
            "s3",
            config=botocore.client.Config(
                # https://github.com/boto/botocore/issues/619
                max_pool_connections=10 * parallelism,
                # don't require credentials to download files
                signature_version=botocore.UNSIGNED,
            ),
        )

        self.local_graph_path = local_graph_path
        self.s3_graph_path = s3_graph_path

        self.bucket_name, self.prefix = self.s3_graph_path[len("s3://") :].split("/", 1)

        self.seen_compression_metadata = False
        self.parallelism = parallelism

    def _download_file(self, obj):
        assert obj.key.startswith(self.prefix)
        relative_path = obj.key.removeprefix(self.prefix).lstrip("/")
        if relative_path == "meta/compression.json":
            # Will copy it last
            self.seen_compression_metadata = True
            return
        local_path = self.local_graph_path / relative_path
        local_path.parent.mkdir(parents=True, exist_ok=True)
        if relative_path.endswith(".bin.zst"):
            # The file was compressed before uploading to S3, we need it
            # to be decompressed locally
            with tempfile.NamedTemporaryFile(
                prefix=local_path.stem, suffix=".bin.zst"
            ) as fd:
                self.client.download_file(
                    Bucket=self.bucket_name,
                    Key=obj.key,
                    Filename=fd.name,
                )
                Command.zstdmt(
                    "--force",
                    "-d",
                    fd.name,
                    "-o",
                    str(local_path)[0:-4],
                ).run()
        else:
            self.client.download_file(
                Bucket=self.bucket_name,
                Key=obj.key,
                Filename=str(local_path),
            )

        return relative_path

    def download_graph(
        self,
        progress_percent_cb: Callable[[int], None],
        progress_status_cb: Callable[[str], None],
    ):
        compression_metadata_path = f"{self.prefix}/meta/compression.json"

        bucket = self.s3.Bucket(self.bucket_name)

        # recursively copy local files to S3, and end with compression metadata
        objects = list(bucket.objects.filter(Prefix=self.prefix))
        with multiprocessing.dummy.Pool(self.parallelism) as p:
            for i, relative_path in tqdm.tqdm(
                enumerate(p.imap_unordered(self._download_file, objects)),
                total=len(objects),
                desc="Downloading",
            ):
                progress_percent_cb(int(i * 100 / len(objects)))
                progress_status_cb(f"Downloaded {relative_path}")

        if not self.s3_graph_path.startswith(
            tuple(
                (
                    f"s3://softwareheritage/graph/{year}-"
                    for year in (2018, 2019, 2020, 2021)
                )
            )
        ):
            # skip metadata download for old graphs, they did not have that file yet
            assert (
                self.seen_compression_metadata
            ), "did not see meta/compression.json in directory listing"

            # Write it last, to act as a stamp
            self.client.get(
                compression_metadata_path,
                self.local_graph_path / "meta" / "export.json",
            )
