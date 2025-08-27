# Copyright (C) 2022-2024 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import multiprocessing.dummy
from pathlib import Path
from typing import Callable

import boto3
import botocore
from botocore.handlers import disable_signing
import tqdm


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
        self.s3.meta.client.meta.events.register("choose-signer.s3.*", disable_signing)
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

        self.compression_metadata_obj = None
        self.parallelism = parallelism

    def _download_file(self, obj, write_if_stamp=False):
        import subprocess

        assert obj.key.startswith(self.prefix)
        relative_path = obj.key.removeprefix(self.prefix).lstrip("/")
        if relative_path == "meta/compression.json" and not write_if_stamp:
            # Will copy it last
            self.compression_metadata_obj = obj
            return
        local_path = self.local_graph_path / relative_path
        local_path.parent.mkdir(parents=True, exist_ok=True)
        if relative_path.endswith(".bin.zst"):
            # The file was compressed before uploading to S3, we need it
            # to be decompressed locally

            # write to a temporary location, so we don't end up with a partial file download
            # or decompression is interrupted.
            tmp_local_path = local_path.with_suffix(".tmp")

            proc = subprocess.Popen(
                ["zstdmt", "--force", "-q", "-d", "-", "-o", str(tmp_local_path)],
                stdin=subprocess.PIPE,
            )
            for chunk in obj.get()["Body"].iter_chunks(102400):
                proc.stdin.write(chunk)
            proc.stdin.close()
            ret = proc.wait()
            if ret != 0:
                raise ValueError(f"zstdmt could not decompress {local_path}")
            tmp_local_path.rename(str(local_path)[0:-4])
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
                self.compression_metadata_obj is not None
            ), "did not see meta/compression.json in directory listing"

            # Write it last, to act as a stamp
            self._download_file(self.compression_metadata_obj, write_if_stamp=True)
