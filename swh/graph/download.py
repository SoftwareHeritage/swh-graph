# Copyright (C) 2022-2025 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from __future__ import annotations

import concurrent
import logging
import os
from pathlib import Path
import re
import secrets
import threading
from typing import Callable, Dict, Optional

import boto3
import botocore
from botocore.handlers import disable_signing
import tqdm

CHUNK_SIZE = 102400

logger = logging.getLogger(__name__)


class GraphDownloader:
    """Utility class to download a compressed Software Heritage graph dataset
    from S3 implementing a resume download feature in case some files fail to
    be downloaded (when connection errors happen for instance).

    Example of use::

        from swh.graph.download import GraphDownloader

        # download "2025-05-18-popular-1k" graph dataset into a sub-directory of the
        # current working directory named "2025-05-18-popular-1k"

        graph_downloader = GraphDownloader(
            local_graph_path="2025-05-18-popular-1k",
            s3_graph_path="s3://softareheritage/graph/2025-05-18-popular-1k/compressed/"
        )

        while not graph_downloader.download_graph():
            continue

    """

    def __init__(
        self,
        local_graph_path: Path,
        s3_graph_path: str,
        parallelism: int = 5,
    ) -> None:
        if not s3_graph_path.startswith("s3://"):
            raise ValueError("Unsupported S3 URL")

        # silence noisy debug logs we are not interested about
        for module in ("boto3", "botocore", "s3transfer", "urllib3"):
            logging.getLogger(module).setLevel(logging.WARNING)

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

        self.compression_metadata_obj: Optional[str] = None
        self.parallelism = parallelism
        self.file_sizes: Dict[str, int] = {}

    def _download_file(
        self,
        obj_key: str,
        event: Optional[threading.Event] = None,
        write_if_stamp: bool = False,
    ) -> str:
        import subprocess

        assert obj_key.startswith(self.prefix)
        relative_path = obj_key.removeprefix(self.prefix).lstrip("/")
        if relative_path == "meta/compression.json" and not write_if_stamp:
            # Will copy it last
            self.compression_metadata_obj = obj_key
            return ""
        local_path = self.local_graph_path / relative_path
        local_path.parent.mkdir(parents=True, exist_ok=True)

        # fetch size of object to download and store it in a dict
        object_metadata = self.client.head_object(Bucket=self.bucket_name, Key=obj_key)
        file_size = object_metadata["ContentLength"]
        self.file_sizes[relative_path] = file_size

        file_part_path = str(local_path) + ".part"
        if os.path.exists(file_part_path):
            # resume previous download that failed by fetching only the missing bytes
            logger.debug("File %s was partially downloaded", obj_key)
            file_part_size = os.path.getsize(file_part_path)
            range_download_path = str(local_path) + "." + secrets.token_hex(4)
            logger.debug(
                "Resuming download of %s from byte %s",
                obj_key,
                file_part_size,
            )
            range_ = f"bytes={file_part_size}-{file_size}"
            assert file_part_size < file_size, f"range {range_} is invalid"
            with open(range_download_path, "wb") as range_file:
                object_ = self.client.get_object(
                    Bucket=self.bucket_name, Key=obj_key, Range=range_
                )
                for chunk in object_["Body"].iter_chunks(CHUNK_SIZE):
                    range_file.write(chunk)
                    if event and event.is_set():
                        # some dataset file failed to be downloaded,
                        # abort current download to save state and immediately inform user
                        range_file.flush()
                        return ""

            # concatenate previously downloaded bytes with the missing ones
            logger.debug(
                "Assembling file %s from %s and %s",
                obj_key,
                file_part_path,
                range_download_path,
            )
            with open(local_path, "wb") as file:
                with open(file_part_path, "rb") as file_part:
                    while data := file_part.read(CHUNK_SIZE):
                        file.write(data)
                with open(range_download_path, "rb") as file_range:
                    while data := file_range.read(CHUNK_SIZE):
                        file.write(data)

            # remove no longer needed files
            os.remove(file_part_path)
            os.remove(range_download_path)
        elif os.path.exists(local_path):
            # file already downloaded, we check if it has the correct size and
            # trigger a new download if it is not
            local_file_size = os.path.getsize(local_path)
            if local_file_size != file_size:
                logger.debug(
                    "File %s exists but has incorrect size, forcing a new download",
                    obj_key,
                )
                os.remove(local_path)
                return self._download_file(obj_key, event, write_if_stamp)

            logger.debug("File %s already downloaded, nothing to do", obj_key)
        else:
            # first download of a dataset file, zstd compressed files are
            # filtered out if already downloaded and uncompressed
            if not relative_path.endswith(".bin.zst") or not os.path.exists(
                str(local_path)[:-4]
            ):
                logger.debug("Downloading file %s: %s", obj_key, relative_path)
                # use a random hex suffix for the file to to detect later if its
                # download was aborted
                file_path_part = str(local_path) + "." + secrets.token_hex(4)
                with open(file_path_part, "wb") as graph_file:
                    object_ = self.client.get_object(
                        Bucket=self.bucket_name, Key=obj_key
                    )
                    for chunk in object_["Body"].iter_chunks(CHUNK_SIZE):
                        graph_file.write(chunk)
                        if event and event.is_set():
                            # some dataset file failed to be downloaded,
                            # abort current download to save state and immediately inform user
                            graph_file.flush()
                            return ""
                os.rename(file_path_part, str(local_path))
            else:
                logger.debug(
                    "File %s already downloaded and uncompressed, nothing to do",
                    obj_key,
                )

        if relative_path.endswith(".bin.zst") and not os.path.exists(
            str(local_path)[:-4]
        ):
            # The file was compressed before uploading to S3, we need it
            # to be decompressed locally
            subprocess.check_call(["unzstd", "-d", "-q", "--rm", local_path])

        return relative_path

    def download_graph(
        self,
        progress_percent_cb: Callable[[int], None] = lambda _: None,
        progress_status_cb: Callable[[str], None] = lambda _: None,
    ) -> bool:
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=self.parallelism)
        event = threading.Event()
        bucket = self.s3.Bucket(self.bucket_name)

        try:
            # recursively copy local files to S3, and end with compression metadata
            objects = list(bucket.objects.filter(Prefix=self.prefix))
            with tqdm.tqdm(total=len(objects), desc="Downloading") as progress:
                not_done = {
                    executor.submit(self._download_file, obj.key, event)
                    for obj in objects
                }
                while not_done:
                    # poll future states every second in order to abort downloads
                    # on first detected error
                    done, not_done = concurrent.futures.wait(
                        not_done,
                        timeout=1,
                        return_when=concurrent.futures.FIRST_COMPLETED,
                    )
                    for future in done:
                        progress.update()
                        progress_percent_cb(int(progress.n * 100 / progress.total))
                        progress_status_cb(f"Downloaded {future.result()}")
                        logger.debug("Downloaded %s", future.result())

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

        except BaseException as exc:
            logger.debug("Error occurred while downloading: %s", exc)
            # notify download threads to immediately terminate
            event.set()
            # shutdown pool of download threads
            executor.shutdown(cancel_futures=True)
            # iterate on downloaded files
            for root, _, files in os.walk(self.local_graph_path):
                for filename in files:
                    file_path = os.path.join(root, filename)
                    if re.match(r"^.*\.[A-Fa-f0-9]{8}$", file_path):
                        # a partial download was detected, create a part file
                        # for resuming it later
                        relative_path = file_path.replace(
                            str(self.local_graph_path) + "/", ""
                        )[:-9]
                        file_part_path = os.path.join(
                            self.local_graph_path, relative_path + ".part"
                        )
                        to_resume = False
                        if not os.path.exists(file_part_path):
                            # no part file exists
                            if (
                                os.path.getsize(file_path)
                                == self.file_sizes[relative_path]
                            ):
                                # file fully downloaded, rename it to its original name
                                os.rename(file_path, file_path[:-9])
                            else:
                                # create part file otherwise
                                to_resume = True
                                os.rename(file_path, file_part_path)
                        else:
                            # part file already exists, newly downloaded bytes must be
                            # appended to it
                            with open(file_part_path, "ab") as file_part:
                                with open(file_path, "rb") as file:
                                    while data := file.read(CHUNK_SIZE):
                                        file_part.write(data)
                                # remove no longer needed file
                                os.remove(file_path)
                            if (
                                os.path.getsize(file_part_path)
                                == self.file_sizes[relative_path]
                            ):
                                # file fully downloaded, rename it to its original name
                                os.rename(file_part_path, file_path[:-9])
                            else:
                                to_resume = True

                        if to_resume:
                            logger.debug(
                                "Downloaded bytes for %s dumped in %s to resume download later",
                                relative_path,
                                file_part_path,
                            )
            return False
        return True
