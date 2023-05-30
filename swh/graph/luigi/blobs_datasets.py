# Copyright (C) 2021-2022 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""
Luigi tasks for blob-centric datasets
=====================================

This module contains `Luigi <https://luigi.readthedocs.io/>`_ tasks
driving the creation of derived datasets centered around a subset of
content objects in the graph. Currently, this means:

* the `license dataset <https://annex.softwareheritage.org/public/dataset/license-blobs/>`_, and
* the `citation dataset <https://annex.softwareheritage.org/public/dataset/citation-blobs/>`_

File layout
-----------

This assumes a local compressed graph (from :mod:`swh.graph.luigi.compressed_graph`)
is present, and generates/manipulates the following files::

    base_dir/
        <date>[_<flavor>]/
            citation-blobs/
                blobs-earliest.csv.zst
                blobs-fileinfo.csv.zst
                blobs-nb-origins.csv.zst
                blobs-origins.csv.zst
                blobs-sample20k.tar.zst
                blobs.tar.zst
                import-dataset.sql
                license-blobs.csv.zst
            license-blobs/
                <same as above, plus these two:>
                blobs-scancode.csv.zst
                blobs-scancode.ndjson.zst
"""

# WARNING: do not import unnecessary things here to keep cli startup time under
# control
import contextlib
import functools
import hashlib
import logging
import os
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    ContextManager,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Set,
    Tuple,
    TypeVar,
    cast,
)

import luigi

from swh.dataset.luigi import CreateAthena, S3PathParameter

from .compressed_graph import LocalGraph

if TYPE_CHECKING:
    import asyncio

    import magic
    from requests import Session

    from swh.graph.grpc.swhgraph_pb2_grpc import TraversalServiceStub


def _s3_url_to_bucket_path(s3_url: str) -> Tuple[str, str]:
    loc = _removeprefix(s3_url, "s3://")
    bucket, path = loc.split("/", 1)
    return bucket, path


# XXX in wait of Python 3.9 for PER 616...
def _removeprefix(s, prefix):
    if s.startswith(prefix):
        return s[len(prefix) :]


logger = logging.getLogger(__name__)

COMPRESS_LEVEL = 19
GRAPH_REQUEST_CONCURRENCY = 70
EMPTY_FILE_SHA1 = "da39a3ee5e6b4b0d3255bfef95601890afd80709"


SELECTION_QUERIES = {
    "citation": r"""
        SELECT
            concat('swh:1:cnt:', t1.target) AS swhid,
            t2.sha1 AS sha1,
            t1.filename AS name
        FROM (
            SELECT DISTINCT target, lower(trim(from_utf8(name))) AS filename
            FROM directory_entry
            WHERE (
                type='file'
                AND (
                    lower(trim(from_utf8(name))) = 'codemeta.json'
                    OR lower(trim(from_utf8(name))) = 'citation.cff'
                )
            )
        ) AS t1
        LEFT JOIN (SELECT sha1,sha1_git FROM content) AS t2
        ON (t1.target=t2.sha1_git)
        ORDER BY sha1
    """,
    "license": r"""
        SELECT
            concat('swh:1:cnt:', t1.target) AS swhid,
            t2.sha1 AS sha1,
            t1.filename AS name
        FROM (
            SELECT DISTINCT target, lower(trim(from_utf8(name))) AS filename
            FROM directory_entry
            WHERE (
                type = 'file' AND
                regexp_like(
                    lower(from_utf8(name)),
                    '^([a-z0-9._-]+\.)?(copying|licen(c|s)(e|ing)|notice|copyright|disclaimer|authors)(\.[a-z0-9\._-]+)?$'
                )
            )
        ) AS t1
        LEFT JOIN (SELECT sha1,sha1_git FROM content) AS t2
        ON (t1.target=t2.sha1_git)
        ORDER BY sha1
    """,
    "readme": r"""
        SELECT
            concat('swh:1:cnt:', t1.target) AS swhid,
            t2.sha1 AS sha1,
            t1.filename AS name
        FROM (
            SELECT DISTINCT target, lower(trim(from_utf8(name))) AS filename
            FROM directory_entry
            WHERE (
                type = 'file' AND
                regexp_like(
                    lower(from_utf8(name)),
                    '^(readme)(\.[a-z0-9\._-]+)?$'
                )
            )
        ) AS t1
        LEFT JOIN (SELECT sha1,sha1_git FROM content) AS t2
        ON (t1.target=t2.sha1_git)
        ORDER BY sha1
    """,
}


_mime_guesser = None


def _init_mime_guesser():
    global _mime_guesser
    if _mime_guesser is None:
        import magic

        _mime_guesser = magic.Magic(mime=True, mime_encoding=True)

    return _mime_guesser


def _guess_mime(path: str) -> Tuple[str, str]:
    _mime_guesser = _init_mime_guesser()
    info = _mime_guesser.from_file(path)
    mime_type, encoding = info.split()
    mime_type, encoding = mime_type.rstrip(";"), _removeprefix(encoding, "charset=")

    return (mime_type, encoding)


@contextlib.contextmanager
def atomic_zstd_writer(result_path: Path):
    """Returns a file-like object, which writes to a temporary file,
    then atomically renames it to the ``result_path`` on success."""
    import pyzstd

    tmp_result_path = Path(f"{result_path}.tmp")
    try:
        with pyzstd.open(
            tmp_result_path, "wt", level_or_option=COMPRESS_LEVEL
        ) as output_fd:
            yield output_fd

        tmp_result_path.replace(result_path)

    except BaseException:
        tmp_result_path.unlink()
        raise


@contextlib.contextmanager
def atomic_csv_zstd_writer(result_path: Path):
    """Returns a ``csv.writer`` object, which writes to a temporary file,
    then atomically renames it to the ``result_path`` on success."""
    import csv

    with atomic_zstd_writer(result_path) as output_fd:
        yield csv.writer(output_fd, lineterminator="\n")


# luigi.Task with some helpers to get paths
class _BaseTask(luigi.Task):
    blob_filter: str
    derived_datasets_path: Path
    previous_derived_datasets_path: Optional[Path]

    def blob_count(self) -> int:
        """Returns the total number of selected blobs"""
        with self.blob_count_path().open() as fd:
            return int(fd.read().strip())

    def blob_size(self) -> int:
        """Returns the total size of selected blobs"""
        with self.blob_size_path().open() as fd:
            return int(fd.read().strip())

    def blob_size_path(self) -> Path:
        return self.derived_datasets_path / self.blob_filter / "stats" / "size.txt"

    def blob_count_path(self) -> Path:
        return self.derived_datasets_path / self.blob_filter / "stats" / "count.txt"

    def blob_dir(self) -> Path:
        return self.derived_datasets_path / self.blob_filter / "blobs"

    def blob_list_path(self) -> Path:
        return self.derived_datasets_path / self.blob_filter / "blobs.csv.zst"

    def blob_tarball_path(self) -> Path:
        return self.derived_datasets_path / self.blob_filter / "blobs.tar.zst"

    def previous_blob_tarball_path(self) -> Optional[Path]:
        if self.previous_derived_datasets_path:
            return (
                self.previous_derived_datasets_path / self.blob_filter / "blobs.tar.zst"
            )
        else:
            return None

    def sample_blob_tarball_path(self) -> Path:
        return self.derived_datasets_path / self.blob_filter / "blobs-sample20k.tar.zst"

    def iter_blobs(
        self, *, unique_sha1: bool, with_tqdm: bool = True
    ) -> Iterator[Tuple[str, str, str]]:
        """Yields ``(swhid, sha1, name)`` by reading :file:`blobs.csv.zst`,
        and uses tqdm for progress report.

        If ``unique_sha1`` is True, skips all but the first occurrence of each sha1."""
        import csv

        import pyzstd
        import tqdm

        last_sha1 = "" * 20
        with pyzstd.open(self.blob_list_path(), "rt") as fd:
            reader = csv.reader(cast(Iterator[str], fd))
            header = next(reader)
            if header != ["swhid", "sha1", "name"]:
                raise ValueError(
                    "Unexpected header in %s: %r", self.blob_list_path(), header
                )

            rows_it = reader
            if with_tqdm:
                rows_it = tqdm.tqdm(rows_it, total=self.blob_count())
            for row in rows_it:
                try:
                    (swhid, sha1, name) = row
                except ValueError:
                    raise ValueError(f"Unexpected row: {row!r}") from None
                if sha1 < last_sha1:
                    raise ValueError(f"Not sorted by sha1 ({last_sha1} before {sha1}")
                if not unique_sha1 or sha1 != last_sha1:
                    yield tuple(row)  # type: ignore[misc]
                last_sha1 = sha1

    def blob_paths(self, sha1: str) -> Tuple[Path, Path]:
        """Returns ``(sharded_path, unsharded_path)``, which are the two possible
        paths for this blob, depending on the blob dir layout."""
        sharded_path = self.blob_dir() / sha1[0:2] / sha1[2:4] / sha1
        unsharded_path = self.blob_dir() / sha1

        return (sharded_path, unsharded_path)

    def complete(self) -> bool:
        if not super().complete():
            return False

        for target in self.output():
            output_path = target.path
            if output_path.endswith(".csv.zst"):
                check_csv(Path(output_path))

        return True


_TCallable = TypeVar("_TCallable", bound=Callable)


def _log_exceptions(f: _TCallable) -> _TCallable:
    """Decorator for functions called by asyncio that would never be awaited if they
    crashed, causing asyncio to silently hide the exception."""

    @functools.wraps(f)
    def newf(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except BaseException:
            logger.exception(
                "Error while calling %s with %r and %r", f.__name__, args, kwargs
            )
            raise

    return newf  # type: ignore[return-value]


class _ConcurrentCsvWritingTask(_BaseTask):
    """Base classes for tasks writing a CSV using asyncio.

    asyncio is only used for gRPC requires to swh-graph; file writes are synchronous
    to keep the code simpler, as performance improvements from making them async
    would be negligeable."""

    CSV_HEADER: Tuple[str, str]

    blob_filter = luigi.ChoiceParameter(choices=list(SELECTION_QUERIES))
    derived_datasets_path = luigi.PathParameter()
    grpc_api = luigi.Parameter()

    stub: "TraversalServiceStub"

    def requires(self) -> luigi.Task:
        """Returns an instance of :class:`LocalGraph` and :class:`CreateAthena`"""
        return SelectBlobs(
            blob_filter=self.blob_filter,
            derived_datasets_path=self.derived_datasets_path,
        )

    def run(self) -> None:
        """Calls the :meth:`process_one` function, and writes its results as
        a two-column CSV to the target defined by :meth:`output`.
        """
        import asyncio

        asyncio.run(self._run_async())

    async def _run_async(self) -> None:
        import asyncio

        import grpc.aio

        import swh.graph.grpc.swhgraph_pb2_grpc as swhgraph_grpc

        input_queue: asyncio.Queue[Tuple[str, str, str]] = asyncio.Queue(maxsize=20)
        result_queue: asyncio.Queue[Tuple[str, str]] = asyncio.Queue(maxsize=20)

        async with grpc.aio.insecure_channel(self.grpc_api) as channel:
            self.stub = swhgraph_grpc.TraversalServiceStub(channel)

            fill_queue_task = asyncio.create_task(self._fill_input_queue(input_queue))
            write_task = asyncio.create_task(self._write_results(result_queue))

            worker_tasks = [
                asyncio.create_task(self._worker(input_queue, result_queue))
                for _ in range(GRAPH_REQUEST_CONCURRENCY)
            ]

            await write_task  # wait for workers to write everything

            await fill_queue_task  # should be instant

            for task in worker_tasks:
                task.cancel()

            await asyncio.gather(
                fill_queue_task,
                write_task,
                *worker_tasks,
                return_exceptions=True,
            )

    @_log_exceptions
    async def _fill_input_queue(
        self, input_queue: "asyncio.Queue[Tuple[str, str, str]]"
    ) -> None:
        for (swhid, sha1, name) in self.iter_blobs(with_tqdm=False, unique_sha1=True):
            if not swhid.startswith("swh:1:"):
                raise ValueError(f"Invalid SWHID: {swhid}")

            await input_queue.put((swhid, sha1, name))

    @_log_exceptions
    async def _worker(
        self,
        input_queue: "asyncio.Queue[Tuple[str, str, str]]",
        result_queue: "asyncio.Queue[Tuple[str, str]]",
    ) -> None:
        while True:  # exit via Task.cancel()
            row = await input_queue.get()
            (swhid, sha1, name) = row
            try:
                res = await self.process_one(row)
            except BaseException as e:
                res = (swhid, "")
                logger.exception("Error while processing %r", row)
                if not isinstance(e, Exception):
                    # KeyboardInterrupt, ...
                    raise
            await result_queue.put(res)

    async def _write_results(
        self, result_queue: "asyncio.Queue[Tuple[str, str]]"
    ) -> None:
        import tqdm.asyncio

        (target,) = self.output()
        result_path = Path(target.path)

        with atomic_csv_zstd_writer(result_path) as writer:

            writer.writerow(self.CSV_HEADER)

            async for i in tqdm.asyncio.trange(self.blob_count()):
                (swhid, result) = await result_queue.get()
                writer.writerow((swhid, result))


def check_csv(csv_path: Path) -> None:
    import re

    import pyzstd

    with cast(ContextManager[Iterator[str]], pyzstd.open(csv_path, "rt")) as fd:
        try:
            header = next(fd)
        except StopIteration:
            raise ValueError(f"{csv_path} is empty") from None
        except pyzstd.ZstdError:
            raise ValueError(f"{csv_path} could not be decompressed as zstd") from None

        # check the header contains no whitespace
        if len(header.split()) != 1:
            raise ValueError(
                f"{csv_path.name} is not comma-separated "
                f"(or has whitespaces in column name)"
            )

        columns = header.split(",")
        if columns[0] != "swhid":
            raise ValueError(
                f"First column of {csv_path.name} is {columns[0]!r} "
                f"but should be 'swhid'"
            )

        try:
            first_line = next(fd)
        except StopIteration:
            raise ValueError(f"{csv_path} has no content") from None

        if not re.match("^swh:1:cnt:[0-9a-f]{40},", first_line):
            raise ValueError(f"{csv_path} has unexpected first row: {first_line}")


class SelectBlobs(_BaseTask):
    blob_filter = luigi.ChoiceParameter(choices=list(SELECTION_QUERIES))
    derived_datasets_path = luigi.PathParameter()
    athena_db_name = luigi.Parameter()
    s3_athena_output_location = S3PathParameter()

    def requires(self) -> List[luigi.Task]:
        """Returns an instance of :class:`LocalGraph` and :class:`CreateAthena`"""
        return [
            LocalGraph(),
            CreateAthena(),
        ]

    def output(self) -> List[luigi.Target]:
        """:file:`blobs.csv.zst` and :file:`stats/count.txt` in
        ``self.derived_datasets_path / self.blob_filter``"""
        return [
            luigi.LocalTarget(self.blob_list_path()),
            luigi.LocalTarget(self.blob_count_path()),
        ]

    def run(self) -> None:
        """Runs an Athena query to get the list of blobs and writes it to
        :file:`blobs.csv.zst`."""
        import csv
        import io
        import re
        import shutil
        import tempfile

        import boto3

        from swh.dataset.athena import query

        from .shell import AtomicFileSink, Command

        athena = boto3.client("athena")
        athena.database_name = self.athena_db_name
        athena.output_location = (
            f"{self.s3_athena_output_location}/select-{self.blob_filter}/"
        )

        result = query(athena, SELECTION_QUERIES[self.blob_filter])

        s3 = boto3.client("s3")

        bucket, path = _s3_url_to_bucket_path(
            result["ResultConfiguration"]["OutputLocation"]
        )

        s3_object = s3.get_object(Bucket=bucket, Key=path)["Body"]

        output_path = self.blob_list_path()
        output_path.parent.mkdir(parents=True, exist_ok=True)

        with tempfile.NamedTemporaryFile(
            prefix="blobs-", suffix=".csv"
        ) as athena_res_fd:
            # Check the first lines matches the expected format
            first_chunk = s3_object.read(1024)

            reader = csv.reader(io.StringIO(first_chunk.decode(errors="ignore")))
            assert next(reader) == ["swhid", "sha1", "name"]
            try:
                first_row = next(reader)
            except StopIteration:
                raise ValueError("Athena returned an empty list of blobs") from None
            assert re.match("^swh:1:cnt:[0-9a-f]{40}$", first_row[0]), first_row
            assert re.match("^[0-9a-f]{40}$", first_row[1]), first_row
            assert first_row[2], first_row
            athena_res_fd.write(first_chunk)

            # Copy the rest of the file
            shutil.copyfileobj(s3_object, athena_res_fd)
            athena_res_fd.flush()

            # In the 2022-04-24 license dataset, the 'egrep' command filters
            # just 5 entries:
            #
            # $ egrep -v '^"[^"]*","[^"]*","[^"]*"$' license-blobs.csv
            # "swh:1:cnt:03e1933241b8c3878d81c0184d7f2fd3d8cd6185","037d40bc6bcb42dfd740be545dbdf2df3405442f","LICENSE
            # "
            # "swh:1:cnt:65a5c662900ee946583147129720563fd4ba286d","40e9258799f752fe25d7518155c615c1c497b7ac","LICENSE.md
            # "
            # "swh:1:cnt:8751b326784c7e145b242637866a4d46e8e274a5","a6bad643d9defc1e667c708d0d9aa9f1d6752fbc","LICENSE
            # "
            # "swh:1:cnt:e69de29bb2d1d6434b8b29ae775ad8c2e48c5391","da39a3ee5e6b4b0d3255bfef95601890afd80709","license.txt
            # "
            # "swh:1:cnt:82714d7648eb4f6cda2ed88fc4768e7d05472fe6","f096063880f4d0329856d3fca51c6d8afa13af9b","LICENSE.txt
            # "

            # fmt: off
            (
                Command.cat(athena_res_fd.name)
                | Command.egrep('^"[^"]*","[^"]*","[^"]*"$')
                | Command.sed(r's/^"\([^"]*\)","\([^"]*\)",/\1,\2,/')
                | Command.zstdmt("-")
                > AtomicFileSink(output_path)
            ).run()
            # fmt: on

            count = sum(1 for _ in self.iter_blobs(with_tqdm=False, unique_sha1=True))
            self.blob_count_path().parent.mkdir(exist_ok=True, parents=True)
            with self.blob_count_path().open("wt") as fd:
                fd.write(f"{count}\n")

        s3.delete_object(Bucket=bucket, Key=path)


class DownloadBlobs(_BaseTask):
    blob_filter = luigi.ChoiceParameter(choices=list(SELECTION_QUERIES))
    derived_datasets_path = luigi.PathParameter()
    previous_derived_datasets_path = luigi.OptionalPathParameter(default=None)
    parallel_downloads = luigi.IntParameter(default=10, significant=False)
    download_url = luigi.Parameter(
        default="https://archive.softwareheritage.org/api/1/content/sha1:{sha1}/raw/",
        significant=False,
    )
    decompression_algo = luigi.ChoiceParameter(
        choices=["none", "gzip"],
        default="none",
        description="""The decompression algorithm to use after downloading.
        Defaults to 'none' to match the SWH API. Should be 'gzip' when downloading from
        s3://softwareheritage/""",
    )

    _session = None
    _session_pid = None

    def requires(self) -> luigi.Task:
        """Returns an instance of :class:`LocalGraph` and :class:`CreateAthena`"""
        return SelectBlobs(
            blob_filter=self.blob_filter,
            derived_datasets_path=self.derived_datasets_path,
        )

    def output(self) -> List[luigi.Target]:
        """:file:`stats/size.txt` in ``self.derived_datasets_path / self.blob_filter``"""
        return [
            luigi.LocalTarget(self.blob_dir()),
            luigi.LocalTarget(self.blob_size_path()),
        ]

    @classmethod
    def _compute_sha1(cls, path: Path) -> str:
        with path.open("rb") as fd:
            h = hashlib.sha1()
            while True:
                data = fd.read(40960)
                if not data:
                    break
                h.update(data)
        return h.hexdigest()

    def _download_blob(self, session: "Session", path: Path, sha1: str) -> int:
        """Returns the size in bytes."""
        import shutil
        import time

        import requests

        while True:
            url = self.download_url.format(sha1=sha1)
            try:
                resp = session.get(url, stream=True)
            except requests.exceptions.ConnectionError:
                logger.exception("Failed to query %s, retrying in 10 seconds:", url)
                time.sleep(10)
                continue
            if resp.status_code == 429:
                rate_limit_reset = int(resp.headers["X-RateLimit-Reset"])
                wait_seconds = max(10, rate_limit_reset - time.time())
                logger.warning("Waiting timeout for %d seconds", wait_seconds)
                time.sleep(wait_seconds)
                continue
            elif 500 <= resp.status_code < 600:
                logger.warning("Got %d error, retrying in 10 seconds", resp.status_code)
                time.sleep(10)
            elif resp.status_code == 200:
                break
            elif resp.status_code == 404:
                logger.error("%s returned 404", url)
                return 0
            else:
                msg = f"Unexpected status code: {resp.status_code}"
                logger.error(msg)
                logger.error(resp.text)
                raise Exception(msg)

        tmp_path = path.parent / f".tmp_{sha1}"

        if self.decompression_algo == "none":
            with tmp_path.open("wb") as fd:
                for chunk in resp.iter_content(chunk_size=40960):
                    fd.write(chunk)
        elif self.decompression_algo == "gzip":
            import gzip

            if not hasattr(gzip, "BadGzipFile"):
                # Python < 3.8
                BadGzipFile = OSError
            else:
                BadGzipFile = gzip.BadGzipFile

            try:
                with gzip.open(resp.raw, "rb") as compressed_fd:
                    with tmp_path.open("wb") as decompressed_fd:
                        shutil.copyfileobj(compressed_fd, decompressed_fd)
            except BadGzipFile as e:
                if e.args[0] == r"Not a gzipped file (b'\x00\x00')":
                    # WTF? https://gitlab.softwareheritage.org/swh/meta/-/issues/5034
                    print(f"{sha1} has null bytes instead of magic value")
                    return 0
                else:
                    raise
        else:
            assert False, f"Unexpected decompression algo: {self.decompression_algo}"

        if self._compute_sha1(tmp_path) != sha1:
            if tmp_path.stat().st_size == 0 and sha1 != EMPTY_FILE_SHA1:
                msg = f"Blob downloaded to {tmp_path} is empty"
            else:
                msg = f"Blob downloaded to {tmp_path} does not match its checksum"
            logger.error(msg)
            raise Exception(msg)

        # Atomically write to destination
        tmp_path.rename(path)

        return path.stat().st_size

    def _download_blob_if_missing(self, session: "Session", sha1: str) -> int:
        """Returns the size in bytes."""
        assert set(sha1) <= set("0123456789abcdef"), sha1
        assert len(sha1) == 40, sha1
        (sharded_path, unsharded_path) = self.blob_paths(sha1)

        for path in (sharded_path, unsharded_path):
            if path.exists():
                if self._compute_sha1(path) != sha1:
                    msg = f"Existing blob at {path} does not match its checksum"
                    logger.error(msg)
                    raise Exception(msg)
                logger.debug("Skipping %s, already exists", sha1)
                return path.stat().st_size
        else:
            logger.debug("Downloading %s", sha1)
            return self._download_blob(session, sharded_path, sha1)

    def run(self) -> None:
        """Reads file SHA1 hashes from :file:`blobs.csv.zst` and downloads them
        to :file:`blobs/`."""

        import multiprocessing
        import multiprocessing.dummy

        import tqdm

        from .shell import Command

        # Create sharded directories for the blobs
        for i in range(256):
            for j in range(256):
                (self.blob_dir() / f"{i:02x}" / f"{j:02x}").mkdir(
                    exist_ok=True, parents=True
                )

        previous_blob_tarball_path = self.previous_blob_tarball_path()
        if previous_blob_tarball_path:
            # reuse blobs from a previous version of the dataset, so we don't have
            # to download them one by one
            print(f"Unpacking previous blobs from {previous_blob_tarball_path}")
            # fmt: off
            (
                Command.pv(previous_blob_tarball_path)
                | Command.zstdcat()
                | Command.tar("-x", "-C", self.blob_dir().parent)  # tar root is blobs/
            ).run()
            # fmt: on
            print("Done unpacking")

        total_size = 0

        # Use a thread pool (more efficient because no IPC) if there is no compression
        # (because then it's IO bound), and a process pool when there is (it would
        # be single-thread CPU bound otherwise)
        Pool = multiprocessing.Pool
        if self.decompression_algo == "none":
            Pool = multiprocessing.dummy.Pool  # type: ignore[assignment]
        with Pool(self.parallel_downloads) as pool:
            for size in tqdm.tqdm(
                pool.imap_unordered(
                    self._worker,
                    self.iter_blobs(unique_sha1=True, with_tqdm=False),
                    chunksize=100,
                ),
                total=self.blob_count(),
            ):
                total_size += size

        with self.blob_size_path().open("wt") as fd:
            fd.write(f"{total_size}\n")

    def session(self):
        if self._session_pid != os.getpid():
            # we forked; create a new session for this process
            import requests

            self._session_pid = os.getpid()

            self._session = requests.Session()
            self._session.headers[
                "User-Agent"
            ] = f"SWH {self.blob_filter} Dataset blob downloader"

            auth_token = os.environ.get("SWH_AUTH_TOKEN")
            if auth_token:
                self._session.headers["Authorization"] = f"Bearer {auth_token}"
        return self._session

    def _worker(self, args):
        (swhid, sha1, name) = args
        return self._download_blob_if_missing(self.session(), sha1)


class MakeBlobTarball(_BaseTask):
    blob_filter = luigi.ChoiceParameter(choices=list(SELECTION_QUERIES))
    derived_datasets_path = luigi.PathParameter()

    def requires(self) -> luigi.Task:
        """Returns an instance of :class:`LocalGraph` and :class:`CreateAthena`"""
        return DownloadBlobs(
            blob_filter=self.blob_filter,
            derived_datasets_path=self.derived_datasets_path,
        )

    def output(self) -> List[luigi.Target]:
        """:file:`blobs.tar.zst` in ``self.derived_datasets_path / self.blob_filter``"""
        return [luigi.LocalTarget(self.blob_tarball_path())]

    def run(self) -> None:
        """Run task."""
        from .shell import AtomicFileSink, Command

        approx_tarball_size = (
            self.blob_size()  # the content itself
            + 512 * self.blob_count()  # assuming one header per file
        )

        cwd = self.derived_datasets_path / self.blob_filter

        # fmt: off
        (
            Command.tar("-c", "--sort=name", "blobs/", cwd=cwd)
            | Command.pv("--size", str(approx_tarball_size))
            | Command.zstdmt(f"-{COMPRESS_LEVEL}")
            > AtomicFileSink(self.blob_tarball_path())
        ).run()
        # fmt: on


class MakeSampleBlobTarball(_BaseTask):
    blob_filter = luigi.ChoiceParameter(choices=list(SELECTION_QUERIES))
    derived_datasets_path = luigi.PathParameter()

    def requires(self) -> luigi.Task:
        """Returns an instance of :class:`LocalGraph` and :class:`CreateAthena`"""
        return DownloadBlobs(
            blob_filter=self.blob_filter,
            derived_datasets_path=self.derived_datasets_path,
        )

    def output(self) -> List[luigi.Target]:
        """:file:`blobs.tar.zst` in ``self.derived_datasets_path / self.blob_filter``"""
        return [luigi.LocalTarget(self.sample_blob_tarball_path())]

    def run(self) -> None:
        """Selects a sample of 20k random blobs and puts them in a tarball."""
        from .shell import AtomicFileSink, Command

        cwd = self.derived_datasets_path / self.blob_filter
        # fmt: off
        (
            Command.zstdcat(self.blob_list_path())
            | Command.tail("-n", "+2")
            | Command.cut("-d", ",", "-f", "2")
            | Command.uniq()
            | Command.shuf("--head-count=20000")
            | Command.sort()
            | Command.sed(r"s#\(\(..\)\(..\)\(..*\)\)#blobs/\2/\3/\1#")
            | Command.tar(
                "-c",
                "--files-from=/dev/stdin",
                "--transform=s/^blobs/blobs-sample20k/",
                cwd=cwd
            )
            | Command.zstdmt(f"-{COMPRESS_LEVEL}")
            > AtomicFileSink(self.sample_blob_tarball_path())
        ).run()
        # fmt: on


class ComputeBlobFileinfo(_BaseTask):
    blob_filter = luigi.ChoiceParameter(choices=list(SELECTION_QUERIES))
    derived_datasets_path = luigi.PathParameter()

    CSV_HEADER = (
        "swhid",
        "mime_type",
        "encoding",
        "line_count",
        "word_count",
        "size",
    )
    READABLE_ENCODINGS = ("us-ascii", "utf-8", "iso-8859-1")

    def requires(self) -> luigi.Task:
        """Returns an instance of :class:`LocalGraph` and :class:`CreateAthena`"""
        return DownloadBlobs(
            blob_filter=self.blob_filter,
            derived_datasets_path=self.derived_datasets_path,
        )

    def output(self) -> List[luigi.Target]:
        """:file:`blobs-fileinfo.csv.zst` in
        ``self.derived_datasets_path / self.blob_filter``"""
        return [
            luigi.LocalTarget(
                self.derived_datasets_path / self.blob_filter / "blobs-fileinfo.csv.zst"
            )
        ]

    def run(self) -> None:
        """Run task."""
        import multiprocessing

        import tqdm

        with atomic_csv_zstd_writer(self.output()[0].path) as writer:
            writer.writerow(self.CSV_HEADER)

            with multiprocessing.Pool() as pool:
                # imap instead of imap_unordered, to preserve sha1 order of blobs
                for row in tqdm.tqdm(
                    pool.imap(
                        self._analyze_blob,
                        self.iter_blobs(unique_sha1=True, with_tqdm=False),
                    ),
                    total=self.blob_count(),
                ):
                    writer.writerow(row)

    def _analyze_blob(self, row) -> Tuple[str, str, str, str, str, str]:
        (swhid, sha1, name) = row

        (path, _) = self.blob_paths(sha1)
        assert path.exists(), f"{path} does not exist"

        size = path.stat().st_size
        mime_type, encoding = _guess_mime(str(path))
        line_count, word_count = None, None

        if mime_type == "text/plain" and encoding in self.READABLE_ENCODINGS:
            line_count = 0
            word_count = 0
            try:
                with open(path, encoding="utf8") as f:
                    for line in f:
                        line_count += 1
                        word_count += len(line.rstrip().split())
            except UnicodeDecodeError:
                line_count = None
                word_count = None

        return (
            swhid,
            mime_type,
            encoding,
            str(line_count) if line_count is not None else "",
            str(word_count) if word_count is not None else "",
            str(size),  # byte count
        )


class BlobScancode(_BaseTask):
    """Runs scancode-toolkit on the blob dataset"""

    blob_filter = luigi.ChoiceParameter(choices=list(SELECTION_QUERIES))
    derived_datasets_path = luigi.PathParameter()

    FIELDNAMES = [
        "swhid",
        "license",
        "score",
    ]

    DEFAULT_MIN_SCORE = 0
    DEFAULT_JOBS = 1
    DEFAULT_TIMEOUT = 120
    MAP_CHUNKSIZE = 1
    WORKER_MAX_TASKS = 1000  # to workaround Scancode get_licenses() memory leaks
    FIELD_SEP = ","
    READABLE_ENCODINGS = ("us-ascii", "utf-8", "iso-8859-1")

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        self.mime_guesser: Optional[magic.Magic] = None  # set in child processes

    def requires(self) -> luigi.Task:
        """Returns an instance of :class:`LocalGraph` and :class:`CreateAthena`"""
        return DownloadBlobs(
            blob_filter=self.blob_filter,
            derived_datasets_path=self.derived_datasets_path,
        )

    def _csv_path(self) -> Path:
        return self.derived_datasets_path / self.blob_filter / "blobs-scancode.csv.zst"

    def _json_path(self) -> Path:
        return (
            self.derived_datasets_path / self.blob_filter / "blobs-scancode.ndjson.zst"
        )

    def output(self) -> List[luigi.Target]:
        """:file:`blobs-scancode.csv.zst` and :file:`blobs-scancode.ndjson.zst` in
        ``self.derived_datasets_path / self.blob_filter``"""
        return [
            luigi.LocalTarget(self._csv_path()),
            luigi.LocalTarget(self._json_path()),
        ]

    def _detect_licenses(self, row) -> Tuple[Set[Tuple[str, str, float]], str]:
        import json
        import time

        # needs to be initialized before importing scancode:
        # https://github.com/nexB/scancode-plugins/issues/30
        _init_mime_guesser()

        from scancode.api import get_copyrights, get_licenses

        (swhid, sha1, name) = row
        (path, _) = self.blob_paths(sha1)
        assert path.exists(), f"{path} does not exist"

        mime_type, encoding = _guess_mime(str(path))
        license_rows = set()

        res: Dict[str, Any] = {"swhid": swhid}

        if mime_type == "text/plain" and encoding in self.READABLE_ENCODINGS:
            deadline = time.time() + self.DEFAULT_TIMEOUT
            res["licenses"] = get_licenses(
                str(path), min_score=self.DEFAULT_MIN_SCORE, deadline=deadline
            )
            license_rows = {
                (
                    swhid,
                    lic["spdx_license_key"],
                    lic["score"],
                )
                for lic in res["licenses"]["licenses"]
            }
            res["copyrights"] = get_copyrights(str(path))

        return (license_rows, json.dumps(res))

    def run(self) -> None:
        """Detect license(s) of license blobs located under blob_dir using scancode.

        Save frequencies to csv_outfile in a 3-column (sha1, license, score) CSV format.

        """
        import csv
        import multiprocessing
        import multiprocessing.pool

        import tqdm

        # ensure clean slate
        if self._csv_path().exists():
            self._csv_path().unlink()
        if self._json_path().exists():
            self._json_path().unlink()

        context = multiprocessing.get_context(method="spawn")
        with atomic_zstd_writer(self._csv_path()) as csvfile, atomic_zstd_writer(
            self._json_path()
        ) as jsonfile:
            csv_writer = csv.writer(csvfile, delimiter=self.FIELD_SEP)
            csv_writer.writerow(self.FIELDNAMES)
            with multiprocessing.pool.Pool(
                maxtasksperchild=self.WORKER_MAX_TASKS,
                context=context,
            ) as pool:
                for (license_rows, results) in tqdm.tqdm(
                    pool.imap_unordered(
                        self._detect_licenses,
                        self.iter_blobs(unique_sha1=True, with_tqdm=False),
                        chunksize=self.MAP_CHUNKSIZE,
                    ),
                    total=self.blob_count(),
                ):
                    # each detect() call can return multiple licenses, flatten them
                    for (sha1, license, score) in license_rows:
                        csv_writer.writerow([sha1, license, str(score)])
                    assert "\n" not in results
                    jsonfile.write(results + "\n")
                print("Done")


class FindBlobOrigins(_ConcurrentCsvWritingTask):
    previous_derived_datasets_path = luigi.OptionalPathParameter(default=None)

    def output(self) -> List[luigi.Target]:
        """:file:`blobs.tar.zst` in ``self.derived_datasets_path / self.blob_filter``"""
        return [
            luigi.LocalTarget(
                self.derived_datasets_path / self.blob_filter / "blobs-origins.csv.zst"
            )
        ]

    CSV_HEADER = ("swhid", "origin_url")

    async def process_one(self, row: Tuple[str, str, str]) -> Tuple[str, str]:
        from google.protobuf.field_mask_pb2 import FieldMask

        import swh.graph.grpc.swhgraph_pb2 as swhgraph

        (swhid, sha1, name) = row
        if not swhid.startswith("swh:1:"):
            raise ValueError(f"Invalid SWHID: {swhid}")

        # If we are running incrementally, skip the request
        origin_url = self.existing_swhids.get(swhid)

        if not origin_url:
            response = self.stub.Traverse(
                swhgraph.TraversalRequest(
                    src=[swhid],
                    direction=swhgraph.GraphDirection.BACKWARD,
                    mask=FieldMask(paths=["ori.url"]),
                    return_nodes=swhgraph.NodeFilter(
                        types="ori",  # return only origins...
                    ),
                    max_matching_nodes=1,  # return at most one
                )
            )
            async for item in response:
                origin_url = item.ori.url
                assert origin_url
                break
            else:
                # no origin found
                origin_url = ""

        assert origin_url is not None

        return (swhid, origin_url)

    def run(self) -> None:
        import pyzstd

        self.existing_swhids: Dict[str, str] = {}

        if self.previous_derived_datasets_path:
            # reuse blobs from a previous version of the dataset, so we don't have
            # to recompute them all
            path = (
                self.previous_derived_datasets_path
                / self.blob_filter
                / "blobs-origins.csv.zst"
            )
            with pyzstd.open(path, "rt") as fd:
                self.existing_swhids = dict(
                    line.strip().split("\t")
                    for line in cast(Iterable[str], fd)
                    if line[-2] != "\t"
                )

        super().run()


class CountBlobOrigins(_ConcurrentCsvWritingTask):
    CSV_HEADER = ("swhid", "origin_count")

    def output(self) -> List[luigi.Target]:
        """:file:`blobs.tar.zst` in ``self.derived_datasets_path / self.blob_filter``"""
        return [
            luigi.LocalTarget(
                self.derived_datasets_path
                / self.blob_filter
                / "blobs-nb-origins.csv.zst"
            )
        ]

    async def process_one(self, row: Tuple[str, str, str]) -> Tuple[str, str]:
        from google.protobuf.field_mask_pb2 import FieldMask

        import swh.graph.grpc.swhgraph_pb2 as swhgraph

        (swhid, sha1, name) = row
        if not swhid.startswith("swh:1:"):
            raise ValueError(f"Invalid SWHID: {swhid}")

        response = await self.stub.CountNodes(
            swhgraph.TraversalRequest(
                src=[swhid],
                direction=swhgraph.GraphDirection.BACKWARD,
                mask=FieldMask(paths=["url"]),
                return_nodes=swhgraph.NodeFilter(
                    types="ori",  # count only origins
                ),
            )
        )

        return (swhid, response.count)


class FindEarliestRevisions(_BaseTask):
    blob_filter = luigi.ChoiceParameter(choices=list(SELECTION_QUERIES))
    derived_datasets_path = luigi.PathParameter()
    local_graph_path = luigi.PathParameter()
    graph_name = luigi.Parameter(default="graph")

    def requires(self) -> luigi.Task:
        """Returns an instance of :class:`LocalGraph` and :class:`CreateAthena`"""
        return SelectBlobs(
            blob_filter=self.blob_filter,
            derived_datasets_path=self.derived_datasets_path,
        )

    def output(self) -> List[luigi.Target]:
        """:file:`blobs-earliest.csv.zst` in
        ``self.derived_datasets_path / self.blob_filter``"""
        return [
            luigi.LocalTarget(
                self.derived_datasets_path / self.blob_filter / "blobs-earliest.csv.zst"
            )
        ]

    def run(self) -> None:
        """Run task."""
        from .shell import AtomicFileSink, Command, Java

        class_name = "org.softwareheritage.graph.utils.FindEarliestRevision"

        # fmt: off
        (
            Command.zstdcat(self.blob_list_path())
            | Command.sed("s/,.*//")
            | Command.tail("-n", "+2")  # skip the header
            | Command.uniq()
            | Java(
                class_name,
                self.local_graph_path / self.graph_name,
                max_ram=100_000_000_000
            )
            | Command.pv("--wait", "--line-mode", "--size", str(self.blob_count()))
            | Command.zstdmt(f"-{COMPRESS_LEVEL}")
            > AtomicFileSink(self.output()[0])
        ).run()
        # fmt: on


class RunBlobDataset(luigi.Task):
    """Runs all tasks to build a blob dataset with the given filter."""

    blob_filter = luigi.ChoiceParameter(choices=list(SELECTION_QUERIES))
    derived_datasets_path = luigi.PathParameter()

    def requires(self) -> List[luigi.Task]:
        """Returns a list of task such that every task in this module are transitively
        depended on."""
        kwargs = dict(
            blob_filter=self.blob_filter,
            derived_datasets_path=self.derived_datasets_path,
        )
        tasks = [
            DownloadBlobs(**kwargs),
            MakeBlobTarball(**kwargs),
            MakeSampleBlobTarball(**kwargs),
            FindBlobOrigins(**kwargs),
            CountBlobOrigins(**kwargs),
            FindEarliestRevisions(**kwargs),
            ComputeBlobFileinfo(**kwargs),
        ]

        if self.blob_filter in ("citation", "readme"):
            pass
        elif self.blob_filter == "license":
            tasks.append(BlobScancode(**kwargs))
        else:
            raise ValueError(f"Unexpected blob filter: {self.blob_filter}")

        return tasks

    def complete(self) -> bool:
        """Always returns False; status is checked by dependencies."""
        return False

    def run(self):
        """Checks all files are in the correct format and contain a well-known SWHID"""
        dir_path = self.derived_datasets_path / self.blob_filter
        for csv_path in dir_path.glob("*.csv.zst"):
            check_csv(csv_path)

        if self.blob_filter == "license":
            # Text of the GPLv3
            swhid = "swh:1:cnt:94a9ed024d3859793618152ea559a168bbcbb5e2"
            expected_fileinfo = f"{swhid},text/plain,us-ascii,674,5644,35147\n"
            min_expected_origins = 2_000_000
        elif self.blob_filter == "citation":
            # Codemeta's own codemeta.json
            swhid = "swh:1:cnt:6daebd857f6f6a98dd9288ef7b942283f7fa4f0e"
            expected_fileinfo = f"{swhid},application/json,us-ascii,,,7173\n"
            min_expected_origins = 5
        else:
            assert False, f"Unexpected blob filter: {self.blob_filter}"

        self._check_fileinfo(
            swhid, expected_fileinfo, dir_path / "blobs-fileinfo.csv.zst"
        )
        if self.blob_filter == "license":
            self._check_scancode(swhid, dir_path)
        self._check_nb_origins(
            swhid, min_expected_origins, dir_path / "blobs-nb-origins.csv.zst"
        )
        self._check_exactly_one_line(swhid, dir_path / "blobs-origins.csv.zst")
        self._check_exactly_one_line(swhid, dir_path / "blobs-earliest.csv.zst")

    def _check_fileinfo(self, swhid: str, expected_fileinfo: str, path: Path) -> None:
        from .shell import Command, Sink

        # fmt: off
        results = (
            Command.zstdcat(path)
            | Command.grep("^" + swhid)
            > Sink()
        ).run()
        # fmt: on

        assert results.decode() == expected_fileinfo, f"Unexpected fileinfo for {swhid}"

    def _check_scancode(self, swhid: str, dir_path: Path) -> None:
        import json

        from .shell import Command, Sink

        assert swhid == "swh:1:cnt:94a9ed024d3859793618152ea559a168bbcbb5e2"

        # fmt: off
        csv_results = (
            Command.zstdcat(dir_path / "blobs-scancode.csv.zst")
            | Command.grep("^" + swhid)
            > Sink()
        ).run().decode()
        # fmt: on

        assert csv_results == (
            f"{swhid},GPL-3.0-only,100.0\r\n"
        ), f"Unexpected scancode CSV for {swhid}: {csv_results!r}"

        # fmt: off
        json_results = (
            Command.zstdcat(dir_path / "blobs-scancode.ndjson.zst")
            | Command.grep(swhid)
            > Sink()
        ).run().decode()
        # fmt: on
        assert (
            json_results.count("\n") == 1
        ), f"Unexpectedly number of results for {swhid} in scancode NDJSON:\n{json_results}"
        result = json.loads(json_results)
        assert result["swhid"] == swhid, result
        licenses = [license for license in result["licenses"]["licenses"]]
        assert (
            len(licenses) == 1
        ), f"Unexpected number of licenses for {swhid}: {licenses}"
        assert licenses[0]["key"] == "gpl-3.0"
        assert licenses[0]["score"] == 100.0

    def _check_exactly_one_line(self, swhid: str, path: Path) -> None:
        from .shell import Command, Sink

        # fmt: off
        results = (
            Command.zstdcat(path)
            | Command.grep("^" + swhid)
            > Sink()
        ).run()
        # fmt: on

        assert (
            results.count(b"\n") == 1
        ), f"Unexpected number of line for {swhid} in {path}:\n{results}"

    def _check_nb_origins(
        self, swhid: str, min_expected_origins: int, path: Path
    ) -> None:

        from .shell import Command, Sink

        # fmt: off
        results = (
            Command.zstdcat(path)
            | Command.grep("^" + swhid)
            > Sink()
        ).run().decode()
        # fmt: on

        assert (
            results.count("\n") == 1
        ), f"Unexpected number of origin counts for {swhid}:\n{results}"
        count = int(results.split("\n")[0].split(",")[1])
        assert (
            min_expected_origins <= count <= 1_000_000_000
        ), f"Unexpected number of origins for {swhid}: {count}"
