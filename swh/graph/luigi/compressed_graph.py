# Copyright (C) 2022 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""
Luigi tasks for compression
===========================

This module contains `Luigi <https://luigi.readthedocs.io/>`_ tasks,
as an alternative to the CLI that can be composed with other tasks,
such as swh-dataset's.

Unlike the CLI, this requires the graph to be named `graph`.

File layout
-----------

In addition to files documented in :ref:`graph-compression` (eg. :file:`graph.graph`,
:file:`graph.mph`, ...), tasks in this module produce this directory structure::

    base_dir/
        <date>[_<flavor>]/
            compressed/
                graph.graph
                graph.mph
                ...
                meta/
                    export.json
                    compression.json

``graph.meta/export.json`` is copied from the ORC dataset exported by
:mod:`swh.dataset.luigi`.

``graph.meta/compression.json``  contains information about the compression itself,
for provenance tracking.
For example:

.. code-block:: json

    [
        {
            "steps": null,
            "export_start": "2022-11-08T11:00:54.998799+00:00",
            "export_end": "2022-11-08T11:05:53.105519+00:00",
            "object_type": [
                "origin",
                "origin_visit"
            ],
            "hostname": "desktop5",
            "conf": {},
            "tool": {
                "name": "swh.graph",
                "version": "2.2.0"
            }
        }
    ]

When the compression pipeline is run in separate steps, each of the steps is recorded
as an object in the root list.

S3 layout
---------

As ``.bin`` files are meant to be accessed randomly, they are uncompressed on disk.
However, this is undesirable on at-rest/long-term storage like on S3, because
some are very sparse (eg. :file:`graph.property.committer_timestamp.bin` can be
quickly compressed from 300 to 1GB).

Therefore, these files are compressed to ``.bin.zst``, and need to be decompressed
when downloading.

The layout is otherwise the same as the file layout.
"""

# WARNING: do not import unnecessary things here to keep cli startup time under
# control
from typing import List

import luigi

from swh.dataset.luigi import Format, LocalExport, ObjectType, S3PathParameter


class CompressGraph(luigi.Task):
    local_export_path = luigi.PathParameter(significant=False)
    local_graph_path = luigi.PathParameter()
    batch_size = luigi.IntParameter(
        default=0,
        significant=False,
        description="""
        Size of work batches to use while compressing.
        Larger is faster, but consumes more resources.
        """,
    )

    object_types = list(ObjectType)
    # To make this configurable, we could use this:
    #   object_types = luigi.EnumListParameter(
    #       enum=ObjectType, default=list(ObjectType), batch_method=merge_lists
    #   )
    # then use swh.dataset.luigi._export_metadata_has_object_types to check in
    # .meta/export.json that all objects are present before skipping the task

    def requires(self) -> List[luigi.Task]:
        """Returns a :class:`LocalExport` task."""
        return [
            LocalExport(
                local_export_path=self.local_export_path,
                formats=[Format.orc],  # type: ignore[attr-defined]
                object_types=self.object_types,
            )
        ]

    def output(self) -> List[luigi.LocalTarget]:
        """Returns the ``meta/*.json`` targets"""
        return [self._export_meta(), self._compression_meta()]

    def _export_meta(self) -> luigi.Target:
        """Returns the metadata on the dataset export"""
        return luigi.LocalTarget(self.local_graph_path / "meta/export.json")

    def _compression_meta(self) -> luigi.Target:
        """Returns the metadata on the compression pipeline"""
        return luigi.LocalTarget(self.local_graph_path / "meta/compression.json")

    def run(self):
        """Runs the full compression pipeline, then writes :file:`meta/compression.json`

        This does not support running individual steps yet."""
        import datetime
        import json
        import shutil
        import socket

        import pkg_resources

        from swh.graph import webgraph

        conf = {}  # TODO: make this configurable
        steps = None  # TODO: make this configurable

        if self.batch_size:
            conf["batch_size"] = self.batch_size

        # Delete stamps. Otherwise interrupting this compression pipeline may leave
        # stamps from a previous successful compression
        if self._export_meta().exists():
            self._export_meta().remove()
        if self._compression_meta().exists():
            self._compression_meta().remove()

        # Make sure we don't accidentally append to existing files
        if self.local_graph_path.exists():
            shutil.rmtree(self.local_graph_path)

        output_directory = self.local_graph_path
        graph_name = "graph"

        def progress_cb(percentage: int, step: webgraph.CompressionStep):
            self.set_progress_percentage(percentage)
            self.set_status_message(f"Running {step.name} (step #{step.value})")

        start_date = datetime.datetime.now(tz=datetime.timezone.utc)
        webgraph.compress(
            graph_name,
            self.local_export_path / "orc",
            output_directory,
            steps,
            conf,
        )
        end_date = datetime.datetime.now(tz=datetime.timezone.utc)

        # Copy dataset export metadata
        with self._export_meta().open("w") as write_fd:
            with (self.local_export_path / "meta" / "export.json").open() as read_fd:
                write_fd.write(read_fd.read())

        # Append metadata about this compression pipeline
        if self._compression_meta().exists():
            with self._compression_meta().open("w") as fd:
                meta = json.load(fd)
        else:
            meta = []

        meta.append(
            {
                "steps": steps,
                "compression_start": start_date.isoformat(),
                "compression_end": end_date.isoformat(),
                "object_type": [object_type.name for object_type in self.object_types],
                "hostname": socket.getfqdn(),
                "conf": conf,
                "tool": {
                    "name": "swh.graph",
                    "version": pkg_resources.get_distribution("swh.graph").version,
                },
            }
        )
        with self._compression_meta().open("w") as fd:
            json.dump(meta, fd, indent=4)


class UploadGraphToS3(luigi.Task):
    """Uploads a local compressed graphto S3; creating automatically if it does
    not exist.

    Example invocation::

        luigi --local-scheduler --module swh.graph.luigi UploadGraphToS3 \
                --local-graph-path=graph/ \
                --s3-graph-path=s3://softwareheritage/graph/swh_2022-11-08/compressed/
    """

    local_graph_path = luigi.PathParameter(significant=False)
    s3_graph_path = S3PathParameter()

    def requires(self) -> List[luigi.Task]:
        """Returns a :class:`CompressGraph` task that writes local files at the
        expected location."""
        return [
            CompressGraph(
                local_graph_path=self.local_graph_path,
            )
        ]

    def output(self) -> List[luigi.Target]:
        """Returns stamp and meta paths on S3."""
        return [self._meta()]

    def _meta(self):
        import luigi.contrib.s3

        return luigi.contrib.s3.S3Target(f"{self.s3_graph_path}/meta/compression.json")

    def run(self) -> None:
        """Copies all files: first the graph itself, then :file:`meta/compression.json`."""
        import subprocess
        import tempfile

        import luigi.contrib.s3
        import tqdm

        compression_metadata_path = self.local_graph_path / "meta" / "compression.json"
        seen_compression_metadata = False

        client = luigi.contrib.s3.S3Client()

        # recursively copy local files to S3, and end with compression metadata
        paths = list(self.local_graph_path.glob("**/*"))
        for (i, path) in tqdm.tqdm(
            list(enumerate(paths)),
            desc="Uploading compressed graph",
        ):
            if path == compression_metadata_path:
                # Write it last
                seen_compression_metadata = True
                continue
            if path.is_dir():
                continue
            relative_path = path.relative_to(self.local_graph_path)
            self.set_progress_percentage(int(i * 100 / len(paths)))

            if path.suffix == ".bin":
                # Large sparse file; store it compressed on S3.
                with tempfile.NamedTemporaryFile(
                    prefix=path.stem, suffix=".bin.zst"
                ) as fd:
                    self.set_status_message(f"Compressing {relative_path}")
                    subprocess.run(
                        ["zstdmt", "--force", "--keep", path, "-o", fd.name], check=True
                    )
                    self.set_status_message(f"Uploading {relative_path} (compressed)")
                    client.put_multipart(
                        fd.name,
                        f"{self.s3_graph_path}/{relative_path}.zst",
                        ACL="public-read",
                    )
            else:
                self.set_status_message(f"Uploading {relative_path}")
                client.put_multipart(
                    path, f"{self.s3_graph_path}/{relative_path}", ACL="public-read"
                )

        assert (
            seen_compression_metadata
        ), "did not see meta/compression.json in directory listing"

        # Write it last, to act as a stamp
        client.put(
            compression_metadata_path,
            self._meta().path,
            ACL="public-read",
        )


class DownloadGraphFromS3(luigi.Task):
    """Downloads a local dataset graph from S3.

    This performs the inverse operation of :class:`UploadGraphToS3`

    Example invocation::

        luigi --local-scheduler --module swh.graph.luigi DownloadGraphFromS3 \
                --local-graph-path=graph/ \
                --s3-graph-path=s3://softwareheritage/graph/swh_2022-11-08/compressed/
    """

    local_graph_path = luigi.PathParameter()
    s3_graph_path = S3PathParameter(significant=False)

    def requires(self) -> List[luigi.Task]:
        """Returns a :class:`ExportGraph` task that writes local files at the
        expected location."""
        return [
            UploadGraphToS3(
                local_graph_path=self.local_graph_path,
                s3_graph_path=self.s3_graph_path,
            )
        ]

    def output(self) -> List[luigi.Target]:
        """Returns stamp and meta paths on the local filesystem."""
        return [self._meta()]

    def _meta(self):
        return luigi.LocalTarget(self.local_graph_path / "meta" / "export.json")

    def run(self) -> None:
        """Copies all files: first the graph itself, then :file:`meta/compression.json`."""
        import subprocess
        import tempfile

        import luigi.contrib.s3
        import tqdm

        client = luigi.contrib.s3.S3Client()

        compression_metadata_path = f"{self.s3_graph_path}/meta/compression.json"
        seen_compression_metadata = False

        # recursively copy local files to S3, and end with compression metadata
        files = list(client.list(self.s3_graph_path))
        for (i, file_) in tqdm.tqdm(
            list(enumerate(files)),
            desc="Downloading",
        ):
            if file_ == compression_metadata_path:
                # Will copy it last
                seen_compression_metadata = True
                continue
            self.set_progress_percentage(int(i * 100 / len(files)))
            local_path = self.local_graph_path / file_
            local_path.parent.mkdir(parents=True, exist_ok=True)
            if file_.endswith(".bin.zst"):
                # The file was compressed before uploading to S3, we need it
                # to be decompressed locally
                with tempfile.NamedTemporaryFile(
                    prefix=local_path.stem, suffix=".bin.zst"
                ) as fd:
                    self.set_status_message(f"Downloading {file_} (compressed)")
                    client.get(
                        f"{self.s3_graph_path}/{file_}",
                        fd.name,
                    )
                    self.set_status_message(f"Decompressing {file_}")
                    subprocess.run(
                        [
                            "zstdmt",
                            "--force",
                            "-d",
                            fd.name,
                            "-o",
                            str(local_path)[0:-4],
                        ],
                        check=True,
                    )
            else:
                self.set_status_message(f"Downloading {file_}")
                client.get(
                    f"{self.s3_graph_path}/{file_}",
                    str(local_path),
                )

        assert (
            seen_compression_metadata
        ), "did not see meta/compression.json in directory listing"

        # Write it last, to act as a stamp
        client.get(
            compression_metadata_path,
            self._meta().path,
        )


class LocalGraph(luigi.Task):
    """Task that depends on a local dataset being present -- either directly from
    :class:`ExportGraph` or via :class:`DownloadGraphFromS3`.
    """

    local_graph_path = luigi.PathParameter()
    compression_task_type = luigi.TaskParameter(
        default=DownloadGraphFromS3,
        significant=False,
        description="""The task used to get the compressed graph if it is not present.
        Should be either ``swh.graph.luigi.CompressGraph`` or
        ``swh.graph.luigi.DownloadGraphFromS3``.""",
    )

    def requires(self) -> List[luigi.Task]:
        """Returns an instance of either :class:`CompressGraph` or
        :class:`DownloadGraphFromS3` depending on the value of
        :attr:`compression_task_type`."""

        if issubclass(self.compression_task_type, CompressGraph):
            return [
                CompressGraph(
                    local_graph_path=self.local_graph_path,
                )
            ]
        elif issubclass(self.compression_task_type, DownloadGraphFromS3):
            return [
                DownloadGraphFromS3(
                    local_graph_path=self.local_graph_path,
                )
            ]
        else:
            raise ValueError(
                f"Unexpected compression_task_type: "
                f"{self.compression_task_type.__name__}"
            )

    def output(self) -> List[luigi.Target]:
        """Returns stamp and meta paths on the local filesystem."""
        return [self._meta()]

    def _meta(self):
        return luigi.LocalTarget(self.local_graph_path / "meta" / "compression.json")
