# Copyright (C) 2022 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""
Luigi tasks
===========

This module contains `Luigi <https://luigi.readthedocs.io/>`_ tasks,
as an alternative to the CLI that can be composed with other tasks,
such as swh-dataset's.

File layout
-----------

(This section assumes a graph named `graph`.

In addition to files documented in :ref:`graph-compression` (eg. :file:`graph.graph`,
:file:`graph.mph`, ...), tasks in this module produce this directory structure:

    swh_<date>[_<flavor>]/
        graph.graph
        graph.mph
        ...
        graph.stamps/
            origin
            snapshot
            ...
        graph.meta/
            export.json
            compression.json

``stamps`` files are written after corresponding directories are written.
Their presence indicates the corresponding graph was fully generated/copied,
This allows skipping work that was already done, while ignoring interrupted jobs.

``graph.meta/export.json`` is copied from the ORC dataset exported by
:mod:`swh.dataset.luigi`.

`graph.meta/compression.json``  contains information about the compression itself,
for provenance tracking.
For example:

.. code-block:: json

    [
        {
            "steps": None,
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
                "version": "2.2.0",
            }
        }
    ]

When the compression pipeline is run in separate steps, each of the steps is recorded
as an object in the root list.
"""

# WARNING: do not import unnecessary things here to keep cli startup time under
# control
from pathlib import Path
from typing import List

import luigi

from swh.dataset.luigi import Format, LocalExport, ObjectType, merge_lists


class CompressGraph(luigi.Task):
    local_export_path = luigi.PathParameter()
    object_types = luigi.EnumListParameter(
        enum=ObjectType, default=list(ObjectType), batch_method=merge_lists
    )
    output_directory = luigi.PathParameter()
    graph_name = luigi.Parameter("graph")
    batch_size = luigi.IntParameter(
        default=0,
        significant=False,
        description="""
        Size of work batches to use while compressing.
        Larger is faster, but consumes more resources.
        """,
    )

    def requires(self) -> List[luigi.Task]:
        return [
            LocalExport(
                local_export_path=self.local_export_path,
                formats=[Format.orc],  # type: ignore[attr-defined]
                object_types=self.object_types,
            )
        ]

    def output(self) -> List[luigi.LocalTarget]:
        return self._stamps() + [self._export_meta(), self._compression_meta()]

    def _stamps_dir(self) -> Path:
        # TODO: read export.json to use it as stamp for the list of object types
        # (instead of adding one file per object type)
        return self.output_directory / f"{self.graph_name}.stamps"

    def _stamps(self) -> List[luigi.Target]:
        return [
            luigi.LocalTarget(self._stamps_dir() / object_type.name)
            for object_type in self.object_types
        ]

    def _export_meta(self) -> luigi.Target:
        """Returns the metadata on the dataset export"""
        return luigi.LocalTarget(
            self.output_directory / f"{self.graph_name}.meta" / "export.json"
        )

    def _compression_meta(self) -> luigi.Target:
        """Returns the metadata on the compression pipeline"""
        return luigi.LocalTarget(
            self.output_directory / f"{self.graph_name}.meta" / "compression.json"
        )

    def run(self):
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
        if self._stamps_dir().exists():
            shutil.rmtree(self._stamps_dir())
        if self._export_meta().exists():
            self._export_meta().remove()
        if self._compression_meta().exists():
            self._compression_meta().remove()

        start_date = datetime.datetime.now(tz=datetime.timezone.utc)
        webgraph.compress(
            self.graph_name,
            self.local_export_path / "orc",
            self.output_directory,
            steps,
            conf,
        )
        end_date = datetime.datetime.now(tz=datetime.timezone.utc)

        # Create stamps
        for output in self._stamps():
            output.makedirs()
            with output.open("w") as fd:
                pass

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
