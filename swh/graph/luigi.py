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
        graph.meta/
            export.json
            compression.json

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
from typing import List

import luigi

from swh.dataset.luigi import Format, LocalExport, ObjectType


class CompressGraph(luigi.Task):
    local_export_path = luigi.PathParameter()
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
        return [
            LocalExport(
                local_export_path=self.local_export_path,
                formats=[Format.orc],  # type: ignore[attr-defined]
                object_types=self.object_types,
            )
        ]

    def output(self) -> List[luigi.LocalTarget]:
        return [self._export_meta(), self._compression_meta()]

    def _export_meta(self) -> luigi.Target:
        """Returns the metadata on the dataset export"""
        return luigi.LocalTarget(f"{self.local_graph_path}.meta/export.json")

    def _compression_meta(self) -> luigi.Target:
        """Returns the metadata on the compression pipeline"""
        return luigi.LocalTarget(f"{self.local_graph_path}.meta/compression.json")

    def run(self):
        import datetime
        import json
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

        output_directory = self.local_graph_path.parent
        graph_name = self.local_graph_path.name

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
