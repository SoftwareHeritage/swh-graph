# Copyright (C) 2022 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""
Luigi tasks
===========

This package contains `Luigi <https://luigi.readthedocs.io/>`_ tasks.
These come in two kinds:

* in :mod:`swh.graph.luigi.compressed_graph`: an alternative to the 'swh graph compress'
  CLI that can be composed with other tasks, such as swh-dataset's
* in other submodules: tasks driving the creation of specific datasets that are
  generated using the compressed graph


The overall directory structure is::

    base_dir/
        <date>[_<flavor>]/
            edges/
                ...
            orc/
                ...
            compressed/
                graph.graph
                graph.mph
                ...
                meta/
                    export.json
                    compression.json
            datasets/
                contribution_graph.csv.zst
            topology/
                topological_order_dfs.csv.zst

And optionally::

    sensitive_base_dir/
        <date>[_<flavor>]/
            persons_sha256_to_name.csv.zst
            datasets/
                contribution_graph.deanonymized.csv.zst
"""

# WARNING: do not import unnecessary things here to keep cli startup time under
# control
from typing import List

import luigi

from .blobs_datasets import *  # noqa
from .compressed_graph import *  # noqa
from .file_names import *  # noqa
from .origin_contributors import *  # noqa
from .provenance import *  # noqa
from .topology import *  # noqa


class RunExportCompressUpload(luigi.Task):
    """Runs dataset export, graph compression, and generates datasets using the graph."""

    def requires(self) -> List[luigi.Task]:
        """Returns instances of :class:`swh.dataset.luigi.RunExportAll`
        and :class:`swh.graph.luigi.compressed_graph.UploadGraphToS3`, which
        recursively depend on the whole export and compression pipeline.
        """
        from swh.dataset.luigi import RunExportAll

        from .compressed_graph import UploadGraphToS3

        return [
            RunExportAll(),
            UploadGraphToS3(),
        ]

    def complete(self) -> bool:
        # Dependencies perform their own completeness check, and this task
        # does no work itself
        return False
