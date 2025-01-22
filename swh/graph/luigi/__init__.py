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
  CLI that can be composed with other tasks, such as swh-export's
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

from .aggregate_datasets import *  # noqa
from .blobs_datasets import *  # noqa
from .compressed_graph import *  # noqa
from .file_names import *  # noqa
from .origin_contributors import *  # noqa
from .provenance import *  # noqa
from .subdataset import *  # noqa
from .topology import *  # noqa


class RunNewGraph(luigi.Task):
    """Runs dataset export, graph compression, and generates datasets using the graph."""

    def requires(self) -> List[luigi.Task]:
        """Returns instances of :class:`swh.export.luigi.RunExportAll`,
        :class:`swh.export.luigi.UploadExportToS3`,
        and :class:`swh.graph.luigi.compressed_graph.UploadGraphToS3`, which
        recursively depend on the whole export and compression pipeline.
        Also runs some of the derived datasets through
        :class:`swh.graph.topology.UploadGenerationsToS3` and
        :class:`swh.graph.aggregate_datasets.RunAggregatedDatasets`.
        """
        from swh.export.luigi import RunExportAll, UploadExportToS3

        from .aggregate_datasets import RunAggregatedDatasets
        from .compressed_graph import UploadGraphToS3
        from .topology import UploadGenerationsToS3

        return [
            RunExportAll(),
            UploadExportToS3(),
            UploadGraphToS3(),
            UploadGenerationsToS3(
                direction="forward", object_types="dir,rev,rel,snp,ori"
            ),
            UploadGenerationsToS3(
                direction="backward", object_types="dir,rev,rel,snp,ori"
            ),
            RunAggregatedDatasets(),
        ]

    def complete(self) -> bool:
        # Dependencies perform their own completeness check, and this task
        # does no work itself
        return False
