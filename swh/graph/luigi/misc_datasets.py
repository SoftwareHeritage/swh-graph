# Copyright (C) 2022 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""
Luigi tasks for various derived datasets
========================================

This module contains `Luigi <https://luigi.readthedocs.io/>`_ tasks
driving the creation of derived datasets.

File layout
-----------

This assumes a local compressed graph (from :mod:`swh.graph.luigi.compressed_graph`)
is present, and generates/manipulates the following files::

    base_dir/
        <date>[_<flavor>]/
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

from .compressed_graph import LocalGraph
from .utils import run_script


class TopoSort(luigi.Task):
    """Creates a file that contains all SWHIDs in topological order from a compressed
    graph."""

    local_graph_path = luigi.PathParameter()
    topological_order_path = luigi.PathParameter()
    graph_name = luigi.Parameter(default="graph")

    def requires(self) -> List[luigi.Task]:
        """Returns an instance of :class:`LocalGraph`."""
        return [LocalGraph(local_graph_path=self.local_graph_path)]

    def output(self) -> luigi.Target:
        """.csv.zst file that contains the topological order."""
        return luigi.LocalTarget(self.topological_order_path)

    def run(self) -> None:
        """Runs org.softwareheritage.graph.utils.TopoSort and compresses"""
        object_types = "rev,rel,snp,ori"
        class_name = "org.softwareheritage.graph.utils.TopoSort"
        script = f"""
        java {class_name} '{self.local_graph_path}/{self.graph_name}' '{object_types}' \
            | pv --line-mode --wait \
            | zstdmt -19
        """
        run_script(script, self.topological_order_path)
