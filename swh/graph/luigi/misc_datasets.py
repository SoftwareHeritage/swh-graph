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
from pathlib import Path
from typing import Dict, List

import luigi

from .compressed_graph import LocalGraph
from .utils import run_script

OBJECT_TYPES = {"ori", "snp", "rel", "rev", "dir", "cnt"}


class TopoSort(luigi.Task):
    """Creates a file that contains all SWHIDs in topological order from a compressed
    graph."""

    local_graph_path = luigi.PathParameter()
    topological_order_dir = luigi.PathParameter()
    graph_name = luigi.Parameter(default="graph")
    object_types = luigi.Parameter()
    direction = luigi.ChoiceParameter(choices=["forward", "backward"])
    algorithm = luigi.ChoiceParameter(choices=["dfs", "bfs"], default="dfs")
    max_ram = luigi.Parameter(default="500G")

    def requires(self) -> List[luigi.Task]:
        """Returns an instance of :class:`LocalGraph`."""
        return [LocalGraph(local_graph_path=self.local_graph_path)]

    def output(self) -> luigi.Target:
        """.csv.zst file that contains the topological order."""
        return luigi.LocalTarget(
            self.topological_order_dir
            / f"topological_order_{self.algorithm}_{self.direction}_{self.object_types}.csv.zst"
        )

    def run(self) -> None:
        """Runs org.softwareheritage.graph.utils.TopoSort and compresses"""
        invalid_object_types = set(self.object_types.split(",")) - OBJECT_TYPES
        if invalid_object_types:
            raise ValueError(f"Invalid object types: {invalid_object_types}")
        class_name = "org.softwareheritage.graph.utils.TopoSort"
        # TODO: pass max_ram to run_script() correctly so it can pass it to
        # check_config(), instead of hardcoding it on the command line here
        script = f"""
        java -Xmx{self.max_ram} {class_name} '{self.local_graph_path}/{self.graph_name}' '{self.algorithm}' '{self.direction}' '{self.object_types}' \
            | pv --line-mode --wait \
            | zstdmt -19
        """  # noqa
        run_script(script, Path(self.output().path))


class PopularContents(luigi.Task):
    """Creates a file that contains all SWHIDs in topological order from a compressed
    graph."""

    local_graph_path = luigi.PathParameter()
    popular_contents_path = luigi.PathParameter()
    graph_name = luigi.Parameter(default="graph")
    max_results_per_content = luigi.IntParameter(default=0)
    popularity_threshold = luigi.IntParameter(default=0)
    max_ram = luigi.Parameter(default="300G")

    def requires(self) -> List[luigi.Task]:
        """Returns an instance of :class:`LocalGraph`."""
        return [LocalGraph(local_graph_path=self.local_graph_path)]

    def output(self) -> luigi.Target:
        """.csv.zst file that contains the topological order."""
        return luigi.LocalTarget(self.popular_contents_path)

    def run(self) -> None:
        """Runs org.softwareheritage.graph.utils.PopularContents and compresses"""
        class_name = "org.softwareheritage.graph.utils.PopularContents"
        # TODO: pass max_ram to run_script() correctly so it can pass it to
        # check_config(), instead of hardcoding it on the command line here
        script = f"""
        java -Xmx{self.max_ram} {class_name} '{self.local_graph_path}/{self.graph_name}'  '{self.max_results_per_content}' '{self.popularity_threshold}' \
            | pv --line-mode --wait \
            | zstdmt -19
        """  # noqa
        run_script(script, Path(self.output().path))


class CountPaths(luigi.Task):
    """Creates a file that lists:

    * the number of paths leading to each node, and starting from all leaves, and
    * the number of paths leading to each node, and starting from all other nodes

    Singleton paths are not counted.
    """

    local_graph_path = luigi.PathParameter()
    topological_order_dir = luigi.PathParameter()
    graph_name = luigi.Parameter(default="graph")
    object_types = luigi.Parameter()
    direction = luigi.ChoiceParameter(choices=["forward", "backward"])
    max_ram = luigi.Parameter(default="200G")

    def requires(self) -> Dict[str, luigi.Task]:
        """Returns an instance of :class:`LocalGraph` and one of :class:`TopoSort`."""
        return {
            "graph": LocalGraph(local_graph_path=self.local_graph_path),
            "toposort": TopoSort(
                local_graph_path=self.local_graph_path,
                graph_name=self.graph_name,
                topological_order_dir=self.topological_order_dir,
                object_types=self.object_types,
                direction=self.direction,
            ),
        }

    def output(self) -> luigi.Target:
        """.csv.zst file that contains the counts."""
        return luigi.LocalTarget(
            self.topological_order_dir
            / f"path_counts_{self.direction}_{self.object_types}.csv.zst"
        )

    def run(self) -> None:
        """Runs org.softwareheritage.graph.utils.CountPaths and compresses"""
        invalid_object_types = set(self.object_types.split(",")) - OBJECT_TYPES
        if invalid_object_types:
            raise ValueError(f"Invalid object types: {invalid_object_types}")
        class_name = "org.softwareheritage.graph.utils.CountPaths"
        topological_order_path = self.input()["toposort"].path
        # TODO: pass max_ram to run_script() correctly so it can pass it to
        # check_config(), instead of hardcoding it on the command line here
        script = f"""
        zstdcat '{topological_order_path}' \
            | pv --line-mode --wait --size $(zstdcat '{topological_order_path}' | wc -l) \
            | java -Xmx{self.max_ram} {class_name} '{self.local_graph_path}/{self.graph_name}' '{self.direction}' \
            | zstdmt -19
        """  # noqa
        run_script(script, Path(self.output().path))
