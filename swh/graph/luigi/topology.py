# Copyright (C) 2022-2023 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""
Luigi tasks to analyze, and produce datasets related to, graph topology
=======================================================================

This module contains `Luigi <https://luigi.readthedocs.io/>`_ tasks
driving the computation of a topological order, and count the number
of paths to every node.

File layout
-----------

This assumes a local compressed graph (from :mod:`swh.graph.luigi.compressed_graph`)
is present, and generates/manipulates the following files::

    base_dir/
        <date>[_<flavor>]/
            topology/
                topological_order_dfs.csv.zst
"""

# WARNING: do not import unnecessary things here to keep cli startup time under
# control
from pathlib import Path
from typing import Any, Dict, List, Tuple

import luigi

from swh.dataset.luigi import S3PathParameter

from .compressed_graph import LocalGraph
from .utils import _CsvToOrcToS3ToAthenaTask, count_nodes

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

    def _max_ram(self):
        # see java/src/main/java/org/softwareheritage/graph/utils/TopoSort.java
        nb_nodes = count_nodes(
            self.local_graph_path, self.graph_name, "ori,snp,rel,rev,dir,cnt"
        )

        seen_bitarray = nb_nodes // 8

        graph_size = nb_nodes * 8

        # The ready set contains the frontier of the BFS. In the worst case,
        # it contains every single node matched by self.object_types (which by default
        # is all node types but contents).
        #
        # However, in practice it contains at most a negligeable fraction of
        # revisions, because the revision subgraph is, topologically, made mostly
        # of very long chains (see Antoine Pietri's thesis).
        #
        # And in the forward DFS case, we are guaranteed not to have more than one
        # content node in the ready set at any time, because it would be visited
        # immediately after and has no successors.
        #
        # TODO: also guess an upper bound on the number of directories? we can
        # probably assume at most a third will be in the ready set at any time,
        # considering their subgraph's topology.
        max_nb_ready_set_nodes = count_nodes(
            self.local_graph_path,
            self.graph_name,
            ",".join(
                ty
                for ty in self.object_types.split(",")
                if ty != "rev"
                and not (
                    self.algorithm == "dfs"
                    and self.direction == "forward"
                    and ty == "cnt"
                )
            ),
        )
        ready_set_size = max_nb_ready_set_nodes * 8

        unvisited_array_size = nb_nodes * 8  # longarray

        spare_space = 1_000_000_000
        return (
            graph_size
            + seen_bitarray
            + ready_set_size
            + unvisited_array_size
            + spare_space
        )

    @property
    def resources(self):
        """Return the estimated RAM use of this task."""
        import socket

        return {f"{socket.getfqdn()}_ram_mb": self._max_ram() / 1_000_000}

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
        from .shell import AtomicFileSink, Command, Java

        invalid_object_types = set(self.object_types.split(",")) - OBJECT_TYPES
        if invalid_object_types:
            raise ValueError(f"Invalid object types: {invalid_object_types}")
        class_name = "org.softwareheritage.graph.utils.TopoSort"

        nb_nodes = count_nodes(
            self.local_graph_path, self.graph_name, self.object_types
        )
        nb_lines = nb_nodes + 1  # CSV header

        # fmt: off
        (
            Java(
                class_name,
                self.local_graph_path / self.graph_name,
                self.algorithm,
                self.direction,
                self.object_types,
                max_ram=self._max_ram(),
            )
            | Command.pv("--line-mode", "--wait", "--size", str(nb_lines))
            | Command.zstdmt("-9")  # not -19 because of CPU usage + little gain
            > AtomicFileSink(self.output())
        ).run()
        # fmt: on


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

    def _max_ram(self):
        nb_nodes = count_nodes(
            self.local_graph_path, self.graph_name, self.object_types
        )
        graph_size = nb_nodes * 8

        # Two arrays of floats (countsFromRoots and countsFromAll)
        count_doublearray = nb_nodes * 8
        spare_space = 100_000_000
        return graph_size + count_doublearray * 2 + spare_space

    @property
    def resources(self):
        """Return the estimated RAM use of this task."""
        import socket

        return {f"{socket.getfqdn()}_ram_mb": self._max_ram() / 1_000_000}

    def requires(self) -> Dict[str, luigi.Task]:
        """Returns an instance of :class:`LocalGraph` and one of :class:`TopoSort`."""
        return {
            "graph": LocalGraph(local_graph_path=self.local_graph_path),
            "toposort": TopoSort(
                local_graph_path=self.local_graph_path,
                graph_name=self.graph_name,
                topological_order_dir=self.topological_order_dir,
                object_types=",".join(
                    ty for ty in self.object_types.split(",") if ty != "cnt"
                ),
                direction=self.direction,
            ),
        }

    def output(self) -> luigi.Target:
        """.csv.zst file that contains the counts."""
        return luigi.LocalTarget(
            self.topological_order_dir
            / f"path_counts_{self.direction}_{self.object_types}.csv.zst"
        )

    def nb_lines(self):
        nb_lines = count_nodes(
            self.local_graph_path, self.graph_name, self.object_types
        )
        nb_lines += 1  # CSV header
        return nb_lines

    def run(self) -> None:
        """Runs org.softwareheritage.graph.utils.CountPaths and compresses"""
        from .shell import AtomicFileSink, Command, Java

        invalid_object_types = set(self.object_types.split(",")) - OBJECT_TYPES
        if invalid_object_types:
            raise ValueError(f"Invalid object types: {invalid_object_types}")
        class_name = "org.softwareheritage.graph.utils.CountPaths"
        topological_order_path = self.input()["toposort"].path

        topo_order_command = Command.zstdcat(topological_order_path)

        if "cnt" in self.object_types.split(","):
            # The toposort does not include content; add them to the topo order.
            #
            # As it has this header:
            #     SWHID,ancestors,successors,sample_ancestor1,sample_ancestor2
            # we have to use sed to add the extra columns. CountPaths.java does not use
            # them, so dummy values are fine.
            content_input = Command.zstdcat(
                self.local_graph_path / f"{self.graph_name}.nodes.csv.zst"
            ) | Command.grep("^swh:1:cnt:")
            if self.direction == "forward":
                topo_order_command = Command.cat(
                    topo_order_command,
                    content_input | Command.sed("s/$/,1,0,,/"),
                )
            else:
                # Extract the header
                header_command = topo_order_command | Command.head("-n", "1")
                topo_order_command = topo_order_command | Command.tail("-n", "+2")

                topo_order_command = Command.cat(
                    header_command,
                    content_input | Command.sed("s/$/,0,1,,/"),
                    topo_order_command,
                )

        # fmt: off
        (
            topo_order_command
            | Java(
                class_name,
                self.local_graph_path / self.graph_name,
                self.direction,
                max_ram=self._max_ram()
            )
            | Command.pv("--line-mode", "--wait", "--size", str(self.nb_lines()))
            | Command.zstdmt("-19")
            > AtomicFileSink(self.output())
        ).run()
        # fmt: on


class PathCountsOrcToS3(_CsvToOrcToS3ToAthenaTask):
    """Reads the CSV from :class:`CountPaths`, converts it to ORC,
    upload the ORC to S3, and create an Athena table for it."""

    topological_order_dir = luigi.PathParameter()
    object_types = luigi.Parameter()
    direction = luigi.ChoiceParameter(choices=["forward", "backward"])
    dataset_name = luigi.Parameter()
    s3_athena_output_location = S3PathParameter()

    def requires(self) -> CountPaths:
        """Returns corresponding CountPaths instance"""
        if self.dataset_name not in str(self.topological_order_dir):
            raise Exception(
                f"Dataset name {self.dataset_name!r} is not part of the "
                f"topological_order_dir {self.topological_order_dir!r}"
            )
        return CountPaths(
            topological_order_dir=self.topological_order_dir,
            object_types=self.object_types,
            direction=self.direction,
        )

    def _base_filename(self) -> str:
        return f"path_counts_{self.direction}_{self.object_types}"

    def _input_csv_path(self) -> Path:
        return self.topological_order_dir / f"{self._base_filename()}.csv.zst"

    def _s3_bucket(self) -> str:
        # TODO: configurable
        return "softwareheritage"

    def _s3_prefix(self) -> str:
        # TODO: configurable
        return f"derived_datasets/{self.dataset_name}/{self._base_filename()}/"

    def _orc_columns(self) -> List[Tuple[str, str]]:
        return [
            ("SWHID", "string"),
            ("paths_from_roots", "double"),
            ("all_paths", "double"),
        ]

    def _approx_nb_rows(self) -> int:
        return self.requires().nb_lines() - 1  # -1 for the header

    def _parse_row(self, row: List[str]) -> Tuple[Any, ...]:
        (swhid, paths_from_roots, all_paths) = row
        return (swhid, float(paths_from_roots), float(all_paths))

    def _pyorc_writer_kwargs(self) -> Dict[str, Any]:
        return {
            **super()._pyorc_writer_kwargs(),
            "bloom_filter_columns": ["SWHID"],
        }

    def _athena_db_name(self) -> str:
        return f"derived_{self.dataset_name.replace('-', '')}"

    def _athena_table_name(self) -> str:
        return self._base_filename().replace(",", "")
