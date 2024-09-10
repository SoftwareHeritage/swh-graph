# Copyright (C) 2023 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""
Luigi tasks to help compute the provenance of content blobs
===========================================================

This module contains `Luigi <https://luigi.readthedocs.io/>`_ tasks
driving the computation of the :ref:`provenance-index`.
"""

# WARNING: do not import unnecessary things here to keep cli startup time under
# control
from pathlib import Path
from typing import Dict

import luigi
import psutil

from .compressed_graph import LocalGraph
from .utils import count_nodes


def default_max_ram_mb() -> int:
    return psutil.virtual_memory().total // 1_000_000


class ListProvenanceNodes(luigi.Task):
    """Lists all nodes reachable from releases and 'head revisions'."""

    local_export_path = luigi.PathParameter()
    local_graph_path = luigi.PathParameter()
    graph_name = luigi.Parameter(default="graph")
    provenance_dir = luigi.PathParameter()
    provenance_node_filter = luigi.Parameter(default="heads")

    def requires(self) -> Dict[str, luigi.Task]:
        """Returns :class:`LocalGraph` and :class:`SortRevrelByDate` instances."""
        return {
            "graph": LocalGraph(local_graph_path=self.local_graph_path),
        }

    def _arrow_output_path(self) -> Path:
        return self.provenance_dir / "nodes"

    def output(self) -> Dict[str, luigi.LocalTarget]:
        """Returns :file:`{provenance_dir}/nodes/`"""
        return luigi.LocalTarget(self._arrow_output_path())

    def run(self) -> None:
        """Runs ``list-provenance-nodes`` from ``tools/provenance``"""
        from ..shell import Rust

        print("listing nodes to", self._arrow_output_path())
        # fmt: off
        (
            Rust(
                "list-provenance-nodes",
                self.local_graph_path / self.graph_name,
                "--node-filter",
                self.provenance_node_filter,
                "--nodes-out",
                str(self._arrow_output_path()),
            )
        ).run()
        # fmt: on


class ComputeEarliestTimestamps(luigi.Task):
    """Creates an array storing, for each directory/content SWHIDs, the author date
    of the first revision/release that contains it.
    """

    local_export_path = luigi.PathParameter()
    local_graph_path = luigi.PathParameter()
    graph_name = luigi.Parameter(default="graph")
    provenance_dir = luigi.PathParameter()
    provenance_node_filter = luigi.Parameter(default="heads")

    def _max_ram(self):
        # see java/src/main/java/org/softwareheritage/graph/utils/ListEarliestRevisions.java
        nb_nodes = count_nodes(
            self.local_graph_path, self.graph_name, "ori,snp,rel,rev,dir,cnt"
        )

        visited_bitarray = nb_nodes // 8
        timestamps_array = nb_nodes * 8

        graph_size = nb_nodes * 8

        spare_space = 1_000_000_000
        return graph_size + visited_bitarray + timestamps_array + spare_space

    @property
    def resources(self):
        """Returns the value of ``self.max_ram_mb``"""
        import socket

        return {f"{socket.getfqdn()}_ram_mb": self._max_ram() // 1_000_000}

    def requires(self) -> Dict[str, luigi.Task]:
        """Returns :class:`LocalGraph` and :class:`SortRevrelByDate` instances."""
        return {
            "graph": LocalGraph(local_graph_path=self.local_graph_path),
        }

    def _bin_timestamps_output_path(self) -> Path:
        return self.provenance_dir / "earliest_timestamps.bin"

    def output(self) -> Dict[str, luigi.LocalTarget]:
        """Returns :file:`{provenance_dir}/revrel_by_author_date/`
        and `:file:`{provenance_dir}/earliest_timestamps.bin`."""
        return {
            "bin_timestamps": luigi.LocalTarget(self._bin_timestamps_output_path()),
        }

    def run(self) -> None:
        """Runs ``compute-earliest-timestamps`` from ``tools/provenance``"""
        from ..shell import Rust

        # fmt: off
        (
            Rust(
                "compute-earliest-timestamps",
                "--node-filter",
                self.provenance_node_filter,
                self.local_graph_path / self.graph_name,
                "--timestamps-out",
                str(self._bin_timestamps_output_path()),
            )
        ).run()
        # fmt: on


class ListDirectoryMaxLeafTimestamp(luigi.Task):
    """Creates a file that contains all directory/content SWHIDs, along with the first
    revision/release author date and SWHIDs they occur in.
    """

    local_export_path = luigi.PathParameter()
    local_graph_path = luigi.PathParameter()
    graph_name = luigi.Parameter(default="graph")
    provenance_dir = luigi.PathParameter()
    provenance_node_filter = luigi.Parameter(default="heads")

    def _max_ram(self):
        # see
        # java/src/main/java/org/softwareheritage/graph/utils/ListDirectoryMaxLeafTimestamp.java
        nb_nodes = count_nodes(
            self.local_graph_path, self.graph_name, "ori,snp,rel,rev,dir,cnt"
        )

        unvisitedchildren_array = maxtimestamps_array = nb_nodes * 8

        graph_size = nb_nodes * 8

        spare_space = 1_000_000_000
        return graph_size + unvisitedchildren_array + maxtimestamps_array + spare_space

    @property
    def resources(self):
        """Returns the value of ``self.max_ram_mb``"""
        import socket

        return {f"{socket.getfqdn()}_ram_mb": self._max_ram() // 1_000_000}

    def requires(self) -> Dict[str, luigi.Task]:
        """Returns :class:`LocalGraph` and :class:`ComputeEarliestTimestamps` instances."""
        kwargs = dict(
            local_export_path=self.local_export_path,
            local_graph_path=self.local_graph_path,
            graph_name=self.graph_name,
            provenance_dir=self.provenance_dir,
            provenance_node_filter=self.provenance_node_filter,
        )
        return {
            "graph": LocalGraph(local_graph_path=self.local_graph_path),
            "earliest_revisions": ComputeEarliestTimestamps(**kwargs),
            "reachable_nodes": ListProvenanceNodes(**kwargs),
        }

    def _output_path(self) -> Path:
        return self.provenance_dir / "max_leaf_timestamps.bin"

    def output(self) -> luigi.LocalTarget:
        """Returns {provenance_dir}/max_leaf_timestamps.bin"""
        return luigi.LocalTarget(self._output_path())

    def run(self) -> None:
        """Runs ``list-directory-with-max-leaf-timestamp`` from ``tools/provenance``"""
        from ..shell import Rust

        # fmt: off
        (
            Rust(
                "list-directory-with-max-leaf-timestamp",
                self.local_graph_path / self.graph_name,
                "--node-filter",
                self.provenance_node_filter,
                "--reachable-nodes",
                self.input()["reachable_nodes"],
                "--timestamps",
                self.input()["earliest_revisions"]["bin_timestamps"],
                "--max-timestamps-out",
                str(self._output_path()),
            )
        ).run()
        # fmt: on


class ComputeDirectoryFrontier(luigi.Task):
    """Creates a file that contains the "directory frontier" as defined by
    `swh-provenance <https://gitlab.softwareheritage.org/swh/devel/swh-provenance/>`_.

    In short, it is a directory which directly contains a file (not a directory),
    which is a non-root directory in a revision newer than the directory timestamp
    computed by ListDirectoryMaxLeafTimestamp.
    """

    local_export_path = luigi.PathParameter()
    local_graph_path = luigi.PathParameter()
    graph_name = luigi.Parameter(default="graph")
    provenance_dir = luigi.PathParameter()
    provenance_node_filter = luigi.Parameter(default="heads")
    max_ram_mb = luigi.IntParameter(default=default_max_ram_mb(), significant=False)

    @property
    def resources(self):
        """Returns the value of ``self.max_ram_mb``"""
        import socket

        return {f"{socket.getfqdn()}_ram_mb": self.max_ram_mb}

    def requires(self) -> Dict[str, luigi.Task]:
        """Returns :class:`LocalGraph` and :class:`ListDirectoryMaxLeafTimestamp`
        instances."""
        return {
            "graph": LocalGraph(local_graph_path=self.local_graph_path),
            "directory_max_leaf_timestamps": ListDirectoryMaxLeafTimestamp(
                local_export_path=self.local_export_path,
                local_graph_path=self.local_graph_path,
                graph_name=self.graph_name,
                provenance_dir=self.provenance_dir,
                provenance_node_filter=self.provenance_node_filter,
            ),
        }

    def _output_path(self) -> Path:
        return self.provenance_dir / "directory_frontier"

    def output(self) -> luigi.LocalTarget:
        """Returns {provenance_dir}/directory_frontier/"""
        return luigi.LocalTarget(self._output_path())

    def run(self) -> None:
        """Runs ``compute-directory-frontier`` from ``tools/provenance``"""
        import multiprocessing

        from ..shell import Rust

        # fmt: off
        (
            Rust(
                "compute-directory-frontier",
                self.local_graph_path / self.graph_name,
                "--thread-buffer-size",
                str(self.max_ram_mb * 1_000_000 // multiprocessing.cpu_count()),
                "--node-filter",
                self.provenance_node_filter,
                "--max-timestamps",
                self.input()["directory_max_leaf_timestamps"],
                "--directories-out",
                self.output(),
            )
        ).run()
        # fmt: on


class ListFrontierDirectoriesInRevisions(luigi.Task):
    """Creates a file that contains the list of revision any "frontier directory"
    (as defined by `swh-provenance
    <https://gitlab.softwareheritage.org/swh/devel/swh-provenance/>`_) is in.

    While a directory is considered frontier only relative to a revision, the produced
    file contains the list of **all** revisions a directory is in, for directories which
    are frontier for **any** revision.
    """

    local_export_path = luigi.PathParameter()
    local_graph_path = luigi.PathParameter()
    graph_name = luigi.Parameter(default="graph")
    provenance_dir = luigi.PathParameter()
    provenance_node_filter = luigi.Parameter(default="heads")
    max_ram_mb = luigi.IntParameter(default=default_max_ram_mb(), significant=False)

    @property
    def resources(self):
        """Returns the value of ``self.max_ram_mb``"""
        import socket

        return {f"{socket.getfqdn()}_ram_mb": self.max_ram_mb}

    def requires(self) -> Dict[str, luigi.Task]:
        """Returns :class:`LocalGraph` and :class:`ComputeDirectoryFrontier`
        instances."""
        kwargs = dict(
            local_export_path=self.local_export_path,
            local_graph_path=self.local_graph_path,
            graph_name=self.graph_name,
            provenance_dir=self.provenance_dir,
            provenance_node_filter=self.provenance_node_filter,
        )
        return {
            "graph": LocalGraph(local_graph_path=self.local_graph_path),
            "reachable_nodes": ListProvenanceNodes(**kwargs),
            "directory_frontier": ComputeDirectoryFrontier(**kwargs),
            "directory_max_leaf_timestamps": ListDirectoryMaxLeafTimestamp(**kwargs),
        }

    def _output_path(self) -> Path:
        return self.provenance_dir / "frontier_directories_in_revisions"

    def output(self) -> luigi.LocalTarget:
        """Returns {provenance_dir}/frontier_directories_in_revisions/"""
        return luigi.LocalTarget(self._output_path())

    def run(self) -> None:
        """Runs ``org.softwareheritage.graph.utils.ListFrontierDirectoriesInRevisions``"""
        import multiprocessing

        from ..shell import Rust

        # fmt: off
        (
            Rust(
                "frontier-directories-in-revisions",
                self.local_graph_path / self.graph_name,
                "--thread-buffer-size",
                str(self.max_ram_mb * 1_000_000 // multiprocessing.cpu_count()),
                "--node-filter",
                self.provenance_node_filter,
                "--reachable-nodes",
                self.input()["reachable_nodes"],
                "--frontier-directories",
                self.input()["directory_frontier"],
                "--max-timestamps",
                self.input()["directory_max_leaf_timestamps"],
                "--directories-out",
                self.output(),
            )
        ).run()
        # fmt: on


class ListContentsInRevisionsWithoutFrontier(luigi.Task):
    """Creates a file that contains the list of (file, revision) where the file is
    reachable from the revision without going through any "directory frontier" as
    defined by
    `swh-provenance <https://gitlab.softwareheritage.org/swh/devel/swh-provenance/>`_.

    In short, it is a directory which directly contains a file (not a directory),
    which is a non-root directory in a revision newer than the directory timestamp
    computed by ListDirectoryMaxLeafTimestamp.
    """

    local_export_path = luigi.PathParameter()
    local_graph_path = luigi.PathParameter()
    graph_name = luigi.Parameter(default="graph")
    provenance_dir = luigi.PathParameter()
    provenance_node_filter = luigi.Parameter(default="heads")
    max_ram_mb = luigi.IntParameter(default=default_max_ram_mb(), significant=False)

    @property
    def resources(self):
        """Returns the value of ``self.max_ram_mb``"""
        import socket

        return {f"{socket.getfqdn()}_ram_mb": self.max_ram_mb}

    def requires(self) -> Dict[str, luigi.Task]:
        """Returns :class:`LocalGraph` and :class:`ListDirectoryMaxLeafTimestamp`
        instances."""
        kwargs = dict(
            local_export_path=self.local_export_path,
            local_graph_path=self.local_graph_path,
            graph_name=self.graph_name,
            provenance_dir=self.provenance_dir,
            provenance_node_filter=self.provenance_node_filter,
        )
        return {
            "graph": LocalGraph(local_graph_path=self.local_graph_path),
            "reachable_nodes": ListProvenanceNodes(**kwargs),
            "directory_frontier": ComputeDirectoryFrontier(
                max_ram_mb=self.max_ram_mb, **kwargs
            ),
        }

    def _output_path(self) -> Path:
        return self.provenance_dir / "contents_in_revisions_without_frontiers"

    def output(self) -> luigi.LocalTarget:
        """Returns {provenance_dir}/contents_in_revisions_without_frontiers"""
        return luigi.LocalTarget(self._output_path())

    def run(self) -> None:
        """Runs ``contents-in-revisions-without-frontier`` from ``tools/provenance``"""
        import multiprocessing

        from ..shell import Rust

        # fmt: off
        (
            Rust(
                "contents-in-revisions-without-frontier",
                self.local_graph_path / self.graph_name,
                "--thread-buffer-size",
                str(self.max_ram_mb * 1_000_000 // multiprocessing.cpu_count()),
                "--node-filter",
                self.provenance_node_filter,
                "--reachable-nodes",
                self.input()["reachable_nodes"],
                "--frontier-directories",
                self.input()["directory_frontier"],
                "--contents-out",
                self.output(),
            )
        ).run()
        # fmt: on


class ListContentsInFrontierDirectories(luigi.Task):
    """Enumerates all contents in all directories returned by
    :class:`ComputeDirectoryFrontier`."""

    local_export_path = luigi.PathParameter()
    local_graph_path = luigi.PathParameter()
    graph_name = luigi.Parameter(default="graph")
    provenance_dir = luigi.PathParameter()
    provenance_node_filter = luigi.Parameter(default="heads")
    max_ram_mb = luigi.IntParameter(default=default_max_ram_mb(), significant=False)

    @property
    def resources(self):
        """Returns the value of ``self.max_ram_mb``"""
        import socket

        return {f"{socket.getfqdn()}_ram_mb": self.max_ram_mb}

    def requires(self) -> Dict[str, luigi.Task]:
        """Returns :class:`LocalGraph` and :class:`ComputeDirectoryFrontier`
        instances."""
        kwargs = dict(
            local_export_path=self.local_export_path,
            local_graph_path=self.local_graph_path,
            graph_name=self.graph_name,
            provenance_dir=self.provenance_dir,
            provenance_node_filter=self.provenance_node_filter,
        )
        return {
            "graph": LocalGraph(local_graph_path=self.local_graph_path),
            "reachable_nodes": ListProvenanceNodes(**kwargs),
            "directory_frontier": ComputeDirectoryFrontier(
                max_ram_mb=self.max_ram_mb, **kwargs
            ),
        }

    def _output_path(self) -> Path:
        return self.provenance_dir / "contents_in_frontier_directories"

    def output(self) -> luigi.LocalTarget:
        """Returns {provenance_dir}/contents_in_frontier_directories/"""
        return luigi.LocalTarget(self._output_path())

    def run(self) -> None:
        """Runs ``contents-in-directories`` from ``tools/provenance``"""
        import multiprocessing

        from ..shell import Rust

        # fmt: off
        (
            Rust(
                "contents-in-directories",
                self.local_graph_path / self.graph_name,
                "--thread-buffer-size",
                str(self.max_ram_mb * 1_000_000 // multiprocessing.cpu_count()),
                "--node-filter",
                self.provenance_node_filter,
                "--frontier-directories",
                self.input()["directory_frontier"],
                "--contents-out",
                self.output(),
            )
        ).run()
        # fmt: on


class RunProvenance(luigi.WrapperTask):
    """(Transitively) depends on all provenance tasks"""

    local_export_path = luigi.PathParameter()
    local_graph_path = luigi.PathParameter()
    graph_name = luigi.Parameter(default="graph")
    provenance_dir = luigi.PathParameter()
    provenance_node_filter = luigi.Parameter(default="heads")
    max_ram_mb = luigi.IntParameter(default=default_max_ram_mb(), significant=False)

    def requires(self):
        """Returns :class:`ListContentsInFrontierDirectories` and
        :class:`ListContentsInRevisionsWithoutFrontier`"""
        kwargs = dict(
            local_export_path=self.local_export_path,
            local_graph_path=self.local_graph_path,
            graph_name=self.graph_name,
            provenance_dir=self.provenance_dir,
            provenance_node_filter=self.provenance_node_filter,
        )
        return [
            ListProvenanceNodes(**kwargs),
            ListContentsInFrontierDirectories(max_ram_mb=self.max_ram_mb, **kwargs),
            ListContentsInRevisionsWithoutFrontier(
                max_ram_mb=self.max_ram_mb, **kwargs
            ),
            ListFrontierDirectoriesInRevisions(max_ram_mb=self.max_ram_mb, **kwargs),
        ]
