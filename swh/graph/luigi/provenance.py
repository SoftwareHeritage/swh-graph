# Copyright (C) 2023 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""
Luigi tasks to help compute the provenance of content blobs
===========================================================

This module contains `Luigi <https://luigi.readthedocs.io/>`_ tasks
driving the computation of a topological order, and count the number
of paths to every node.

File layout
-----------

This assumes a local compressed graph (from :mod:`swh.graph.luigi.compressed_graph`)
is present, and generates/manipulates the following files::

    base_dir/
        <date>[_<flavor>]/
            provenance/
                topological_order_dfs.csv.zst
"""

# WARNING: do not import unnecessary things here to keep cli startup time under
# control
from pathlib import Path
from typing import Dict

import luigi

from .compressed_graph import LocalGraph
from .utils import count_nodes


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
        return {
            "parquet": luigi.LocalTarget(self._arrow_output_path()),
        }

    def run(self) -> None:
        """Runs ``list-provenance-nodes`` from ``tools/provenance``"""
        from ..shell import Rust

        print("listing nodes to", self._arrow_output_path())
        # fmt: off
        (
            Rust(
                "list-provenance-nodes",
                "-vv",
                self.local_graph_path / self.graph_name,
                "--node-filter",
                self.provenance_node_filter,
                "--nodes-out",
                str(self._arrow_output_path()),
            )
        ).run()
        # fmt: on
        print("listed nodes to", list(self._arrow_output_path().iterdir()))


class ComputeEarliestTimestamps(luigi.Task):
    """Creates an array storing, for each directory/content SWHIDs, the author date
    of the first revision/release that contains it.
    """

    local_export_path = luigi.PathParameter()
    local_graph_path = luigi.PathParameter()
    graph_name = luigi.Parameter(default="graph")
    provenance_dir = luigi.PathParameter()

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
                "-vv",
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
        return {
            "graph": LocalGraph(local_graph_path=self.local_graph_path),
            "earliest_revisions": ComputeEarliestTimestamps(
                local_export_path=self.local_export_path,
                local_graph_path=self.local_graph_path,
                graph_name=self.graph_name,
                provenance_dir=self.provenance_dir,
            ),
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
                "-vv",
                self.local_graph_path / self.graph_name,
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
    batch_size = luigi.IntParameter(default=1000)

    def _max_ram(self):
        # see java/src/main/java/org/softwareheritage/graph/utils/ComputeDirectoryFrontier.java
        nb_nodes = count_nodes(
            self.local_graph_path, self.graph_name, "ori,snp,rel,rev,dir,cnt"
        )

        # maxtimestamps_array = nb_nodes * 8  # Actually it's mmapped
        maxtimestamps_array = 0

        num_threads = 96
        min_buf_size = 1140  # see findFrontiersInRevisionChunk
        # it's unlikely to have 5000 more dirs than expected (averaged over
        # all threads running at any given time)
        worst_case_buf_size_ratio = 5000
        buf_size = (
            num_threads * self.batch_size * min_buf_size * worst_case_buf_size_ratio
        )

        graph_size = nb_nodes * 8

        spare_space = 1_000_000_000
        return graph_size + maxtimestamps_array + buf_size + spare_space

    @property
    def resources(self):
        """Returns the value of ``self.max_ram_mb``"""
        import socket

        return {f"{socket.getfqdn()}_ram_mb": self._max_ram() // 1_000_000}

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
            ),
        }

    def _output_path(self) -> Path:
        return self.provenance_dir / "directory_frontier"

    def output(self) -> luigi.LocalTarget:
        """Returns {provenance_dir}/directory_frontier/"""
        return luigi.LocalTarget(self._output_path())

    def run(self) -> None:
        """Runs ``compute-directory-frontier`` from ``tools/provenance``"""
        from ..shell import Rust

        # fmt: off
        (
            Rust(
                "compute-directory-frontier",
                "-vv",
                self.local_graph_path / self.graph_name,
                "--max-timestamps",
                self.input()["directory_max_leaf_timestamps"],
                "--directories-out",
                self.output(),
            )
        ).run()
        # fmt: on


class ListFrontierDirectoriesInRevisions(luigi.Task):
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
    batch_size = luigi.IntParameter(default=1000)

    def _max_ram(self):
        # see
        # java/src/main/java/org/softwareheritage/graph/utils/ListContentsInRevisionsWithoutFrontier.java
        nb_nodes = count_nodes(
            self.local_graph_path, self.graph_name, "ori,snp,rel,rev,dir,cnt"
        )

        # maxtimestamps_array = nb_nodes * 8  # Actually it's mmapped
        maxtimestamps_array = 0

        num_threads = 96
        min_buf_size = 1140  # see processRevisionChunk
        # it's unlikely to have 5000 more dirs than expected (averaged over
        # all threads running at any given time)
        worst_case_buf_size_ratio = 5000
        buf_size = (
            num_threads * self.batch_size * min_buf_size * worst_case_buf_size_ratio
        )

        graph_size = nb_nodes * 8

        spare_space = 1_000_000_000
        return graph_size + maxtimestamps_array + buf_size + spare_space

    @property
    def resources(self):
        """Returns the value of ``self.max_ram_mb``"""
        import socket

        return {f"{socket.getfqdn()}_ram_mb": self._max_ram() // 1_000_000}

    def requires(self) -> Dict[str, luigi.Task]:
        """Returns :class:`LocalGraph` and :class:`ComputeDirectoryFrontier`
        instances."""
        return {
            "graph": LocalGraph(local_graph_path=self.local_graph_path),
            "directory_frontier": ComputeDirectoryFrontier(
                local_export_path=self.local_export_path,
                local_graph_path=self.local_graph_path,
                graph_name=self.graph_name,
                provenance_dir=self.provenance_dir,
            ),
            "directory_max_leaf_timestamps": ListDirectoryMaxLeafTimestamp(
                local_export_path=self.local_export_path,
                local_graph_path=self.local_graph_path,
                graph_name=self.graph_name,
                provenance_dir=self.provenance_dir,
            ),
        }

    def _output_path(self) -> Path:
        return self.provenance_dir / "frontier_directories_in_revisions"

    def output(self) -> luigi.LocalTarget:
        """Returns {provenance_dir}/frontier_directories_in_revisions/"""
        return luigi.LocalTarget(self._output_path())

    def run(self) -> None:
        """Runs ``org.softwareheritage.graph.utils.ListFrontierDirectoriesInRevisions``"""
        from ..shell import Rust

        # fmt: off
        (
            Rust(
                "frontier-directories-in-revisions",
                "-vv",
                self.local_graph_path / self.graph_name,
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
    batch_size = luigi.IntParameter(default=1000)

    def _max_ram(self):
        # see
        # java/src/main/java/org/softwareheritage/graph/utils/ListContentsInRevisionsWithoutFrontier.java
        nb_nodes = count_nodes(
            self.local_graph_path, self.graph_name, "ori,snp,rel,rev,dir,cnt"
        )

        # maxtimestamps_array = nb_nodes * 8  # Actually it's mmapped
        maxtimestamps_array = 0

        num_threads = 96
        min_buf_size = 1140  # see processRevisionChunk
        # it's unlikely to have 5000 more dirs than expected (averaged over
        # all threads running at any given time)
        worst_case_buf_size_ratio = 5000
        buf_size = (
            num_threads * self.batch_size * min_buf_size * worst_case_buf_size_ratio
        )

        graph_size = nb_nodes * 8

        spare_space = 1_000_000_000
        return graph_size + maxtimestamps_array + buf_size + spare_space

    @property
    def resources(self):
        """Returns the value of ``self.max_ram_mb``"""
        import socket

        return {f"{socket.getfqdn()}_ram_mb": self._max_ram() // 1_000_000}

    def requires(self) -> Dict[str, luigi.Task]:
        """Returns :class:`LocalGraph` and :class:`ListDirectoryMaxLeafTimestamp`
        instances."""
        return {
            "graph": LocalGraph(local_graph_path=self.local_graph_path),
            "directory_frontier": ComputeDirectoryFrontier(
                local_export_path=self.local_export_path,
                local_graph_path=self.local_graph_path,
                graph_name=self.graph_name,
                provenance_dir=self.provenance_dir,
            ),
        }

    def _output_path(self) -> Path:
        return self.provenance_dir / "contents_in_revisions_without_frontiers"

    def output(self) -> luigi.LocalTarget:
        """Returns {provenance_dir}/contents_in_revisions_without_frontiers"""
        return luigi.LocalTarget(self._output_path())

    def run(self) -> None:
        """Runs ``contents-in-revisions-without-frontier`` from ``tools/provenance``"""
        from ..shell import Rust

        # fmt: off
        (
            Rust(
                "contents-in-revisions-without-frontier",
                "-vv",
                self.local_graph_path / self.graph_name,
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

    def _max_ram(self):
        # see java/src/main/java/org/softwareheritage/graph/utils/ComputeDirectoryFrontier.java
        nb_nodes = count_nodes(
            self.local_graph_path, self.graph_name, "ori,snp,rel,rev,dir,cnt"
        )

        num_threads = 96
        thread_buf_size = 1_000_000_000  # rough overestimate of the average size
        buf_size = num_threads * thread_buf_size

        graph_size = nb_nodes * 8

        spare_space = 1_000_000_000
        return graph_size + buf_size + spare_space

    @property
    def resources(self):
        """Returns the value of ``self.max_ram_mb``"""
        import socket

        return {f"{socket.getfqdn()}_ram_mb": self._max_ram() // 1_000_000}

    def requires(self) -> Dict[str, luigi.Task]:
        """Returns :class:`LocalGraph` and :class:`ComputeDirectoryFrontier`
        instances."""
        return {
            "graph": LocalGraph(local_graph_path=self.local_graph_path),
            "directory_frontier": ComputeDirectoryFrontier(
                local_export_path=self.local_export_path,
                local_graph_path=self.local_graph_path,
                graph_name=self.graph_name,
                provenance_dir=self.provenance_dir,
            ),
        }

    def _output_path(self) -> Path:
        return self.provenance_dir / "contents_in_frontier_directories"

    def output(self) -> luigi.LocalTarget:
        """Returns {provenance_dir}/contents_in_frontier_directories/"""
        return luigi.LocalTarget(self._output_path())

    def run(self) -> None:
        """Runs ``contents-in-directories`` from ``tools/provenance``"""
        from ..shell import Rust

        # fmt: off
        (
            Rust(
                "contents-in-directories",
                "-vv",
                self.local_graph_path / self.graph_name,
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

    def requires(self):
        """Returns :class:`ListContentsInFrontierDirectories` and
        :class:`ListContentsInRevisionsWithoutFrontier`"""
        kwargs = dict(
            local_export_path=self.local_export_path,
            local_graph_path=self.local_graph_path,
            graph_name=self.graph_name,
            provenance_dir=self.provenance_dir,
        )
        return [
            ListProvenanceNodes(**kwargs),
            ListContentsInFrontierDirectories(**kwargs),
            ListContentsInRevisionsWithoutFrontier(**kwargs),
            ListFrontierDirectoriesInRevisions(**kwargs),
        ]
