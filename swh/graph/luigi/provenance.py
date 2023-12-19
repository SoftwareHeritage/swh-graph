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
import os
from pathlib import Path
from typing import Dict

import luigi

from swh.dataset.luigi import Format, LocalExport

from .compressed_graph import LocalGraph
from .utils import count_nodes


class SortRevrelByDate(luigi.Task):
    """Creates a file that contains all revision/release author dates and their SWHIDs
    in date order from a graph export."""

    local_export_path = luigi.PathParameter()
    local_graph_path = luigi.PathParameter()
    graph_name = luigi.Parameter(default="graph")
    provenance_dir = luigi.PathParameter()

    def requires(self) -> Dict[str, luigi.Task]:
        """Returns :class:`LocalGraph` and :class:`LocalExport` instances"""
        return {
            "export": LocalExport(
                local_export_path=self.local_export_path,
                formats=[Format.orc],  # type: ignore[attr-defined]
            ),
            "graph": LocalGraph(local_graph_path=self.local_graph_path),
        }

    def _output_path(self) -> Path:
        return self.provenance_dir / "revrel_by_author_date.csv.zst"

    def output(self) -> luigi.LocalTarget:
        """Returns {provenance_dir}/revrel_by_author_date.csv.zst"""
        return luigi.LocalTarget(self._output_path())

    def run(self) -> None:
        """For each ORC revision or release file, read it with pyorc, produce a
        "date,swhid" CSV, and sort it with GNU sort.
        Then merge all outputs with GNU sort.
        """
        import math
        import multiprocessing
        import multiprocessing.dummy
        import tempfile

        import tqdm

        from .shell import AtomicFileSink, Command
        from .utils import count_nodes

        sort_env = {"LC_ALL": "C"}  # fastest locale to sort
        if os.environ.get("TMPDIR"):
            sort_env["TMPDIR"] = os.environ["TMPDIR"]

        with tempfile.TemporaryDirectory() as tempdir_:
            tempdir = Path(tempdir_)

            jobs = [
                (swhid_object_type, path, tempdir, sort_env)
                for (swhid_object_type, object_type) in [
                    ("rev", "revision"),
                    ("rel", "release"),
                ]
                for path in (self.local_export_path / "orc" / object_type).glob("*.orc")
            ]

            assert jobs

            # pick number of processes slightly slightly higher than cpu_count
            # so that it's a divisor of the number of jobs.
            # so eg. if there are 200 jobs and 96 CPUs, 100 jobs will be running
            # at any given time in order to avoid running 96, then 96, then 8,
            # which would waste time at the end.
            cpu_count = multiprocessing.cpu_count()
            jobs_per_core = max(1, math.floor(len(jobs) / cpu_count))
            processes = len(jobs) // jobs_per_core

            with multiprocessing.dummy.Pool(processes) as p:
                sorted_files = list(
                    tqdm.tqdm(
                        p.imap_unordered(self._worker, jobs),
                        total=len(jobs),
                        desc="Sorting individual .orc files",
                    )
                )

            assert sorted_files

            self._output_path().parent.mkdir(parents=True, exist_ok=True)

            # fmt: off
            nb_nodes = count_nodes(self.local_graph_path, self.graph_name, "rev,rel")
            (
                Command.sort(
                    "--merge",
                    "--parallel",
                    str(processes),
                    "-S",
                    "100M",
                    *sorted_files,
                    env=sort_env
                )
                | Command.pv("--wait", "--line-mode", "--size", str(nb_nodes))
                | Command.cat(Command.echo("author_date,SWHID\r"), "-")
                | Command.zstdmt("-10")
                > AtomicFileSink(self._output_path())
            ).run()
            # fmt: on

    def _worker(self, args) -> None:
        import sys
        import uuid

        from .shell import AtomicFileSink, Command

        (swhid_object_type, orc_path, tempdir, sort_env) = args

        output_path = tempdir / f"{swhid_object_type}_{uuid.uuid4()}.csv"

        script = (
            f"from {__name__} import {self.__class__.__name__} as cls; cls.orc_to_csv()"
        )

        # fmt: off
        (
            Command(sys.executable, "-c", script, swhid_object_type, orc_path)
            | Command.sort("-S", "100M", env=sort_env)
            > AtomicFileSink(output_path)
        ).run()
        # fmt: on

        return output_path

    @staticmethod
    def orc_to_csv():
        """Must be called as a CLI script. Syntax: {rev,rel} path/to/dataset/file.orc

        Reads an ORC file containing revisions or releases, and writes a CSV to its
        stdout, containing a date and a SWHID on each row."""
        import csv
        import datetime
        import sys

        import pyorc

        from swh.dataset.exporters.orc import SWHTimestampConverter

        (_, swhid_object_type, orc_path) = sys.argv

        csv_file = csv.writer(sys.stdout)
        with open(orc_path, "rb") as fd:
            orc_file = pyorc.Reader(
                fd,
                column_names=("id", "date"),
                converters={pyorc.TypeKind.TIMESTAMP: SWHTimestampConverter},
            )
            for (id, date) in orc_file:
                if date is None:
                    continue
                try:
                    (seconds, microseconds) = date
                    date = datetime.datetime.utcfromtimestamp(
                        seconds + microseconds / 1000000
                    )
                except (OverflowError, OSError, ValueError):
                    continue
                csv_file.writerow(
                    (
                        date.isoformat(),
                        f"swh:1:{swhid_object_type}:{id}",
                    )
                )


class ListEarliestRevisions(luigi.Task):
    """Creates a file that contains all directory/content SWHIDs, along with the first
    revision/release author date and SWHIDs they occur in.
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
            "sorted_revrel": SortRevrelByDate(
                local_export_path=self.local_export_path,
                local_graph_path=self.local_graph_path,
                graph_name=self.graph_name,
                provenance_dir=self.provenance_dir,
            ),
        }

    def _csv_output_path(self) -> Path:
        return self.provenance_dir / "earliest_revrel_for_cntdir.csv.zst"

    def _bin_timestamps_output_path(self) -> Path:
        return self.provenance_dir / "earliest_timestamps.bin"

    def output(self) -> Dict[str, luigi.LocalTarget]:
        """Returns :file:`{provenance_dir}/revrel_by_author_date.csv.zst`
        and `:file:`{provenance_dir}/earliest_timestamps.bin`."""
        return {
            "csv": luigi.LocalTarget(self._csv_output_path()),
            "bin_timestamps": luigi.LocalTarget(self._bin_timestamps_output_path()),
        }

    def run(self) -> None:
        """Runs ``org.softwareheritage.graph.utils.ListEarliestRevisions``"""
        from .shell import AtomicFileSink, Command, Java
        from .utils import count_nodes

        nb_nodes = count_nodes(self.local_graph_path, self.graph_name, "cnt,dir")

        class_name = "org.softwareheritage.graph.utils.ListEarliestRevisions"

        # fmt: off
        (
            Command.zstdcat(self.input()["sorted_revrel"])
            | Java(
                class_name,
                self.local_graph_path / self.graph_name,
                str(self._bin_timestamps_output_path()),
                max_ram=self._max_ram(),
            )
            | Command.pv("--wait", "--line-mode", "--size", str(nb_nodes))
            | Command.zstdmt("-10")
            > AtomicFileSink(self._csv_output_path())
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
    topological_order_dir = luigi.PathParameter()

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
        """Returns :class:`LocalGraph`, :class:`TopoSort`, and
        :class:`ListEarliestRevisions` instances."""
        from .topology import TopoSort

        return {
            "graph": LocalGraph(local_graph_path=self.local_graph_path),
            "toposort": TopoSort(
                local_graph_path=self.local_graph_path,
                graph_name=self.graph_name,
                direction="backward",
                object_types="dir,rev,rel,snp,ori",
                topological_order_dir=self.topological_order_dir,
            ),
            "earliest_revisions": ListEarliestRevisions(
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
        """Runs ``org.softwareheritage.graph.utils.ListDirectoryMaxLeafTimestamp``"""
        from .shell import Command, Java

        class_name = "org.softwareheritage.graph.utils.ListDirectoryMaxLeafTimestamp"

        # fmt: off
        (
            Command.zstdcat(self.input()["toposort"].path)
            | Java(
                class_name,
                self.local_graph_path / self.graph_name,
                self.input()["earliest_revisions"]["bin_timestamps"],
                str(self._output_path()),
                max_ram=self._max_ram(),
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
    topological_order_dir = luigi.PathParameter()
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
                topological_order_dir=self.topological_order_dir,
            ),
        }

    def _output_path(self) -> Path:
        return self.provenance_dir / "directory_frontier.csv.zst"

    def output(self) -> luigi.LocalTarget:
        """Returns {provenance_dir}/directory_frontier.csv.zst"""
        return luigi.LocalTarget(self._output_path())

    def run(self) -> None:
        """Runs ``org.softwareheritage.graph.utils.ComputeDirectoryFrontier``"""
        from .shell import AtomicFileSink, Command, Java

        class_name = "org.softwareheritage.graph.utils.ComputeDirectoryFrontier"

        # fmt: off
        (
            Java(
                class_name,
                self.local_graph_path / self.graph_name,
                self.input()["directory_max_leaf_timestamps"],
                str(self.batch_size),
                max_ram=self._max_ram(),
            )
            | Command.zstdmt("-12")
            > AtomicFileSink(self._output_path())
        ).run()
        # fmt: on


class DeduplicateFrontierDirectories(luigi.Task):
    """Reads the output of :class:`ComputeDirectoryFrontier` (which outputs
    `(directory, revision)` pairs), and returns the set of directories in it."""

    local_export_path = luigi.PathParameter()
    local_graph_path = luigi.PathParameter()
    graph_name = luigi.Parameter(default="graph")
    provenance_dir = luigi.PathParameter()
    topological_order_dir = luigi.PathParameter()
    batch_size = luigi.IntParameter(default=1000)

    def requires(self) -> Dict[str, luigi.Task]:
        """Returns :class:`LocalGraph` and :class:`ListDirectoryMaxLeafTimestamp`
        instances."""
        return {
            "graph": LocalGraph(local_graph_path=self.local_graph_path),
            "frontier_directories": ComputeDirectoryFrontier(
                local_export_path=self.local_export_path,
                local_graph_path=self.local_graph_path,
                graph_name=self.graph_name,
                provenance_dir=self.provenance_dir,
                topological_order_dir=self.topological_order_dir,
            ),
        }

    def _output_path(self) -> Path:
        return self.provenance_dir / "directory_frontier.deduplicated.csv.zst"

    def output(self) -> luigi.LocalTarget:
        """Returns {provenance_dir}/directory_frontier.deduplicated.csv.zst"""
        return luigi.LocalTarget(self._output_path())

    def run(self):
        """Runs ``cut | sort --uniq`` to produce unique directory SWHIDs from
        ``directory_frontier.csv.zst``."""
        from .shell import AtomicFileSink, Command

        sort_env = {"LC_ALL": "C"}  # fastest locale to sort
        if os.environ.get("TMPDIR"):
            sort_env["TMPDIR"] = os.environ["TMPDIR"]

        # FIXME: This assumes paths contain no newline or carriage return character,
        # in order to avoid parsing CSVs.
        # The grep command provides a crude filter for the continuation of such lines,
        # but could match files containing a newline followed by "<number>,swh:1:dir:"

        # fmt: off
        (
            Command.pv(self.input()["frontier_directories"])
            | Command.zstdcat()
            | Command.grep("-E", "^-?[0-9]+,swh:1:dir:", env=sort_env)
            | Command.cut("-d", ",", "-f", "2")
            | Command.sort(
                "-S", "1G", "--parallel=96", "--unique", "--compress-program=zstd"
            )
            | Command.cat(  # prepend header
                Command.echo("frontier_dir_SWHID"),
                "-",
            )
            | Command.zstdmt("-10")
            > AtomicFileSink(self._output_path())
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
    topological_order_dir = luigi.PathParameter()
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
            "frontier": DeduplicateFrontierDirectories(
                local_export_path=self.local_export_path,
                local_graph_path=self.local_graph_path,
                graph_name=self.graph_name,
                provenance_dir=self.provenance_dir,
                topological_order_dir=self.topological_order_dir,
            ),
        }

    def _output_path(self) -> Path:
        return self.provenance_dir / "contents_in_revisions_without_frontiers.csv.zst"

    def output(self) -> luigi.LocalTarget:
        """Returns {provenance_dir}/directory_frontier.csv.zst"""
        return luigi.LocalTarget(self._output_path())

    def run(self) -> None:
        """Runs ``org.softwareheritage.graph.utils.ListContentsInRevisionsWithoutFrontier``"""
        from .shell import AtomicFileSink, Command, Java

        class_name = (
            "org.softwareheritage.graph.utils.ListContentsInRevisionsWithoutFrontier"
        )

        # fmt: off
        (
            Command.pv(self.input()["frontier"])
            | Command.zstdcat()
            | Java(
                class_name,
                self.local_graph_path / self.graph_name,
                str(self.batch_size),
                max_ram=self._max_ram(),
            )
            | Command.zstdmt("-10")
            > AtomicFileSink(self._output_path())
        ).run()
        # fmt: on


class ListContentsInFrontierDirectories(luigi.Task):
    """Enumerates all contents in all directories returned by
    :class:`ComputeDirectoryFrontier`."""

    local_export_path = luigi.PathParameter()
    local_graph_path = luigi.PathParameter()
    graph_name = luigi.Parameter(default="graph")
    provenance_dir = luigi.PathParameter()
    topological_order_dir = luigi.PathParameter()

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
            "directory_frontier": DeduplicateFrontierDirectories(
                local_export_path=self.local_export_path,
                local_graph_path=self.local_graph_path,
                graph_name=self.graph_name,
                provenance_dir=self.provenance_dir,
                topological_order_dir=self.topological_order_dir,
            ),
        }

    def _output_path(self) -> Path:
        return self.provenance_dir / "contents_in_frontier_directories.csv.zst"

    def output(self) -> luigi.LocalTarget:
        """Returns {provenance_dir}/contents_in_frontier_directories.csv.zst"""
        return luigi.LocalTarget(self._output_path())

    def run(self) -> None:
        """Runs ``org.softwareheritage.graph.utils.ListContentsInDirectories``"""
        from .shell import AtomicFileSink, Command, Java

        class_name = "org.softwareheritage.graph.utils.ListContentsInDirectories"

        # fmt: off
        (
            Command.pv("--wait", self.input()["directory_frontier"])
            | Command.zstdcat()
            | Java(
                class_name,
                self.local_graph_path / self.graph_name,
                "1",  # DeduplicateFrontierDirectories only contains directory SWHIDs
                max_ram=self._max_ram(),
            )
            | Command.zstdmt("-14")
            > AtomicFileSink(self._output_path())
        ).run()
        # fmt: on


class RunProvenance(luigi.WrapperTask):
    """(Transitively) depends on all provenance tasks"""

    local_export_path = luigi.PathParameter()
    local_graph_path = luigi.PathParameter()
    graph_name = luigi.Parameter(default="graph")
    provenance_dir = luigi.PathParameter()
    topological_order_dir = luigi.PathParameter()

    def requires(self):
        """Returns :class:`ListContentsInFrontierDirectories` and
        :class:`ListContentsInRevisionsWithoutFrontier`"""
        kwargs = dict(
            local_export_path=self.local_export_path,
            local_graph_path=self.local_graph_path,
            graph_name=self.graph_name,
            provenance_dir=self.provenance_dir,
            topological_order_dir=self.topological_order_dir,
        )
        return [
            ListContentsInFrontierDirectories(**kwargs),
            ListContentsInRevisionsWithoutFrontier(**kwargs),
        ]
