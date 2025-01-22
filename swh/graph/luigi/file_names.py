# Copyright (C) 2022-2023 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""
Luigi tasks for producing the most common names of every content and datasets based on file names
=================================================================================================

"""  # noqa

# WARNING: do not import unnecessary things here to keep cli startup time under
# control
from pathlib import Path
from typing import List, Tuple

import luigi

from swh.export.luigi import S3PathParameter

from .compressed_graph import LocalGraph
from .utils import _ParquetToS3ToAthenaTask, estimate_node_count


class PopularContentNames(luigi.Task):
    """Creates a CSV file that contains the most popular name(s) of each content"""

    local_graph_path = luigi.PathParameter()
    popular_contents_path = luigi.PathParameter()
    graph_name = luigi.Parameter(default="graph")
    max_results_per_content = luigi.IntParameter(default=1)
    popularity_threshold = luigi.IntParameter(default=0)

    def _max_ram(self):
        nb_nodes = estimate_node_count(
            self.local_graph_path, self.graph_name, "ori,snp,rel,rev,dir,cnt"
        )

        graph_size = nb_nodes * 8

        # Does not keep any large array for all nodes, but uses temporary HashMaps
        # to store counts of names locally.
        # 1GB should be more than enough.
        spare_space = 1_000_000_000
        return graph_size + spare_space

    @property
    def resources(self):
        """Return the estimated RAM use of this task."""
        import socket

        return {f"{socket.getfqdn()}_ram_mb": self._max_ram() / 1_000_000}

    def requires(self) -> List[luigi.Task]:
        """Returns an instance of :class:`LocalGraph`."""
        return [LocalGraph(local_graph_path=self.local_graph_path)]

    def output(self) -> luigi.LocalTarget:
        """.csv.zst file that contains the topological order."""
        return luigi.LocalTarget(self.popular_contents_path)

    def run(self) -> None:
        """Runs ``popular-content-names`` from :file:`tools/file_names`"""
        from ..shell import Rust

        # fmt: on
        (
            Rust(
                "popular-content-names",
                self.local_graph_path / self.graph_name,
                "--max-results",
                str(self.max_results_per_content),
                "--min-occurrences",
                str(self.popularity_threshold),
                "--out",
                self.output(),
            )
        ).run()
        # fmt: off


class PopularContentPaths(luigi.Task):
    """Creates a CSV file that contains the most popular path of each content/directory
    given as input"""

    local_graph_path = luigi.PathParameter()
    popular_contents_path = luigi.PathParameter()
    graph_name = luigi.Parameter(default="graph")
    input_swhids = luigi.PathParameter()
    max_depth = luigi.IntParameter(default=2)

    def _max_ram(self):
        nb_nodes = estimate_node_count(
            self.local_graph_path, self.graph_name, "ori,snp,rel,rev,dir,cnt"
        )

        graph_size = nb_nodes * 8

        # Does not keep any large array for all nodes, but uses temporary HashMaps
        # to store counts of names locally.
        # 10GB should be more than enough.
        spare_space = 10_000_000_000
        return graph_size + spare_space

    @property
    def resources(self):
        """Return the estimated RAM use of this task."""
        import socket

        return {f"{socket.getfqdn()}_ram_mb": self._max_ram() / 1_000_000}

    def requires(self) -> List[luigi.Task]:
        """Returns an instance of :class:`LocalGraph`."""
        return [LocalGraph(local_graph_path=self.local_graph_path)]

    def output(self) -> luigi.LocalTarget:
        """.csv.zst file that contains the topological order."""
        return luigi.LocalTarget(self.popular_contents_path)

    def run(self) -> None:
        """Runs org.softwareheritage.graph.utils.PopularContentPaths and compresses"""
        import multiprocessing.dummy

        from ..shell import Command, Rust, wc

        input_swhid_files = list(self.input_swhids.iterdir())

        zstd_opts = ["--memory=1024MB"]  # Needed for Antonio's highly-compressed files

        with multiprocessing.dummy.Pool() as p:
            nb_contents = sum(
                p.imap_unordered(
                    lambda path: wc(Command.zstdcat(*zstd_opts, path), "-l") - 1,
                    input_swhid_files,
                )
            )

        # Stream the header from all inputs but the first
        input_streams = [Command.zstdcat(*zstd_opts, input_swhid_files[0])]
        for input_swhid_file in input_swhid_files[1:]:
            input_streams.append(
                Command.zstdcat(*zstd_opts, input_swhid_file) | Command.tail("-n", "+2")
            )

        # fmt: on
        (
            Command.cat(*input_streams)
            | Rust(
                "popular-content-paths",
                self.local_graph_path / self.graph_name,
                "--expected-nodes",
                str(nb_contents),
                "--depth",
                str(self.max_depth),
                "--out",
                self.output(),
            )
        ).run()
        # fmt: off


class PopularContentNamesOrcToS3(_ParquetToS3ToAthenaTask):
    """Reads the CSV from :class:`PopularContents`, converts it to ORC,
    upload the ORC to S3, and create an Athena table for it."""

    popular_contents_path = luigi.PathParameter()
    dataset_name = luigi.Parameter()
    s3_athena_output_location = S3PathParameter()

    def requires(self) -> PopularContentNames:
        """Returns corresponding PopularContentNames instance"""
        if self.dataset_name not in str(self.popular_contents_path):
            raise Exception(
                f"Dataset name {self.dataset_name!r} is not part of the "
                f"popular_contents_path {self.popular_contents_path!r}"
            )
        return PopularContentNames(
            popular_contents_path=self.popular_contents_path,
        )

    def _input_parquet_path(self) -> Path:
        return self.popular_contents_path

    def _s3_bucket(self) -> str:
        # TODO: configurable
        return "softwareheritage"

    def _s3_prefix(self) -> str:
        # TODO: configurable
        return f"derived_datasets/{self.dataset_name}/popular_contents/"

    def _parquet_columns(self) -> List[Tuple[str, str]]:
        return [
            ("SWHID", "string"),
            ("length", "bigint"),
            ("filename", "binary"),
            ("occurrences", "bigint"),
        ]

    def _athena_db_name(self) -> str:
        return f"derived_{self.dataset_name.replace('-', '')}"

    def _athena_table_name(self) -> str:
        return "popular_contents"


class ListFilesByName(luigi.Task):
    """From every refs/heads/master, refs/heads/main, or HEAD branch in any snapshot,
    browse the whole directory tree looking for files named <filename>, and lists
    them to stdout."""

    local_graph_path = luigi.PathParameter()
    graph_name = luigi.Parameter(default="graph")
    output_path = luigi.PathParameter()
    file_name = luigi.Parameter()

    def _max_ram(self):
        nb_nodes = estimate_node_count(
            self.local_graph_path, self.graph_name, "ori,snp,rel,rev,dir,cnt"
        )

        graph_size = nb_nodes * 8

        num_threads = 96

        # see listFilesInSnapshotChunk
        csv_buffers = self.batch_size * num_threads * 1000000

        # Does not keep any large array for all nodes, but uses temporary stacks
        # and hash sets.
        # 1GB per thread should be more than enough.
        bfs_buffers = 1_000_000_000 * num_threads

        spare_space = 1_000_000_000
        return graph_size + csv_buffers + bfs_buffers + spare_space

    @property
    def resources(self):
        """Return the estimated RAM use of this task."""
        import socket

        return {f"{socket.getfqdn()}_ram_mb": self._max_ram() / 1_000_000}

    def requires(self) -> List[luigi.Task]:
        """Returns an instance of :class:`LocalGraph`."""
        return [LocalGraph(local_graph_path=self.local_graph_path)]

    def output(self) -> luigi.LocalTarget:
        """Directory of .csv.zst files containing the list of file occurrences with
        that name."""
        return luigi.LocalTarget(self.output_path)

    def run(self) -> None:
        """Runs org.softwareheritage.graph.utils.PopularContentNames and compresses"""
        from ..shell import Rust

        # fmt: on
        (
            Rust(
                "list-files-by-name",
                self.local_graph_path / self.graph_name,
                self.file_name,
                "--out",
                self.output(),
            )
        ).run()
        # fmt: off
