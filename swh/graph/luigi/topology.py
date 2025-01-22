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
from typing import Dict, List, Tuple

import luigi

from swh.export.luigi import S3PathParameter

from .compressed_graph import LocalGraph
from .utils import _ParquetToS3ToAthenaTask, count_nodes

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

    def requires(self) -> List[luigi.Task]:
        """Returns an instance of :class:`LocalGraph`."""
        return [LocalGraph(local_graph_path=self.local_graph_path)]

    def output(self) -> luigi.LocalTarget:
        """.csv.zst file that contains the topological order."""
        return luigi.LocalTarget(
            self.topological_order_dir
            / f"topological_order_{self.algorithm}_{self.direction}_{self.object_types}.csv.zst"
        )

    def run(self) -> None:
        """Runs 'toposort' command from tools/topology and compresses"""
        from ..shell import AtomicFileSink, Command, Rust

        invalid_object_types = set(self.object_types.split(",")) - OBJECT_TYPES
        if invalid_object_types:
            raise ValueError(f"Invalid object types: {invalid_object_types}")

        nb_nodes = count_nodes(
            self.local_graph_path, self.graph_name, self.object_types
        )
        nb_lines = nb_nodes + 1  # CSV header

        # fmt: off
        (
            Rust(
                "toposort",
                self.local_graph_path / self.graph_name,
                "--algorithm", self.algorithm,
                "--direction", self.direction,
                "--node-types", self.object_types,
            )
            | Command.pv("--line-mode", "--wait", "--size", str(nb_lines))
            | Command.zstdmt("-9")  # not -19 because of CPU usage + little gain
            > AtomicFileSink(self.output())
        ).run()
        # fmt: on


class ComputeGenerations(luigi.Task):
    """Creates a file that contains all SWHIDs in topological order from a compressed
    graph."""

    local_graph_path = luigi.PathParameter()
    topological_order_dir = luigi.PathParameter()
    graph_name = luigi.Parameter(default="graph")
    object_types = luigi.Parameter()
    direction = luigi.ChoiceParameter(choices=["forward", "backward"])

    def requires(self) -> List[luigi.Task]:
        """Returns an instance of :class:`LocalGraph`."""
        return [LocalGraph(local_graph_path=self.local_graph_path)]

    def _topo_order_path(self) -> Path:
        return (
            self.topological_order_dir
            / f"topological_order_{self.direction}_{self.object_types}.bitstream"
        )

    def _topo_order_offsets_path(self) -> Path:
        return (
            self.topological_order_dir
            / f"topological_order_{self.direction}_{self.object_types}.bitstream.offsets"
        )

    def _depths_path(self) -> Path:
        return (
            self.topological_order_dir
            / f"depths_{self.direction}_{self.object_types}.bin"
        )

    def output(self) -> dict[str, luigi.Target]:
        """.csv.zst file that contains the topological order."""
        return {
            "topo_order": luigi.LocalTarget(self._topo_order_path()),
            "topo_order_offsets": luigi.LocalTarget(self._topo_order_offsets_path()),
            "generations": luigi.LocalTarget(self._depths_path()),
        }

    def run(self) -> None:
        """Runs 'toposort' command from tools/topology and compresses"""
        from ..shell import Rust

        invalid_object_types = set(self.object_types.split(",")) - OBJECT_TYPES
        if invalid_object_types:
            raise ValueError(f"Invalid object types: {invalid_object_types}")

        # fmt: off
        (
            Rust(
                "generations",
                self.local_graph_path / self.graph_name,
                "--direction", self.direction,
                "--node-types", self.object_types,
                "--output-order", self._topo_order_path(),
                "--output-depths", self._depths_path(),
            )
        ).run()
        # fmt: on


class UploadGenerationsToS3(luigi.Task):
    """Uploads the output of :class:`ComputeGenerations` to S3"""

    local_graph_path = luigi.PathParameter()
    topological_order_dir = luigi.PathParameter()
    dataset_name = luigi.Parameter()
    graph_name = luigi.Parameter(default="graph")
    object_types = luigi.Parameter()
    direction = luigi.ChoiceParameter(choices=["forward", "backward"])

    def requires(self) -> luigi.Task:
        """Returns an instance of :class:`ComputeGenerations`."""
        return ComputeGenerations(
            local_graph_path=self.local_graph_path,
            topological_order_dir=self.topological_order_dir,
            graph_name=self.graph_name,
            object_types=self.object_types,
            direction=self.direction,
        )

    def output(self) -> List[luigi.Target]:
        """Returns .bitstream and .bin paths on S3."""
        import luigi.contrib.s3

        return [
            luigi.contrib.s3.S3Target(f"{self._s3_prefix()}/{filename}")
            for filename in self._filenames()
        ]

    def _s3_prefix(self) -> str:
        return f"s3://softwareheritage/derived_datasets/{self.dataset_name}/topology"

    def _filenames(self) -> List[str]:
        return [
            f"topological_order_{self.direction}_{self.object_types}.bitstream",
            f"topological_order_{self.direction}_{self.object_types}.bitstream.offsets",
            f"depths_{self.direction}_{self.object_types}.bin",
        ]

    def run(self) -> None:
        """Copies the files"""
        import multiprocessing.dummy

        import tqdm

        self.__status_messages: Dict[Path, str] = {}

        actual_filenames = {Path(target.path).name for target in self.input().values()}
        filenames = set(self._filenames())
        assert (
            actual_filenames == filenames
        ), f"Expected ComputeGenerations to return {filenames}, got {actual_filenames}"
        with multiprocessing.dummy.Pool(len(filenames)) as p:
            for i, relative_path in tqdm.tqdm(
                enumerate(p.imap_unordered(self._upload_file, filenames)),
                total=len(filenames),
                desc=f"Uploading to {self._s3_prefix()}",
            ):
                self.set_progress_percentage(int(i * 100 / len(filenames)))
                self.set_status_message("\n".join(self.__status_messages.values()))

    def _upload_file(self, filename: Path) -> Path:
        import luigi.contrib.s3

        client = luigi.contrib.s3.S3Client()

        self.__status_messages[filename] = f"Uploading {filename}"

        client.put_multipart(
            f"{self.topological_order_dir}/{filename}",
            f"{self._s3_prefix()}/{filename}",
            ACL="public-read",
        )

        del self.__status_messages[filename]

        return filename


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

    def output(self) -> luigi.LocalTarget:
        """.csv.zst file that contains the counts."""
        return luigi.LocalTarget(
            self.topological_order_dir
            / f"path_counts_{self.direction}_{self.object_types}/"
        )

    def nb_lines(self):
        nb_lines = count_nodes(
            self.local_graph_path, self.graph_name, self.object_types
        )
        nb_lines += 1  # CSV header
        return nb_lines

    def run(self) -> None:
        """Runs 'count_paths' command from tools/topology and compresses"""
        from ..shell import Command, Rust

        invalid_object_types = set(self.object_types.split(",")) - OBJECT_TYPES
        if invalid_object_types:
            raise ValueError(f"Invalid object types: {invalid_object_types}")
        topological_order_path = self.input()["toposort"].path

        topo_order_command = Command.zstdcat(topological_order_path)

        if "cnt" in self.object_types.split(","):
            # The toposort does not include content; add them to the topo order.
            #
            # As it has this header:
            #     SWHID,ancestors,successors,sample_ancestor1,sample_ancestor2
            # we have to use sed to add the extra columns. CountPaths.java does not use
            # them, so dummy values are fine.
            if (self.local_graph_path / f"{self.graph_name}.nodes.csv.zst").exists():
                # pre-2024 graph
                content_input = Command.zstdcat(
                    self.local_graph_path / f"{self.graph_name}.nodes.csv.zst"
                ) | Command.grep("^swh:1:cnt:", check=False)
            else:
                nodes_dir = self.local_graph_path / f"{self.graph_name}.nodes"
                content_input = Command.cat(
                    *[
                        Command.zstdcat(nodes_shard)
                        | Command.grep("^swh:1:cnt:", check=False)
                        for nodes_shard in nodes_dir.iterdir()
                    ]
                )
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
            | Rust(
                "count_paths",
                self.local_graph_path / self.graph_name,
                "--direction",
                self.direction,
                "--out",
                self.output()
            )
        ).run()
        # fmt: on


class PathCountsParquetToS3(_ParquetToS3ToAthenaTask):
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

    def _input_parquet_path(self) -> Path:
        return self.topological_order_dir / f"{self._base_filename()}.csv.zst"

    def _s3_bucket(self) -> str:
        # TODO: configurable
        return "softwareheritage"

    def _s3_prefix(self) -> str:
        # TODO: configurable
        return f"derived_datasets/{self.dataset_name}/{self._base_filename()}"

    def _orc_columns(self) -> List[Tuple[str, str]]:
        return [
            ("SWHID", "string"),
            ("paths_from_roots", "double"),
            ("all_paths", "double"),
        ]

    def _athena_db_name(self) -> str:
        return f"derived_{self.dataset_name.replace('-', '')}"

    def _athena_table_name(self) -> str:
        return self._base_filename().replace(",", "")
