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
import os
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Set, Tuple, Union, cast

import luigi

from swh.dataset.luigi import AthenaDatabaseTarget, S3PathParameter

from .compressed_graph import LocalGraph

if TYPE_CHECKING:
    import multiprocessing

OBJECT_TYPES = {"ori", "snp", "rel", "rev", "dir", "cnt"}


# singleton written to signal to workers they should stop
class _EndOfQueue:
    pass


_ENF_OF_QUEUE = _EndOfQueue()


def count_nodes(local_graph_path: Path, graph_name: str, object_types: str) -> int:
    """Returns the number of nodes of the given types (in the 'cnt,dir,rev,rel,snp,ori'
    format) in the graph.
    """
    node_stats = (local_graph_path / f"{graph_name}.nodes.stats.txt").read_text()
    nb_nodes_per_type = dict(line.split() for line in node_stats.split("\n") if line)
    return sum(int(nb_nodes_per_type[type_]) for type_ in object_types.split(","))


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


class PopularContentNames(luigi.Task):
    """Creates a CSV file that contains the most popular name(s) of each content"""

    local_graph_path = luigi.PathParameter()
    popular_contents_path = luigi.PathParameter()
    graph_name = luigi.Parameter(default="graph")
    max_results_per_content = luigi.IntParameter(default=1)
    popularity_threshold = luigi.IntParameter(default=0)

    def _max_ram(self):
        nb_nodes = count_nodes(
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

    def output(self) -> luigi.Target:
        """.csv.zst file that contains the topological order."""
        return luigi.LocalTarget(self.popular_contents_path)

    def run(self) -> None:
        """Runs org.softwareheritage.graph.utils.PopularContentNames and compresses"""
        from .shell import AtomicFileSink, Command, Java

        if self.max_results_per_content == 1 and self.popularity_threshold == 0:
            # In this case, we know approximately how many results are expected:
            # the same as the number of contents (minus contents not referenced by
            # any directory, which should be negligeable)"
            nb_contents = count_nodes(self.local_graph_path, self.graph_name, "cnt")
            pv = Command.pv("--line-mode", "--wait", "--size", str(nb_contents))
        else:
            pv = Command.pv("--line-mode", "--wait")

        class_name = "org.softwareheritage.graph.utils.PopularContentNames"
        # fmt: on
        (
            Java(
                class_name,
                self.local_graph_path / self.graph_name,
                str(self.max_results_per_content),
                str(self.popularity_threshold),
            )
            | pv
            | Command.zstdmt("-19")
            > AtomicFileSink(self.output())
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
        nb_nodes = count_nodes(
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

    def output(self) -> luigi.Target:
        """.csv.zst file that contains the topological order."""
        return luigi.LocalTarget(self.popular_contents_path)

    def run(self) -> None:
        """Runs org.softwareheritage.graph.utils.PopularContentPaths and compresses"""
        import multiprocessing.dummy

        from .shell import AtomicFileSink, Command, Java, wc

        input_swhid_files = list(self.input_swhids.iterdir())

        with multiprocessing.dummy.Pool() as p:
            nb_contents = sum(
                p.imap_unordered(
                    lambda path: wc(Command.zstdcat(path), "-l") - 1,
                    input_swhid_files,
                )
            )

        # Stream the header from all inputs but the first
        input_streams = [Command.zstdcat(input_swhid_files[0])]
        for input_swhid_file in input_swhid_files[1:]:
            input_streams.append(
                Command.zstdcat(input_swhid_file) | Command.tail("-n", "+2")
            )

        class_name = "org.softwareheritage.graph.utils.PopularContentPaths"
        # fmt: on
        (
            Command.cat(*input_streams)
            | Java(
                class_name,
                self.local_graph_path / self.graph_name,
                str(self.max_depth),
                max_ram=self._max_ram(),
            )
            | Command.pv("--line-mode", "--wait", "--size", str(nb_contents + 1))
            | Command.zstdmt("-19")
            > AtomicFileSink(self.output())
        ).run()
        # fmt: off


class _CsvToOrcToS3ToAthenaTask(luigi.Task):
    """Base class for tasks which take a CSV as input, convert it to ORC,
    upload the ORC to S3, and create an Athena table for it."""

    def _input_csv_path(self) -> Path:
        raise NotImplementedError(f"{self.__class__.__name__}._input_csv_path")

    def _s3_bucket(self) -> str:
        raise NotImplementedError(f"{self.__class__.__name__}._s3_bucket")

    def _s3_prefix(self) -> str:
        raise NotImplementedError(f"{self.__class__.__name__}._s3_prefix")

    def _orc_columns(self) -> List[Tuple[str, str]]:
        """Returns a list of ``(column_name, orc_type)``"""
        raise NotImplementedError(f"{self.__class__.__name__}._orc_columns")

    def _approx_nb_rows(self) -> int:
        """Returns number of rows in the CSV file. Used only for progress reporting"""
        from .shell import Command, wc

        # This is a rough estimate, because some rows can contain newlines;
        # but it is good enough for a progress report
        return wc(Command.zstdcat(self._input_csv_path()), "-l")

    def _parse_row(self, row: List[str]) -> Tuple[Any, ...]:
        """Parses a row from the CSV file"""
        raise NotImplementedError(f"{self.__class__.__name__}._parse_row")

    def _pyorc_writer_kwargs(self) -> Dict[str, Any]:
        """Arguments to pass to :cls:`pyorc.Writer`'s constructor"""
        import pyorc

        return {
            "compression": pyorc.CompressionKind.ZSTD,
            # We are highly parallel and want to store for a long time ->
            # don't use the default "SPEED" strategy
            "compression_strategy": pyorc.CompressionStrategy.COMPRESSION,
        }

    def _athena_db_name(self) -> str:
        raise NotImplementedError(f"{self.__class__.__name__}._athena_db_name")

    def _athena_table_name(self) -> str:
        raise NotImplementedError(f"{self.__class__.__name__}._athena_table_name")

    def _create_athena_tables(self) -> Set[str]:
        raise NotImplementedError(f"{self.__class__.__name__}._create_athena_tables")

    def output(self) -> luigi.Target:
        return AthenaDatabaseTarget(self._athena_db_name(), {self._athena_table_name()})

    def run(self) -> None:
        """Copies all files: first the graph itself, then :file:`meta/compression.json`."""
        import csv
        import subprocess

        columns = self._orc_columns()
        expected_header = list(dict(columns))

        self.total_rows = 0

        self._clean_s3_directory()

        # We are CPU-bound by the csv module. In order not to add even more stuff
        # to do in the same thread, we shell out to zstd instead of using pyorc.
        zstd_proc = subprocess.Popen(
            ["zstdmt", "-d", self._input_csv_path(), "--stdout"],
            stdout=subprocess.PIPE,
            encoding="utf8",
        )
        try:
            reader = csv.reader(cast(Iterator[str], zstd_proc.stdout))
            header = next(reader)
            if header != expected_header:
                raise Exception(f"Expected {expected_header} as header, got {header}")

            self._convert_csv_to_orc_on_s3(reader)
        finally:
            zstd_proc.kill()

        self._create_athena_tables()

    def _clean_s3_directory(self) -> None:
        """Checks the S3 directory is either missing or contains aborted only .orc
        files. In the latter case, deletes them."""
        import boto3

        s3 = boto3.client("s3")

        orc_files = []
        prefix = self._s3_prefix()
        assert prefix.endswith("/"), prefix
        base_url = f"{self._s3_bucket()}/{prefix}"
        paginator = s3.get_paginator("list_objects")
        pages = paginator.paginate(Bucket=self._s3_bucket(), Prefix=prefix)
        for page in pages:
            if "Contents" not in page:
                # no match at all
                assert not page["IsTruncated"]
                break
            for object_ in page["Contents"]:
                key = object_["Key"]
                assert key.startswith(prefix)
                filename = key[len(prefix) :]
                if "/" in filename:
                    raise Exception(
                        f"{base_url} unexpectedly contains a subdirectory: "
                        f"{filename.split('/')[0]}"
                    )
                if not filename.endswith(".orc"):
                    raise Exception(
                        f"{base_url} unexpected contains a non-ORC: {filename}"
                    )
                orc_files.append(filename)

        for orc_file in orc_files:
            s3.delete_object(Bucket=self._s3_bucket(), Key=f"{prefix}{orc_file}")

    def _convert_csv_to_orc_on_s3(self, reader) -> None:
        import multiprocessing

        # with parallelism higher than this, reading the CSV is guaranteed to be
        # the bottleneck
        parallelism = min(multiprocessing.cpu_count(), 10)

        # pairs of (orc_writer, orc_uploader)
        row_batches: multiprocessing.Queue[
            Union[_EndOfQueue, List[tuple]]
        ] = multiprocessing.Queue(maxsize=parallelism)

        try:
            orc_writers = []
            orc_uploaders = []
            for _ in range(parallelism):
                # Write to the pipe with pyorc, read from the pipe with boto3
                (read_fd, write_fd) = os.pipe()

                proc = multiprocessing.Process(
                    target=self._write_orc_shard, args=(write_fd, row_batches)
                )
                orc_writers.append(proc)
                proc.start()
                os.close(write_fd)

                proc = multiprocessing.Process(
                    target=self._upload_orc_shard, args=(read_fd,)
                )
                orc_uploaders.append(proc)
                proc.start()
                os.close(read_fd)

            # Read the CSV and write to the row_batches queues.
            # Blocks until reading the CSV completes.
            self.set_status_message("Reading CSV")
            self._read_csv(reader, row_batches)

            # Signal to all orc writers they should stop
            for _ in range(parallelism):
                row_batches.put(_ENF_OF_QUEUE)

            self.set_status_message("Waiting for ORC writers to complete")
            for orc_writer in orc_writers:
                orc_writer.join()

            self.set_status_message("Waiting for ORC uploaders to complete")
            for orc_uploader in orc_uploaders:
                orc_uploader.join()
        except BaseException:
            for orc_uploader in orc_uploaders:
                orc_uploader.kill()
            for orc_writer in orc_writers:
                orc_writer.kill()
            raise

    def _read_csv(
        self,
        reader,
        row_batches: "multiprocessing.Queue[Union[_EndOfQueue, List[tuple]]]",
    ) -> None:
        import tqdm

        from swh.core.utils import grouper

        # we need to pick a value somehow; so might as well use the same batch size
        # as pyorc. Experimentally, it doesn't seem to matter
        batch_size = self._pyorc_writer_kwargs().get("batch_size", 1024)

        for row_batch in grouper(
            tqdm.tqdm(
                reader,
                desc="Reading CSV",
                unit_scale=True,
                unit="row",
                total=self._approx_nb_rows(),
            ),
            batch_size,
        ):
            row_batch = list(map(self._parse_row, row_batch))
            row_batches.put(row_batch)
            self.total_rows += len(row_batch)
            self.set_status_message(f"{self.total_rows} rows read from CSV")

    def _write_orc_shard(
        self,
        write_fd: int,
        row_batches: "multiprocessing.Queue[Union[_EndOfQueue, List[tuple]]]",
    ) -> None:
        import pyorc

        write_file = os.fdopen(write_fd, "wb")
        fields = ",".join(f"{col}:{type_}" for (col, type_) in self._orc_columns())
        with pyorc.Writer(
            write_file,
            f"struct<{fields}>",
            **self._pyorc_writer_kwargs(),
        ) as writer:
            while True:
                batch = row_batches.get()
                if isinstance(batch, _EndOfQueue):
                    # No more work to do
                    return

                for row in batch:
                    writer.write(row)

        # pyorc closes the FD itself, signaling to uploaders that the file ends.
        assert write_file.closed

    def _upload_orc_shard(self, read_fd: int) -> None:
        import uuid

        import boto3

        s3 = boto3.client("s3")

        path = f"{self._s3_prefix().strip('/')}/{uuid.uuid4()}.orc"
        s3.upload_fileobj(os.fdopen(read_fd, "rb"), self._s3_bucket(), path)
        # boto3 closes the FD itself


class PopularContentNamesOrcToS3(_CsvToOrcToS3ToAthenaTask):
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

    def _input_csv_path(self) -> Path:
        return self.popular_contents_path

    def _s3_bucket(self) -> str:
        # TODO: configurable
        return "softwareheritage"

    def _s3_prefix(self) -> str:
        # TODO: configurable
        return f"derived_datasets/{self.dataset_name}/popular_contents/"

    def _orc_columns(self) -> List[Tuple[str, str]]:
        return [
            ("SWHID", "string"),
            ("length", "int"),
            ("filename", "binary"),
            ("occurrences", "bigint"),
        ]

    def _approx_nb_rows(self) -> int:
        return self.requires().nb_lines() - 1  # -1 for the header

    def _parse_row(self, row: List[str]) -> Tuple[Any, ...]:
        (swhid, length, filename, occurrences) = row
        return (swhid, int(length), filename.encode(), int(occurrences))

    def _pyorc_writer_kwargs(self) -> Dict[str, Any]:
        return {
            **super()._pyorc_writer_kwargs(),
            "bloom_filter_columns": ["SWHID", "filename"],
        }

    def _athena_db_name(self) -> str:
        return f"derived_{self.dataset_name.replace('-', '')}"

    def _athena_table_name(self) -> str:
        return "popular_contents"

    def _create_athena_tables(self):
        import boto3

        from swh.dataset.athena import query

        client = boto3.client("athena")
        client.output_location = self.s3_athena_output_location

        client.database_name = "default"  # we have to pick some existing database
        query(
            client,
            f"CREATE DATABASE IF NOT EXISTS {self._athena_db_name()};",
            desc=f"Creating {self._athena_db_name()} database",
        )
        client.database_name = self._athena_db_name()

        query(
            client,
            f"""
            CREATE EXTERNAL TABLE IF NOT EXISTS {self._athena_db_name()}.popular_content
            (
                SWHID string,
                length int,
                filename binary,
                occurrences bigint
            )
            STORED AS ORC
            LOCATION 's3://{self._s3_bucket()}/{self._s3_prefix()}'
            TBLPROPERTIES ("orc.compress"="ZSTD");
            """,
            desc="Creating table popular_content",
        )


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

    def _create_athena_tables(self):
        import boto3

        from swh.dataset.athena import query

        client = boto3.client("athena")
        client.output_location = self.s3_athena_output_location

        client.database_name = "default"  # we have to pick some existing database
        query(
            client,
            f"CREATE DATABASE IF NOT EXISTS {self._athena_db_name()};",
            desc=f"Creating {self._athena_db_name()} database",
        )
        client.database_name = self._athena_db_name()

        query(
            client,
            f"""
            CREATE EXTERNAL TABLE IF NOT EXISTS
                {self._athena_db_name()}.{self._athena_table_name()}
            (
                SWHID string,
                paths_from_roots double,
                all_paths double
            )
            STORED AS ORC
            LOCATION 's3://{self._s3_bucket()}/{self._s3_prefix()}'
            TBLPROPERTIES ("orc.compress"="ZSTD");
            """,
            desc=f"Creating table {self._athena_table_name()}",
        )
