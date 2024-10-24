# Copyright (C) 2022-2024 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

# WARNING: do not import unnecessary things here to keep cli startup time under
# control
from pathlib import Path
from typing import Dict, List, Tuple

import luigi

from swh.dataset.luigi import AthenaDatabaseTarget

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


class _ParquetToS3ToAthenaTask(luigi.Task):
    """Base class for tasks which take a CSV as input, convert it to ORC,
    upload the ORC to S3, and create an Athena table for it."""

    parallelism = 10

    def _input_parquet_path(self) -> Path:
        raise NotImplementedError(f"{self.__class__.__name__}._input_parquet_path")

    def _s3_bucket(self) -> str:
        raise NotImplementedError(f"{self.__class__.__name__}._s3_bucket")

    def _s3_prefix(self) -> str:
        raise NotImplementedError(f"{self.__class__.__name__}._s3_prefix")

    def _parquet_columns(self) -> List[Tuple[str, str]]:
        """Returns a list of ``(column_name, parquet_type)``"""
        raise NotImplementedError(f"{self.__class__.__name__}._parquet_columns")

    def _approx_nb_rows(self) -> int:
        """Returns number of rows in the CSV file. Used only for progress reporting"""
        import pyarrow.parquet

        return sum(
            pyarrow.parquet.read_metadata(file).num_rows
            for file in self._input_parquet_path().iterdir()
        )

    def _athena_db_name(self) -> str:
        raise NotImplementedError(f"{self.__class__.__name__}._athena_db_name")

    def _athena_table_name(self) -> str:
        raise NotImplementedError(f"{self.__class__.__name__}._athena_table_name")

    def output(self) -> luigi.Target:
        return AthenaDatabaseTarget(self._athena_db_name(), {self._athena_table_name()})

    def run(self) -> None:
        """Copies all files: first the graph itself, then :file:`meta/compression.json`."""
        import multiprocessing

        import tqdm

        self.__status_messages: Dict[Path, str] = {}

        paths = list(self._input_parquet_path().glob("**/*.parquet"))

        with multiprocessing.Pool(self.parallelism) as p:
            for i, relative_path in tqdm.tqdm(
                enumerate(p.imap_unordered(self._upload_file, paths)),
                total=len(paths),
                desc=f"Uploading {self._input_parquet_path()} to "
                f"s3://{self._s3_bucket()}/{self._s3_prefix()}/",
            ):
                self.set_progress_percentage(int(i * 100 / len(paths)))
                self.set_status_message("\n".join(self.__status_messages.values()))

        self._create_athena_table()

    def _upload_file(self, path):
        import luigi.contrib.s3

        client = luigi.contrib.s3.S3Client()

        relative_path = path.relative_to(self._input_parquet_path())

        self.__status_messages[path] = f"Uploading {relative_path}"

        client.put_multipart(
            path,
            f"s3://{self._s3_bucket()}/{self._s3_prefix()}/{relative_path}",
            ACL="public-read",
        )

        del self.__status_messages[path]

        return relative_path

    def _create_athena_table(self):
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

        columns = ", ".join(
            f"{col} {type_}" for (col, type_) in self._parquet_columns()
        )

        query(
            client,
            f"""
            CREATE EXTERNAL TABLE IF NOT EXISTS
            {self._athena_db_name()}.{self._athena_table_name()}
            ({columns})
            {self.create_table_extras()}
            STORED AS PARQUET
            LOCATION 's3://{self._s3_bucket()}/{self._s3_prefix()}';
            """,
            desc=f"Creating table {self._athena_table_name()}",
        )

        # needed for partitioned tables
        query(
            client,
            f"MSCK REPAIR TABLE `{self._athena_table_name()}`",
            desc=f"'Repairing' table {self._athena_table_name()}",
        )

    def create_table_extras(self) -> str:
        """Extra clauses to add to the ``CREATE EXTERNAL TABLE`` statement."""
        return ""
