# Copyright (C) 2024 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""
Luigi tasks for producing the aggregated derived datasets
=========================================================

"""  # noqa

# WARNING: do not import unnecessary things here to keep cli startup time under
# control
from pathlib import Path
from typing import Dict, List, Tuple

import luigi

from swh.dataset.luigi import S3PathParameter

from .compressed_graph import LocalGraph
from .file_names import PopularContentNames
from .provenance import ComputeEarliestTimestamps
from .utils import _ParquetToS3ToAthenaTask


class ExportNodesTable(luigi.Task):
    """Creates a Parquet dataset that contains the id and SWHID of each node"""

    local_graph_path = luigi.PathParameter()
    graph_name = luigi.Parameter(default="graph")
    aggregate_datasets_path = luigi.PathParameter()

    def requires(self) -> Dict[str, luigi.Task]:
        """Returns an instance of :class:`LocalGraph`."""
        return {
            "graph": LocalGraph(local_graph_path=self.local_graph_path),
        }

    def output(self) -> luigi.LocalTarget:
        """Directory of Parquet files."""
        return luigi.LocalTarget(self.aggregate_datasets_path / "nodes")

    def run(self) -> None:
        """Runs ``export-nodes`` from :file:`tools/aggregate`"""
        from ..shell import Rust

        # fmt: on
        (
            Rust(
                "export-nodes",
                self.local_graph_path / self.graph_name,
                "--nodes-out",
                self.output(),
            )
        ).run()
        # fmt: off


class AggregateContentDatasets(luigi.Task):
    """Creates a Parquet dataset that contains a column for each of:

    * the content id
    * the content's length
    * the most popular name of each content
    * number of occurrences of that name for the content
    * its date of first occurrence in a revision or release, if any
    * said revision or release, if any
    * an origin containing said revision or release, if any
    """

    local_graph_path = luigi.PathParameter()
    graph_name = luigi.Parameter(default="graph")
    popular_content_names_path = luigi.PathParameter()
    provenance_dir = luigi.PathParameter()
    aggregate_datasets_path = luigi.PathParameter()

    def requires(self) -> Dict[str, luigi.Task]:
        """Returns an instance of :class:`LocalGraph`."""
        return {
            "graph": LocalGraph(local_graph_path=self.local_graph_path),
            "popular_content_names": PopularContentNames(
                local_graph_path=self.local_graph_path,
                popular_contents_path=self.popular_content_names_path,
                max_results_per_content=1,
            ),
            "earliest_timestamps": ComputeEarliestTimestamps(
                local_graph_path=self.local_graph_path,
                provenance_dir=self.provenance_dir,
                provenance_node_filter="all",
            ),
        }

    def output(self) -> luigi.LocalTarget:
        """Directory of Parquet files."""
        return luigi.LocalTarget(self.aggregate_datasets_path / "contents")

    def run(self) -> None:
        """Runs ``aggregate-content-datasets`` from :file:`tools/aggregate`"""
        from ..shell import Rust

        # fmt: on
        (
            Rust(
                "aggregate-content-datasets",
                self.local_graph_path / self.graph_name,
                "--file-names",
                self.popular_content_names_path,
                "--earliest-timestamps",
                self.provenance_dir / "earliest_timestamps.bin",
                "--out",
                self.output(),
            )
        ).run()
        # fmt: off


class UploadNodesTable(_ParquetToS3ToAthenaTask):
    """Uploads the result of :class:`AggregateContentDatasets` to S3 and registers
    a table on Athena to query it"""

    aggregate_datasets_path = luigi.PathParameter()
    dataset_name = luigi.Parameter()
    s3_athena_output_location = S3PathParameter()

    def requires(self) -> luigi.Task:
        """Returns an instance of :class:`ExportNodesTable`."""
        return ExportNodesTable(aggregate_datasets_path=self.aggregate_datasets_path)

    def _input_parquet_path(self) -> Path:
        return self.aggregate_datasets_path / "nodes"

    def _s3_bucket(self) -> str:
        # TODO: configurable
        return "softwareheritage"

    def _s3_prefix(self) -> str:
        # TODO: configurable
        return f"derived_datasets/{self.dataset_name}/nodes"

    def _parquet_columns(self) -> List[Tuple[str, str]]:
        return [
            ("id", "bigint"),
            ("swhid", "string"),
            ("url", "string"),
        ]

    def _athena_db_name(self) -> str:
        return f"derived_{self.dataset_name.replace('-', '')}"

    def _athena_table_name(self) -> str:
        return "nodes"

    def create_table_extras(self) -> str:
        """Extra clauses to add to the ``CREATE EXTERNAL TABLE`` statement."""
        return "PARTITIONED BY (node_type string)"


class UploadAggregatedContentDataset(_ParquetToS3ToAthenaTask):
    """Uploads the result of :class:`AggregateContentDatasets` to S3 and registers
    a table on Athena to query it"""

    aggregate_datasets_path = luigi.PathParameter()
    dataset_name = luigi.Parameter()
    s3_athena_output_location = S3PathParameter()

    def requires(self) -> luigi.Task:
        """Returns an instance of :class:`AggregateContentDatasets`."""
        return AggregateContentDatasets(
            aggregate_datasets_path=self.aggregate_datasets_path
        )

    def _input_parquet_path(self) -> Path:
        return self.aggregate_datasets_path / "contents"

    def _s3_bucket(self) -> str:
        # TODO: configurable
        return "softwareheritage"

    def _s3_prefix(self) -> str:
        # TODO: configurable
        return f"derived_datasets/{self.dataset_name}/contents"

    def _parquet_columns(self) -> List[Tuple[str, str]]:
        return [
            ("id", "bigint"),
            ("length", "bigint"),
            ("filename", "binary"),
            ("filename_occurrences", "bigint"),
            ("first_occurrence_timestamp", "bigint"),
            ("first_occurrence_revrel", "bigint"),
            ("first_occurrence_origin", "bigint"),
        ]

    def _athena_db_name(self) -> str:
        return f"derived_{self.dataset_name.replace('-', '')}"

    def _athena_table_name(self) -> str:
        return "contents"


class RunAggregatedDatasets(luigi.WrapperTask):
    """Runs :class:`UploadNodesTable`, :class:`UploadAggregatedContentDataset`,
    and their recursive dependencies."""

    aggregate_datasets_path = luigi.PathParameter()
    dataset_name = luigi.Parameter()
    s3_athena_output_location = S3PathParameter()

    def requires(self) -> Dict[str, luigi.Task]:
        """Returns an instance of :class:`AggregateContentDatasets`."""
        kwargs = dict(
            aggregate_datasets_path=self.aggregate_datasets_path,
            dataset_name=self.dataset_name,
            s3_athena_output_location=self.s3_athena_output_location,
        )
        return {
            "upload_nodes": UploadNodesTable(**kwargs),
            "upload_contents": UploadAggregatedContentDataset(**kwargs),
        }
