# Copyright (C) 2023-2025  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime
from pathlib import Path

import datafusion
import pytest
import pyzstd

from swh.graph.example_dataset import DATASET_DIR
from swh.graph.luigi.aggregate_datasets import (
    AggregateContentDatasets,
    ExportNodesTable,
)
from swh.graph.luigi.provenance import ComputeEarliestTimestamps, ListRevisionsInOrigins

from .test_file_names import CSV_HEADER_NAMES as POPULAR_FILE_NAMES_CSV_HEADER
from .test_file_names import EXPECTED_LINES_DEPTH1 as POPULAR_FILE_NAMES_CSV

utc = datetime.timezone.utc


@pytest.mark.parametrize("provenance_node_filter", ["heads", "all"])
def test_aggregate_content_datasets(tmpdir, provenance_node_filter):
    tmpdir = Path(tmpdir)
    aggregate_datasets_path = tmpdir / "aggregated"

    # Create file names dataset
    popular_content_names_path = tmpdir / "popcon"
    popular_content_names_path.mkdir()
    (popular_content_names_path / "0.csv.zst").write_bytes(
        pyzstd.compress(
            (POPULAR_FILE_NAMES_CSV_HEADER + "\n" + POPULAR_FILE_NAMES_CSV).encode()
        )
    )

    # Create earliest_timestamps.bin
    provenance_dir = tmpdir / "provenance"
    provenance_dir.mkdir()
    ComputeEarliestTimestamps(
        local_export_path=DATASET_DIR,
        local_graph_path=DATASET_DIR / "compressed",
        graph_name="example",
        provenance_dir=provenance_dir,
        provenance_node_filter=provenance_node_filter,
    ).run()

    # Create revisions_in_origins/ table
    provenance_dir = tmpdir / "provenance"
    ListRevisionsInOrigins(
        local_export_path=DATASET_DIR,
        local_graph_path=DATASET_DIR / "compressed",
        graph_name="example",
        provenance_dir=provenance_dir,
        provenance_node_filter=provenance_node_filter,
    ).run()

    ExportNodesTable(
        local_graph_path=DATASET_DIR / "compressed",
        graph_name="example",
        aggregate_datasets_path=aggregate_datasets_path,
    ).run()
    AggregateContentDatasets(
        local_graph_path=DATASET_DIR / "compressed",
        graph_name="example",
        aggregate_datasets_path=aggregate_datasets_path,
        popular_content_names_path=popular_content_names_path,
        provenance_dir=provenance_dir,
    ).run()

    ctx = datafusion.SessionContext()
    ctx.sql(
        f"""
        CREATE EXTERNAL TABLE node
        STORED AS PARQUET
        PARTITIONED BY (node_type)
        LOCATION '{aggregate_datasets_path}/nodes/'
        """
    )
    ctx.sql(
        f"""
        CREATE EXTERNAL TABLE content
        STORED AS PARQUET
        LOCATION '{aggregate_datasets_path}/contents/'
        """
    )

    rows = ctx.sql(
        """
        SELECT
            content_node.swhid AS content,
            length,
            filename,
            filename_occurrences,
            first_occurrence_timestamp,
            revrel_node.swhid AS first_occurrence_revrel,
            origin_node.url AS first_occurrence_origin
        FROM content
        INNER JOIN
            node AS content_node
            ON (content_node.id=content.id)
        LEFT JOIN
            node AS revrel_node
            -- node_type clause is optional, but should improve performance in production
            ON (revrel_node.id=content.first_occurrence_revrel AND revrel_node.node_type IN ('rev', 'rel'))
        LEFT JOIN
            node AS origin_node
            -- ditto
            ON (origin_node.id=content.first_occurrence_origin AND origin_node.node_type IN ('ori'))
        ORDER BY content
        """  # noqa
    ).to_pylist()

    if provenance_node_filter == "heads":
        date1 = datetime.datetime(2005, 3, 18, 11, 14, tzinfo=utc)
        revrel1 = "swh:1:rev:0000000000000000000000000000000000000009"
        date2 = None  # not referenced by any head
        revrel2 = None
        origin2 = None
    elif provenance_node_filter == "all":
        date1 = datetime.datetime(2005, 3, 18, 5, 3, 40, tzinfo=utc)
        revrel1 = "swh:1:rev:0000000000000000000000000000000000000003"
        date2 = datetime.datetime(2005, 3, 18, 17, 24, 20, tzinfo=utc)
        revrel2 = "swh:1:rev:0000000000000000000000000000000000000013"
        origin2 = "https://example.com/swh/graph2"
    else:
        assert False

    # can turn into "https://example.com/swh/graph" after regenerating the graph
    first_origin = "https://example.com/swh/graph2"

    assert rows == [
        {
            "content": "swh:1:cnt:0000000000000000000000000000000000000001",
            "filename": b"README.md",
            "filename_occurrences": 2,
            "first_occurrence_timestamp": date1,
            "first_occurrence_revrel": revrel1,
            "first_occurrence_origin": first_origin,
            "length": 42,
        },
        {
            "content": "swh:1:cnt:0000000000000000000000000000000000000004",
            "filename": b"README.md",
            "filename_occurrences": 1,
            "first_occurrence_timestamp": datetime.datetime(
                2005, 3, 18, 11, 14, tzinfo=utc
            ),
            "first_occurrence_revrel": "swh:1:rev:0000000000000000000000000000000000000009",
            "first_occurrence_origin": first_origin,
            "length": 404,
        },
        {
            "content": "swh:1:cnt:0000000000000000000000000000000000000005",
            "filename": b"parser.c",
            "filename_occurrences": 1,
            "first_occurrence_timestamp": datetime.datetime(
                2005, 3, 18, 11, 14, tzinfo=utc
            ),
            "first_occurrence_revrel": "swh:1:rev:0000000000000000000000000000000000000009",
            "first_occurrence_origin": first_origin,
            "length": 1337,
        },
        {
            "content": "swh:1:cnt:0000000000000000000000000000000000000007",
            "filename": b"parser.c",
            "filename_occurrences": 1,
            "first_occurrence_timestamp": datetime.datetime(
                2005, 3, 18, 11, 14, tzinfo=utc
            ),
            "first_occurrence_revrel": "swh:1:rev:0000000000000000000000000000000000000009",
            "first_occurrence_origin": first_origin,
            "length": 666,
        },
        {
            "content": "swh:1:cnt:0000000000000000000000000000000000000011",
            "filename": b"README.md",
            "filename_occurrences": 1,
            "first_occurrence_timestamp": date2,
            "first_occurrence_revrel": revrel2,
            "first_occurrence_origin": origin2,
            "length": 313,
        },
        {
            "content": "swh:1:cnt:0000000000000000000000000000000000000014",
            "filename": b"TODO.txt",
            "filename_occurrences": 1,
            "first_occurrence_timestamp": datetime.datetime(
                2005, 3, 18, 20, 29, 30, tzinfo=utc
            ),
            "first_occurrence_revrel": "swh:1:rev:0000000000000000000000000000000000000018",
            "first_occurrence_origin": "https://example.com/swh/graph2",
            "length": 14,
        },
        {
            "content": "swh:1:cnt:0000000000000000000000000000000000000015",
            "filename": b"TODO.txt",
            "filename_occurrences": 1,
            "first_occurrence_timestamp": datetime.datetime(
                2005, 3, 18, 20, 29, 30, tzinfo=utc
            ),
            "first_occurrence_revrel": "swh:1:rev:0000000000000000000000000000000000000018",
            "first_occurrence_origin": "https://example.com/swh/graph2",
            "length": 404,
        },
    ]
