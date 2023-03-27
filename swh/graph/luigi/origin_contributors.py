# Copyright (C) 2022 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""
Luigi tasks for contribution graph
==================================

This module contains `Luigi <https://luigi.readthedocs.io/>`_ tasks
driving the creation of the graph of contributions of people (pseudonymized
by default).
"""
# WARNING: do not import unnecessary things here to keep cli startup time under
# control
from pathlib import Path
from typing import Dict, Iterable, List, Tuple, cast

import luigi

from .compressed_graph import LocalGraph
from .misc_datasets import TopoSort


class ListOriginContributors(luigi.Task):
    """Creates a file that contains all SWHIDs in topological order from a compressed
    graph."""

    local_graph_path = luigi.PathParameter()
    topological_order_dir = luigi.PathParameter()
    origin_contributors_path = luigi.PathParameter()
    origin_urls_path = luigi.PathParameter()
    graph_name = luigi.Parameter(default="graph")
    max_ram_mb = luigi.IntParameter(default=500_000)

    @property
    def resources(self):
        """Returns the value of ``self.max_ram_mb``"""
        import socket

        return {f"{socket.getfqdn()}_ram_mb": self.max_ram_mb}

    def requires(self) -> Dict[str, luigi.Task]:
        """Returns an instance of :class:`swh.graph.luigi.compressed_graph.LocalGraph`
        and :class:`swh.graph.luigi.misc_datasets.TopoSort`."""
        return {
            "graph": LocalGraph(local_graph_path=self.local_graph_path),
            "toposort": TopoSort(
                local_graph_path=self.local_graph_path,
                topological_order_dir=self.topological_order_dir,
                graph_name=self.graph_name,
                direction="backward",
                object_types="rev,rel,snp,ori",
            ),
        }

    def output(self) -> List[luigi.Target]:
        """.csv.zst file that contains the origin_id<->contributor_id map
        and the list of origins"""
        return [
            luigi.LocalTarget(self.origin_contributors_path),
            luigi.LocalTarget(self.origin_urls_path),
        ]

    def run(self) -> None:
        """Runs org.softwareheritage.graph.utils.TopoSort and compresses"""
        import tempfile

        from .shell import AtomicFileSink, Command, Java, wc

        topological_order_path = Path(self.input()["toposort"].path)

        class_name = "org.softwareheritage.graph.utils.ListOriginContributors"
        with tempfile.NamedTemporaryFile(
            prefix="origin_urls_", suffix=".csv"
        ) as origin_urls_fd:
            # fmt: off
            nb_lines = wc(Command.zstdcat(topological_order_path), "-l")
            (
                Command.zstdcat(topological_order_path)
                | Java(
                    class_name,
                    self.local_graph_path / self.graph_name,
                    origin_urls_fd.name,
                    max_ram=self.max_ram_mb * 1_000_000,
                )
                | Command.pv("--line-mode", "--wait", "--size", str(nb_lines))
                | Command.zstdmt("-19")
                > AtomicFileSink(self.origin_contributors_path)
            ).run()
            (
                Command.pv(origin_urls_fd.name)
                | Command.zstdmt("-19")
                > AtomicFileSink(self.origin_urls_path)
            ).run()
            # fmt: on


class ExportDeanonymizationTable(luigi.Task):
    """Exports (from swh-storage) a .csv.zst file that contains the columns:
    ``base64(sha256(full_name))`, ``base64(full_name)``, and ``escape(full_name)``.

    The first column is the anonymized full name found in :file:`graph.persons.csv.zst`
    in the compressed graph, and the latter two are the original name."""

    storage_dsn = luigi.Parameter(
        default="service=swh",
        description="postgresql DSN of the swh-storage database to read from.",
    )
    deanonymization_table_path = luigi.PathParameter()

    def output(self) -> luigi.Target:
        """.csv.zst file that contains the table."""
        return luigi.LocalTarget(self.deanonymization_table_path)

    def run(self) -> None:
        """Runs a postgresql query to compute the table."""
        import shutil

        from .shell import AtomicFileSink, Command

        if shutil.which("psql") is None:
            raise RuntimeError("psql CLI is not installed")

        query = """
            COPY (
                SELECT
                    encode(digest(fullname, 'sha256'), 'base64') as sha256_base64, \
                    encode(fullname, 'base64') as base64, \
                    encode(fullname, 'escape') as escaped \
                FROM person  \
            ) TO STDOUT CSV HEADER \
        """

        # fmt: off
        (
            Command.psql(self.storage_dsn, "-c", query)
            | Command.zstdmt("-19")
            > AtomicFileSink(self.deanonymization_table_path)
        ).run()
        # fmt: on


class DeanonymizeOriginContributors(luigi.Task):
    """Generates a .csv.zst file similar to :class:`ListOriginContributors`'s,
    but with ``contributor_base64`` and ``contributor_escaped`` columns in addition to
    ``contributor_id``.

    This assumes that :file:`graph.persons.csv.zst` is anonymized (SHA256 of names
    instead of names); which may not be true depending on how the swh-dataset export
    was configured.
    """

    local_graph_path = luigi.PathParameter()
    graph_name = luigi.Parameter(default="graph")
    origin_contributors_path = luigi.PathParameter()
    deanonymization_table_path = luigi.PathParameter()
    deanonymized_origin_contributors_path = luigi.PathParameter()

    def requires(self) -> List[luigi.Task]:
        """Returns instances of :class:`LocalGraph`, :class:`ListOriginContributors`,
        and :class:`ExportDeanonymizationTable`."""
        return [
            LocalGraph(local_graph_path=self.local_graph_path),
            ListOriginContributors(
                local_graph_path=self.local_graph_path,
                origin_contributors_path=self.origin_contributors_path,
            ),
            ExportDeanonymizationTable(
                deanonymization_table_path=self.deanonymization_table_path,
            ),
        ]

    def output(self) -> luigi.Target:
        """.csv.zst file similar to :meth:`ListOriginContributors.output`'s,
        but with ``contributor_base64`` and ``contributor_escaped`` columns in addition
        to ``contributor_id``"""
        return luigi.LocalTarget(self.deanonymized_origin_contributors_path)

    def run(self) -> None:
        """Loads the list of persons (``graph.persons.csv.zst`` in the graph dataset
        and the deanonymization table in memory, then uses them to map each row
        in the original (anonymized) contributors list to the deanonymized one."""
        # TODO: .persons.csv.zst may be already deanonymized (if the swh-dataset export
        # was configured to do so); this should add support for it.

        import base64
        import csv

        import pyzstd

        # Load the deanonymization table, to map sha256(name) to base64(name)
        # and escape(name)
        sha256_to_names: Dict[bytes, Tuple[bytes, str]] = {}
        with pyzstd.open(self.deanonymization_table_path, "rt") as fd:
            # TODO: remove that cast once we dropped Python 3.7 support
            csv_reader = csv.reader(cast(Iterable[str], fd))
            header = next(csv_reader)
            assert header == ["sha256_base64", "base64", "escaped"], header
            for line in csv_reader:
                (base64_sha256_name, base64_name, escaped_name) = line
                sha256_name = base64.b64decode(base64_sha256_name)
                name = base64.b64decode(base64_name)
                sha256_to_names[sha256_name] = (name, escaped_name)

        # Combine with the list of sha256(name), to get the list of base64(name)
        # and escape(name)
        persons_path = self.local_graph_path / f"{self.graph_name}.persons.csv.zst"
        with pyzstd.open(persons_path, "rb") as fd:
            person_id_to_names: List[Tuple[bytes, str]] = [
                sha256_to_names.pop(base64.b64decode(line.strip()), (b"", ""))
                for line in fd
            ]

        # Read the set of person ids from the main table
        person_ids = set()
        with pyzstd.open(self.origin_contributors_path, "rt") as input_fd:
            # TODO: remove that cast once we dropped Python 3.7 support
            csv_reader = csv.reader(cast(Iterable[str], input_fd))
            header = next(csv_reader)
            assert header == ["origin_id", "contributor_id"], header
            for (origin_id, person_id_str) in csv_reader:
                if person_id_str == "null":
                    # FIXME: workaround for a bug in contribution graphs generated
                    # before 2022-12-01. Those were only used in tests and never
                    # published, so the conditional can be removed when this is
                    # productionized
                    continue
                person_ids.add(int(person_id_str))

        # Finally, write a new table of all persons.
        tmp_output_path = Path(f"{self.deanonymized_origin_contributors_path}.tmp")
        tmp_output_path.parent.mkdir(parents=True, exist_ok=True)
        with pyzstd.open(tmp_output_path, "wt") as output_fd:
            csv_writer = csv.writer(output_fd, lineterminator="\r\n")
            # write header
            csv_writer.writerow(
                ("contributor_id", "contributor_base64", "contributor_escaped")
            )

            for person_id in sorted(person_ids):
                (name, escaped_name) = person_id_to_names[person_id]
                base64_name = base64.b64encode(name).decode("ascii")
                csv_writer.writerow((person_id, base64_name, escaped_name))

        tmp_output_path.replace(self.deanonymized_origin_contributors_path)
