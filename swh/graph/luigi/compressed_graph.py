# Copyright (C) 2022 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""
Luigi tasks for compression
===========================

This module contains `Luigi <https://luigi.readthedocs.io/>`_ tasks,
as an alternative to the CLI that can be composed with other tasks,
such as swh-dataset's.

It implements the task DAG described in :ref:`swh-graph-compression-steps`.

Unlike the CLI, this requires the graph to be named `graph`.

Filtering
---------

The ``object_types`` parameter (``--object-types`` on the CLI) specifies
the set of node types to read from the dataset export, and it defaults to
all types: ``ori,snp,rel,rev,dir,cnt``.

Because the dataset export is keyed by edge sources, some objects
without any of these types will be present in the input dataset. For example,
if exporting ``ori,snp,rel,rev``, root Directory of every release and revision
will be present, though without its labels (as well as a few Content objects
pointed by some Releases).

File layout
-----------

In addition to files documented in :ref:`graph-compression` (eg. :file:`graph.graph`,
:file:`graph.mph`, ...), tasks in this module produce this directory structure::

    base_dir/
        <date>[_<flavor>]/
            compressed/
                graph.graph
                graph.mph
                ...
                meta/
                    export.json
                    compression.json

``graph.meta/export.json`` is copied from the ORC dataset exported by
:mod:`swh.dataset.luigi`.

``graph.meta/compression.json``  contains information about the compression itself,
for provenance tracking.
For example:

.. code-block:: json

    [
        {
            "steps": null,
            "export_start": "2022-11-08T11:00:54.998799+00:00",
            "export_end": "2022-11-08T11:05:53.105519+00:00",
            "object_types": [
                "origin",
                "origin_visit"
            ],
            "hostname": "desktop5",
            "conf": {},
            "tool": {
                "name": "swh.graph",
                "version": "2.2.0"
            }
        }
    ]

When the compression pipeline is run in separate steps, each of the steps is recorded
as an object in the root list.

S3 layout
---------

As ``.bin`` files are meant to be accessed randomly, they are uncompressed on disk.
However, this is undesirable on at-rest/long-term storage like on S3, because
some are very sparse (eg. :file:`graph.property.committer_timestamp.bin` can be
quickly compressed from 300 to 1GB).

Therefore, these files are compressed to ``.bin.zst``, and need to be decompressed
when downloading.

The layout is otherwise the same as the file layout.
"""

import collections
import itertools
import math
from pathlib import Path
from typing import Dict, List, Set

# WARNING: do not import unnecessary things here to keep cli startup time under
# control
import luigi

from swh.dataset.luigi import Format, LocalExport
from swh.dataset.luigi import ObjectType as Table
from swh.dataset.luigi import S3PathParameter
from swh.graph.webgraph import CompressionStep, do_step

_LOW_XMX = 128_000_000
"""Arbitrary value that should work as -Xmx for Java processes which don't need
much memory and as spare for processes which do"""


def _govmph_bitarray_size(nb_nodes: int) -> int:
    """Returns the size of the bitarray used by
    it.unimi.dsi.sux4j.mph.GOVMinimalPerfectHashFunction depending on the number of
    nodes
    """
    # https://github.com/vigna/Sux4J/blob/e9fd7412204272a2796e3038e95beb1d8cbc244a/src/it/unimi/dsi/sux4j/mph/GOVMinimalPerfectHashFunction.java#L157-L160
    c_times_256 = math.floor((1.09 + 0.01) * 256)

    # https://github.com/vigna/Sux4J/blob/e9fd7412204272a2796e3038e95beb1d8cbc244a/src/it/unimi/dsi/sux4j/mph/GOVMinimalPerfectHashFunction.java#L355
    return int(2 * (1 + (nb_nodes * c_times_256 >> 8)) / 8)


# This mirrors the long switch statement in
# java/src/main/java/org/softwareheritage/graph/compress/ORCGraphDataset.java
# but maps only to the main tables (see swh/dataset/relational.py)
#
# Note that swh-dataset's "object type" (which we refer to as "table" in the module
# to avoid confusion) corresponds to a "main table" of a relational DB, eg.
# "directory" or "origin_visit", but not relational table like "directory_entries".
#
# In swh-graph, "object type" is cnt,dir,rev,rel,snp,ori; which roughly matches
# main tables, but with some mashed together.
_TABLES_PER_OBJECT_TYPE = {
    "cnt": [Table.skipped_content, Table.content],  # type: ignore[attr-defined]
    "dir": [Table.directory],  # type: ignore[attr-defined]
    "rev": [Table.revision],  # type: ignore[attr-defined]
    "rel": [Table.release],  # type: ignore[attr-defined]
    "snp": [Table.snapshot],  # type: ignore[attr-defined]
    "ori": [
        Table.origin,  # type: ignore[attr-defined]
        Table.origin_visit,  # type: ignore[attr-defined]
        Table.origin_visit_status,  # type: ignore[attr-defined]
    ],
}

assert set(itertools.chain.from_iterable(_TABLES_PER_OBJECT_TYPE.values())) == set(
    Table
)


def _tables_for_object_types(object_types: List[str]) -> List[Table]:
    """Returns the list of ORC tables required to produce a compressed graph with
    the given object types."""
    tables = []
    for object_type in object_types:
        tables.extend(_TABLES_PER_OBJECT_TYPE[object_type])
    return tables


class ObjectTypesParameter(luigi.Parameter):
    """A parameter type whose value is either ``*`` or a set of comma-separated
    object types (eg. ``ori,snp,rel,rev,dir,cnt``).
    """

    def __init__(self, *args, **kwargs):
        """"""
        # Only present to override the docstring defined by Luigi; which
        # contains a reference to batch_method, which doesn't exist in SWH's doc
        super().__init__(*args, **kwargs)

    def parse(self, s: str) -> List[str]:
        if s == "*":
            return self.parse(",".join(_TABLES_PER_OBJECT_TYPE))
        else:
            types = s.split(",")
            invalid_types = set(types) - set(_TABLES_PER_OBJECT_TYPE)
            if invalid_types:
                raise ValueError(f"Invalid object types: {invalid_types!r}")
            return types

    def serialize(self, value: List[str]) -> str:
        return ",".join(value)


class _CompressionStepTask(luigi.Task):
    STEP: CompressionStep

    INPUT_FILES: Set[str]
    """Dependencies of this step."""

    OUTPUT_FILES: Set[str]
    """List of files which this task produces, without the graph name as prefix.
    """

    EXPORT_AS_INPUT: bool = False
    """Whether this task should depend directly on :class:`LocalExport`.
    If not, it is assumed it depends transitiviely via one of the
    :attr:`PREVIOUS_STEPS`.
    """

    local_export_path = luigi.PathParameter(significant=False)
    graph_name = luigi.Parameter(default="graph")
    local_graph_path = luigi.PathParameter()

    # TODO: Only add this parameter to tasks that use it
    batch_size = luigi.IntParameter(
        default=0,
        significant=False,
        description="""
        Size of work batches to use while compressing.
        Larger is faster, but consumes more resources.
        """,
    )

    object_types = ObjectTypesParameter()

    def _get_count(self, count_name: str, task_name: str) -> int:
        count_path = self.local_graph_path / f"{self.graph_name}.{count_name}.count.txt"
        if not count_path.exists():
            # ExtractNodes didn't run yet so we do not know how many there are.
            # As it means this task cannot run right now anyway, pretend there are so
            # many nodes the task wouldn't fit in memory
            print(
                f"warning: {self.__class__.__name__}._nb_{count_name} called but "
                f"{task_name} did not run yet"
            )
            return 10**100
        return int(count_path.read_text())

    def _nb_nodes(self) -> int:
        return self._get_count("nodes", "ExtractNodes")

    def _nb_edges(self) -> int:
        return self._get_count("edges", "ExtractNodes")

    def _nb_labels(self) -> int:
        return self._get_count("labels", "ExtractNodes")

    def _nb_persons(self) -> int:
        return self._get_count("persons", "ExtractPersons")

    def _bvgraph_allocation(self):
        """Returns the memory needed to load the input .graph of this task."""
        # In reverse order of their generationg (and therefore size)
        suffixes = [
            "-transposed.graph",
            ".graph",
            "-bfs-simplified.graph",
            "-bfs.graph",
            "-base.graph",
        ]
        suffixes_in_input = set(suffixes) & self.INPUT_FILES
        try:
            (suffix_in_input,) = suffixes_in_input
        except ValueError:
            raise ValueError(
                f"Expected {self.__class__.__name__} to have exactly one graph as "
                f"input, got: {suffixes_in_input}"
            ) from None

        graph_path = self.local_graph_path / f"{self.graph_name}{suffix_in_input}"
        if graph_path.exists():
            graph_size = graph_path.stat().st_size
        else:
            # This graph wasn't generated yet.
            # As it means this task cannot run right now anyway, assume it is the
            # worst case: it's the same size as the previous graph.
            for suffix in suffixes[suffixes.index(suffix_in_input) :]:
                graph_path = self.local_graph_path / f"{self.graph_name}{suffix}"
                if graph_path.exists():
                    graph_size = graph_path.stat().st_size
                    break
            else:
                # No graph was generated yet, so we have to assume the worst
                # possible compression ratio (a whole long int for each edge in
                # the adjacency lists)
                graph_size = 8 * self._nb_edges()

        bitvector_size = self._nb_nodes() / 8
        # https://github.com/vigna/fastutil/blob/master/src/it/unimi/dsi/fastutil/io/FastMultiByteArrayInputStream.java
        fis_size = 1 << 30

        fis_size += graph_size

        # https://github.com/vigna/webgraph-big/blob/3.7.0/src/it/unimi/dsi/big/webgraph/BVGraph.java#L1443
        # https://github.com/vigna/webgraph-big/blob/3.7.0/src/it/unimi/dsi/big/webgraph/BVGraph.java#L1503
        offsets_size = self._nb_nodes() * 8

        return int(bitvector_size + fis_size + offsets_size)

    def _mph_size(self):
        # The 2022-12-07 export had 27 billion nodes and its .mph weighted 7.2GB.
        # (so, about 0.25 bytes per node)
        # We can reasonably assume it won't ever take more than 2 bytes per node.
        return self._nb_nodes() * 2

    def _persons_mph_size(self):
        # ditto, but there were 3 billion labels and .mph was 8GB
        # (about 2.6 bytes per node)
        return self._nb_labels() * 8

    def _labels_mph_size(self):
        # ditto, but there were 3 billion labels and .mph was 8GB
        # (about 2.6 bytes per node)
        return self._nb_labels() * 8

    def _large_java_allocations(self) -> int:
        """Returns the value to set as the JVM's -Xmx parameter, in bytes"""
        raise NotImplementedError(f"{self.__class__.__name__}._large_java_allocations")

    def _expected_memory(self) -> int:
        """Returns the expected total memory usage of this task, in bytes."""
        extra_memory_coef = 1.1  # accounts for the JVM using more than -Xmx
        return int((self._large_java_allocations() + _LOW_XMX) * extra_memory_coef)

    @property
    def resources(self) -> Dict[str, int]:
        import socket

        hostname = socket.getfqdn()
        d = {
            f"{hostname}_ram_mb": self._expected_memory() // (1024 * 1024),
        }
        return d

    def _stamp(self) -> Path:
        """Returns the path of this tasks's stamp file"""
        return self.local_graph_path / "meta" / "stamps" / f"{self.STEP}.json"

    def complete(self) -> bool:
        """Returns whether the files are written AND this task's stamp is present in
        :file:`meta/compression.json`"""
        import json

        if not super().complete():
            return False

        for output in self.output():
            path = Path(output.path)
            if not path.exists():
                raise Exception(f"expected output {path} does not exist")
            if not path.is_file():
                raise Exception(f"expected output {path} is not a file")
            if path.stat().st_size == 0:
                if (
                    path.name.endswith(
                        (".labels.fcl.bytearray", ".labels.fcl.pointers")
                    )
                    and "dir" not in self.object_types
                    and "snp" not in self.object_types
                ):
                    # It's expected that .labels.fcl.bytearray is empty when both dir
                    # and snp are excluded, because these are the only objects
                    # with labels on their edges.
                    continue
                raise Exception(f"expected output {path} is empty")

        if not self._stamp().is_file():
            with self._stamp().open() as fd:
                json.load(fd)  # Check it was fully written

        return True

    def requires(self) -> List[luigi.Task]:
        """Returns a list of luigi tasks matching :attr:`PREVIOUS_STEPS`."""
        requirements_d = {}
        for input_file in self.INPUT_FILES:
            for cls in _CompressionStepTask.__subclasses__():
                if input_file in cls.OUTPUT_FILES:
                    kwargs = dict(
                        local_export_path=self.local_export_path,
                        graph_name=self.graph_name,
                        local_graph_path=self.local_graph_path,
                        object_types=self.object_types,
                    )
                    if self.batch_size:
                        kwargs["batch_size"] = self.batch_size
                    requirements_d[cls.STEP] = cls(**kwargs)
                    break
            else:
                assert False, f"Found no task outputting file '{input_file}'."

        requirements = list(requirements_d.values())

        if self.EXPORT_AS_INPUT:
            requirements.append(
                LocalExport(
                    local_export_path=self.local_export_path,
                    formats=[Format.orc],  # type: ignore[attr-defined]
                    object_types=_tables_for_object_types(self.object_types),
                )
            )

        return requirements

    def output(self) -> List[luigi.LocalTarget]:
        """Returns a list of luigi targets matching :attr:`OUTPUT_FILES`."""
        return [luigi.LocalTarget(self._stamp())] + [
            luigi.LocalTarget(f"{self.local_graph_path / self.graph_name}{name}")
            for name in self.OUTPUT_FILES
        ]

    def run(self) -> None:
        """Runs the step, by shelling out to the relevant Java program"""
        import datetime
        import json
        import socket
        import time

        import pkg_resources

        from swh.graph.config import check_config_compress

        for input_file in self.INPUT_FILES:
            path = self.local_graph_path / f"{self.graph_name}{input_file}"
            if not path.exists():
                raise Exception(f"expected input {path} does not exist")
            if not path.is_file():
                raise Exception(f"expected input {path} is not a file")
            if path.stat().st_size == 0:
                raise Exception(f"expected input {path} is empty")

        if self.EXPORT_AS_INPUT:
            export_meta = json.loads(
                (self.local_export_path / "meta/export.json").read_text()
            )
            expected_tables = {
                table.name for table in _tables_for_object_types(self.object_types)
            }
            missing_tables = expected_tables - set(export_meta["object_types"])
            if missing_tables:
                raise Exception(
                    f"Want to compress graph with object types {self.object_types!r} "
                    f"(meaning tables {expected_tables!r} are needed) "
                    f"but export has only tables {export_meta['object_types']!r}. "
                    f"Missing tables: {missing_tables!r}"
                )

        conf = {
            "object_types": ",".join(self.object_types),
            "max_ram": f"{(self._large_java_allocations() + _LOW_XMX)//(1024*1024)}M",
            # TODO: make this more configurable
        }
        if self.batch_size:
            conf["batch_size"] = self.batch_size

        conf = check_config_compress(
            conf,
            graph_name=self.graph_name,
            in_dir=self.local_export_path / "orc",
            out_dir=self.local_graph_path,
        )

        start_date = datetime.datetime.now(tz=datetime.timezone.utc)
        start_time = time.monotonic()
        do_step(step=self.STEP, conf=conf)
        end_time = time.monotonic()
        end_date = datetime.datetime.now(tz=datetime.timezone.utc)

        stamp_content = {
            "start": start_date.isoformat(),
            "end": end_date.isoformat(),
            "runtime": end_time - start_time,
            "hostname": socket.getfqdn(),
            "object_types": self.object_types,
            "conf": conf,
            "tool": {
                "name": "swh.graph",
                "version": pkg_resources.get_distribution("swh.graph").version,
            },
        }
        self._stamp().parent.mkdir(parents=True, exist_ok=True)
        with self._stamp().open("w") as fd:
            json.dump(stamp_content, fd, indent=4)


class ExtractNodes(_CompressionStepTask):
    STEP = CompressionStep.EXTRACT_NODES
    EXPORT_AS_INPUT = True
    INPUT_FILES: Set[str] = set()
    OUTPUT_FILES = {
        ".labels.csv.zst",
        ".nodes.csv.zst",
        ".edges.count.txt",
        ".edges.stats.txt",
        ".nodes.count.txt",
        ".nodes.stats.txt",
        ".labels.count.txt",
    }

    def _large_java_allocations(self) -> int:
        import multiprocessing

        # Memory usage is mostly in subprocesses; the JVM itself needs only to read
        # ORC files
        # 128MB is enough in practice, but let's play it safe. ExtractNodes can't
        # run in parallel with anything else anyway, so we have plenty of available RAM
        # to avoid stressing the GC
        orc_buffers_size = 256_000_000

        nb_orc_readers = multiprocessing.cpu_count()

        return orc_buffers_size * nb_orc_readers


class Mph(_CompressionStepTask):
    STEP = CompressionStep.MPH
    INPUT_FILES = {".nodes.csv.zst"}
    OUTPUT_FILES = {".mph"}

    def _large_java_allocations(self) -> int:
        bitvector_size = _govmph_bitarray_size(self._nb_nodes())
        extra_size = self._nb_nodes()  # TODO: why is this needed?
        return bitvector_size + extra_size


class Bv(_CompressionStepTask):
    STEP = CompressionStep.BV
    EXPORT_AS_INPUT = True
    INPUT_FILES = {".mph"}
    OUTPUT_FILES = {"-base.graph"}

    def _large_java_allocations(self) -> int:
        import psutil

        # TODO: deduplicate this formula with the one for DEFAULT_BATCH_SIZE in
        # ScatteredArcsORCGraph.java
        batch_size = psutil.virtual_memory().total * 0.4 / (8 * 2)
        return int(self._mph_size() + batch_size)


class Bfs(_CompressionStepTask):
    STEP = CompressionStep.BFS
    INPUT_FILES = {"-base.graph"}
    OUTPUT_FILES = {"-bfs.order"}

    def _large_java_allocations(self) -> int:
        bvgraph_size = self._bvgraph_allocation()
        visitorder_size = self._nb_nodes() * 8  # longarray in BFS.java
        extra_size = 500_000_000  # TODO: why is this needed?
        return bvgraph_size + visitorder_size + extra_size


class PermuteBfs(_CompressionStepTask):
    STEP = CompressionStep.PERMUTE_BFS
    INPUT_FILES = {"-base.graph", "-bfs.order"}
    OUTPUT_FILES = {"-bfs.graph"}

    def _large_java_allocations(self) -> int:
        bvgraph_size = self._bvgraph_allocation()

        # https://github.com/vigna/webgraph-big/blob/3.7.0/src/it/unimi/dsi/big/webgraph/Transform.java#L2196
        permutation_size = self._nb_nodes() * 8

        # https://github.com/vigna/webgraph-big/blob/3.7.0/src/it/unimi/dsi/big/webgraph/Transform.java#L2064
        # TODO: should we pass self.batch_size to the CLI instead?
        batch_size = 1000000

        # https://github.com/vigna/webgraph-big/blob/3.7.0/src/it/unimi/dsi/big/webgraph/Transform.java#L2196
        source_batch_size = target_batch_size = batch_size * 8  # longarrays

        extra_size = self._nb_nodes() * 16  # FIXME: why is this needed?
        return (
            bvgraph_size
            + permutation_size
            + source_batch_size
            + target_batch_size
            + extra_size
        )


class TransposeBfs(_CompressionStepTask):
    STEP = CompressionStep.TRANSPOSE_BFS
    INPUT_FILES = {"-bfs.graph"}
    OUTPUT_FILES = {"-bfs-transposed.graph"}

    def _large_java_allocations(self) -> int:
        from swh.graph.config import check_config

        permutation_size = self._nb_nodes() * 8  # longarray

        if self.batch_size:
            batch_size = self.batch_size
        else:
            batch_size = check_config({})["batch_size"]

        # https://github.com/vigna/webgraph-big/blob/3.7.0/src/it/unimi/dsi/big/webgraph/Transform.java#L1039
        source_batch_size = target_batch_size = start_batch_size = (
            batch_size * 8
        )  # longarrays

        return (
            permutation_size + source_batch_size + target_batch_size + start_batch_size
        )


class Simplify(_CompressionStepTask):
    STEP = CompressionStep.SIMPLIFY
    INPUT_FILES = {"-bfs.graph", "-bfs-transposed.graph"}
    OUTPUT_FILES = {"-bfs-simplified.graph"}

    def _large_java_allocations(self) -> int:
        import multiprocessing

        bvgraph_size = self._bvgraph_allocation()
        permutation_size = self._nb_nodes() * 8  # longarray
        write_buffer = 100_000_000  # approx; used by the final ImmutableGraph.store()
        return (
            bvgraph_size + permutation_size + write_buffer * multiprocessing.cpu_count()
        )


class Llp(_CompressionStepTask):
    STEP = CompressionStep.LLP
    INPUT_FILES = {"-bfs-simplified.graph"}
    OUTPUT_FILES = {"-llp.order"}

    def _large_java_allocations(self) -> int:
        # actually it loads the simplified graph, but we reuse the size of the
        # base BVGraph, to simplify this code
        bvgraph_size = self._bvgraph_allocation()

        label_array_size = volume_array_size = permutation_size = major_array_size = (
            self._nb_nodes() * 8
        )  # longarrays
        canchange_array_size = (
            self._nb_nodes() * 4
        )  # bitarray, not optimized like a bitvector
        return (
            label_array_size
            + bvgraph_size
            + volume_array_size
            + permutation_size * 3  # 'intLabel', 'major', and next value of 'major'
            + major_array_size
            + canchange_array_size
        )


class PermuteLlp(_CompressionStepTask):
    STEP = CompressionStep.PERMUTE_LLP
    INPUT_FILES = {"-llp.order", "-bfs.graph"}
    OUTPUT_FILES = {".graph", ".offsets", ".properties"}

    def _large_java_allocations(self) -> int:
        from swh.graph.config import check_config

        # TODO: deduplicate this with PermuteBfs; it runs the same code except for the
        # batch size

        if self.batch_size:
            batch_size = self.batch_size
        else:
            batch_size = check_config({})["batch_size"]

        # actually it loads the simplified graph, but we reuse the size of the
        # base BVGraph, to simplify this code
        bvgraph_size = self._bvgraph_allocation()

        # https://github.com/vigna/webgraph-big/blob/3.7.0/src/it/unimi/dsi/big/webgraph/Transform.java#L2196
        permutation_size = self._nb_nodes() * 8

        # https://github.com/vigna/webgraph-big/blob/3.7.0/src/it/unimi/dsi/big/webgraph/Transform.java#L2196
        source_batch_size = target_batch_size = batch_size * 8  # longarrays

        extra_size = self._nb_nodes() * 16  # FIXME: why is this needed?
        return (
            bvgraph_size
            + permutation_size
            + source_batch_size
            + target_batch_size
            + extra_size
        )


class Obl(_CompressionStepTask):
    STEP = CompressionStep.OBL
    INPUT_FILES = {".graph"}
    OUTPUT_FILES = {".obl"}

    def _large_java_allocations(self) -> int:
        bvgraph_size = self._bvgraph_allocation()
        return bvgraph_size


class ComposeOrders(_CompressionStepTask):
    STEP = CompressionStep.COMPOSE_ORDERS
    INPUT_FILES = {"-llp.order", "-bfs.order"}
    OUTPUT_FILES = {".order"}

    def _large_java_allocations(self) -> int:
        permutation_size = self._nb_nodes() * 8  # longarray
        return permutation_size * 3


class Stats(_CompressionStepTask):
    STEP = CompressionStep.STATS
    INPUT_FILES = {".graph"}
    OUTPUT_FILES = {".stats"}

    def _large_java_allocations(self) -> int:
        indegree_array_size = self._nb_nodes() * 8  # longarray
        bvgraph_size = self._bvgraph_allocation()
        return indegree_array_size + bvgraph_size


class Transpose(_CompressionStepTask):
    STEP = CompressionStep.TRANSPOSE
    # .obl is an optional input; but we need to make sure it's not being written
    # while Transpose is starting, or Transpose would error with EOF while reading it
    INPUT_FILES = {".graph", ".obl"}
    OUTPUT_FILES = {"-transposed.graph", "-transposed.properties"}

    def _large_java_allocations(self) -> int:
        from swh.graph.config import check_config

        if self.batch_size:
            batch_size = self.batch_size
        else:
            batch_size = check_config({})["batch_size"]

        permutation_size = self._nb_nodes() * 8  # longarray
        source_batch_size = target_batch_size = start_batch_size = (
            batch_size * 8
        )  # longarrays
        return (
            permutation_size + source_batch_size + target_batch_size + start_batch_size
        )


class TransposeObl(_CompressionStepTask):
    STEP = CompressionStep.TRANSPOSE_OBL
    INPUT_FILES = {"-transposed.graph"}
    OUTPUT_FILES = {"-transposed.obl"}

    def _large_java_allocations(self) -> int:
        bvgraph_size = self._bvgraph_allocation()
        return bvgraph_size


class Maps(_CompressionStepTask):
    STEP = CompressionStep.MAPS
    INPUT_FILES = {".mph", ".order", ".nodes.csv.zst"}
    OUTPUT_FILES = {".node2swhid.bin"}

    def _large_java_allocations(self) -> int:
        mph_size = self._mph_size()

        bfsmap_size = self._nb_nodes() * 8  # longarray
        return mph_size + bfsmap_size


class ExtractPersons(_CompressionStepTask):
    STEP = CompressionStep.EXTRACT_PERSONS
    INPUT_FILES: Set[str] = set()
    EXPORT_AS_INPUT = True
    OUTPUT_FILES = {".persons.csv.zst"}

    def _large_java_allocations(self) -> int:
        return 0


class MphPersons(_CompressionStepTask):
    STEP = CompressionStep.MPH_PERSONS
    INPUT_FILES = {".persons.csv.zst"}
    OUTPUT_FILES = {".persons.mph"}

    def _large_java_allocations(self) -> int:
        bitvector_size = _govmph_bitarray_size(self._nb_persons())
        return bitvector_size


class NodeProperties(_CompressionStepTask):
    STEP = CompressionStep.NODE_PROPERTIES
    INPUT_FILES = {".order", ".mph", ".persons.mph", ".node2swhid.bin"}
    EXPORT_AS_INPUT = True
    OUTPUT_FILES = {
        f".property.{name}.bin"
        for name in (
            "author_id",
            "author_timestamp",
            "author_timestamp_offset",
            "committer_id",
            "committer_timestamp",
            "committer_timestamp_offset",
            "content.is_skipped",
            "content.length",
            "message",
            "message.offset",
            "tag_name",
            "tag_name.offset",
        )
    }

    def output(self) -> List[luigi.LocalTarget]:
        """Returns a list of luigi targets matching :attr:`OUTPUT_FILES`."""
        excluded_files = set()
        if "cnt" not in self.object_types:
            excluded_files |= {
                "content.is_skipped",
                "content.length",
            }
        if "rev" not in self.object_types and "rel" not in self.object_types:
            excluded_files |= {
                "author_id",
                "author_timestamp",
                "author_timestamp_offset",
                "message",
                "message.offset",
            }
        if "rel" not in self.object_types:
            excluded_files |= {
                "tag_name",
                "tag_name.offset",
            }
        if "rev" not in self.object_types:
            excluded_files |= {
                "committer_id",
                "committer_timestamp",
                "committer_timestamp_offset",
            }

        excluded_files = {f".property.{name}.bin" for name in excluded_files}

        return [luigi.LocalTarget(self._stamp())] + [
            luigi.LocalTarget(f"{self.local_graph_path / self.graph_name}{name}")
            for name in self.OUTPUT_FILES - excluded_files
        ]

    def _large_java_allocations(self) -> int:
        # each property has its own arrays, but they don't run at the same time.
        # The biggest are:
        # * content length/writeMessages/writeTagNames (long array)
        # * writeTimestamps (long array + short array)
        # * writePersonIds (int array, but also loads the person MPH)
        subtask_size = max(
            self._nb_nodes() * (8 + 2),  # writeTimestamps
            self._nb_nodes() * 4 + self._persons_mph_size(),
        )
        return self._mph_size() + self._persons_mph_size() + subtask_size


class MphLabels(_CompressionStepTask):
    STEP = CompressionStep.MPH_LABELS
    INPUT_FILES = {".labels.csv.zst"}
    OUTPUT_FILES = {".labels.mph"}

    def _large_java_allocations(self) -> int:
        import multiprocessing

        # TODO: compute memory_per_thread dynamically
        memory_per_thread = 4_000_000

        # https://github.com/vigna/Sux4J/blob/e9fd7412204272a2796e3038e95beb1d8cbc244a/src/it/unimi/dsi/sux4j/mph/GOV3Function.java#L425
        bucket_size = 1500

        # https://github.com/vigna/Sux4J/blob/e9fd7412204272a2796e3038e95beb1d8cbc244a/src/it/unimi/dsi/sux4j/mph/GOV3Function.java#L514
        num_buckets = int(self._nb_nodes() / bucket_size) + 1

        # https://github.com/vigna/Sux4J/blob/e9fd7412204272a2796e3038e95beb1d8cbc244a/src/it/unimi/dsi/sux4j/mph/GOV3Function.java#L519
        offsets_and_seeds_size = num_buckets * 8

        # why is this needed?
        extra_size = self._nb_nodes()

        return (
            memory_per_thread * multiprocessing.cpu_count()
            + offsets_and_seeds_size
            + extra_size
        )


class FclLabels(_CompressionStepTask):
    STEP = CompressionStep.FCL_LABELS
    INPUT_FILES = {".labels.csv.zst", ".labels.mph"}
    OUTPUT_FILES = {
        ".labels.fcl.bytearray",
        ".labels.fcl.pointers",
        ".labels.fcl.properties",
    }

    def _large_java_allocations(self) -> int:
        return self._labels_mph_size()


class EdgeLabels(_CompressionStepTask):
    STEP = CompressionStep.EDGE_LABELS
    INPUT_FILES = {
        ".labels.mph",
        ".mph",
        ".graph",
        ".order",
        ".node2swhid.bin",
        ".properties",
        "-transposed.properties",
    }
    EXPORT_AS_INPUT = True
    OUTPUT_FILES = {
        "-labelled.labeloffsets",
        "-labelled.labels",
        "-labelled.properties",
        "-transposed-labelled.labeloffsets",
        "-transposed-labelled.labels",
        "-transposed-labelled.properties",
    }

    def _large_java_allocations(self) -> int:
        import multiprocessing

        # See ExtractNodes._large_java_allocations for this constant
        orc_buffers_size = 256_000_000

        nb_orc_readers = multiprocessing.cpu_count()

        return (
            orc_buffers_size * nb_orc_readers
            + self._mph_size()
            + self._labels_mph_size()
        )


class EdgeLabelsObl(_CompressionStepTask):
    STEP = CompressionStep.EDGE_LABELS_OBL
    INPUT_FILES = {
        "-labelled.labeloffsets",
        "-labelled.labels",
        "-labelled.properties",
    }
    EXPORT_AS_INPUT = True
    OUTPUT_FILES = {
        "-labelled.labelobl",
    }

    def _large_java_allocations(self) -> int:
        # "an element occupies a number of bits bounded by two plus the logarithm of
        # the average gap."
        # https://sux4j.di.unimi.it/docs/it/unimi/dsi/sux4j/util/EliasFanoMonotoneLongBigList.html
        # The number of elements is the number of nodes; and 22 should be way over
        # the logarithm
        offsets_size = self._nb_nodes() * 24

        return offsets_size


class EdgeLabelsTransposeObl(_CompressionStepTask):
    STEP = CompressionStep.EDGE_LABELS_TRANSPOSE_OBL
    INPUT_FILES = {
        "-transposed-labelled.labeloffsets",
        "-transposed-labelled.labels",
        "-transposed-labelled.properties",
    }
    EXPORT_AS_INPUT = True
    OUTPUT_FILES = {
        "-transposed-labelled.labelobl",
    }

    def _large_java_allocations(self) -> int:
        # See EdgeLabelsObl._large_java_allocations
        offsets_size = self._nb_nodes() * 24

        return offsets_size


_duplicate_steps = [
    step
    for (step, count) in collections.Counter(
        cls.STEP for cls in _CompressionStepTask.__subclasses__()
    ).items()
    if count != 1
]
assert not _duplicate_steps, f"Duplicate steps: {_duplicate_steps}"

_duplicate_outputs = [
    filename
    for (filename, count) in collections.Counter(
        filename
        for cls in _CompressionStepTask.__subclasses__()
        for filename in cls.OUTPUT_FILES
    ).items()
    if count != 1
]
assert not _duplicate_outputs, f"Duplicate outputs: {_duplicate_outputs}"


class CompressGraph(luigi.Task):
    local_export_path = luigi.PathParameter(significant=False)
    graph_name = luigi.Parameter(default="graph")
    local_graph_path = luigi.PathParameter()
    batch_size = luigi.IntParameter(
        default=0,
        significant=False,
        description="""
        Size of work batches to use while compressing.
        Larger is faster, but consumes more resources.
        """,
    )

    object_types = ObjectTypesParameter(default=list(_TABLES_PER_OBJECT_TYPE))

    def requires(self) -> List[luigi.Task]:
        """Returns a :class:`LocalExport` task, and leaves of the compression dependency
        graph"""
        kwargs = dict(
            local_export_path=self.local_export_path,
            graph_name=self.graph_name,
            local_graph_path=self.local_graph_path,
            object_types=self.object_types,
        )
        return [
            LocalExport(
                local_export_path=self.local_export_path,
                formats=[Format.orc],  # type: ignore[attr-defined]
                object_types=_tables_for_object_types(self.object_types),
            ),
            EdgeLabelsObl(**kwargs),
            EdgeLabelsTransposeObl(**kwargs),
            Stats(**kwargs),
            Obl(**kwargs),
            TransposeObl(**kwargs),
            Maps(**kwargs),
            NodeProperties(**kwargs),
            FclLabels(**kwargs),
        ]

    def output(self) -> List[luigi.LocalTarget]:
        """Returns the ``meta/*.json`` targets"""
        return [self._export_meta(), self._compression_meta()]

    def _export_meta(self) -> luigi.Target:
        """Returns the metadata on the dataset export"""
        return luigi.LocalTarget(self.local_graph_path / "meta/export.json")

    def _compression_meta(self) -> luigi.Target:
        """Returns the metadata on the compression pipeline"""
        return luigi.LocalTarget(self.local_graph_path / "meta/compression.json")

    def run(self):
        """Runs the full compression pipeline, then writes :file:`meta/compression.json`

        This does not support running individual steps yet."""
        import json
        import socket

        import pkg_resources

        from swh.graph.config import check_config_compress

        conf = {
            "object_types": ",".join(self.object_types),
        }

        if self.batch_size:
            conf["batch_size"] = self.batch_size

        conf = check_config_compress(
            conf,
            graph_name=self.graph_name,
            in_dir=self.local_export_path,
            out_dir=self.local_graph_path,
        )

        step_stamp_paths = []
        for step in CompressionStep:
            if step == CompressionStep.CLEAN_TMP:
                # This step is not run as its own Luigi task
                continue
            step_stamp_paths.append(
                self.local_graph_path / "meta" / "stamps" / f"{step}.json"
            )

        steps = [json.loads(path.read_text()) for path in step_stamp_paths]

        do_step(CompressionStep.CLEAN_TMP, conf=conf)

        # Copy export metadata
        with open(self._export_meta().path, "wb") as fd:
            fd.write((self.local_export_path / "meta" / "export.json").read_bytes())

        if self._compression_meta().exists():
            with self._compression_meta().open("r") as fd:
                meta = json.load(fd)
        else:
            meta = []

        meta.append(
            {
                "steps": steps,
                "start": min(step["start"] for step in steps),
                "end": max(step["end"] for step in steps),
                "total_runtime": sum(step["runtime"] for step in steps),
                "object_types": ",".join(self.object_types),
                "hostname": socket.getfqdn(),
                "conf": conf,
                "tool": {
                    "name": "swh.graph",
                    "version": pkg_resources.get_distribution("swh.graph").version,
                },
            }
        )
        with self._compression_meta().open("w") as fd:
            json.dump(meta, fd, indent=4)

        for path in step_stamp_paths:
            path.unlink()


class UploadGraphToS3(luigi.Task):
    """Uploads a local compressed graphto S3; creating automatically if it does
    not exist.

    Example invocation::

        luigi --local-scheduler --module swh.graph.luigi UploadGraphToS3 \
                --local-graph-path=graph/ \
                --s3-graph-path=s3://softwareheritage/graph/swh_2022-11-08/compressed/
    """

    local_graph_path = luigi.PathParameter(significant=False)
    s3_graph_path = S3PathParameter()
    parallelism = luigi.IntParameter(default=10, significant=False)

    def requires(self) -> List[luigi.Task]:
        """Returns a :class:`CompressGraph` task that writes local files at the
        expected location."""
        return [
            CompressGraph(
                local_graph_path=self.local_graph_path,
            )
        ]

    def output(self) -> List[luigi.Target]:
        """Returns stamp and meta paths on S3."""
        return [self._meta()]

    def _meta(self):
        import luigi.contrib.s3

        return luigi.contrib.s3.S3Target(f"{self.s3_graph_path}/meta/compression.json")

    def run(self) -> None:
        """Copies all files: first the graph itself, then :file:`meta/compression.json`."""
        import multiprocessing.dummy

        import luigi.contrib.s3
        import tqdm

        compression_metadata_path = self.local_graph_path / "meta" / "compression.json"
        seen_compression_metadata = False

        # recursively copy local files to S3, and end with compression metadata
        paths = []

        for path in self.local_graph_path.glob("**/*"):
            if path == compression_metadata_path:
                # Write it last
                seen_compression_metadata = True
                continue
            if path.is_dir():
                continue

            paths.append(path)

        assert (
            seen_compression_metadata
        ), "did not see meta/compression.json in directory listing"

        self.__status_messages: Dict[Path, str] = {}

        client = luigi.contrib.s3.S3Client()

        with multiprocessing.dummy.Pool(self.parallelism) as p:
            for (i, relative_path) in tqdm.tqdm(
                enumerate(p.imap_unordered(self._upload_file, paths)),
                total=len(paths),
                desc="Uploading compressed graph",
            ):
                self.set_progress_percentage(int(i * 100 / len(paths)))
                self.set_status_message("\n".join(self.__status_messages.values()))

            # Write it last, to act as a stamp
            client.put(
                compression_metadata_path,
                self._meta().path,
                ACL="public-read",
            )

    def _upload_file(self, path):
        import subprocess
        import tempfile

        import luigi.contrib.s3

        client = luigi.contrib.s3.S3Client()

        relative_path = path.relative_to(self.local_graph_path)
        if path.suffix == ".bin":
            # Large sparse file; store it compressed on S3.
            with tempfile.NamedTemporaryFile(prefix=path.stem, suffix=".bin.zst") as fd:
                self.__status_messages[path] = f"Compressing {relative_path}"
                subprocess.run(
                    ["zstdmt", "--force", "--keep", path, "-o", fd.name], check=True
                )
                self.__status_messages[path] = f"Uploading {relative_path} (compressed)"
                client.put_multipart(
                    fd.name,
                    f"{self.s3_graph_path}/{relative_path}.zst",
                    ACL="public-read",
                )
        else:
            self.__status_messages[path] = f"Uploading {relative_path}"
            client.put_multipart(
                path, f"{self.s3_graph_path}/{relative_path}", ACL="public-read"
            )

        del self.__status_messages[path]

        return relative_path


class DownloadGraphFromS3(luigi.Task):
    """Downloads a local dataset graph from S3.

    This performs the inverse operation of :class:`UploadGraphToS3`

    Example invocation::

        luigi --local-scheduler --module swh.graph.luigi DownloadGraphFromS3 \
                --local-graph-path=graph/ \
                --s3-graph-path=s3://softwareheritage/graph/swh_2022-11-08/compressed/
    """

    local_graph_path = luigi.PathParameter()
    s3_graph_path = S3PathParameter(significant=False)

    def requires(self) -> List[luigi.Task]:
        """Returns a :class:`UploadGraphToS3` task that writes local files to S3."""
        return [
            UploadGraphToS3(
                local_graph_path=self.local_graph_path,
                s3_graph_path=self.s3_graph_path,
            )
        ]

    def output(self) -> List[luigi.Target]:
        """Returns stamp and meta paths on the local filesystem."""
        return [self._meta()]

    def _meta(self):
        return luigi.LocalTarget(self.local_graph_path / "meta" / "export.json")

    def run(self) -> None:
        """Copies all files: first the graph itself, then :file:`meta/compression.json`."""
        import subprocess
        import tempfile

        import luigi.contrib.s3
        import tqdm

        client = luigi.contrib.s3.S3Client()

        compression_metadata_path = f"{self.s3_graph_path}/meta/compression.json"
        seen_compression_metadata = False

        # recursively copy local files to S3, and end with compression metadata
        files = list(client.list(self.s3_graph_path))
        for (i, file_) in tqdm.tqdm(
            list(enumerate(files)),
            desc="Downloading",
        ):
            if file_ == compression_metadata_path:
                # Will copy it last
                seen_compression_metadata = True
                continue
            self.set_progress_percentage(int(i * 100 / len(files)))
            local_path = self.local_graph_path / file_
            local_path.parent.mkdir(parents=True, exist_ok=True)
            if file_.endswith(".bin.zst"):
                # The file was compressed before uploading to S3, we need it
                # to be decompressed locally
                with tempfile.NamedTemporaryFile(
                    prefix=local_path.stem, suffix=".bin.zst"
                ) as fd:
                    self.set_status_message(f"Downloading {file_} (compressed)")
                    client.get(
                        f"{self.s3_graph_path}/{file_}",
                        fd.name,
                    )
                    self.set_status_message(f"Decompressing {file_}")
                    subprocess.run(
                        [
                            "zstdmt",
                            "--force",
                            "-d",
                            fd.name,
                            "-o",
                            str(local_path)[0:-4],
                        ],
                        check=True,
                    )
            else:
                self.set_status_message(f"Downloading {file_}")
                client.get(
                    f"{self.s3_graph_path}/{file_}",
                    str(local_path),
                )

        assert (
            seen_compression_metadata
        ), "did not see meta/compression.json in directory listing"

        # Write it last, to act as a stamp
        client.get(
            compression_metadata_path,
            self._meta().path,
        )


class LocalGraph(luigi.Task):
    """Task that depends on a local dataset being present -- either directly from
    :class:`ExportGraph` or via :class:`DownloadGraphFromS3`.
    """

    local_graph_path = luigi.PathParameter()
    compression_task_type = luigi.TaskParameter(
        default=DownloadGraphFromS3,
        significant=False,
        description="""The task used to get the compressed graph if it is not present.
        Should be either ``swh.graph.luigi.CompressGraph`` or
        ``swh.graph.luigi.DownloadGraphFromS3``.""",
    )

    def requires(self) -> List[luigi.Task]:
        """Returns an instance of either :class:`CompressGraph` or
        :class:`DownloadGraphFromS3` depending on the value of
        :attr:`compression_task_type`."""

        if issubclass(self.compression_task_type, CompressGraph):
            return [
                CompressGraph(
                    local_graph_path=self.local_graph_path,
                )
            ]
        elif issubclass(self.compression_task_type, DownloadGraphFromS3):
            return [
                DownloadGraphFromS3(
                    local_graph_path=self.local_graph_path,
                )
            ]
        else:
            raise ValueError(
                f"Unexpected compression_task_type: "
                f"{self.compression_task_type.__name__}"
            )

    def output(self) -> List[luigi.Target]:
        """Returns stamp and meta paths on the local filesystem."""
        return [self._meta()]

    def _meta(self):
        return luigi.LocalTarget(self.local_graph_path / "meta" / "compression.json")
