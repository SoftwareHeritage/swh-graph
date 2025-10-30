# Copyright (C) 2022-2023 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

r"""
Luigi tasks for compression
===========================

This module contains `Luigi <https://luigi.readthedocs.io/>`_ tasks,
as an alternative to the CLI that can be composed with other tasks,
such as swh-export's.

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
:mod:`swh.export.luigi`.

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
            },
            "commands": [
                {
                    "command": [
                        "bash",
                        "-c",
                        "java it.unimi.dsi.big.webgraph.labelling.BitStreamArcLabelledImmutableGraph --list ..."
                    ],
                    "cgroup": "/sys/fs/cgroup/user.slice/user-1002.slice/user@1002.service/app.slice/swh.graph@103038/bash@0",
                    "cgroup_stats": {
                        "memory.events": "low 0\nhigh 0\nmax 0\noom 0\noom_kill 0\noom_group_kill 0",
                        "memory.events.local": "low 0\nhigh 0\nmax 0\noom 0\noom_kill 0\noom_group_kill 0",
                        "memory.swap.current": "0",
                        "memory.zswap.current": "0",
                        "memory.swap.events": "high 0\nmax 0\nfail 0",
                        "cpu.stat": "usage_usec 531350\nuser_usec 424286\nsystem_usec 107063\n...",
                        "memory.current": "614400",
                        "memory.stat": "anon 0\nfile 110592\nkernel 176128\nkernel_stack 0\n...",
                        "memory.numa_stat": "anon N0=0\nfile N0=110592\nkernel_stack N0=0\n...",
                        "memory.peak": "49258496"
                    }
                }
            ]
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
"""  # noqa

import collections
import itertools
import math
from pathlib import Path
from typing import Any, Dict, List, MutableSequence, Optional, Sequence, Set

# WARNING: do not import unnecessary things here to keep cli startup time under
# control
import luigi

from swh.export.luigi import ExportGraph, Format, LocalExport
from swh.export.luigi import ObjectType as Table
from swh.export.luigi import S3PathParameter
from swh.graph.webgraph import CompressionStep, do_step


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
# but maps only to the main tables (see swh/export/relational.py)
#
# Note that swh-export's "object type" (which we refer to as "table" in the module
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

    SENSITIVE_INPUT_FILES: Set[str] = set()
    """Sensitive dependencies of this step."""

    OUTPUT_FILES: Set[str]
    """List of files which this task produces, without the graph name as prefix.
    """

    SENSITIVE_OUTPUT_FILES: Set[str] = set()
    """List of sensitive files which this task produces, without the graph name as prefix.
    """

    EXPORT_AS_INPUT: bool = False
    """Whether this task should depend directly on :class:`LocalExport`.
    If not, it is assumed it depends transitiviely via one of the tasks returned
    by :meth:`requires`.
    """

    USES_ALL_CPU_THREADS: bool = False
    """:const:`True` on tasks that use all available CPU for their entire runtime.

    These tasks should be scheduled in such a way they do not run at the same time,
    because running them concurrently does not improve run time."""

    MINIMUM_OBJECT_TYPES: Set[str] = {"ori", "snp", "rel", "rev", "dir", "cnt"}
    """Which object types should trigger the task.

    The task should be triggered if and only if at least one of these
    object types is included in the export
    """

    local_export_path = luigi.PathParameter(significant=False)
    local_sensitive_export_path: Optional[Path] = luigi.OptionalPathParameter(
        default=None
    )
    graph_name = luigi.Parameter(default="graph")
    local_graph_path: Path = luigi.PathParameter()
    local_sensitive_graph_path: Optional[Path] = luigi.OptionalPathParameter(
        default=None
    )

    # TODO: Only add this parameter to tasks that use it
    batch_size = luigi.IntParameter(
        default=0,
        significant=False,
        description="""
        Size of work batches to use while compressing.
        Larger is faster, but consumes more resources.
        """,
    )

    rust_executable_dir = luigi.Parameter(
        default="./target/release/",
        significant=False,
        description="Path to the Rust executable used to manipulate the graph.",
    )

    object_types: list[str] = ObjectTypesParameter()  # type: ignore[assignment]

    check_flavor = luigi.Parameter(
        default="full",
        significant=False,
        description="Flavor for end-to-end check during compression",
    )

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
        return self._get_count("nodes", "NodeStats")

    def _nb_edges(self) -> int:
        return self._get_count("edges", "EdgeStats")

    def _nb_labels(self) -> int:
        return self._get_count("labels", "LabelStats")

    def _nb_persons(self) -> int:
        return self._get_count("persons", "PersonsStats")

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
        return self._nb_persons() * 8

    def _labels_mph_size(self):
        # ditto, but there were 3 billion labels and .mph was 8GB
        # (about 2.6 bytes per node)
        return self._nb_labels() * 8

    def _large_allocations(self) -> int:
        """Returns an upper bound of how much RAM the process's internal buffers and
        arrays are going to use."""
        raise NotImplementedError(f"{self.__class__.__name__}._large_allocations")

    def _expected_memory(self) -> int:
        """Returns the expected total memory usage of this task, in bytes."""
        base_work_memory = 1_000_000
        return int(self._large_allocations() + base_work_memory)

    @property
    def resources(self) -> Dict[str, int]:
        import socket

        hostname = socket.getfqdn()
        d = {
            f"{hostname}_ram_mb": self._expected_memory() // (1024 * 1024),
            f"{hostname}_max_cpu": int(self.USES_ALL_CPU_THREADS),
        }
        return d

    @resources.setter
    def resources(self, value):
        raise NotImplementedError("setting 'resources' attribute")

    def _stamp(self) -> Path:
        """Returns the path of this tasks's stamp file"""
        return self.local_graph_path / "meta" / "stamps" / f"{self.STEP}.json"

    def complete(self) -> bool:
        """Returns whether the files are written AND this task's stamp is present in
        :file:`meta/compression.json`"""
        import json

        if self.MINIMUM_OBJECT_TYPES.isdisjoint(set(self.object_types)):
            return True

        if not super().complete():
            return False

        for output in self.output():
            path = Path(output.path)
            if not path.exists():
                raise Exception(f"expected output {path} does not exist")
            if path.is_file():
                if path.stat().st_size == 0:
                    if path.name.endswith(
                        (
                            ".labels.fcl.bytearray",
                            ".labels.fcl.pointers",
                        )
                    ) and {"dir", "snp", "ori"}.isdisjoint(set(self.object_types)):
                        # It's expected that .labels.fcl.bytearray is empty when both dir
                        # and snp are excluded, because these are the only objects
                        # with labels on their edges.
                        continue
                    elif path.name.endswith("-bfs.roots.txt") and {"ori"}.isdisjoint(
                        set(self.object_types)
                    ):
                        continue
                    raise Exception(f"expected output file {path} is empty")
            elif path.is_dir():
                if next(path.iterdir(), None) is None:
                    raise Exception(f"expected input directory {path} is empty")
            else:
                raise Exception(f"expected output {path} is not a file or directory")

        if not self._stamp().is_file():
            with self._stamp().open() as fd:
                json.load(fd)  # Check it was fully written

        return True

    def _is_expected_output_file(self, filename: str) -> bool:
        if filename.endswith(
            (
                "-labelled.labels",
                "-labelled.ef",
                "-labelled.properties",
            )
        ):
            # These files are only generated for graphs that have labels
            return not {"dir", "snp", "ori"}.isdisjoint(set(self.object_types))
        else:
            return True

    def requires(self) -> Sequence[luigi.Task]:
        """Returns a list of luigi tasks matching :attr:`PREVIOUS_STEPS`."""
        requirements_d = {}
        for input_file in self.INPUT_FILES.union(self.SENSITIVE_INPUT_FILES):
            if not self._is_expected_output_file(input_file):
                continue
            if self.MINIMUM_OBJECT_TYPES.isdisjoint(set(self.object_types)):
                continue
            for cls in _CompressionStepTask.__subclasses__():
                if input_file in cls.OUTPUT_FILES.union(cls.SENSITIVE_OUTPUT_FILES):
                    kwargs = dict(
                        local_export_path=self.local_export_path,
                        local_sensitive_export_path=self.local_sensitive_export_path,
                        local_sensitive_graph_path=self.local_sensitive_graph_path,
                        graph_name=self.graph_name,
                        local_graph_path=self.local_graph_path,
                        object_types=self.object_types,
                        rust_executable_dir=self.rust_executable_dir,
                        check_flavor=self.check_flavor,
                    )
                    if self.batch_size:
                        kwargs["batch_size"] = self.batch_size
                    requirements_d[cls.STEP] = cls(**kwargs)
                    break
            else:
                assert False, f"Found no task outputting file '{input_file}'."

        requirements: MutableSequence[luigi.Task] = list(requirements_d.values())

        if self.EXPORT_AS_INPUT:
            requirements.append(
                LocalExport(
                    local_export_path=self.local_export_path,
                    local_sensitive_export_path=self.local_sensitive_export_path,
                    formats=[Format.orc],  # type: ignore[attr-defined]
                    object_types=_tables_for_object_types(self.object_types),
                )
            )

        return requirements

    def output(self) -> List[luigi.LocalTarget]:
        """
        Returns a list of luigi targets matching :attr:`OUTPUT_FILES` and
        :attr:`SENSITIVE_OUTPUT_FILES`.
        """
        return (
            [luigi.LocalTarget(self._stamp())]
            + [
                luigi.LocalTarget(f"{self.local_graph_path / self.graph_name}{name}")
                for name in self.OUTPUT_FILES
            ]
            + [
                luigi.LocalTarget(
                    f"{self.local_sensitive_graph_path / self.graph_name}{name}"
                )
                for name in self.SENSITIVE_OUTPUT_FILES
                if self.local_sensitive_graph_path is not None
                and self.SENSITIVE_OUTPUT_FILES is not None
            ]
        )

    def run(self) -> None:
        """Runs the step, by shelling out to the relevant Rust program"""
        import datetime
        from importlib.metadata import version
        import json
        import socket
        import time

        from swh.graph.config import check_config_compress

        if self.MINIMUM_OBJECT_TYPES.isdisjoint(set(self.object_types)):
            return

        def _check_task_files(input_files: Set[str], graph_path: Path):
            for input_file in input_files:
                if not self._is_expected_output_file(input_file):
                    continue
                path = graph_path / f"{self.graph_name}{input_file}"
                if not path.exists():
                    raise Exception(f"expected input {path} does not exist")
                if path.is_file():
                    if path.stat().st_size == 0:
                        if path.name.endswith("-bfs.roots.txt") and {"ori"}.isdisjoint(
                            set(self.object_types)
                        ):
                            continue
                        raise Exception(f"expected input file {path} is empty")
                elif path.is_dir():
                    if next(path.iterdir(), None) is None:
                        raise Exception(f"expected input directory {path} is empty")
                else:
                    raise Exception(
                        f"expected output {path} is not a file or directory"
                    )

        _check_task_files(self.INPUT_FILES, self.local_graph_path)
        if self.local_sensitive_graph_path is not None:
            _check_task_files(
                self.SENSITIVE_INPUT_FILES, self.local_sensitive_graph_path
            )

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

        conf: dict[str, Any] = {
            "object_types": ",".join(self.object_types),
            "max_ram": f"{self._large_allocations() // (1024 * 1024)}M",
            # TODO: make this more configurable
        }
        if self.batch_size:
            conf["batch_size"] = self.batch_size
        if self.STEP == CompressionStep.LLP and self.gammas:  # type: ignore[attr-defined]
            conf["llp_gammas"] = self.gammas  # type: ignore[attr-defined]
        conf["rust_executable_dir"] = self.rust_executable_dir

        conf = check_config_compress(
            conf,
            graph_name=self.graph_name,
            in_dir=self.local_export_path / "orc",
            out_dir=self.local_graph_path,
            sensitive_in_dir=self.local_sensitive_export_path,
            sensitive_out_dir=self.local_sensitive_graph_path,
            check_flavor=self.check_flavor,
        )

        start_date = datetime.datetime.now(tz=datetime.timezone.utc)
        start_time = time.monotonic()
        run_results = do_step(step=self.STEP, conf=conf)
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
                "version": version("swh.graph"),
            },
            "commands": [
                {
                    "command": res.command,
                    "cgroup": str(res.cgroup) if res.cgroup else None,
                    "cgroup_stats": res.cgroup_stats,
                }
                for res in (run_results or [])
            ],
        }
        self._stamp().parent.mkdir(parents=True, exist_ok=True)
        with self._stamp().open("w") as fd:
            json.dump(stamp_content, fd, indent=4)


class ExtractNodes(_CompressionStepTask):
    STEP = CompressionStep.EXTRACT_NODES
    EXPORT_AS_INPUT = True
    INPUT_FILES: Set[str] = set()
    OUTPUT_FILES = {
        ".nodes/",
    }
    USES_ALL_CPU_THREADS = False

    def _large_allocations(self) -> int:
        return 0


class ExtractLabels(_CompressionStepTask):
    STEP = CompressionStep.EXTRACT_LABELS
    EXPORT_AS_INPUT = True
    INPUT_FILES: Set[str] = set()
    OUTPUT_FILES = {
        ".labels.csv.zst",
    }
    USES_ALL_CPU_THREADS = False
    MINIMUM_OBJECT_TYPES = {"ori", "snp", "dir"}

    priority = -100
    """low priority, because it is not on the critical path"""

    def _large_allocations(self) -> int:
        return 0


class NodeStats(_CompressionStepTask):
    STEP = CompressionStep.NODE_STATS
    INPUT_FILES = {".nodes/"}
    OUTPUT_FILES = {
        ".nodes.count.txt",
        ".nodes.stats.txt",
    }

    priority = 100
    """high priority, to help the scheduler allocate resources"""

    def _large_allocations(self) -> int:
        return 0


class EdgeStats(_CompressionStepTask):
    STEP = CompressionStep.EDGE_STATS
    EXPORT_AS_INPUT = True
    INPUT_FILES: Set[str] = set()
    OUTPUT_FILES = {
        ".edges.count.txt",
        ".edges.stats.txt",
    }

    priority = 100
    """high priority, to help the scheduler allocate resources"""

    def _large_allocations(self) -> int:
        return 0


class LabelStats(_CompressionStepTask):
    STEP = CompressionStep.LABEL_STATS
    INPUT_FILES = {".labels.csv.zst"}
    OUTPUT_FILES = {
        ".labels.count.txt",
    }
    USES_ALL_CPU_THREADS = True
    MINIMUM_OBJECT_TYPES = {"ori", "snp", "dir"}

    priority = 100
    """high priority, to help the scheduler allocate resources"""

    def _large_allocations(self) -> int:
        return 0


class Mph(_CompressionStepTask):
    STEP = CompressionStep.MPH
    INPUT_FILES = {".nodes/", ".nodes.count.txt"}
    OUTPUT_FILES = {".pthash"}
    USES_ALL_CPU_THREADS = True

    def _large_allocations(self) -> int:
        return 0


class Bv(_CompressionStepTask):
    STEP = CompressionStep.BV
    EXPORT_AS_INPUT = True
    INPUT_FILES = {".pthash"}
    OUTPUT_FILES = {"-base.graph"}

    def _large_allocations(self) -> int:
        import psutil

        # TODO: deduplicate this formula with the one for DEFAULT_BATCH_SIZE in
        # ScatteredArcsORCGraph.java
        batch_size = psutil.virtual_memory().total * 0.4 / (8 * 2)
        return int(self._mph_size() + batch_size)


class BvEf(_CompressionStepTask):
    STEP = CompressionStep.BV_EF
    INPUT_FILES = {"-base.graph"}
    OUTPUT_FILES = {"-base.ef"}

    def _large_allocations(self) -> int:
        return 0


class BfsRoots(_CompressionStepTask):
    STEP = CompressionStep.BFS_ROOTS
    EXPORT_AS_INPUT = True
    INPUT_FILES = set()
    OUTPUT_FILES = {"-bfs.roots.txt"}

    def _large_allocations(self) -> int:
        return 0


class Bfs(_CompressionStepTask):
    STEP = CompressionStep.BFS
    INPUT_FILES = {"-base.graph", "-base.ef", "-bfs.roots.txt", ".pthash"}
    OUTPUT_FILES = {"-bfs.order"}

    def _large_allocations(self) -> int:
        bvgraph_size = self._bvgraph_allocation()
        visitorder_size = self._nb_nodes() * 8  # array of u64
        extra_size = 500_000_000  # TODO: why is this needed?
        return bvgraph_size + visitorder_size + extra_size


class PermuteAndSimplifyBfs(_CompressionStepTask):
    STEP = CompressionStep.PERMUTE_AND_SIMPLIFY_BFS
    INPUT_FILES = {"-base.graph", "-base.ef", "-bfs.order"}
    OUTPUT_FILES = {"-bfs-simplified.graph"}

    def _large_allocations(self) -> int:
        return 0


class BfsEf(_CompressionStepTask):
    STEP = CompressionStep.BFS_EF
    INPUT_FILES = {"-bfs-simplified.graph"}
    OUTPUT_FILES = {"-bfs-simplified.ef"}

    def _large_allocations(self) -> int:
        return 0


class BfsDcf(_CompressionStepTask):
    STEP = CompressionStep.BFS_DCF
    INPUT_FILES = {"-bfs-simplified.graph"}
    OUTPUT_FILES = {"-bfs-simplified.dcf"}

    def _large_allocations(self) -> int:
        return 0


class Llp(_CompressionStepTask):
    STEP = CompressionStep.LLP
    INPUT_FILES = {"-bfs-simplified.graph", "-bfs-simplified.ef", "-bfs-simplified.dcf"}
    OUTPUT_FILES = {"-llp.order"}

    gammas = luigi.Parameter(significant=False, default=None)

    def _large_allocations(self) -> int:
        # TODO: this was written for the Java implementation; update this for Rust

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
    INPUT_FILES = {".pthash.order", "-base.graph", "-base.ef"}
    OUTPUT_FILES = {".graph", ".offsets", ".properties"}

    def _large_allocations(self) -> int:
        # TODO: this was written for the Java implementation; update this for Rust

        from swh.graph.config import check_config

        # TODO: deduplicate this with PermuteBfs; it runs the same code except for the
        # batch size

        if self.batch_size:
            batch_size = self.batch_size
        else:
            batch_size = check_config({})["batch_size"]

        # https://github.com/vigna/webgraph-big/blob/3.7.0/src/it/unimi/dsi/big/webgraph/Transform.java#L2196
        permutation_size = self._nb_nodes() * 8

        # https://github.com/vigna/webgraph-big/blob/3.7.0/src/it/unimi/dsi/big/webgraph/Transform.java#L2196
        source_batch_size = target_batch_size = batch_size * 8  # longarrays

        extra_size = self._nb_nodes() * 16  # FIXME: why is this needed?
        return permutation_size + source_batch_size + target_batch_size + extra_size


class Ef(_CompressionStepTask):
    STEP = CompressionStep.EF
    INPUT_FILES = {".graph", ".offsets"}
    OUTPUT_FILES = {".ef"}

    def _large_allocations(self) -> int:
        return 0


class ComposeOrders(_CompressionStepTask):
    STEP = CompressionStep.COMPOSE_ORDERS
    INPUT_FILES = {"-llp.order", "-bfs.order"}
    OUTPUT_FILES = {".pthash.order"}

    def _large_allocations(self) -> int:
        permutation_size = self._nb_nodes() * 8  # longarray
        return permutation_size * 3


class Transpose(_CompressionStepTask):
    STEP = CompressionStep.TRANSPOSE
    # .obl is an optional input; but we need to make sure it's not being written
    # while Transpose is starting, or Transpose would error with EOF while reading it
    INPUT_FILES = {".graph", ".ef"}
    OUTPUT_FILES = {
        "-transposed.graph",
        "-transposed.offsets",
        "-transposed.properties",
    }

    def _large_allocations(self) -> int:
        from swh.graph.config import check_config

        if self.batch_size:
            batch_size = self.batch_size
        else:
            batch_size = check_config({})["batch_size"]

        permutation_size = self._nb_nodes() * 8  # array of u64
        source_batch_size = target_batch_size = start_batch_size = (
            batch_size * 8
        )  # array of u64
        return (
            permutation_size + source_batch_size + target_batch_size + start_batch_size
        )


class TransposeEf(_CompressionStepTask):
    STEP = CompressionStep.TRANSPOSE_EF
    INPUT_FILES = {"-transposed.graph", "-transposed.offsets"}
    OUTPUT_FILES = {"-transposed.ef"}

    def _large_allocations(self) -> int:
        return 0


class Maps(_CompressionStepTask):
    STEP = CompressionStep.MAPS
    INPUT_FILES = {".pthash", ".pthash.order", ".nodes/"}
    OUTPUT_FILES = {".node2swhid.bin", ".node2type.bin"}

    def _large_allocations(self) -> int:
        mph_size = self._mph_size()

        bfsmap_size = self._nb_nodes() * 8  # array of u64
        return mph_size + bfsmap_size


class ExtractPersons(_CompressionStepTask):
    STEP = CompressionStep.EXTRACT_PERSONS
    INPUT_FILES: Set[str] = set()
    EXPORT_AS_INPUT = True
    OUTPUT_FILES = {".persons.csv.zst"}
    USES_ALL_CPU_THREADS = True
    MINIMUM_OBJECT_TYPES = {"rel", "rev"}

    def _large_allocations(self) -> int:
        return 0


class PersonsStats(_CompressionStepTask):
    STEP = CompressionStep.PERSONS_STATS
    INPUT_FILES = {".persons.csv.zst"}
    OUTPUT_FILES = {".persons.count.txt"}
    MINIMUM_OBJECT_TYPES = {"rel", "rev"}

    def _large_allocations(self) -> int:
        return 0


class MphPersons(_CompressionStepTask):
    STEP = CompressionStep.MPH_PERSONS
    INPUT_FILES = {".persons.csv.zst", ".persons.count.txt"}
    OUTPUT_FILES = {".persons.pthash"}
    MINIMUM_OBJECT_TYPES = {"rel", "rev"}

    def _large_allocations(self) -> int:
        # TODO: estimate PTHash instead of GOV
        bitvector_size = _govmph_bitarray_size(self._nb_persons())
        return bitvector_size


class ExtractFullnames(_CompressionStepTask):
    STEP = CompressionStep.EXTRACT_FULLNAMES
    INPUT_FILES = {".persons.pthash"}
    EXPORT_AS_INPUT = True
    OUTPUT_FILES = set()
    SENSITIVE_OUTPUT_FILES = {".persons", ".persons.lengths"}
    MINIMUM_OBJECT_TYPES = {"rel", "rev"}

    def _large_allocations(self) -> int:
        return 0


class FullnamesEf(_CompressionStepTask):
    STEP = CompressionStep.FULLNAMES_EF
    INPUT_FILES = set()
    SENSITIVE_INPUT_FILES = {".persons", ".persons.lengths"}
    OUTPUT_FILES = set()
    SENSITIVE_OUTPUT_FILES = {".persons.ef"}
    MINIMUM_OBJECT_TYPES = {"rel", "rev"}

    def _large_allocations(self) -> int:
        return 0


class NodeProperties(_CompressionStepTask):
    STEP = CompressionStep.NODE_PROPERTIES

    @property
    def INPUT_FILES(self) -> Set[str]:  # type: ignore[override]
        if {"rel", "rev"}.isdisjoint(self.object_types):
            return {".pthash.order", ".pthash"}
        else:
            return self._INPUT_FILES

    _INPUT_FILES = {".pthash.order", ".pthash", ".persons.pthash"}

    EXPORT_AS_INPUT = True
    OUTPUT_FILES = {
        ".property.content.is_skipped.bits",
    } | {
        f".property.{name}.bin"
        for name in (
            "author_id",
            "author_timestamp",
            "author_timestamp_offset",
            "committer_id",
            "committer_timestamp",
            "committer_timestamp_offset",
            "content.length",
            "message",
            "message.offset",
            "tag_name",
            "tag_name.offset",
        )
    }
    MINIMUM_OBJECT_TYPES = {"rel", "rev", "cnt"}

    priority = 10
    """semi-high priority because it takes a very long time to run"""

    def output(self) -> List[luigi.LocalTarget]:
        """Returns a list of luigi targets matching :attr:`OUTPUT_FILES`."""
        excluded_files = set()
        if "cnt" not in self.object_types:
            excluded_files |= {
                "content.is_skipped.bits",
                "content.length.bin",
            }
        if "rev" not in self.object_types and "rel" not in self.object_types:
            excluded_files |= {
                "author_id.bin",
                "author_timestamp.bin",
                "author_timestamp_offset.bin",
                "message.bin",
                "message.offset.bin",
            }
        if "rel" not in self.object_types:
            excluded_files |= {
                "tag_name.bin",
                "tag_name.offset.bin",
            }
        if "rev" not in self.object_types:
            excluded_files |= {
                "committer_id.bin",
                "committer_timestamp.bin",
                "committer_timestamp_offset.bin",
            }

        excluded_files = {f".property.{name}" for name in excluded_files}

        return [luigi.LocalTarget(self._stamp())] + [
            luigi.LocalTarget(f"{self.local_graph_path / self.graph_name}{name}")
            for name in self.OUTPUT_FILES - excluded_files
        ]

    def _large_allocations(self) -> int:
        # each property has its own arrays, but they don't run at the same time.
        # The biggest are:
        # * content length/writeMessages/writeTagNames (long array)
        # * writeTimestamps (long array + short array)
        # * writePersonIds (int array, but also loads the person MPH)
        if {"rel", "rev"}.isdisjoint(self.object_types):
            return self._mph_size() + self._nb_nodes() * (8 + 2)
        subtask_size = max(
            self._nb_nodes() * (8 + 2),  # writeTimestamps
            self._nb_nodes() * 4 + self._persons_mph_size(),
        )
        return self._mph_size() + self._persons_mph_size() + subtask_size


class PthashLabels(_CompressionStepTask):
    STEP = CompressionStep.MPH_LABELS
    INPUT_FILES = {".labels.csv.zst", ".labels.count.txt"}
    OUTPUT_FILES = {".labels.pthash"}
    MINIMUM_OBJECT_TYPES = {"ori", "snp", "dir"}

    def _large_allocations(self) -> int:
        return 0


class LabelsOrder(_CompressionStepTask):
    STEP = CompressionStep.LABELS_ORDER
    INPUT_FILES = {".labels.csv.zst", ".labels.pthash", ".labels.count.txt"}
    OUTPUT_FILES = {".labels.pthash.order"}
    MINIMUM_OBJECT_TYPES = {"ori", "snp", "dir"}

    def _large_allocations(self) -> int:
        return 0


class FclLabels(_CompressionStepTask):
    STEP = CompressionStep.FCL_LABELS
    INPUT_FILES = {".labels.csv.zst", ".labels.count.txt"}
    OUTPUT_FILES = {
        ".labels.fcl.bytearray",
        ".labels.fcl.pointers",
        ".labels.fcl.properties",
    }
    MINIMUM_OBJECT_TYPES = {"snp", "dir"}

    def _large_allocations(self) -> int:
        return self._labels_mph_size()


class EdgeLabels(_CompressionStepTask):
    STEP = CompressionStep.EDGE_LABELS
    INPUT_FILES = {
        ".labels.pthash",
        ".labels.pthash.order",
        ".pthash",
        ".pthash.order",
    }
    EXPORT_AS_INPUT = True
    OUTPUT_FILES = {
        "-labelled.labeloffsets",
        "-labelled.labels",
        "-labelled.properties",
    }
    MINIMUM_OBJECT_TYPES = {"ori", "snp", "dir"}

    priority = 10
    """semi-high priority because it takes a long time to run"""

    def _large_allocations(self) -> int:
        import multiprocessing

        # See ExtractNodes._large_allocations for this constant
        orc_buffers_size = 256_000_000

        nb_orc_readers = multiprocessing.cpu_count()

        return (
            orc_buffers_size * nb_orc_readers
            + self._mph_size()
            + self._labels_mph_size()
        )


class EdgeLabelsTranspose(_CompressionStepTask):
    STEP = CompressionStep.EDGE_LABELS_TRANSPOSE
    INPUT_FILES = {
        ".labels.pthash",
        ".labels.pthash.order",
        ".pthash",
        ".pthash.order",
    }
    EXPORT_AS_INPUT = True
    OUTPUT_FILES = {
        "-transposed-labelled.labeloffsets",
        "-transposed-labelled.labels",
        "-transposed-labelled.properties",
    }
    MINIMUM_OBJECT_TYPES = {"ori", "snp", "dir"}

    priority = 10
    """semi-high priority because it takes a long time to run"""

    def _large_allocations(self) -> int:
        import multiprocessing

        # See ExtractNodes._large_allocations for this constant
        orc_buffers_size = 256_000_000

        nb_orc_readers = multiprocessing.cpu_count()

        return (
            orc_buffers_size * nb_orc_readers
            + self._mph_size()
            + self._labels_mph_size()
        )


class EdgeLabelsEf(_CompressionStepTask):
    STEP = CompressionStep.EDGE_LABELS_EF
    INPUT_FILES = {"-labelled.labels", "-labelled.labeloffsets"}
    OUTPUT_FILES = {"-labelled.ef"}
    MINIMUM_OBJECT_TYPES = {"ori", "snp", "dir"}

    def _large_allocations(self) -> int:
        return 0


class EdgeLabelsTransposeEf(_CompressionStepTask):
    STEP = CompressionStep.EDGE_LABELS_TRANSPOSE_EF
    INPUT_FILES = {"-transposed-labelled.labels", "-transposed-labelled.labeloffsets"}
    OUTPUT_FILES = {"-transposed-labelled.ef"}
    MINIMUM_OBJECT_TYPES = {"ori", "snp", "dir"}

    def _large_allocations(self) -> int:
        return 0


class Stats(_CompressionStepTask):
    STEP = CompressionStep.STATS
    INPUT_FILES = {".graph", ".ef", "-transposed.graph", "-transposed.ef"}
    OUTPUT_FILES = {".stats"}

    def _large_allocations(self) -> int:
        return 0


class EndToEndCheck(_CompressionStepTask):
    STEP = CompressionStep.E2E_CHECK
    INPUT_FILES = {
        ".ef",
        ".graph",
        ".node2type.bin",
        ".node2swhid.bin",
        ".properties",
        "-labelled.labels",
        "-labelled.ef",
        "-labelled.properties",
        "-transposed.graph",
        "-transposed.ef",
        "-transposed-labelled.labels",
        "-transposed-labelled.ef",
        "-transposed-labelled.properties",
        ".stats",
    }
    OUTPUT_FILES = set()

    def _large_allocations(self) -> int:
        return 0


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


def _make_dot_diagram() -> str:
    import io

    def normalize_filename(filename: str) -> str:
        return filename.replace("-", "_").replace(".", "_").replace("/", "_dir")

    def is_node_properties_file(filename: str) -> bool:
        return filename.startswith(".property.")

    s = io.StringIO()
    s.write('digraph "Compression steps" {\n')
    s.write("    node [shape = none];\n\n")
    s.write('    orc_dataset [label="ORC Graph\nDataset"];\n')

    filenames = set()
    for cls in _CompressionStepTask.__subclasses__():
        if isinstance(cls.INPUT_FILES, property):
            input_files = cls._INPUT_FILES
        else:
            input_files = cls.INPUT_FILES | cls.SENSITIVE_INPUT_FILES
        filenames.update(input_files)
        filenames.update(cls.OUTPUT_FILES | cls.SENSITIVE_OUTPUT_FILES)

    # filter out the many graph.properties.* files
    filenames = {
        filename for filename in filenames if not is_node_properties_file(filename)
    }

    # TODO: auto-generate this from CLEAN_TMP step
    temporary_filenames = {
        filename
        for filename in filenames
        if filename.startswith(("-base.", "-bfs.", "-bfs-", "-llp."))
        or filename.endswith(".csv.zst")
        or filename in (".nodes/",)
    }

    # non-temporary file nodes
    s.write('    node_properties [label="graph.properties.*"];\n')
    for filename in filenames - temporary_filenames:
        s.write(f'    {normalize_filename(filename)} [label="graph{filename}"]\n')

    # temporary file nodes
    s.write("    subgraph {\n")
    s.write("    node [fontcolor=darkgray];\n")
    for filename in temporary_filenames:
        s.write(f'    {normalize_filename(filename)} [label="graph{filename}"]\n')
    s.write("    }\n\n")

    # task nodes
    s.write("    subgraph {\n")
    s.write('        node [shape=box, fontname="Courier New"];\n')
    for cls in _CompressionStepTask.__subclasses__():
        s.write(f"        {cls.STEP};\n")
    s.write("    }\n\n")

    # cluster label-related steps together
    s.write("    subgraph cluster_labels {\n")
    s.write('        style = "dashed";\n')
    s.write('        label = "edge labels";\n')
    for filename in filenames:
        if "label" in filename:
            s.write(f"        {normalize_filename(filename)}\n")
    for cls in _CompressionStepTask.__subclasses__():
        if "label" in str(cls.STEP).lower():
            s.write(f"        {cls.STEP};\n")
    s.write("    }\n\n")

    """
    # cluster extract/stats generation steps together
    s.write("    subgraph cluster_extract_and_stats {\n")
    s.write('        style = "dashed";\n')
    s.write('        label = "extraction and counts";\n')
    for cls in _CompressionStepTask.__subclasses__():
        if str(cls.STEP).startswith("EXTRACT_") or str(cls.STEP).endswith("_STATS"):
            s.write(f"        {cls.STEP};\n")
            for filename in cls.OUTPUT_FILES:
                s.write(f"        {normalize_filename(filename)}\n")
    s.write("    }\n\n")
    """

    # cluster node ordering-related steps together
    s.write("    subgraph cluster_node_order {\n")
    s.write('        style = "dashed";\n')
    s.write('        label = "node ordering";\n')
    for cls in _CompressionStepTask.__subclasses__():
        if cls.STEP in {
            CompressionStep.MPH,
            CompressionStep.BV,
            CompressionStep.BV_EF,
            CompressionStep.LLP,
            CompressionStep.COMPOSE_ORDERS,
        } or "BFS" in str(cls.STEP):
            s.write(f"        {cls.STEP};\n")
            for filename in itertools.chain(
                cls.OUTPUT_FILES, cls.SENSITIVE_OUTPUT_FILES
            ):
                s.write(f"        {normalize_filename(filename)}\n")
    s.write("    }\n\n")

    # cluster author/committer properties generation together
    s.write("    subgraph cluster_persons {\n")
    s.write('        style = "dashed";\n')
    s.write('        label = "authors and committers";\n')
    for cls in _CompressionStepTask.__subclasses__():
        if cls.STEP in {
            CompressionStep.EXTRACT_PERSONS,
            CompressionStep.PERSONS_STATS,
            CompressionStep.MPH_PERSONS,
            CompressionStep.EXTRACT_FULLNAMES,
            CompressionStep.FULLNAMES_EF,
        }:
            s.write(f"        {cls.STEP};\n")
            for filename in itertools.chain(
                cls.OUTPUT_FILES, cls.SENSITIVE_OUTPUT_FILES
            ):
                s.write(f"        {normalize_filename(filename)}\n")
    s.write("    }\n\n")

    # cluster .graph generation steps together
    s.write("    subgraph cluster_arclists {\n")
    s.write('        style = "dashed";\n')
    s.write('        label = "arc lists";\n')
    for cls in _CompressionStepTask.__subclasses__():
        if cls.STEP in {
            CompressionStep.PERMUTE_LLP,
            CompressionStep.EF,
            CompressionStep.TRANSPOSE,
            CompressionStep.TRANSPOSE_EF,
        }:
            s.write(f"        {cls.STEP};\n")
            for filename in itertools.chain(
                cls.OUTPUT_FILES, cls.SENSITIVE_OUTPUT_FILES
            ):
                s.write(f"        {normalize_filename(filename)}\n")
    s.write("    }\n\n")

    # arcs
    for cls in _CompressionStepTask.__subclasses__():
        if cls.EXPORT_AS_INPUT:
            s.write(f"orc_dataset -> {cls.STEP};\n")
        if isinstance(cls.INPUT_FILES, property):
            input_files = cls._INPUT_FILES
        else:
            input_files = cls.INPUT_FILES | cls.SENSITIVE_INPUT_FILES
        for filename in input_files:
            assert not is_node_properties_file(filename)
            s.write(f"{normalize_filename(filename)} -> {cls.STEP};\n")
        if cls.STEP == CompressionStep.NODE_PROPERTIES:
            s.write(f"{cls.STEP} -> node_properties;\n")
            assert all(map(is_node_properties_file, cls.OUTPUT_FILES))
        else:
            for filename in cls.OUTPUT_FILES | cls.SENSITIVE_OUTPUT_FILES:
                assert not is_node_properties_file(filename), cls.STEP
                s.write(f"{cls.STEP} -> {normalize_filename(filename)};\n")

    s.write("}\n")

    return s.getvalue()


class CompressGraph(luigi.Task):
    local_export_path = luigi.PathParameter(significant=False)
    local_sensitive_export_path: Optional[Path] = luigi.OptionalPathParameter(
        default=None
    )
    graph_name = luigi.Parameter(default="graph")
    local_graph_path: Path = luigi.PathParameter()
    local_sensitive_graph_path: Optional[Path] = luigi.OptionalPathParameter(
        default=None
    )
    batch_size = luigi.IntParameter(
        default=0,
        significant=False,
        description="""
        Size of work batches to use while compressing.
        Larger is faster, but consumes more resources.
        """,
    )

    rust_executable_dir = luigi.Parameter(
        default="./target/release/",
        significant=False,
        description="Path to the Rust executable used to manipulate the graph.",
    )

    object_types: list[str] = ObjectTypesParameter(  # type: ignore[assignment]
        default=list(_TABLES_PER_OBJECT_TYPE)
    )

    check_flavor = luigi.Parameter(
        default="full",
        significant=False,
        description="Flavor for end-to-end check during compression",
    )

    def requires(self) -> List[luigi.Task]:
        """Returns a :class:`LocalExport` task, and leaves of the compression dependency
        graph"""
        kwargs = dict(
            local_export_path=self.local_export_path,
            local_sensitive_export_path=self.local_sensitive_export_path,
            local_sensitive_graph_path=self.local_sensitive_graph_path,
            graph_name=self.graph_name,
            local_graph_path=self.local_graph_path,
            object_types=self.object_types,
            rust_executable_dir=self.rust_executable_dir,
            check_flavor=self.check_flavor,
        )
        if set(self.object_types).isdisjoint({"dir", "snp", "ori"}):
            # Only nodes of these three types have outgoing arcs with labels
            label_tasks = []
        else:
            label_tasks = [
                EdgeStats(**kwargs),
                LabelStats(**kwargs),
                FclLabels(**kwargs),
                EdgeLabelsEf(**kwargs),
                EdgeLabelsTransposeEf(**kwargs),
            ]
        local_export = LocalExport(
            local_export_path=self.local_export_path,
            local_sensitive_export_path=self.local_sensitive_export_path,
            formats=[Format.orc],  # type: ignore[attr-defined]
            object_types=_tables_for_object_types(self.object_types),
        )
        fullname_tasks = (
            [ExtractFullnames(**kwargs), FullnamesEf(**kwargs)]
            if issubclass(local_export.export_task_type, ExportGraph)
            and not {"rel", "rev"}.isdisjoint(set(self.object_types))
            and self.local_sensitive_export_path is not None
            and self.local_sensitive_graph_path is not None
            else []
        )
        if {"ori", "snp", "rel", "rev", "dir", "cnt"}.issubset(set(self.object_types)):
            e2e_check_task = [EndToEndCheck(**kwargs)]
        else:
            e2e_check_task = []

        return [
            local_export,
            NodeStats(**kwargs),
            TransposeEf(**kwargs),
            Maps(**kwargs),
            *fullname_tasks,
            NodeProperties(**kwargs),
            Stats(**kwargs),
            *e2e_check_task,
            *label_tasks,
        ]

    def output(self) -> List[luigi.LocalTarget]:
        """Returns the ``meta/*.json`` targets"""
        return [self._export_meta(), self._compression_meta()]

    def _export_meta(self) -> luigi.LocalTarget:
        """Returns the metadata on the dataset export"""
        return luigi.LocalTarget(self.local_graph_path / "meta/export.json")

    def _compression_meta(self) -> luigi.LocalTarget:
        """Returns the metadata on the compression pipeline"""
        return luigi.LocalTarget(self.local_graph_path / "meta/compression.json")

    def run(self):
        """Runs the full compression pipeline, then writes :file:`meta/compression.json`

        This does not support running individual steps yet."""
        from importlib.metadata import version
        import json
        import socket

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
            sensitive_in_dir=self.local_sensitive_export_path,
            sensitive_out_dir=self.local_sensitive_graph_path,
            check_flavor=self.check_flavor,
        )

        step_stamp_paths = []
        for step in CompressionStep:
            if step == CompressionStep.CLEAN_TMP:
                # This step is not run as its own Luigi task
                continue
            path = self.local_graph_path / "meta" / "stamps" / f"{step}.json"
            if path.exists():
                step_stamp_paths.append(path)

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
                    "version": version("swh.graph"),
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

        # working threads import it, we need to make sure it is imported so they don't
        # race to the import
        from ..shell import Command  # noqa

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
            for i, relative_path in tqdm.tqdm(
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
        import tempfile

        import luigi.contrib.s3

        from ..shell import Command

        client = luigi.contrib.s3.S3Client()

        relative_path = path.relative_to(self.local_graph_path)
        if path.suffix == ".bin":
            # Large sparse file; store it compressed on S3.
            with tempfile.NamedTemporaryFile(prefix=path.stem, suffix=".bin.zst") as fd:
                self.__status_messages[path] = f"Compressing {relative_path}"
                Command.zstdmt(
                    "--force", "--force", "--keep", path, "-o", fd.name
                ).run()
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

    local_graph_path: Path = luigi.PathParameter()
    s3_graph_path: str = S3PathParameter(significant=False)  # type: ignore[assignment]

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
        from swh.graph.download import GraphDownloader

        GraphDownloader(
            local_path=self.local_graph_path,
            s3_url=self.s3_graph_path,
            parallelism=10,
        ).download(
            progress_percent_cb=self.set_progress_percentage,
            progress_status_cb=self.set_status_message,
        )


class LocalGraph(luigi.Task):
    """Task that depends on a local dataset being present -- either directly from
    :class:`ExportGraph` or via :class:`DownloadGraphFromS3`.
    """

    local_graph_path: Path = luigi.PathParameter()
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
