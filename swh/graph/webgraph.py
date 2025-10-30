# Copyright (C) 2019-2023  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""WebGraph driver"""

from datetime import datetime
from enum import Enum
import logging
import os
from pathlib import Path
import subprocess
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Set,
    TypeVar,
    Union,
)

# WARNING: do not import unnecessary things here to keep cli startup time under
# control
from swh.graph.config import check_config_compress

from .shell import AtomicFileSink, Command, CommandException, Rust

if TYPE_CHECKING:
    from .shell import RunResult

logger = logging.getLogger(__name__)


class CompressionSubprocessError(Exception):
    def __init__(self, message: str, log_path: Path):
        super().__init__(f"{message}; full logs at {log_path}")
        self.message = message
        self.log_path = log_path


class CompressionStep(Enum):
    EXTRACT_NODES = -20
    EXTRACT_LABELS = -10
    NODE_STATS = 0
    EDGE_STATS = 3
    LABEL_STATS = 6
    MPH = 10
    BV = 30
    BV_EF = 40
    BFS_ROOTS = 50
    BFS = 60
    PERMUTE_AND_SIMPLIFY_BFS = 70
    BFS_EF = 80
    BFS_DCF = 90
    LLP = 100
    COMPOSE_ORDERS = 110
    PERMUTE_LLP = 120
    EF = 140
    TRANSPOSE = 160
    TRANSPOSE_EF = 170
    MAPS = 180
    EXTRACT_PERSONS = 190
    PERSONS_STATS = 195
    MPH_PERSONS = 200
    EXTRACT_FULLNAMES = 203
    FULLNAMES_EF = 207
    NODE_PROPERTIES = 210
    MPH_LABELS = 220
    LABELS_ORDER = 225
    FCL_LABELS = 230
    EDGE_LABELS = 240
    EDGE_LABELS_TRANSPOSE = 250
    EDGE_LABELS_EF = 270
    EDGE_LABELS_TRANSPOSE_EF = 280
    STATS = 290
    E2E_CHECK = 295
    CLEAN_TMP = 300

    def __str__(self):
        return self.name


# full compression pipeline
COMP_SEQ = list(CompressionStep)


CompressionStepCommand = Union[
    Callable[[dict, dict], Optional[Command]],
    Callable[[dict, dict], Optional[AtomicFileSink]],
    Callable[[dict, dict], Union[Command, AtomicFileSink]],
    Callable[[dict, dict], Callable[[logging.Logger], None]],
]

COMP_CMD: Dict[CompressionStep, CompressionStepCommand] = {}

T = TypeVar("T", bound=CompressionStepCommand)


def _compression_step(f: T) -> T:
    step_name = f.__name__.upper().lstrip("_")
    try:
        step = getattr(CompressionStep, step_name)
    except AttributeError as e:
        raise Exception(f"Unknown compression step name: {step_name}") from e
    COMP_CMD[step] = f
    return f


@_compression_step
def _extract_nodes(conf: Dict[str, Any], env: Dict[str, str]) -> Command:
    return Rust(
        "swh-graph-extract",
        "extract-nodes",
        "--format",
        "orc",
        "--allowed-node-types",
        conf.get("object_types", "*"),
        f"{conf['in_dir']}",
        f"{conf['out_dir']}/{conf['graph_name']}.nodes/",
        conf=conf,
        env=env,
    )


@_compression_step
def _extract_labels(conf: Dict[str, Any], env: Dict[str, str]) -> Optional[Command]:
    if {"dir", "snp", "*"}.isdisjoint(set(conf.get("object_types", "*").split(","))):
        return None
    return Rust(
        "swh-graph-extract",
        "extract-labels",
        "--format",
        "orc",
        "--allowed-node-types",
        conf.get("object_types", "*"),
        conf["in_dir"],
        conf=conf,
        env=env,
    ) | Command.zstdmt() > AtomicFileSink(
        Path(f"{conf['out_dir']}/{conf['graph_name']}.labels.csv.zst")
    )


@_compression_step
def _node_stats(conf: Dict[str, Any], env: Dict[str, str]) -> Command:
    return Rust(
        "swh-graph-extract",
        "node-stats",
        "--format",
        "orc",
        "--swhids-dir",
        f"{conf['out_dir']}/{conf['graph_name']}.nodes/",
        "--target-stats",
        f"{conf['out_dir']}/{conf['graph_name']}.nodes.stats.txt",
        "--target-count",
        f"{conf['out_dir']}/{conf['graph_name']}.nodes.count.txt",
        conf=conf,
        env=env,
    )


@_compression_step
def _edge_stats(conf: Dict[str, Any], env: Dict[str, str]) -> Command:
    return Rust(
        "swh-graph-extract",
        "edge-stats",
        "--format",
        "orc",
        "--allowed-node-types",
        conf.get("object_types", "*"),
        "--dataset-dir",
        f"{conf['in_dir']}",
        "--target-stats",
        f"{conf['out_dir']}/{conf['graph_name']}.edges.stats.txt",
        "--target-count",
        f"{conf['out_dir']}/{conf['graph_name']}.edges.count.txt",
        conf=conf,
        env=env,
    )


@_compression_step
def _label_stats(conf: Dict[str, Any], env: Dict[str, str]) -> AtomicFileSink:
    if {"dir", "snp", "*"}.isdisjoint(set(conf.get("object_types", "*").split(","))):
        return Command.echo("0") > AtomicFileSink(
            Path(f"{conf['out_dir']}/{conf['graph_name']}.labels.count.txt")
        )
    return Command.zstdcat(
        f"{conf['out_dir']}/{conf['graph_name']}.labels.csv.zst"
    ) | Command.wc("-l") > AtomicFileSink(
        Path(f"{conf['out_dir']}/{conf['graph_name']}.labels.count.txt")
    )


@_compression_step
def _mph(conf: Dict[str, Any], env: Dict[str, str]) -> Command:
    with open(
        f"{conf['out_dir']}/{conf['graph_name']}.nodes.count.txt", "r"
    ) as nodes_count:
        num_nodes = nodes_count.readline().splitlines()
        assert len(num_nodes) == 1
    return Rust(
        "swh-graph-compress",
        "pthash-swhids",
        "--num-nodes",
        num_nodes[0],
        f"{conf['out_dir']}/{conf['graph_name']}.nodes/",
        f"{conf['out_dir']}/{conf['graph_name']}.pthash",
        conf=conf,
        env=env,
    )


@_compression_step
def _bv(conf: Dict[str, Any], env: Dict[str, str]) -> Command:
    with open(f"{conf['out_dir']}/{conf['graph_name']}.nodes.count.txt") as nodes_count:
        num_nodes = nodes_count.readline().splitlines()
        assert len(num_nodes) == 1
    batch_size = int(conf.get("batch_size", "0"))
    batching = ["--sort-batch-size", str(batch_size)] if batch_size else []
    return Rust(
        "swh-graph-extract",
        "bv",
        "--allowed-node-types",
        conf.get("object_types", "*"),
        "--mph-algo",
        "pthash",
        "--function",
        f"{conf['out_dir']}/{conf['graph_name']}",
        "--num-nodes",
        num_nodes[0],
        *batching,
        f"{conf['in_dir']}",
        f"{conf['out_dir']}/{conf['graph_name']}-base",
        conf=conf,
        env=env,
    )


@_compression_step
def _bv_ef(conf: Dict[str, Any], env: Dict[str, str]) -> Command:
    return Rust(
        "swh-graph-index",
        "ef",
        f"{conf['out_dir']}/{conf['graph_name']}-base",
        conf=conf,
        env=env,
    )


@_compression_step
def _bfs_roots(conf: Dict[str, Any], env: Dict[str, str]) -> Command:
    return Rust(
        "swh-graph-extract",
        "bfs-roots",
        "--allowed-node-types",
        conf.get("object_types", "*"),
        f"{conf['in_dir']}",
        f"{conf['out_dir']}/{conf['graph_name']}-bfs.roots.txt",
        conf=conf,
        env=env,
    )


@_compression_step
def _bfs(conf: Dict[str, Any], env: Dict[str, str]) -> Command:
    return Rust(
        "swh-graph-compress",
        "bfs",
        "--mph-algo",
        "pthash",
        "--function",
        f"{conf['out_dir']}/{conf['graph_name']}.pthash",
        "--init-roots",
        f"{conf['out_dir']}/{conf['graph_name']}-bfs.roots.txt",
        f"{conf['out_dir']}/{conf['graph_name']}-base",
        f"{conf['out_dir']}/{conf['graph_name']}-bfs.order",
        conf=conf,
        env=env,
    )


@_compression_step
def _permute_and_simplify_bfs(conf: Dict[str, Any], env: Dict[str, str]) -> Command:
    batch_size = int(conf.get("batch_size", "0"))
    batching = ["--sort-batch-size", str(batch_size)] if batch_size else []
    return Rust(
        "swh-graph-compress",
        "permute-and-symmetrize",
        f"{conf['out_dir']}/{conf['graph_name']}-base",
        f"{conf['out_dir']}/{conf['graph_name']}-bfs-simplified",
        "--permutation",
        f"{conf['out_dir']}/{conf['graph_name']}-bfs.order",
        *batching,
        conf=conf,
        env=env,
    )


@_compression_step
def _bfs_ef(conf: Dict[str, Any], env: Dict[str, str]) -> Command:
    return Rust(
        "swh-graph-index",
        "ef",
        f"{conf['out_dir']}/{conf['graph_name']}-bfs-simplified",
        conf=conf,
        env=env,
    )


@_compression_step
def _bfs_dcf(conf: Dict[str, Any], env: Dict[str, str]) -> Command:
    return Rust(
        "swh-graph-index",
        "dcf",
        f"{conf['out_dir']}/{conf['graph_name']}-bfs-simplified",
        conf=conf,
        env=env,
    )


@_compression_step
def _llp(conf: Dict[str, Any], env: Dict[str, str]) -> Command:
    return Rust(
        "swh-graph-compress",
        "llp",
        "-g",
        f"{conf['llp_gammas']}",
        f"{conf['out_dir']}/{conf['graph_name']}-bfs-simplified",
        f"{conf['out_dir']}/{conf['graph_name']}-llp.order",
        conf=conf,
        env=env,
    )


@_compression_step
def _compose_orders(conf: Dict[str, Any], env: Dict[str, str]) -> Command:
    with open(f"{conf['out_dir']}/{conf['graph_name']}.nodes.count.txt") as nodes_count:
        num_nodes = nodes_count.readline().splitlines()
        assert len(num_nodes) == 1
    return Rust(
        "swh-graph-compress",
        "compose-orders",
        "--num-nodes",
        num_nodes[0],
        "--input",
        f"{conf['out_dir']}/{conf['graph_name']}-bfs.order",
        "--input",
        f"{conf['out_dir']}/{conf['graph_name']}-llp.order",
        "--output",
        f"{conf['out_dir']}/{conf['graph_name']}.pthash.order",
        conf=conf,
        env=env,
    )


@_compression_step
def _permute_llp(conf: Dict[str, Any], env: Dict[str, str]) -> Command:
    batch_size = int(conf.get("batch_size", "0"))
    batching = ["--sort-batch-size", str(batch_size)] if batch_size else []
    return Rust(
        "swh-graph-compress",
        "permute",
        f"{conf['out_dir']}/{conf['graph_name']}-base",
        f"{conf['out_dir']}/{conf['graph_name']}",
        "--permutation",
        f"{conf['out_dir']}/{conf['graph_name']}.pthash.order",
        *batching,
        conf=conf,
        env=env,
    )


@_compression_step
def _ef(conf: Dict[str, Any], env: Dict[str, str]) -> Command:
    return Rust(
        "swh-graph-index",
        "ef",
        f"{conf['out_dir']}/{conf['graph_name']}",
        conf=conf,
        env=env,
    )


@_compression_step
def _transpose(conf: Dict[str, Any], env: Dict[str, str]) -> Command:
    batch_size = int(conf.get("batch_size", "0"))
    batching = ["--sort-batch-size", str(batch_size)] if batch_size else []
    return Rust(
        "swh-graph-compress",
        "transpose",
        f"{conf['out_dir']}/{conf['graph_name']}",
        f"{conf['out_dir']}/{conf['graph_name']}-transposed",
        *batching,
        conf=conf,
        env=env,
    )


@_compression_step
def _transpose_ef(conf: Dict[str, Any], env: Dict[str, str]) -> Command:
    return Rust(
        "swh-graph-index",
        "ef",
        f"{conf['out_dir']}/{conf['graph_name']}-transposed",
        conf=conf,
        env=env,
    )


@_compression_step
def _maps(conf: Dict[str, Any], env: Dict[str, str]) -> Command:
    with open(f"{conf['out_dir']}/{conf['graph_name']}.nodes.count.txt") as nodes_count:
        num_nodes = nodes_count.readline().splitlines()
        assert len(num_nodes) == 1
    return Rust(
        "swh-graph-compress",
        "maps",
        "--num-nodes",
        num_nodes[0],
        "--swhids-dir",
        f"{conf['out_dir']}/{conf['graph_name']}.nodes/",
        "--mph-algo",
        "pthash",
        "--function",
        f"{conf['out_dir']}/{conf['graph_name']}.pthash",
        "--order",
        f"{conf['out_dir']}/{conf['graph_name']}.pthash.order",
        "--node2swhid",
        f"{conf['out_dir']}/{conf['graph_name']}.node2swhid.bin",
        "--node2type",
        f"{conf['out_dir']}/{conf['graph_name']}.node2type.bin",
        conf=conf,
        env=env,
    )


@_compression_step
def _extract_persons(conf: Dict[str, Any], env: Dict[str, str]) -> AtomicFileSink:
    if {"rel", "rev", "*"}.isdisjoint(set(conf.get("object_types", "*").split(","))):
        return Command.echo("") | Command.zstdmt() > AtomicFileSink(
            Path(f"{conf['out_dir']}/{conf['graph_name']}.persons.csv.zst")
        )
    return Rust(
        "swh-graph-extract",
        "extract-persons",
        "--allowed-node-types",
        conf.get("object_types", "*"),
        f"{conf['in_dir']}",
        conf=conf,
        env=env,
    ) | Command.zstdmt() > AtomicFileSink(
        Path(f"{conf['out_dir']}/{conf['graph_name']}.persons.csv.zst")
    )


@_compression_step
def _mph_persons(
    conf: Dict[str, Any], env: Dict[str, str]
) -> Union[Command, AtomicFileSink]:
    with open(
        f"{conf['out_dir']}/{conf['graph_name']}.persons.count.txt"
    ) as persons_count:
        num_persons = persons_count.readline().splitlines()
        assert len(num_persons) == 1
    if num_persons[0] == "0":
        return Command.echo("") > AtomicFileSink(
            Path(f"{conf['out_dir']}/{conf['graph_name']}.persons.pthash")
        )
    return Rust(
        "swh-graph-compress",
        "pthash-persons",
        "--num-persons",
        num_persons[0],
        Command.zstdcat(
            f"{conf['out_dir']}/{conf['graph_name']}.persons.csv.zst",
        ),
        f"{conf['out_dir']}/{conf['graph_name']}.persons.pthash",
        conf=conf,
        env=env,
    )


@_compression_step
def _extract_fullnames(conf: Dict[str, Any], env: Dict[str, str]) -> Optional[Command]:
    if {"rel", "rev", "*"}.isdisjoint(set(conf.get("object_types", "*").split(","))):
        return None
    if "sensitive_in_dir" not in conf:
        return None
    if "sensitive_out_dir" not in conf:
        return None
    if not (
        Path(f"{conf['out_dir']}/{conf['graph_name']}.persons.count.txt").exists()
        and Path(f"{conf['sensitive_in_dir']}/orc/person").exists()
    ):
        return None
    with open(
        f"{conf['out_dir']}/{conf['graph_name']}.persons.count.txt"
    ) as persons_count:
        num_persons = persons_count.readline().splitlines()
        assert len(num_persons) == 1
    if num_persons[0] == "0":
        return None
    return Rust(
        "swh-graph-extract",
        "extract-fullnames",
        "--person-function",
        f"{conf['out_dir']}/{conf['graph_name']}.persons.pthash",
        f"{conf['sensitive_in_dir']}/orc",
        f"{conf['sensitive_out_dir']}/{conf['graph_name']}.persons",
        f"{conf['sensitive_out_dir']}/{conf['graph_name']}.persons.lengths",
        conf=conf,
        env=env,
    )


@_compression_step
def _fullnames_ef(conf: Dict[str, Any], env: Dict[str, str]) -> Optional[Command]:
    if {"rel", "rev", "*"}.isdisjoint(set(conf.get("object_types", "*").split(","))):
        return None
    if "sensitive_in_dir" not in conf:
        return None
    if "sensitive_out_dir" not in conf:
        return None
    if not (
        Path(f"{conf['out_dir']}/{conf['graph_name']}.persons.count.txt").exists()
        and Path(f"{conf['sensitive_in_dir']}/orc/person").exists()
    ):
        return None
    with open(
        f"{conf['out_dir']}/{conf['graph_name']}.persons.count.txt"
    ) as persons_count:
        num_persons = persons_count.readline().splitlines()
        assert len(num_persons) == 1
    if num_persons[0] == "0":
        return None
    return Rust(
        "swh-graph-index",
        "fullnames-ef",
        "--num-persons",
        num_persons[0],
        f"{conf['sensitive_out_dir']}/{conf['graph_name']}.persons",
        f"{conf['sensitive_out_dir']}/{conf['graph_name']}.persons.lengths",
        f"{conf['sensitive_out_dir']}/{conf['graph_name']}.persons.ef",
        conf=conf,
        env=env,
    )


@_compression_step
def _persons_stats(conf: Dict[str, Any], env: Dict[str, str]) -> AtomicFileSink:
    if {"rel", "rev", "*"}.isdisjoint(set(conf.get("object_types", "*").split(","))):
        return Command.echo("0") > AtomicFileSink(
            Path(f"{conf['out_dir']}/{conf['graph_name']}.persons.count.txt")
        )
    return Command.zstdcat(
        f"{conf['out_dir']}/{conf['graph_name']}.persons.csv.zst"
    ) | Command.wc("-l") > AtomicFileSink(
        Path(f"{conf['out_dir']}/{conf['graph_name']}.persons.count.txt")
    )


@_compression_step
def _node_properties(conf: Dict[str, Any], env: Dict[str, str]) -> Command:
    with open(f"{conf['out_dir']}/{conf['graph_name']}.nodes.count.txt") as nodes_count:
        num_nodes = nodes_count.readline().splitlines()
        assert len(num_nodes) == 1
    return Rust(
        "swh-graph-extract",
        "node-properties",
        "--format",
        "orc",
        "--allowed-node-types",
        conf.get("object_types", "*"),
        "--mph-algo",
        "pthash",
        "--function",
        f"{conf['out_dir']}/{conf['graph_name']}",
        "--order",
        f"{conf['out_dir']}/{conf['graph_name']}.pthash.order",
        "--person-function",
        f"{conf['out_dir']}/{conf['graph_name']}.persons.pthash",
        "--num-nodes",
        num_nodes[0],
        f"{conf['in_dir']}",
        f"{conf['out_dir']}/{conf['graph_name']}",
        conf=conf,
        env=env,
    )


@_compression_step
def _mph_labels(conf: Dict[str, Any], env: Dict[str, str]) -> Optional[Command]:
    if {"dir", "snp", "*"}.isdisjoint(set(conf.get("object_types", "*").split(","))):
        return None
    with open(
        f"{conf['out_dir']}/{conf['graph_name']}.labels.count.txt"
    ) as labels_count:
        num_labels = labels_count.readline().splitlines()
        assert len(num_labels) == 1
    if num_labels[0] == "0":
        return None
    return Rust(
        "swh-graph-compress",
        "pthash-labels",
        "--num-labels",
        num_labels[0],
        Command.zstdcat(f"{conf['out_dir']}/{conf['graph_name']}.labels.csv.zst"),
        f"{conf['out_dir']}/{conf['graph_name']}.labels.pthash",
        conf=conf,
        env=env,
    )


@_compression_step
def _labels_order(conf: Dict[str, Any], env: Dict[str, str]) -> Optional[Command]:
    if {"dir", "snp", "*"}.isdisjoint(set(conf.get("object_types", "*").split(","))):
        return None
    with open(
        f"{conf['out_dir']}/{conf['graph_name']}.labels.count.txt"
    ) as labels_count:
        num_labels = labels_count.readline().splitlines()
        assert len(num_labels) == 1
    if num_labels[0] == "0":
        return None
    return Rust(
        "swh-graph-compress",
        "pthash-labels-order",
        "--num-labels",
        num_labels[0],
        Command.zstdcat(f"{conf['out_dir']}/{conf['graph_name']}.labels.csv.zst"),
        f"{conf['out_dir']}/{conf['graph_name']}.labels.pthash",
        f"{conf['out_dir']}/{conf['graph_name']}.labels.pthash.order",
        conf=conf,
        env=env,
    )


@_compression_step
def _fcl_labels(conf: Dict[str, Any], env: Dict[str, str]) -> Optional[Command]:
    if {"dir", "snp", "*"}.isdisjoint(set(conf.get("object_types", "*").split(","))):
        return None
    with open(
        f"{conf['out_dir']}/{conf['graph_name']}.labels.count.txt"
    ) as labels_count:
        num_labels = labels_count.readline().splitlines()
        assert len(num_labels) == 1
    return Rust(
        "swh-graph-compress",
        "fcl",
        "--num-lines",
        num_labels[0],
        Command.zstdcat(f"{conf['out_dir']}/{conf['graph_name']}.labels.csv.zst"),
        f"{conf['out_dir']}/{conf['graph_name']}.labels.fcl",
        conf=conf,
        env=env,
    )


@_compression_step
def _edge_labels(conf: Dict[str, Any], env: Dict[str, str]) -> Optional[Command]:
    if {"dir", "snp", "ori", "*"}.isdisjoint(
        set(conf.get("object_types", "*").split(","))
    ):
        return None
    with open(
        f"{conf['out_dir']}/{conf['graph_name']}.labels.count.txt"
    ) as labels_count:
        num_labels = labels_count.readline().splitlines()
        assert len(num_labels) == 1
    if num_labels[0] == "0":
        return None
    with open(f"{conf['out_dir']}/{conf['graph_name']}.nodes.count.txt") as nodes_count:
        num_nodes = nodes_count.readline().splitlines()
        assert len(num_nodes) == 1
    if num_nodes[0] == "0":
        return None
    batch_size = int(conf.get("batch_size", "0"))
    batching = ["--sort-batch-size", str(batch_size)] if batch_size else []
    return Rust(
        "swh-graph-extract",
        "edge-labels",
        "--allowed-node-types",
        conf.get("object_types", "*"),
        "--mph-algo",
        "pthash",
        "--function",
        f"{conf['out_dir']}/{conf['graph_name']}",
        "--order",
        f"{conf['out_dir']}/{conf['graph_name']}.pthash.order",
        "--label-name-mphf",
        f"{conf['out_dir']}/{conf['graph_name']}.labels.pthash",
        "--label-name-order",
        f"{conf['out_dir']}/{conf['graph_name']}.labels.pthash.order",
        "--num-nodes",
        num_nodes[0],
        *batching,
        f"{conf['in_dir']}",
        f"{conf['out_dir']}/{conf['graph_name']}",
        conf=conf,
        env=env,
    )


@_compression_step
def _edge_labels_transpose(
    conf: Dict[str, Any], env: Dict[str, str]
) -> Optional[Command]:
    if {"dir", "snp", "ori", "*"}.isdisjoint(
        set(conf.get("object_types", "*").split(","))
    ):
        return None
    with open(
        f"{conf['out_dir']}/{conf['graph_name']}.labels.count.txt"
    ) as labels_count:
        num_labels = labels_count.readline().splitlines()
        assert len(num_labels) == 1
    if num_labels[0] == "0":
        return None
    with open(f"{conf['out_dir']}/{conf['graph_name']}.nodes.count.txt") as nodes_count:
        num_nodes = nodes_count.readline().splitlines()
        assert len(num_nodes) == 1
    if num_nodes[0] == "0":
        return None
    batch_size = int(conf.get("batch_size", "0"))
    batching = ["--sort-batch-size", str(batch_size)] if batch_size else []
    return Rust(
        "swh-graph-extract",
        "edge-labels",
        "--allowed-node-types",
        conf.get("object_types", "*"),
        "--mph-algo",
        "pthash",
        "--function",
        f"{conf['out_dir']}/{conf['graph_name']}",
        "--order",
        f"{conf['out_dir']}/{conf['graph_name']}.pthash.order",
        "--label-name-mphf",
        f"{conf['out_dir']}/{conf['graph_name']}.labels.pthash",
        "--label-name-order",
        f"{conf['out_dir']}/{conf['graph_name']}.labels.pthash.order",
        "--num-nodes",
        num_nodes[0],
        "--transposed",
        *batching,
        f"{conf['in_dir']}",
        f"{conf['out_dir']}/{conf['graph_name']}-transposed",
        conf=conf,
        env=env,
    )


@_compression_step
def _edge_labels_ef(conf: Dict[str, Any], env: Dict[str, str]) -> Optional[Command]:
    if {"dir", "snp", "ori", "*"}.isdisjoint(
        set(conf.get("object_types", "*").split(","))
    ):
        return None
    with open(
        f"{conf['out_dir']}/{conf['graph_name']}.labels.count.txt"
    ) as labels_count:
        num_labels = labels_count.readline().splitlines()
        assert len(num_labels) == 1
    if num_labels[0] == "0":
        return None
    with open(f"{conf['out_dir']}/{conf['graph_name']}.nodes.count.txt") as nodes_count:
        num_nodes = nodes_count.readline().splitlines()
        assert len(num_nodes) == 1
    return Rust(
        "swh-graph-index",
        "labels-ef",
        f"{conf['out_dir']}/{conf['graph_name']}-labelled",
        num_nodes[0],
        conf=conf,
        env=env,
    )


@_compression_step
def _edge_labels_transpose_ef(
    conf: Dict[str, Any], env: Dict[str, str]
) -> Optional[Command]:
    if {"dir", "snp", "*"}.isdisjoint(set(conf.get("object_types", "*").split(","))):
        return None
    with open(
        f"{conf['out_dir']}/{conf['graph_name']}.labels.count.txt"
    ) as labels_count:
        num_labels = labels_count.readline().splitlines()
        assert len(num_labels) == 1
    if num_labels[0] == "0":
        return None
    with open(f"{conf['out_dir']}/{conf['graph_name']}.nodes.count.txt") as nodes_count:
        num_nodes = nodes_count.readline().splitlines()
        assert len(num_nodes) == 1
    return Rust(
        "swh-graph-index",
        "labels-ef",
        f"{conf['out_dir']}/{conf['graph_name']}-transposed-labelled",
        num_nodes[0],
        conf=conf,
        env=env,
    )


@_compression_step
def _stats(conf: Dict[str, Any], env: Dict[str, str]) -> Command:
    return Rust(
        "swh-graph-compress",
        "stats",
        "--graph",
        f"{conf['out_dir']}/{conf['graph_name']}",
        "--stats",
        f"{conf['out_dir']}/{conf['graph_name']}.stats",
        conf=conf,
        env=env,
    )


@_compression_step
def _e2e_check(
    conf: Dict[str, Any], env: Dict[str, str]
) -> Callable[[logging.Logger], None]:
    from swh.graph.e2e_check import run_e2e_check

    return lambda logger: run_e2e_check(
        graph_name=conf["graph_name"],
        in_dir=conf["in_dir"],
        out_dir=conf["out_dir"],
        sensitive_in_dir=conf.get("sensitive_in_dir"),
        sensitive_out_dir=conf.get("sensitive_out_dir"),
        check_flavor=conf.get("check_flavor", "full"),
        profile=conf.get("profile", "release"),
        logger=logger,
    )


@_compression_step
def _clean_tmp(conf: Dict[str, Any], env: Dict[str, str]) -> Command:
    return Command.rm(
        "-rf",
        f"{conf['out_dir']}/{conf['graph_name']}-base.*",
        f"{conf['out_dir']}/{conf['graph_name']}-bfs-simplified.*",
        f"{conf['out_dir']}/{conf['graph_name']}-bfs.order",
        f"{conf['out_dir']}/{conf['graph_name']}-llp.order",
        f"{conf['out_dir']}/{conf['graph_name']}.nodes/",
        f"{conf['out_dir']}/{conf['graph_name']}-bfs.roots.txt",
        f"{conf['out_dir']}/{conf['graph_name']}.labels.csv.zst",
        f"{conf['out_dir']}/{conf['graph_name']}.persons.csv.zst",
        f"{conf['tmp_dir']}",
    )


def do_step(step, conf, env=None) -> "List[RunResult]":
    if env is None:
        env = os.environ.copy()
        env["TMPDIR"] = conf["tmp_dir"]
        env["TZ"] = "UTC"

    log_dir = Path(conf["out_dir"]) / "logs"
    log_dir.mkdir(exist_ok=True)

    step_logger = logger.getChild(f"steps.{step.name.lower()}")
    if not step_logger.isEnabledFor(logging.INFO):
        # Ensure that at least INFO messages are sent, because it is the level we use
        # for the stdout of Rust processes. These processes can take a long time to
        # run, and it would be very annoying to have to run them again just because
        # they crashed with no log.
        step_logger.setLevel(logging.INFO)
    log_path = log_dir / (
        f"{conf['graph_name']}"
        f"-{int(datetime.now().timestamp() * 1000)}"
        f"-{str(step).lower()}.log"
    )
    step_handler = logging.FileHandler(log_path)
    step_logger.addHandler(step_handler)

    step_start_time = datetime.now()
    step_logger.info("Starting compression step %s at %s", step, step_start_time)

    command = COMP_CMD[step](conf, env)

    if command is None:
        step_logger.info("Compression step %s skipped", step)
        step_logger.removeHandler(step_handler)
        step_handler.close()
        return []

    step_start_time = datetime.now()
    step_logger.info("Starting compression step %s at %s", step, step_start_time)
    step_logger.info("Running: %s", command.__str__())

    if isinstance(command, (Command, AtomicFileSink)):
        running_command = command._run(
            stdin=None, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
        )
        with running_command.stdout() as stdout:
            for line in stdout:
                step_logger.info(line.rstrip().decode(errors="replace"))
        try:
            results = running_command.wait()
        except CommandException as e:
            msg = f"Compression step {step} returned non-zero exit code {e.returncode}"
            step_logger.critical(msg)
            raise CompressionSubprocessError(msg, log_path)
        step_end_time = datetime.now()
        step_duration = step_end_time - step_start_time
        step_logger.info(
            "Compression step %s finished at %s (in %s)",
            step,
            step_end_time,
            step_duration,
        )
        step_logger.removeHandler(step_handler)
        step_handler.close()
    else:
        # This allows for calling Python functions directly
        try:
            results = command(step_logger)
        except Exception as exc:
            msg = f"Compression step {step} failed with the following error: {exc}"
            step_logger.critical(msg)
            raise CompressionSubprocessError(msg, log_path)

    return results


def compress(
    graph_name: str,
    in_dir: str,
    out_dir: str,
    sensitive_in_dir: Optional[str],
    sensitive_out_dir: Optional[str],
    check_flavor: Optional[str],
    steps: Set[CompressionStep] = set(COMP_SEQ),
    conf: Dict[str, str] = {},
    progress_cb: Callable[[int, CompressionStep], None] = lambda percentage, step: None,
):
    """graph compression pipeline driver from nodes/edges files to compressed
    on-disk representation

    Args:
        graph_name: graph base name, relative to in_dir
        in_dir: input directory, where the uncompressed graph can be found
        out_dir: output directory, where the compressed graph will be stored
        sensitive_in_dir: sensitive input directory, where the uncompressed
            sensitive graph can be found
        sensitive_out_dir: sensitive output directory, where the compressed
            sensitive graph will be stored
        check_flavor: which flavor of checks to run
        steps: compression steps to run (default: all steps)
        conf: compression configuration, supporting the following keys (all are
          optional, so an empty configuration is fine and is the default)

          - batch_size: batch size for `WebGraph transformations
            <http://webgraph.di.unimi.it/docs/it/unimi/dsi/webgraph/Transform.html>`_;
            defaults to 1 billion
          - tmp_dir: temporary directory, defaults to the "tmp" subdir of
            out_dir
          - object_types: comma-separated list of object types to extract
            (eg. ``ori,snp,rel,rev``). Defaults to ``*``.
        progress_cb: a callable taking a percentage and step as argument,
          which is called every time a step starts.

    """
    if not steps:
        steps = set(COMP_SEQ)

    conf = check_config_compress(
        conf,
        graph_name,
        in_dir,
        out_dir,
        sensitive_in_dir,
        sensitive_out_dir,
        check_flavor,
    )

    compression_start_time = datetime.now()
    logger.info("Starting compression at %s", compression_start_time)
    seq_no = 0
    for step in COMP_SEQ:
        if step not in steps:
            logger.debug("Skipping compression step %s", step)
            continue
        seq_no += 1
        logger.info("Running compression step %s (%s/%s)", step, seq_no, len(steps))
        progress_cb(seq_no * 100 // len(steps), step)
        do_step(step, conf)
    compression_end_time = datetime.now()
    compression_duration = compression_end_time - compression_start_time
    logger.info("Completed compression in %s", compression_duration)
