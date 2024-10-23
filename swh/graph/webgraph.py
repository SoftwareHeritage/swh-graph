# Copyright (C) 2019-2023  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""WebGraph driver

"""

from datetime import datetime
from enum import Enum
import logging
import os
from pathlib import Path
import shlex
import subprocess
from typing import TYPE_CHECKING, Callable, Dict, List, Set

# WARNING: do not import unnecessary things here to keep cli startup time under
# control
from swh.graph.config import check_config_compress

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
    OFFSETS = 130
    EF = 140
    TRANSPOSE = 160
    TRANSPOSE_OFFSETS = 170
    TRANSPOSE_EF = 175
    MAPS = 180
    EXTRACT_PERSONS = 190
    PERSONS_STATS = 195
    MPH_PERSONS = 200
    NODE_PROPERTIES = 210
    MPH_LABELS = 220
    LABELS_ORDER = 225
    FCL_LABELS = 230
    EDGE_LABELS = 240
    EDGE_LABELS_TRANSPOSE = 250
    EDGE_LABELS_EF = 270
    EDGE_LABELS_TRANSPOSE_EF = 280
    STATS = 290
    CLEAN_TMP = 300

    def __str__(self):
        return self.name


# full compression pipeline
COMP_SEQ = list(CompressionStep)

# Mapping from compression steps to shell commands implementing them. Commands
# will be executed by the shell, so be careful with meta characters. They are
# specified here as lists of tokens that will be joined together only for ease
# of line splitting. In commands, {tokens} will be interpolated with
# configuration values, see :func:`compress`.
STEP_ARGV: Dict[CompressionStep, List[str]] = {
    CompressionStep.EXTRACT_NODES: [
        "{rust_executable_dir}/swh-graph-extract",
        "extract-nodes",
        "--format",
        "orc",
        "--allowed-node-types",
        "{object_types}",
        "{in_dir}",
        "{out_dir}/{graph_name}.nodes/",
    ],
    CompressionStep.EXTRACT_LABELS: [
        "{rust_executable_dir}/swh-graph-extract",
        "extract-labels",
        "--format",
        "orc",
        "--allowed-node-types",
        "{object_types}",
        "{in_dir}",
        "| zstdmt > {out_dir}/{graph_name}.labels.csv.zst",
    ],
    CompressionStep.NODE_STATS: [
        "{rust_executable_dir}/swh-graph-extract",
        "node-stats",
        "--format",
        "orc",
        "--swhids-dir",
        "{out_dir}/{graph_name}.nodes/",
        "--target-stats",
        "{out_dir}/{graph_name}.nodes.stats.txt",
        "--target-count",
        "{out_dir}/{graph_name}.nodes.count.txt",
    ],
    CompressionStep.EDGE_STATS: [
        "{rust_executable_dir}/swh-graph-extract",
        "edge-stats",
        "--format",
        "orc",
        "--allowed-node-types",
        "{object_types}",
        "--dataset-dir",
        "{in_dir}",
        "--target-stats",
        "{out_dir}/{graph_name}.edges.stats.txt",
        "--target-count",
        "{out_dir}/{graph_name}.edges.count.txt",
    ],
    CompressionStep.LABEL_STATS: [
        "zstdcat {out_dir}/{graph_name}.labels.csv.zst "
        "| wc -l "
        "> {out_dir}/{graph_name}.labels.count.txt"
    ],
    CompressionStep.MPH: [
        "{rust_executable_dir}/swh-graph-compress",
        "pthash-swhids",
        "--num-nodes",
        "$(cat {out_dir}/{graph_name}.nodes.count.txt)",
        "{out_dir}/{graph_name}.nodes/",
        "{out_dir}/{graph_name}.pthash",
    ],
    CompressionStep.BV: [
        "{rust_executable_dir}/swh-graph-extract",
        "bv",
        "--allowed-node-types",
        "{object_types}",
        "--mph-algo",
        "pthash",
        "--function",
        "{out_dir}/{graph_name}",
        "--num-nodes",
        "$(cat {out_dir}/{graph_name}.nodes.count.txt)",
        "{in_dir}",
        "{out_dir}/{graph_name}-base",
    ],
    CompressionStep.BV_EF: [
        "{rust_executable_dir}/swh-graph-index",
        "ef",
        "{out_dir}/{graph_name}-base",
    ],
    CompressionStep.BFS_ROOTS: [
        "{rust_executable_dir}/swh-graph-extract",
        "bfs-roots",
        "{in_dir}",
        "{out_dir}/{graph_name}-bfs.roots.txt",
    ],
    CompressionStep.BFS: [
        "{rust_executable_dir}/swh-graph-compress",
        "bfs",
        "--mph-algo",
        "pthash",
        "--function",
        "{out_dir}/{graph_name}.pthash",
        "--init-roots",
        "{out_dir}/{graph_name}-bfs.roots.txt",
        "{out_dir}/{graph_name}-base",
        "{out_dir}/{graph_name}-bfs.order",
    ],
    CompressionStep.PERMUTE_AND_SIMPLIFY_BFS: [
        "{rust_executable_dir}/swh-graph-compress",
        "permute-and-symmetrize",
        "{out_dir}/{graph_name}-base",
        "{out_dir}/{graph_name}-bfs-simplified",
        "--permutation",
        "{out_dir}/{graph_name}-bfs.order",
    ],
    CompressionStep.BFS_EF: [
        "{rust_executable_dir}/swh-graph-index",
        "ef",
        "{out_dir}/{graph_name}-bfs-simplified",
    ],
    CompressionStep.BFS_DCF: [
        "{rust_executable_dir}/swh-graph-index",
        "dcf",
        "{out_dir}/{graph_name}-bfs-simplified",
    ],
    CompressionStep.LLP: [
        "{rust_executable_dir}/swh-graph-compress",
        "llp",
        "-g",
        "{llp_gammas}",
        "{out_dir}/{graph_name}-bfs-simplified",
        "{out_dir}/{graph_name}-llp.order",
    ],
    CompressionStep.COMPOSE_ORDERS: [
        "{rust_executable_dir}/swh-graph-compress",
        "compose-orders",
        "--num-nodes",
        "$(cat {out_dir}/{graph_name}.nodes.count.txt)",
        "--input",
        "{out_dir}/{graph_name}-bfs.order",
        "--input",
        "{out_dir}/{graph_name}-llp.order",
        "--output",
        "{out_dir}/{graph_name}.pthash.order",
    ],
    CompressionStep.PERMUTE_LLP: [
        "{rust_executable_dir}/swh-graph-compress",
        "permute",
        "{out_dir}/{graph_name}-base",
        "{out_dir}/{graph_name}",
        "--permutation",
        "{out_dir}/{graph_name}.pthash.order",
    ],
    CompressionStep.OFFSETS: [
        "{rust_executable_dir}/swh-graph-index",
        "offsets",
        "{out_dir}/{graph_name}",
    ],
    CompressionStep.EF: [
        "{rust_executable_dir}/swh-graph-index",
        "ef",
        "{out_dir}/{graph_name}",
    ],
    CompressionStep.TRANSPOSE: [
        "{rust_executable_dir}/swh-graph-compress",
        "transpose",
        "{out_dir}/{graph_name}",
        "{out_dir}/{graph_name}-transposed",
    ],
    CompressionStep.TRANSPOSE_OFFSETS: [
        "{rust_executable_dir}/swh-graph-index",
        "offsets",
        "{out_dir}/{graph_name}-transposed",
    ],
    CompressionStep.TRANSPOSE_EF: [
        "{rust_executable_dir}/swh-graph-index",
        "ef",
        "{out_dir}/{graph_name}-transposed",
    ],
    CompressionStep.MAPS: [
        "{rust_executable_dir}/swh-graph-compress",
        "maps",
        "--num-nodes",
        "$(cat {out_dir}/{graph_name}.nodes.count.txt)",
        "--swhids-dir",
        "{out_dir}/{graph_name}.nodes/",
        "--mph-algo",
        "pthash",
        "--function",
        "{out_dir}/{graph_name}.pthash",
        "--order",
        "{out_dir}/{graph_name}.pthash.order",
        "--node2swhid",
        "{out_dir}/{graph_name}.node2swhid.bin",
        "--node2type",
        "{out_dir}/{graph_name}.node2type.bin",
    ],
    CompressionStep.EXTRACT_PERSONS: [
        "{rust_executable_dir}/swh-graph-extract",
        "extract-persons",
        "--allowed-node-types",
        "{object_types}",
        "{in_dir}",
        "| zstdmt > {out_dir}/{graph_name}.persons.csv.zst",
    ],
    CompressionStep.MPH_PERSONS: [
        # skip this step when compressing a graph with no revisions or releases
        'if [[ $(cat {out_dir}/{graph_name}.persons.count.txt) != "0" ]]; then\n',
        "{rust_executable_dir}/swh-graph-compress",
        "pthash-labels",
        "--num-labels",
        "$(cat {out_dir}/{graph_name}.persons.count.txt)",
        "<(zstdcat {out_dir}/{graph_name}.persons.csv.zst)",
        "{out_dir}/{graph_name}.persons.pthash\n",
        "else\n",
        "echo '' > {out_dir}/{graph_name}.persons.pthash;",
        "fi",
    ],
    CompressionStep.PERSONS_STATS: [
        "zstdcat {out_dir}/{graph_name}.persons.csv.zst "
        "| wc -l"
        "> {out_dir}/{graph_name}.persons.count.txt",
    ],
    CompressionStep.NODE_PROPERTIES: [
        "{rust_executable_dir}/swh-graph-extract",
        "node-properties",
        "--format",
        "orc",
        "--allowed-node-types",
        "{object_types}",
        "--mph-algo",
        "pthash",
        "--function",
        "{out_dir}/{graph_name}",
        "--order",
        "{out_dir}/{graph_name}.pthash.order",
        "--person-function",
        "{out_dir}/{graph_name}.persons.pthash",
        "--num-nodes",
        "$(cat {out_dir}/{graph_name}.nodes.count.txt)",
        "{in_dir}",
        "{out_dir}/{graph_name}",
    ],
    CompressionStep.MPH_LABELS: [
        "{rust_executable_dir}/swh-graph-compress",
        "pthash-labels",
        "--num-labels",
        "$(cat {out_dir}/{graph_name}.labels.count.txt)",
        "<(zstdcat {out_dir}/{graph_name}.labels.csv.zst)",
        "{out_dir}/{graph_name}.labels.pthash",
    ],
    CompressionStep.LABELS_ORDER: [
        "{rust_executable_dir}/swh-graph-compress",
        "pthash-labels-order",
        "--num-labels",
        "$(cat {out_dir}/{graph_name}.labels.count.txt)",
        "<(zstdcat {out_dir}/{graph_name}.labels.csv.zst)",
        "{out_dir}/{graph_name}.labels.pthash",
        "{out_dir}/{graph_name}.labels.pthash.order",
    ],
    CompressionStep.FCL_LABELS: [
        "{rust_executable_dir}/swh-graph-compress",
        "fcl",
        "--num-lines",
        "$(cat {out_dir}/{graph_name}.labels.count.txt)",
        "<(zstdcat {out_dir}/{graph_name}.labels.csv.zst)",
        "{out_dir}/{graph_name}.labels.fcl",
    ],
    CompressionStep.EDGE_LABELS: [
        "{rust_executable_dir}/swh-graph-extract",
        "edge-labels",
        "--allowed-node-types",
        "{object_types}",
        "--mph-algo",
        "pthash",
        "--function",
        "{out_dir}/{graph_name}",
        "--order",
        "{out_dir}/{graph_name}.pthash.order",
        "--label-name-mphf",
        "{out_dir}/{graph_name}.labels.pthash",
        "--label-name-order",
        "{out_dir}/{graph_name}.labels.pthash.order",
        "--num-nodes",
        "$(cat {out_dir}/{graph_name}.nodes.count.txt)",
        "{in_dir}",
        "{out_dir}/{graph_name}",
    ],
    CompressionStep.EDGE_LABELS_TRANSPOSE: [
        "{rust_executable_dir}/swh-graph-extract",
        "edge-labels",
        "--allowed-node-types",
        "{object_types}",
        "--mph-algo",
        "pthash",
        "--function",
        "{out_dir}/{graph_name}",
        "--order",
        "{out_dir}/{graph_name}.pthash.order",
        "--label-name-mphf",
        "{out_dir}/{graph_name}.labels.pthash",
        "--label-name-order",
        "{out_dir}/{graph_name}.labels.pthash.order",
        "--num-nodes",
        "$(cat {out_dir}/{graph_name}.nodes.count.txt)",
        "--transposed",
        "{in_dir}",
        "{out_dir}/{graph_name}-transposed",
    ],
    CompressionStep.EDGE_LABELS_EF: [
        "{rust_executable_dir}/swh-graph-index",
        "labels-ef",
        "{out_dir}/{graph_name}-labelled",
        "$((1+ $(cat {out_dir}/{graph_name}.nodes.count.txt)))",
    ],
    CompressionStep.EDGE_LABELS_TRANSPOSE_EF: [
        "{rust_executable_dir}/swh-graph-index",
        "labels-ef",
        "{out_dir}/{graph_name}-transposed-labelled",
        "$((1+ $(cat {out_dir}/{graph_name}.nodes.count.txt)))",
    ],
    CompressionStep.STATS: [
        "{rust_executable_dir}/swh-graph-compress",
        "stats",
        "--graph",
        "{out_dir}/{graph_name}",
        "--stats",
        "{out_dir}/{graph_name}.stats",
    ],
    CompressionStep.CLEAN_TMP: [
        "rm",
        "-rf",
        "{out_dir}/{graph_name}-base.*",
        "{out_dir}/{graph_name}-bfs-simplified.*",
        "{out_dir}/{graph_name}-bfs.order",
        "{out_dir}/{graph_name}-llp.order",
        "{out_dir}/{graph_name}.nodes/",
        "{out_dir}/{graph_name}.persons/",
        "{out_dir}/{graph_name}-bfs.roots.txt",
        "{tmp_dir}",
    ],
}


def do_step(step, conf) -> "List[RunResult]":
    from .shell import Command, CommandException

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

    cmd = " ".join(STEP_ARGV[step]).format(
        **{k: shlex.quote(str(v)) for (k, v) in conf.items()}
    )
    cmd_env = os.environ.copy()
    cmd_env["TMPDIR"] = conf["tmp_dir"]
    cmd_env["TZ"] = "UTC"
    cmd_env["RUST_MIN_STACK"] = "8388608"  # 8MiB; avoids stack overflows in LLP
    command = Command.bash(
        "-c",
        cmd,
        env=cmd_env,
        encoding="utf8",
        stderr=subprocess.STDOUT,
    )._run(stdin=None, stdout=subprocess.PIPE)
    step_logger.info("Running: %s", cmd)

    with command.proc.stdout as stdout:
        for line in stdout:
            step_logger.info(line.rstrip())
    try:
        results = command.wait()
    except CommandException as e:
        raise CompressionSubprocessError(
            f"Compression step {step} returned non-zero exit code {e.returncode}",
            log_path,
        )
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
    return results


def compress(
    graph_name: str,
    in_dir: Path,
    out_dir: Path,
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

    conf = check_config_compress(conf, graph_name, in_dir, out_dir)

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
