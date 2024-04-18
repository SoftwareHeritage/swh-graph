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
    EXTRACT_NODES = 0
    MPH = 10
    CONVERT_MPH = 20
    BV = 30
    BV_OFFSETS = 40
    BV_EF = 50
    BFS = 60
    PERMUTE_AND_SIMPLIFY_BFS = 70
    BFS_EF = 80
    BFS_DCF = 90
    LLP = 100
    COMPOSE_ORDERS = 110
    PERMUTE_LLP = 120
    LLP_OFFSETS = 130
    OBL = 140
    STATS = 150
    TRANSPOSE = 160
    TRANSPOSE_OBL = 170
    MAPS = 180
    EXTRACT_PERSONS = 190
    MPH_PERSONS = 200
    NODE_PROPERTIES = 210
    MPH_LABELS = 220
    FCL_LABELS = 230
    EDGE_LABELS = 240
    EDGE_LABELS_OBL = 250
    EDGE_LABELS_TRANSPOSE_OBL = 260
    CLEAN_TMP = 270

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
        "{java}",
        "org.softwareheritage.graph.compress.ExtractNodes",
        "--format",
        "orc",
        "--temp-dir",
        "{tmp_dir}",
        "--allowed-node-types",
        "{object_types}",
        "{in_dir}",
        "{out_dir}/{graph_name}",
    ],
    CompressionStep.MPH: [
        "{java}",
        "it.unimi.dsi.sux4j.mph.GOVMinimalPerfectHashFunction",
        "--byte-array",
        "--temp-dir",
        "{tmp_dir}",
        "--decompressor",
        "com.github.luben.zstd.ZstdInputStream",
        "{out_dir}/{graph_name}.mph",
        "{out_dir}/{graph_name}.nodes.csv.zst",
    ],
    CompressionStep.CONVERT_MPH: [
        "{java}",
        "org.softwareheritage.graph.utils.Mph2Cmph",
        "{out_dir}/{graph_name}.mph",
        "{out_dir}/{graph_name}.cmph",
    ],
    CompressionStep.BV: [
        "{rust_executable_dir}/swh-graph-extract",
        "bv",
        "--allowed-node-types",
        "{object_types}",
        "--mph-algo",
        "cmph",
        "--function",
        "{out_dir}/{graph_name}",
        "--num-nodes",
        "$(cat {out_dir}/{graph_name}.nodes.count.txt)",
        "{in_dir}",
        "{out_dir}/{graph_name}-base",
    ],
    CompressionStep.BV_OFFSETS: [
        "{rust_executable_dir}/swh-graph-index",
        "offsets",
        "{out_dir}/{graph_name}-base",
    ],
    CompressionStep.BV_EF: [
        "{rust_executable_dir}/swh-graph-index",
        "ef",
        "{out_dir}/{graph_name}-base",
    ],
    CompressionStep.BFS: [
        "{rust_executable_dir}/swh-graph-compress",
        "bfs",
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
    CompressionStep.LLP_OFFSETS: [
        "{rust_executable_dir}/swh-graph-index",
        "offsets",
        "{out_dir}/{graph_name}",
    ],
    CompressionStep.COMPOSE_ORDERS: [
        "{java}",
        "org.softwareheritage.graph.compress.ComposePermutations",
        "{out_dir}/{graph_name}-bfs.order",
        "{out_dir}/{graph_name}-llp.order",
        "{out_dir}/{graph_name}.order",
    ],
    CompressionStep.PERMUTE_LLP: [
        "{rust_executable_dir}/swh-graph-compress",
        "permute",
        "{out_dir}/{graph_name}-base",
        "{out_dir}/{graph_name}",
        "--permutation",
        "{out_dir}/{graph_name}.order",
    ],
    CompressionStep.OBL: [
        "{java}",
        "it.unimi.dsi.big.webgraph.BVGraph",
        "--list",
        "{out_dir}/{graph_name}",
    ],
    CompressionStep.STATS: [
        "{java}",
        "it.unimi.dsi.big.webgraph.Stats",
        "{out_dir}/{graph_name}",
    ],
    CompressionStep.TRANSPOSE: [
        "{java}",
        "it.unimi.dsi.big.webgraph.Transform",
        "transposeOffline",
        "{out_dir}/{graph_name}",
        "{out_dir}/{graph_name}-transposed",
        "{batch_size}",
        "{tmp_dir}",
    ],
    CompressionStep.TRANSPOSE_OBL: [
        "{java}",
        "it.unimi.dsi.big.webgraph.BVGraph",
        "--list",
        "{out_dir}/{graph_name}-transposed",
    ],
    CompressionStep.MAPS: [
        "{java}",
        "org.softwareheritage.graph.compress.NodeMapBuilder",
        "{out_dir}/{graph_name}",
        "{tmp_dir}",
        "< {out_dir}/{graph_name}.nodes.csv.zst",
    ],
    CompressionStep.EXTRACT_PERSONS: [
        "{java}",
        "org.softwareheritage.graph.compress.ExtractPersons",
        "--temp-dir",
        "{tmp_dir}",
        "--allowed-node-types",
        "{object_types}",
        "{in_dir}",
        "{out_dir}/{graph_name}",
    ],
    CompressionStep.MPH_PERSONS: [
        "{java}",
        "it.unimi.dsi.sux4j.mph.GOVMinimalPerfectHashFunction",
        "--byte-array",
        "--decompressor",
        "com.github.luben.zstd.ZstdInputStream",
        "--temp-dir",
        "{tmp_dir}",
        "{out_dir}/{graph_name}.persons.mph",
        "{out_dir}/{graph_name}.persons.csv.zst",
    ],
    CompressionStep.NODE_PROPERTIES: [
        "{java}",
        "org.softwareheritage.graph.compress.WriteNodeProperties",
        "{in_dir}",
        "--allowed-node-types",
        "{object_types}",
        "{out_dir}/{graph_name}",
    ],
    CompressionStep.MPH_LABELS: [
        "{java}",
        "it.unimi.dsi.sux4j.mph.LcpMonotoneMinimalPerfectHashFunction",
        "--byte-array",
        "--temp-dir",
        "{tmp_dir}",
        "--decompressor",
        "com.github.luben.zstd.ZstdInputStream",
        "{out_dir}/{graph_name}.labels.mph",
        "{out_dir}/{graph_name}.labels.csv.zst",
    ],
    CompressionStep.FCL_LABELS: [
        "{java}",
        "it.unimi.dsi.big.util.MappedFrontCodedStringBigList",
        "--decompressor",
        "com.github.luben.zstd.ZstdInputStream",
        "{out_dir}/{graph_name}.labels.fcl",
        "< {out_dir}/{graph_name}.labels.csv.zst",
    ],
    CompressionStep.EDGE_LABELS: [
        "{java}",
        "org.softwareheritage.graph.compress.LabelMapBuilder",
        "--temp-dir",
        "{tmp_dir}",
        "--allowed-node-types",
        "{object_types}",
        "--batch-size",
        "{batch_size}",
        "{in_dir}",
        "{out_dir}/{graph_name}",
    ],
    CompressionStep.EDGE_LABELS_OBL: [
        "{java}",
        "it.unimi.dsi.big.webgraph.labelling.BitStreamArcLabelledImmutableGraph",
        "--list",
        "{out_dir}/{graph_name}-labelled",
    ],
    CompressionStep.EDGE_LABELS_TRANSPOSE_OBL: [
        "{java}",
        "it.unimi.dsi.big.webgraph.labelling.BitStreamArcLabelledImmutableGraph",
        "--list",
        "{out_dir}/{graph_name}-transposed-labelled",
    ],
    CompressionStep.CLEAN_TMP: [
        "rm",
        "-rf",
        "{out_dir}/{graph_name}-base.graph",
        "{out_dir}/{graph_name}-base.offsets",
        "{out_dir}/{graph_name}-base.properties",
        "{out_dir}/{graph_name}-bfs-simplified.graph",
        "{out_dir}/{graph_name}-bfs-simplified.offsets",
        "{out_dir}/{graph_name}-bfs-simplified.properties",
        "{out_dir}/{graph_name}-bfs-transposed.graph",
        "{out_dir}/{graph_name}-bfs-transposed.offsets",
        "{out_dir}/{graph_name}-bfs-transposed.properties",
        "{out_dir}/{graph_name}-bfs.graph",
        "{out_dir}/{graph_name}-bfs.offsets",
        "{out_dir}/{graph_name}-bfs.order",
        "{out_dir}/{graph_name}-bfs.properties",
        "{out_dir}/{graph_name}-llp.order",
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
        # for the stdout of Java processes. These processes can take a long time to
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
    cmd_env["JAVA_TOOL_OPTIONS"] = conf["java_tool_options"]
    cmd_env["CLASSPATH"] = conf["classpath"]
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
          - classpath: java classpath, defaults to swh-graph JAR only
          - java: command to run java VM, defaults to "java"
          - java_tool_options: value for JAVA_TOOL_OPTIONS environment
            variable; defaults to various settings for high memory machines
          - logback: path to a logback.xml configuration file; if not provided
            a temporary one will be created and used
          - max_ram: maximum RAM to use for compression; defaults to available
            virtual memory
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
