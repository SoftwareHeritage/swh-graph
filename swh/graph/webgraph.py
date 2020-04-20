# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""WebGraph driver

"""

import logging
import os
import subprocess

from enum import Enum
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Set
from click import ParamType

from swh.graph.config import check_config_compress


class CompressionStep(Enum):
    MPH = 1
    BV = 2
    BV_OBL = 3
    BFS = 4
    PERMUTE = 5
    PERMUTE_OBL = 6
    STATS = 7
    TRANSPOSE = 8
    TRANSPOSE_OBL = 9
    MAPS = 10
    CLEAN_TMP = 11

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
    CompressionStep.MPH: [
        "{java}",
        "it.unimi.dsi.sux4j.mph.GOVMinimalPerfectHashFunction",
        "--temp-dir",
        "{tmp_dir}",
        "{out_dir}/{graph_name}.mph",
        "<( zstdcat {in_dir}/{graph_name}.nodes.csv.zst )",
    ],
    # use process substitution (and hence FIFO) above as MPH class load the
    # entire file in memory when reading from stdin
    CompressionStep.BV: [
        "zstdcat",
        "{in_dir}/{graph_name}.edges.csv.zst",
        "|",
        "{java}",
        "it.unimi.dsi.big.webgraph.ScatteredArcsASCIIGraph",
        "--temp-dir",
        "{tmp_dir}",
        "--function",
        "{out_dir}/{graph_name}.mph",
        "{out_dir}/{graph_name}-bv",
    ],
    CompressionStep.BV_OBL: [
        "{java}",
        "it.unimi.dsi.big.webgraph.BVGraph",
        "--list",
        "{out_dir}/{graph_name}-bv",
    ],
    CompressionStep.BFS: [
        "{java}",
        "it.unimi.dsi.law.big.graph.BFS",
        "{out_dir}/{graph_name}-bv",
        "{out_dir}/{graph_name}.order",
    ],
    CompressionStep.PERMUTE: [
        "{java}",
        "it.unimi.dsi.big.webgraph.Transform",
        "mapOffline",
        "{out_dir}/{graph_name}-bv",
        "{out_dir}/{graph_name}",
        "{out_dir}/{graph_name}.order",
        "{batch_size}",
        "{tmp_dir}",
    ],
    CompressionStep.PERMUTE_OBL: [
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
        "zstdcat",
        "{in_dir}/{graph_name}.nodes.csv.zst",
        "|",
        "{java}",
        "org.softwareheritage.graph.backend.MapBuilder",
        "{out_dir}/{graph_name}",
        "{tmp_dir}",
    ],
    CompressionStep.CLEAN_TMP: [
        "rm",
        "-rf",
        "{out_dir}/{graph_name}-bv.graph",
        "{out_dir}/{graph_name}-bv.obl",
        "{out_dir}/{graph_name}-bv.offsets",
        "{tmp_dir}",
    ],
}


class StepOption(ParamType):
    """click type for specifying a compression step on the CLI

    parse either individual steps, specified as step names or integers, or step
    ranges

    """

    name = "compression step"

    def convert(self, value, param, ctx) -> Set[CompressionStep]:
        steps: Set[CompressionStep] = set()

        specs = value.split(",")
        for spec in specs:
            if "-" in spec:  # step range
                (raw_l, raw_r) = spec.split("-", maxsplit=1)
                if raw_l == "":  # no left endpoint
                    raw_l = COMP_SEQ[0].name
                if raw_r == "":  # no right endpoint
                    raw_r = COMP_SEQ[-1].name
                l_step = self.convert(raw_l, param, ctx)
                r_step = self.convert(raw_r, param, ctx)
                if len(l_step) != 1 or len(r_step) != 1:
                    self.fail(f"invalid step specification: {value}, " f"see --help")
                l_idx = l_step.pop()
                r_idx = r_step.pop()
                steps = steps.union(
                    set(map(CompressionStep, range(l_idx.value, r_idx.value + 1)))
                )
            else:  # singleton step
                try:
                    steps.add(CompressionStep(int(spec)))  # integer step
                except ValueError:
                    try:
                        steps.add(CompressionStep[spec.upper()])  # step name
                    except KeyError:
                        self.fail(
                            f"invalid step specification: {value}, " f"see --help"
                        )

        return steps


def do_step(step, conf):
    cmd = " ".join(STEP_ARGV[step]).format(**conf)

    cmd_env = os.environ.copy()
    cmd_env["JAVA_TOOL_OPTIONS"] = conf["java_tool_options"]
    cmd_env["CLASSPATH"] = conf["classpath"]

    logging.info(f"running: {cmd}")
    process = subprocess.Popen(
        ["/bin/bash", "-c", cmd],
        env=cmd_env,
        encoding="utf8",
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    with process.stdout as stdout:
        for line in stdout:
            logging.info(line.rstrip())
    rc = process.wait()
    if rc != 0:
        raise RuntimeError(
            f"compression step {step} returned non-zero " f"exit code {rc}"
        )
    else:
        return rc


def compress(
    graph_name: str,
    in_dir: Path,
    out_dir: Path,
    steps: Set[CompressionStep] = set(COMP_SEQ),
    conf: Dict[str, str] = {},
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

    """
    if not steps:
        steps = set(COMP_SEQ)

    conf = check_config_compress(conf, graph_name, in_dir, out_dir)

    compression_start_time = datetime.now()
    logging.info(f"starting compression at {compression_start_time}")
    seq_no = 0
    for step in COMP_SEQ:
        if step not in steps:
            logging.debug(f"skipping compression step {step}")
            continue
        seq_no += 1
        step_start_time = datetime.now()
        logging.info(
            f"starting compression step {step} "
            f"({seq_no}/{len(steps)}) at {step_start_time}"
        )
        do_step(step, conf)
        step_end_time = datetime.now()
        step_duration = step_end_time - step_start_time
        logging.info(
            f"completed compression step {step} "
            f"({seq_no}/{len(steps)}) "
            f"at {step_end_time} in {step_duration}"
        )
    compression_end_time = datetime.now()
    compression_duration = compression_end_time - compression_start_time
    logging.info(f"completed compression in {compression_duration}")
