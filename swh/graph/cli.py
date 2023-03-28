# Copyright (C) 2019-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
from pathlib import Path
import shlex
from typing import TYPE_CHECKING, Any, Dict, Set, Tuple

# WARNING: do not import unnecessary things here to keep cli startup time under
# control
import click

from swh.core.cli import CONTEXT_SETTINGS, AliasedGroup
from swh.core.cli import swh as swh_cli_group

if TYPE_CHECKING:
    from swh.graph.webgraph import CompressionStep  # noqa

logger = logging.getLogger(__name__)


class StepOption(click.ParamType):
    """click type for specifying a compression step on the CLI

    parse either individual steps, specified as step names or integers, or step
    ranges

    """

    name = "compression step"

    def convert(self, value, param, ctx):  # type: (...) -> Set[CompressionStep]
        from swh.graph.webgraph import COMP_SEQ, CompressionStep  # noqa

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
                    set(CompressionStep(i) for i in range(l_idx.value, r_idx.value + 1))
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


class PathlibPath(click.Path):
    """A Click path argument that returns a pathlib Path, not a string"""

    def convert(self, value, param, ctx):
        return Path(super().convert(value, param, ctx))


DEFAULT_CONFIG: Dict[str, Tuple[str, Any]] = {"graph": ("dict", {})}


@swh_cli_group.group(name="graph", context_settings=CONTEXT_SETTINGS, cls=AliasedGroup)
@click.option(
    "--config-file",
    "-C",
    default=None,
    type=click.Path(
        exists=True,
        dir_okay=False,
    ),
    help="YAML configuration file",
)
@click.pass_context
def graph_cli_group(ctx, config_file):
    """Software Heritage graph tools."""
    from swh.core import config

    ctx.ensure_object(dict)
    conf = config.read(config_file, DEFAULT_CONFIG)
    if "graph" not in conf:
        raise ValueError(
            'no "graph" stanza found in configuration file %s' % config_file
        )
    ctx.obj["config"] = conf


@graph_cli_group.command(name="rpc-serve")
@click.option(
    "--host",
    "-h",
    default="0.0.0.0",
    metavar="IP",
    show_default=True,
    help="host IP address to bind the server on",
)
@click.option(
    "--port",
    "-p",
    default=5009,
    type=click.INT,
    metavar="PORT",
    show_default=True,
    help="port to bind the server on",
)
@click.option(
    "--graph", "-g", required=True, metavar="GRAPH", help="compressed graph basename"
)
@click.pass_context
def serve(ctx, host, port, graph):
    """run the graph RPC service"""
    import aiohttp.web

    from swh.graph.http_rpc_server import make_app

    config = ctx.obj["config"]
    config.setdefault("graph", {})
    config["graph"]["path"] = graph
    app = make_app(config=config)

    aiohttp.web.run_app(app, host=host, port=port)


@graph_cli_group.command(name="grpc-serve")
@click.option(
    "--port",
    "-p",
    default=50091,
    type=click.INT,
    metavar="PORT",
    show_default=True,
    help=(
        "port to bind the server on (note: host is not configurable "
        "for now and will be 0.0.0.0)"
    ),
)
@click.option(
    "--java-home",
    "-j",
    default=None,
    metavar="JAVA_HOME",
    help="absolute path to the Java Runtime Environment (JRE)",
)
@click.option(
    "--graph", "-g", required=True, metavar="GRAPH", help="compressed graph basename"
)
@click.pass_context
def grpc_serve(ctx, port, java_home, graph):
    """start the graph GRPC service

    This command uses execve to execute the java GRPC service.
    """
    import os
    from pathlib import Path

    from swh.graph.grpc_server import build_grpc_server_cmdline

    config = ctx.obj["config"]
    config.setdefault("graph", {})
    config["graph"]["path"] = graph
    config["graph"]["port"] = port

    logger.debug("Building gPRC server command line")
    cmd, port = build_grpc_server_cmdline(**config["graph"])

    java_bin = cmd[0]
    if java_home is not None:
        java_bin = str(Path(java_home) / "bin" / java_bin)

    # XXX: shlex.join() is in 3.8
    # logger.info("Starting gRPC server: %s", shlex.join(cmd))
    logger.info("Starting gRPC server: %s", " ".join(shlex.quote(x) for x in cmd))
    os.execvp(java_bin, cmd)


@graph_cli_group.command()
@click.option(
    "--input-dataset",
    "-i",
    required=True,
    type=PathlibPath(),
    help="graph dataset directory, in ORC format",
)
@click.option(
    "--output-directory",
    "-o",
    required=True,
    type=PathlibPath(),
    help="directory where to store compressed graph",
)
@click.option(
    "--graph-name",
    "-g",
    default="graph",
    metavar="NAME",
    help="name of the output graph (default: 'graph')",
)
@click.option(
    "--steps",
    "-s",
    metavar="STEPS",
    type=StepOption(),
    help="run only these compression steps (default: all steps)",
)
@click.pass_context
def compress(ctx, input_dataset, output_directory, graph_name, steps):
    """Compress a graph using WebGraph

    Input: a directory containing a graph dataset in ORC format

    Output: a directory containing a WebGraph compressed graph

    Compression steps are: (1) extract_nodes, (2) mph, (3) bv, (4) bfs, (5)
    permute_bfs, (6) transpose_bfs, (7) simplify, (8) llp, (9) permute_llp,
    (10) obl, (11) compose_orders, (12) stats, (13) transpose, (14)
    transpose_obl, (15) maps, (16) extract_persons, (17) mph_persons, (18)
    node_properties, (19) mph_labels, (20) fcl_labels, (21) edge_labels, (22)
    edge_labels_obl, (23) edge_labels_transpose_obl, (24) clean_tmp.
    Compression steps can be selected by name or number using --steps,
    separating them with commas; step ranges (e.g., 3-9, 6-, etc.) are also
    supported.

    """
    from swh.graph import webgraph

    try:
        conf = ctx.obj["config"]["graph"]["compress"]
    except KeyError:
        conf = {}  # use defaults

    webgraph.compress(graph_name, input_dataset, output_directory, steps, conf)


def main():
    return graph_cli_group(auto_envvar_prefix="SWH_GRAPH")


if __name__ == "__main__":
    main()
