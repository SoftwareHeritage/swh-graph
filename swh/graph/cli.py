# Copyright (C) 2019-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
from pathlib import Path
import shlex
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set, Tuple

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
    config["graph"]["grpc_server"]["path"] = graph
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


def get_all_subclasses(cls):
    all_subclasses = []

    for subclass in cls.__subclasses__():
        all_subclasses.append(subclass)
        all_subclasses.extend(get_all_subclasses(subclass))

    return all_subclasses


@graph_cli_group.command()
@click.option(
    "--base-directory",
    required=True,
    type=PathlibPath(),
    help="""The base directory where all datasets and compressed graphs are.
    Its subdirectories should be named after a date (and optional flavor).
    For example: ``/poolswh/softwareheritage/``.""",
)
@click.option(
    "--base-sensitive-directory",
    required=False,
    type=PathlibPath(),
    help="""The base directory for any data that should not be publicly available
    (eg. because it contains people's names).
    For example: ``/poolswh/softwareheritage/``.""",
)
@click.option(
    "--athena-prefix",
    required=False,
    type=str,
    help="""A prefix for the Athena Database that will be created and/or used.
    For example: ``swh``.""",
)
@click.option(
    "--s3-prefix",
    required=False,
    type=str,
    help="""The base S3 "directory" where all datasets and compressed graphs are.
    Its subdirectories should be named after a date (and optional flavor).
    For example: ``s3://softwareheritage/graph/``.""",
)
@click.option(
    "--graph-base-directory",
    required=False,
    type=PathlibPath(),
    help="""Overrides the path of the graph to use. Defaults to the value of
    ``{base_directory}/{dataset_name}/{compressed}/``.
    For example: ``/dev/shm/swh-graph/default/``.""",
)
@click.option(
    "--dataset-name",
    required=True,
    type=str,
    help="""Should be a date and optionally a flavor, which will be used
    as directory name. For example: ``2022-04-25`` or ``2022-11-12_staging``.""",
)
@click.option(
    "--luigi-config",
    type=PathlibPath(),
    help="""Extra options to add to ``luigi.cfg``, following the same format.
    This overrides any option that would be other set automatically.""",
)
@click.argument("luigi_param", nargs=-1)
@click.pass_context
def luigi(
    ctx,
    base_directory: Path,
    graph_base_directory: Optional[Path],
    base_sensitive_directory: Optional[Path],
    s3_prefix: Optional[str],
    athena_prefix: Optional[str],
    dataset_name: str,
    luigi_config: Optional[Path],
    luigi_param: List[str],
):
    """
    Calls Luigi with the given task and params, and automatically
    configures paths based on --base-directory and --dataset-name.

    The list of Luigi params should be prefixed with ``--`` so they are not interpreted
    by the ``swh`` CLI. For example::

        swh graph luigi \
                --base-directory ~/tmp/ \
                --dataset-name 2022-12-05_test ListOriginContributors \
                -- \
                RunAll \
                --local-scheduler

    to pass ``RunAll --local-scheduler`` as Luigi params
    """
    import configparser
    import os
    import secrets
    import subprocess
    import tempfile

    import luigi

    # Popular the list of subclasses of luigi.Task
    import swh.dataset.luigi  # noqa
    import swh.graph.luigi  # noqa

    config = configparser.ConfigParser()

    dataset_path = base_directory / dataset_name

    default_values = dict(
        local_export_path=dataset_path,
        export_task_type="ExportGraph",
        compression_task_type="CompressGraph",
        local_graph_path=dataset_path / "compressed",
        topological_order_path=dataset_path / "topology/topological_order_dfs.csv.zst",
        origin_contributors_path=dataset_path / "datasets/contribution_graph.csv.zst",
        export_id=f"{dataset_name}-{secrets.token_hex(10)}",
    )

    if graph_base_directory:
        default_values["local_graph_path"] = graph_base_directory

    if s3_prefix:
        dataset_s3_prefix = f"{s3_prefix.rstrip('/')}/{dataset_name}"
        default_values["s3_export_path"] = dataset_s3_prefix
        default_values["s3_graph_path"] = f"{dataset_s3_prefix}/compressed"

    if base_sensitive_directory:
        sensitive_path = base_sensitive_directory / dataset_name
        default_values["deanonymized_origin_contributors_path"] = (
            sensitive_path / "datasets/contribution_graph.deanonymized.csv.zst"
        )
        default_values["deanonymization_table_path"] = (
            sensitive_path / "persons_sha256_to_name.csv.zst"
        )

    if athena_prefix:
        default_values[
            "athena_db_name"
        ] = f"{athena_prefix}_{dataset_name.replace('-', '')}"

    for task_cls in get_all_subclasses(luigi.Task):
        task_name = task_cls.__name__
        # If the task has an argument with one of the known name, add the default value
        # to its config.
        task_config = {
            arg_name: str(arg_value)
            for arg_name, arg_value in default_values.items()
            if hasattr(task_cls, arg_name)
        }
        if task_config:
            config[task_name] = task_config

    # If any config is provided, add it.
    # This may override default arguments configured above.
    if luigi_config is not None:
        config.read(luigi_config)

    with tempfile.NamedTemporaryFile(mode="w+t", prefix="luigi_", suffix=".cfg") as fd:
        config.write(fd)
        fd.flush()

        proc = subprocess.run(
            [
                "luigi",
                "--module",
                "swh.dataset.luigi",
                "--module",
                "swh.graph.luigi",
                *luigi_param,
            ],
            env={
                "LUIGI_CONFIG_PATH": fd.name,
                **os.environ,
            },
        )
        exit(proc.returncode)


def main():
    return graph_cli_group(auto_envvar_prefix="SWH_GRAPH")


if __name__ == "__main__":
    main()
