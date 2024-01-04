# Copyright (C) 2019-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
from pathlib import Path
import shlex
import sys
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


DEFAULT_CONFIG: Dict[str, Tuple[str, Any]] = {
    "graph": ("dict", {"cls": "local", "grpc_server": {}})
}


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
    if "graph" not in config:
        raise ValueError('no "graph" stanza found in configuration')
    if "grpc_server" not in config["graph"]:
        raise ValueError(
            'no "grpc_server" stanza found in the "graph" section of configuration'
        )
    config["graph"]["grpc_server"]["path"] = graph
    app = make_app(config=config)

    aiohttp.web.run_app(app, host=host, port=port)


@graph_cli_group.command(name="grpc-serve")
@click.option(
    "--port",
    "-p",
    default=None,
    type=click.INT,
    metavar="PORT",
    show_default=True,
    help=(
        "port to bind the server on (note: host is not configurable "
        "for now and will be 0.0.0.0). Defaults to 50091"
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
    config["graph"]["path"] = graph
    if port is None and "port" not in config["graph"]:
        port = 50091
    if port is not None:
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

    try:
        webgraph.compress(graph_name, input_dataset, output_directory, steps, conf)
    except webgraph.CompressionSubprocessError as e:
        try:
            if e.log_path.is_file():
                with e.log_path.open("rb") as f:
                    if e.log_path.stat().st_size > 1000:
                        f.seek(-1000, 2)  # read only the last 1kB
                        f.readline()  # skip first line, might be partial
                        sys.stderr.write("[...]\n")
                    sys.stderr.write("\n")
                    sys.stderr.flush()
                    sys.stderr.buffer.write(f.read())
                    sys.stderr.flush()
        except Exception:
            raise
            pass
        raise click.ClickException(e.message)


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
    "--max-ram",
    help="""Value to pass to -Xmx for Java processes""",
)
@click.option(
    "--batch-size",
    type=int,
    help="""Default value for compression tasks handling objects in batch""",
)
@click.option(
    "--grpc-api", help="""Default value for the <hostname>:<port> of the gRPC server"""
)
@click.option(
    "--s3-athena-output-location",
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
    "--export-name",
    required=False,
    type=str,
    help="""Should be a date and optionally a flavor, which will be used
    as directory name for the export (not the compressed graph).
    For example: ``2022-04-25`` or ``2022-11-12_staging``.
    Defaults to the value of --dataset-name""",
)
@click.option(
    "--previous-dataset-name",
    required=False,
    type=str,
    help="""When regenerating a derived dataset, this can be set to the name of
    a previous dataset the derived dataset was generated for.
    Some results from the previous generated dataset will be reused to speed-up
    regeneration.""",
)
@click.option(
    "--luigi-config",
    type=PathlibPath(),
    help="""Extra options to add to ``luigi.cfg``, following the same format.
    This overrides any option that would be other set automatically.""",
)
@click.option(
    "--retry-luigi-delay",
    type=int,
    default=10,
    help="""Time to wait before re-running Luigi, if some tasks are pending
    but stuck.""",
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
    max_ram: Optional[str],
    batch_size: Optional[int],
    grpc_api: Optional[str],
    s3_athena_output_location: Optional[str],
    dataset_name: str,
    export_name: Optional[str],
    previous_dataset_name: Optional[str],
    retry_luigi_delay: int,
    luigi_config: Optional[Path],
    luigi_param: List[str],
):
    r"""
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

    Or, to compute a derived dataset::

        swh graph luigi \
                --graph-base-directory /dev/shm/swh-graph/default/ \
                --base-directory /poolswh/softwareheritage/vlorentz/ \
                --athena-prefix swh \
                --dataset-name 2022-04-25 \
                --s3-athena-output-location s3://some-bucket/tmp/athena \
                -- \
                --log-level INFO \
                FindEarliestRevisions \
                --scheduler-url http://localhost:50092/ \
                --blob-filter citation
    """
    import configparser
    import os
    import secrets
    import socket
    import subprocess
    import tempfile
    import time

    import luigi
    import psutil

    # Popular the list of subclasses of luigi.Task
    import swh.dataset.luigi  # noqa
    import swh.graph.luigi  # noqa

    config = configparser.ConfigParser()

    # By default, Luigi always returns code 0.
    # See https://luigi.readthedocs.io/en/stable/configuration.html#retcode
    config["retcode"] = {
        "already_running": "128",
        "missing_data": "129",
        "not_run": "130",
        "task_failed": "131",
        "scheduling_error": "132",
        "unhandled_exception": "133",
    }

    if max_ram:
        if max_ram.endswith("G"):
            max_ram_mb = int(max_ram[:-1]) * 1024
        elif max_ram.endswith("M"):
            max_ram_mb = int(max_ram[:-1])
        else:
            raise click.ClickException(
                "--max-ram must be an integer followed by M or G"
            )
    else:
        max_ram_mb = psutil.virtual_memory().total // (1024 * 1024)

    # Only used by the local scheduler; otherwise it needs to be configured
    # in luigid.cfg
    hostname = socket.getfqdn()
    config["resources"] = {
        f"{hostname}_ram_mb": str(max_ram_mb),
    }

    export_name = export_name or dataset_name

    export_path = base_directory / export_name
    dataset_path = base_directory / dataset_name

    default_values = dict(
        local_export_path=export_path,
        local_graph_path=dataset_path / "compressed",
        derived_datasets_path=dataset_path,
        topological_order_dir=dataset_path / "topology/",
        origin_contributors_path=dataset_path / "datasets/contribution_graph.csv.zst",
        origin_urls_path=dataset_path / "datasets/origin_urls.csv.zst",
        export_id=f"{export_name}-{secrets.token_hex(10)}",
        export_name=export_name,
        dataset_name=dataset_name,
        previous_dataset_name=previous_dataset_name,
    )

    if graph_base_directory:
        default_values["local_graph_path"] = graph_base_directory

    if s3_prefix:
        default_values["s3_export_path"] = f"{s3_prefix.rstrip('/')}/{export_name}"
        default_values[
            "s3_graph_path"
        ] = f"{s3_prefix.rstrip('/')}/{dataset_name}/compressed"

    if s3_athena_output_location:
        default_values["s3_athena_output_location"] = s3_athena_output_location

    if max_ram:
        default_values["max_ram_mb"] = max_ram_mb

    if batch_size:
        default_values["batch_size"] = batch_size

    if grpc_api:
        default_values["grpc_api"] = grpc_api

    if base_sensitive_directory:
        sensitive_path = base_sensitive_directory / dataset_name
        default_values["deanonymized_origin_contributors_path"] = (
            sensitive_path / "datasets/contributors_deanonymized.csv.zst"
        )
        default_values["deanonymization_table_path"] = (
            sensitive_path / "persons_sha256_to_name.csv.zst"
        )

    if previous_dataset_name:
        default_values["previous_derived_datasets_path"] = (
            base_directory / previous_dataset_name
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

        while True:
            start_time = time.time()
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

            # If all tasks are stuck, run another loop to wait for them to be unstuck.
            # The normal way to do this would be to set config["worker"]["keep_alive"]
            # to True, but we do it this way to force Luigi to recompute resources
            # because most tasks in compressed_graph.py can only know their resource
            # usage after ExtractNodes and ExtractPersons ran.
            if proc.returncode != int(config["retcode"]["not_run"]):
                break

            # wait a few seconds between loops to avoid wasting CPU
            time.sleep(max(0, retry_luigi_delay - (time.time() - start_time)))
        exit(proc.returncode)


def main():
    return graph_cli_group(auto_envvar_prefix="SWH_GRAPH")


if __name__ == "__main__":
    main()
