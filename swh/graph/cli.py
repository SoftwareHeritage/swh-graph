# Copyright (C) 2019-2025  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
import os
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
@click.option(
    "--profile",
    type=str,
    help="Which Rust profile to use executables from, usually 'release' "
    "(the default) or 'debug'.",
)
@click.pass_context
def graph_cli_group(ctx, config_file, profile):
    """Software Heritage graph tools."""
    from swh.core import config

    ctx.ensure_object(dict)
    if not config_file:
        config_file = os.environ.get("SWH_CONFIG_FILENAME")
    conf = config.read(config_file, DEFAULT_CONFIG)
    if "graph" not in conf:
        raise ValueError(
            'no "graph" stanza found in configuration file %s' % config_file
        )
    ctx.obj["config"] = conf

    if profile is not None:
        conf["profile"] = profile


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
    "--graph",
    "-g",
    "graph_path",
    default=None,
    metavar="GRAPH",
    help="compressed graph basename",
)
@click.pass_context
def serve(ctx, host, port, graph_path):
    """Run the compressed graph HTTP RPC service.

    The documentation of the HTTP RPC API is available on
    https://docs.softwareheritage.org/devel/swh-graph/api.html
    """
    import aiohttp.web

    from swh.graph.http_rpc_server import make_app

    config = ctx.obj["config"]
    config_graph = config["graph"]
    cls = config_graph["cls"]
    if cls in ("local", "local_rust"):
        if graph_path is None:
            raise ValueError(
                "Please, specify the graph path (-g <path>) for a 'local' rpc instance"
            )
        # Only required when spawning a local rpc instance
        config["graph"]["grpc_server"]["path"] = graph_path
    elif cls == "remote":
        if "grpc_server" not in config_graph:
            raise ValueError(
                'Please, specify the "grpc_server" key configuration in the "graph" section'
            )
    else:
        raise ValueError(
            f'Value for "cls" must be "local"/"local_rust" or "remote", not {cls!r}'
        )

    app = make_app(config=config)

    aiohttp.web.run_app(app, host=host, port=port)


@graph_cli_group.command(name="download")
@click.option(
    "--s3-url",
    default=None,
    help="S3 directory containing the graph to download. "
    "Defaults to '{s3_prefix}/{name}/compressed/'",
)
@click.option(
    "--s3-prefix",
    default="s3://softwareheritage/graph/",
    help="Base directory of Software Heritage's graphs on S3",
)
@click.option(
    "--name",
    default=None,
    help="Name of the dataset to download. This is an ISO8601 date, optionally with a "
    "suffix. See https://docs.softwareheritage.org/devel/swh-export/graph/dataset.html",
)
@click.option(
    "--parallelism",
    "-j",
    default=5,
    help="Number of threads used to download/decompress files.",
)
@click.argument(
    "target-dir",
    type=click.Path(
        file_okay=False,
        writable=True,
        path_type=Path,
    ),
)
@click.pass_context
def download(
    ctx,
    s3_url: Optional[str],
    s3_prefix: str,
    name: Optional[str],
    parallelism: int,
    target_dir: Path,
):
    """Downloads a compressed SWH graph to the given target directory.

    If some files fail to be fully downloaded, their downloads will be
    resumed when re-executing the same download command.
    """
    from swh.graph.download import GraphDownloader

    if s3_url and name:
        raise click.ClickException("--s3-url and --name are mutually exclusive")
    elif not s3_url and not name:
        raise click.ClickException("Either --s3-url or --name must be provided")
    elif not s3_url:
        s3_url = f"{s3_prefix.rstrip('/')}/{name}/compressed/"

    target_dir.mkdir(parents=True, exist_ok=True)

    graph_downloader = GraphDownloader(
        local_path=target_dir,
        s3_url=s3_url,
        parallelism=parallelism,
    )

    while not graph_downloader.download():
        click.echo(
            "Some dataset files were not fully downloaded, resuming their downloads."
        )
    click.echo(f"Graph dataset {name} successfully downloaded to path {target_dir}.")


@graph_cli_group.command(name="list-datasets")
@click.option(
    "--s3-bucket",
    default="softwareheritage",
    help="S3 bucket name containing Software Heritage graph datasets. "
    "Defaults to 'sotwareheritage'",
)
@click.pass_context
def list_datasets(
    ctx,
    s3_bucket: str,
):
    """List graph datasets available for download.

    Print the names of the Software Heritage graph datasets that can be
    downloaded with the following command:

    $ swh graph download --name <dataset_name> <target_directory>

    The list may contain datasets that are not suitable for production, or not yet
    fully available. See
    https://docs.softwareheritage.org/devel/swh-export/graph/dataset.html
    for the official list of datasets, along with release notes.
    """
    import boto3
    from botocore import UNSIGNED
    from botocore.client import Config

    click.echo(
        "The following list is automatically generated from Software Heritage's "
        "S3 bucket.",
        err=True,
    )
    click.echo(
        "It may contain datasets that are not suitable for production, or not yet "
        "fully available.",
        err=True,
    )
    click.echo(
        "See https://docs.softwareheritage.org/devel/swh-export/graph/dataset.html "
        "for the official list of datasets, along with release notes.",
        err=True,
    )

    s3_client = boto3.client("s3", config=Config(signature_version=UNSIGNED))
    paginator = s3_client.get_paginator("list_objects_v2")

    for dataset_prefix in [
        prefix["Prefix"]
        for page in paginator.paginate(Bucket=s3_bucket, Prefix="graph/", Delimiter="/")
        for prefix in page["CommonPrefixes"]
    ]:
        if "Contents" in s3_client.list_objects_v2(
            Bucket=s3_bucket,
            Prefix=dataset_prefix + "compressed/",
            Delimiter="/",
        ):
            click.echo(dataset_prefix.split("/")[1])


@graph_cli_group.command(name="reindex")
@click.option(
    "--force",
    is_flag=True,
    help="Regenerate files even if they already exist. Implies --ef",
)
@click.option(
    "--ef", is_flag=True, help="Regenerate .ef files even if they already exist"
)
@click.argument(
    "graph",
    type=click.Path(
        writable=True,
    ),
)
@click.pass_context
def reindex(ctx, force: bool, ef: bool, graph: str):
    """Reindex a SWH GRAPH to the latest graph format.

    GRAPH should be composed of the graph folder followed by the graph prefix
    (by default "graph") eg. "graph_folder/graph".
    """
    import os.path

    from swh.graph.shell import Rust

    ef = ef or force
    conf = ctx.obj["config"]
    if "profile" not in conf:
        conf["profile"] = "release"

    if (
        ef
        or not os.path.exists(f"{graph}.ef")
        or not os.path.exists(f"{graph}-transposed.ef")
    ):
        logger.info("Recreating Elias-Fano indexes on adjacency lists")
        Rust("swh-graph-index", "ef", f"{graph}", conf=conf).run()
        Rust("swh-graph-index", "ef", f"{graph}-transposed", conf=conf).run()

    if (
        ef
        or not os.path.exists(f"{graph}-labelled.ef")
        or not os.path.exists(f"{graph}-labelled-transposed.ef")
    ):
        with open(f"{graph}.nodes.count.txt", "rt") as f:
            node_count = f.read().strip()

        # ditto
        logger.info("Recreating Elias-Fano indexes on arc labels")
        Rust(
            "swh-graph-index",
            "labels-ef",
            f"{graph}-labelled",
            node_count,
            conf=conf,
        ).run()
        Rust(
            "swh-graph-index",
            "labels-ef",
            f"{graph}-transposed-labelled",
            node_count,
            conf=conf,
        ).run()

    node2type_fname = f"{graph}.node2type.bin"
    if force or not os.path.exists(node2type_fname):
        logger.info("Creating node2type.bin")
        if os.path.exists(node2type_fname):
            os.unlink(node2type_fname)
        Rust("swh-graph-node2type", graph, conf=conf).run()


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
@click.option("--graph", "-g", metavar="GRAPH", help="compressed graph basename")
@click.pass_context
def grpc_serve(ctx, port, graph):
    """Run the compressed graph gRPC service.

    This command uses execve to execute the Rust GRPC service.

    The documentation of the gRPC API is available on
    https://docs.softwareheritage.org/devel/swh-graph/grpc-api.html
    """
    import os

    from swh.graph.grpc_server import build_rust_grpc_server_cmdline

    config = ctx.obj["config"]

    profile = ctx.obj["config"].get("profile", "release")
    config["graph"]["profile"] = profile

    if graph is not None:
        config["graph"]["path"] = graph
    else:
        if "path" not in config["graph"]:
            raise ValueError("No graph base name provided")

    if port is None:
        port = config["graph"].get("port", 50091)
    config["graph"]["port"] = port

    logger.debug("Building gPRC server command line")
    cmd, port = build_rust_grpc_server_cmdline(**config["graph"])

    rust_bin = cmd[0]

    # XXX: shlex.join() is in 3.8
    # logger.info("Starting gRPC server: %s", shlex.join(cmd))
    logger.info("Starting gRPC server: %s", " ".join(shlex.quote(x) for x in cmd))
    os.execvp(rust_bin, cmd)


@graph_cli_group.command()
@click.option(
    "--input-dataset",
    "-i",
    type=PathlibPath(),
    help="graph dataset directory, in ORC format",
)
@click.option(
    "--sensitive-input-dataset",
    type=PathlibPath(),
    help="graph sensitive dataset directory, in ORC format",
)
@click.option(
    "--output-directory",
    "-o",
    type=PathlibPath(),
    help="directory where to store compressed graph",
)
@click.option(
    "--sensitive-output-directory",
    type=PathlibPath(),
    help="directory where to store sensitive compressed graph data",
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
@click.option("--check-flavor", type=str, help="Check flavor")
@click.pass_context
def compress(
    ctx,
    input_dataset,
    output_directory,
    sensitive_input_dataset,
    sensitive_output_directory,
    graph_name,
    steps,
    check_flavor,
):
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
        conf["profile"] = ctx.obj["config"]["profile"]
    except KeyError:
        conf["profile"] = "release"  # use release builds by default

    if check_flavor is None:
        # TODO: see is this can be None
        check_flavor = conf.get("check_flavor", "full")
    conf["check_flavor"] = check_flavor

    try:
        webgraph.compress(
            graph_name,
            input_dataset,
            output_directory,
            sensitive_input_dataset,
            sensitive_output_directory,
            check_flavor,
            steps,
            conf,
        )
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


@graph_cli_group.command(hidden=True)
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
    For example: ``/poolswh/softwareheritage-sensitive/``.""",
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
    help="""Maximum RAM that some scripts will try not to exceed""",
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
    "--export-base-directory",
    required=False,
    type=PathlibPath(),
    help="""Overrides the path of the export to use. Defaults to the value of
    ``--base-directory``.""",
)
@click.option(
    "--dataset-name",
    required=True,
    type=str,
    help="""Should be a date and optionally a flavor, which will be used
    as directory name. For example: ``2022-04-25`` or ``2022-11-12_staging``.""",
)
@click.option(
    "--parent-dataset-name",
    required=False,
    type=str,
    help="""When generating a subdataset (eg. ``2024-08-23-python3k``), this is the name
    of a full export (eg. ``2024-08-23``) the subdataset should be built from.""",
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
    "--parent-export-name",
    required=False,
    type=str,
    help="""When generating a subdataset (eg. ``2024-08-23-python3k``), this is the name
    of a full export (eg. ``2024-08-23``) the subdataset should be built from.
    Defaults to the value of --parent-dataset-name""",
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
    export_base_directory: Optional[Path],
    base_sensitive_directory: Optional[Path],
    s3_prefix: Optional[str],
    athena_prefix: Optional[str],
    max_ram: Optional[str],
    batch_size: Optional[int],
    grpc_api: Optional[str],
    s3_athena_output_location: Optional[str],
    dataset_name: str,
    parent_dataset_name: Optional[str],
    parent_export_name: Optional[str],
    export_name: Optional[str],
    retry_luigi_delay: int,
    luigi_config: Optional[Path],
    luigi_param: List[str],
):
    r"""
    Internal command of swh-graph. Use 'swh datasets luigi' instead.
    """
    import configparser
    import os
    import secrets
    import socket
    import subprocess
    import sys
    import tempfile
    import time

    import luigi
    import psutil

    from swh.core.config import merge_configs

    if "pytest" not in sys.modules:
        raise click.ClickException(
            "Use 'swh datasets luigi' instead of 'swh graph luigi'."
        )

    # Popular the list of subclasses of luigi.Task
    import swh.export.luigi  # noqa
    from swh.graph.config import check_config, check_config_compress
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

    base_rust_executable_dir = ctx.obj["config"].get("base_rust_executable_dir")
    swh_config = check_config(ctx.obj["config"], base_rust_executable_dir)
    if "compress" in swh_config.get("graph", {}):
        swh_config = check_config_compress(
            swh_config,
            graph_name=swh_config["graph"].get("path"),
            in_dir=swh_config["graph"]["compress"].get("in_dir"),
            out_dir=swh_config["graph"]["compress"].get("out_dir"),
            sensitive_in_dir=swh_config["graph"]["compress"].get("sensitive_in_dir"),
            sensitive_out_dir=swh_config["graph"]["compress"].get("sensitive_out_dir"),
            check_flavor=swh_config["graph"]["compress"].get("check_flavor"),
        )

    export_name = export_name or dataset_name
    parent_export_name = parent_export_name or parent_dataset_name

    export_path = (export_base_directory or base_directory) / export_name
    dataset_path = base_directory / dataset_name

    default_values = dict(
        local_export_path=export_path,
        local_graph_path=dataset_path / "compressed",
        derived_datasets_path=dataset_path,
        topological_order_dir=dataset_path / "topology/",
        origin_urls_path=dataset_path / "origin_urls.csv.zst",
        export_id=f"{export_name}-{secrets.token_hex(10)}",
        export_name=export_name,
        dataset_name=dataset_name,
    )

    if base_sensitive_directory:
        sensitive_path = base_sensitive_directory / dataset_name
        sensitive_export_path = base_sensitive_directory / export_name
        sensitive_graph_path = base_sensitive_directory / export_name
        default_values["local_sensitive_export_path"] = sensitive_export_path
        default_values["local_sensitive_graph_path"] = sensitive_graph_path
        default_values["deanonymized_origin_contributors_path"] = (
            sensitive_path / "contributors_deanonymized.csv.zst"
        )
        default_values["deanonymization_table_path"] = (
            sensitive_path / "persons_sha256_to_name.csv.zst"
        )

    default_values = merge_configs(default_values, swh_config)

    if graph_base_directory:
        default_values["local_graph_path"] = graph_base_directory

    if s3_prefix:
        default_values["s3_export_path"] = f"{s3_prefix.rstrip('/')}/{export_name}"
        default_values["s3_graph_path"] = (
            f"{s3_prefix.rstrip('/')}/{dataset_name}/compressed"
        )
        if parent_dataset_name:
            default_values["s3_parent_export_path"] = (
                f"{s3_prefix.rstrip('/')}/{parent_export_name}"
            )
            default_values["s3_parent_graph_path"] = (
                f"{s3_prefix.rstrip('/')}/{parent_dataset_name}/compressed"
            )

    if s3_athena_output_location:
        default_values["s3_athena_output_location"] = s3_athena_output_location

    if max_ram:
        default_values["max_ram_mb"] = max_ram_mb

    if batch_size:
        default_values["batch_size"] = batch_size

    if grpc_api:
        default_values["grpc_api"] = grpc_api

    if athena_prefix:
        default_values["athena_db_name"] = (
            f"{athena_prefix}_{dataset_name.replace('-', '')}"
        )
        if parent_dataset_name:
            default_values["athena_parent_db_name"] = (
                f"{athena_prefix}_{parent_dataset_name.replace('-', '')}"
            )

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
                    "swh.export.luigi",
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


@graph_cli_group.command(name="find-context")
@click.option(
    "-g",
    "--graph-grpc-server",
    default="localhost:50091",
    metavar="GRAPH_GRPC_SERVER",
    show_default=True,
    help="Graph RPC server address: as host:port",
)
@click.option(
    "-c",
    "--content-swhid",
    default="swh:1:cnt:3b997e8ef2e38d5b31fb353214a54686e72f0870",
    metavar="CNTSWHID",
    show_default=True,
    help="SWHID of the content",
)
@click.option(
    "-f",
    "--filename",
    default="",
    metavar="FILENAME",
    show_default=True,
    help="Name of file to search for",
)
@click.option(
    "-o",
    "--origin-url",
    default="",
    metavar="ORIGINURL",
    show_default=True,
    help="URL of the origin where we look for a content",
)
@click.option(
    "--all-origins/--no-all-origins",
    default=False,
    help="Compute fqswhid for all origins",
)
@click.option(
    "--fqswhid/--no-fqswhid",
    default=True,
    help="Compute fqswhid. If disabled, print only the origins.",
)
@click.option(
    "--trace/--no-trace",
    default=False,
    help="Print nodes examined while building fully qualified SWHID.",
)
@click.option(
    "--random-origin/--no-random-origin",
    default=True,
    help="Compute fqswhid for a random origin",
)
@click.pass_context
def find_context(ctx, **kwargs):
    """Utility to get the fully qualified SWHID for a given core SWHID.
    Uses the graph traversal to find the shortest path to an origin, and
    retains the first seen revision or release as anchor for cnt and dir types."""
    from swh.graph.find_context import main as lookup

    return lookup(**kwargs)


@graph_cli_group.command(name="link")
@click.argument(
    "source-path", type=click.Path(dir_okay=True, exists=True, path_type=Path)
)
@click.argument(
    "destination-path", type=click.Path(dir_okay=True, writable=True, path_type=Path)
)
@click.option(
    "--verbose", "-v", is_flag=True, default=False, help="Explain what is being done"
)
@click.argument("force-copy", type=click.Path(path_type=Path), nargs=-1)
@click.pass_context
def link(
    ctx,
    source_path: Path,
    destination_path: Path,
    force_copy: Tuple[Path],
    verbose: bool,
):
    """
    Symlink (or copy) an existing graph to the desired location.

    By default, all files are symlinked, but files and directories can be
    specified to be copied instead.

    This functionality is intended for internal use, and is there to ease the
    process of sharing an existing graph between multiple users on the same
    machine.
    """
    import shutil

    from tqdm.contrib.concurrent import thread_map

    destination_path.mkdir(parents=True, exist_ok=True)

    for file_or_dir in source_path.rglob("*"):
        if file_or_dir in force_copy:
            continue
        link_target = destination_path / file_or_dir.relative_to(source_path)
        if file_or_dir.is_dir():
            link_target.mkdir(parents=True, exist_ok=True)
            continue
        link_target.symlink_to(file_or_dir)
        if verbose:
            logger.info(f"Creating symlink from {file_or_dir} to {link_target}")

    def _copy(source_item: Path):
        if verbose:
            logger.info(f"Copying {source_item} to {destination_path}")
        if source_item.is_file():
            shutil.copy(source_item, destination_path)
        else:
            shutil.copytree(source_item, destination_path)

    if len(force_copy) > 0:
        thread_map(_copy, force_copy, desc="Copying files", max_workers=len(force_copy))


def main():
    return graph_cli_group(auto_envvar_prefix="SWH_GRAPH")


if __name__ == "__main__":
    main()
