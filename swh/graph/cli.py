# Copyright (C) 2019-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
from pathlib import Path
import sys
from typing import TYPE_CHECKING, Any, Dict, Set, Tuple

# WARNING: do not import unnecessary things here to keep cli startup time under
# control
import click

from swh.core.cli import CONTEXT_SETTINGS, AliasedGroup
from swh.core.cli import swh as swh_cli_group

if TYPE_CHECKING:
    from swh.graph.webgraph import CompressionStep  # noqa


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
    type=click.Path(exists=True, dir_okay=False,),
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


@graph_cli_group.command("api-client")
@click.option("--host", default="localhost", help="Graph server host")
@click.option("--port", default="5009", help="Graph server port")
@click.pass_context
def api_client(ctx, host, port):
    """client for the graph REST service"""
    from swh.graph import client

    url = "http://{}:{}".format(host, port)
    app = client.RemoteGraphClient(url)

    # TODO: run web app
    print(app.stats())


@graph_cli_group.group("map")
@click.pass_context
def map(ctx):
    """Manage swh-graph on-disk maps"""
    pass


def dump_swhid2node(filename):
    from swh.graph.swhid import SwhidToNodeMap

    for (swhid, int) in SwhidToNodeMap(filename):
        print("{}\t{}".format(swhid, int))


def dump_node2swhid(filename):
    from swh.graph.swhid import NodeToSwhidMap

    for (int, swhid) in NodeToSwhidMap(filename):
        print("{}\t{}".format(int, swhid))


def restore_swhid2node(filename):
    """read a textual SWHID->int map from stdin and write its binary version to
    filename

    """
    from swh.graph.swhid import SwhidToNodeMap

    with open(filename, "wb") as dst:
        for line in sys.stdin:
            (str_swhid, str_int) = line.split()
            SwhidToNodeMap.write_record(dst, str_swhid, int(str_int))


def restore_node2swhid(filename, length):
    """read a textual int->SWHID map from stdin and write its binary version to
    filename

    """
    from swh.graph.swhid import NodeToSwhidMap

    node2swhid = NodeToSwhidMap(filename, mode="wb", length=length)
    for line in sys.stdin:
        (str_int, str_swhid) = line.split()
        node2swhid[int(str_int)] = str_swhid
    node2swhid.close()


@map.command("dump")
@click.option(
    "--type",
    "-t",
    "map_type",
    required=True,
    type=click.Choice(["swhid2node", "node2swhid"]),
    help="type of map to dump",
)
@click.argument("filename", required=True, type=click.Path(exists=True))
@click.pass_context
def dump_map(ctx, map_type, filename):
    """Dump a binary SWHID<->node map to textual format."""
    if map_type == "swhid2node":
        dump_swhid2node(filename)
    elif map_type == "node2swhid":
        dump_node2swhid(filename)
    else:
        raise ValueError("invalid map type: " + map_type)
    pass


@map.command("restore")
@click.option(
    "--type",
    "-t",
    "map_type",
    required=True,
    type=click.Choice(["swhid2node", "node2swhid"]),
    help="type of map to dump",
)
@click.option(
    "--length",
    "-l",
    type=int,
    help="""map size in number of logical records
              (required for node2swhid maps)""",
)
@click.argument("filename", required=True, type=click.Path())
@click.pass_context
def restore_map(ctx, map_type, length, filename):
    """Restore a binary SWHID<->node map from textual format."""
    if map_type == "swhid2node":
        restore_swhid2node(filename)
    elif map_type == "node2swhid":
        if length is None:
            raise click.UsageError(
                "map length is required when restoring {} maps".format(map_type), ctx
            )
        restore_node2swhid(filename, length)
    else:
        raise ValueError("invalid map type: " + map_type)


@map.command("write")
@click.option(
    "--type",
    "-t",
    "map_type",
    required=True,
    type=click.Choice(["swhid2node", "node2swhid"]),
    help="type of map to write",
)
@click.argument("filename", required=True, type=click.Path())
@click.pass_context
def write(ctx, map_type, filename):
    """Write a map to disk sequentially.

    read from stdin a textual SWHID->node mapping (for swhid2node, or a simple
    sequence of SWHIDs for node2swhid) and write it to disk in the requested binary
    map format

    note that no sorting is applied, so the input should already be sorted as
    required by the chosen map type (by SWHID for swhid2node, by int for node2swhid)

    """
    from swh.graph.swhid import NodeToSwhidMap, SwhidToNodeMap

    with open(filename, "wb") as f:
        if map_type == "swhid2node":
            for line in sys.stdin:
                (swhid, int_str) = line.rstrip().split(maxsplit=1)
                SwhidToNodeMap.write_record(f, swhid, int(int_str))
        elif map_type == "node2swhid":
            for line in sys.stdin:
                swhid = line.rstrip()
                NodeToSwhidMap.write_record(f, swhid)
        else:
            raise ValueError("invalid map type: " + map_type)


@map.command("lookup")
@click.option(
    "--graph", "-g", required=True, metavar="GRAPH", help="compressed graph basename"
)
@click.argument("identifiers", nargs=-1)
def map_lookup(graph, identifiers):
    """Lookup identifiers using on-disk maps.

    Depending on the identifier type lookup either a SWHID into a SWHID->node (and
    return the node integer identifier) or, vice-versa, lookup a node integer
    identifier into a node->SWHID (and return the SWHID).  The desired behavior is
    chosen depending on the syntax of each given identifier.

    Identifiers can be passed either directly on the command line or on
    standard input, separate by blanks. Logical lines (as returned by
    readline()) in stdin will be preserved in stdout.

    """
    from swh.graph.backend import NODE2SWHID_EXT, SWHID2NODE_EXT
    from swh.graph.swhid import NodeToSwhidMap, SwhidToNodeMap
    import swh.model.exceptions
    from swh.model.identifiers import ExtendedSWHID

    success = True  # no identifiers failed to be looked up
    swhid2node = SwhidToNodeMap(f"{graph}.{SWHID2NODE_EXT}")
    node2swhid = NodeToSwhidMap(f"{graph}.{NODE2SWHID_EXT}")

    def lookup(identifier):
        nonlocal success, swhid2node, node2swhid
        is_swhid = None
        try:
            int(identifier)
            is_swhid = False
        except ValueError:
            try:
                ExtendedSWHID.from_string(identifier)
                is_swhid = True
            except swh.model.exceptions.ValidationError:
                success = False
                logging.error(f'invalid identifier: "{identifier}", skipping')

        try:
            if is_swhid:
                return str(swhid2node[identifier])
            else:
                return node2swhid[int(identifier)]
        except KeyError:
            success = False
            logging.error(f'identifier not found: "{identifier}", skipping')

    if identifiers:  # lookup identifiers passed via CLI
        for identifier in identifiers:
            print(lookup(identifier))
    else:  # lookup identifiers passed via stdin, preserving logical lines
        for line in sys.stdin:
            results = [lookup(id) for id in line.rstrip().split()]
            if results:  # might be empty if all IDs on the same line failed
                print(" ".join(results))

    sys.exit(0 if success else 1)


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
    """run the graph REST service"""
    import aiohttp

    from swh.graph.backend import Backend
    from swh.graph.server.app import make_app

    backend = Backend(graph_path=graph, config=ctx.obj["config"])
    app = make_app(backend=backend)

    with backend:
        aiohttp.web.run_app(app, host=host, port=port)


@graph_cli_group.command()
@click.option(
    "--graph",
    "-g",
    required=True,
    metavar="GRAPH",
    type=PathlibPath(),
    help="input graph basename",
)
@click.option(
    "--outdir",
    "-o",
    "out_dir",
    required=True,
    metavar="DIR",
    type=PathlibPath(),
    help="directory where to store compressed graph",
)
@click.option(
    "--steps",
    "-s",
    metavar="STEPS",
    type=StepOption(),
    help="run only these compression steps (default: all steps)",
)
@click.pass_context
def compress(ctx, graph, out_dir, steps):
    """Compress a graph using WebGraph

    Input: a pair of files g.nodes.csv.gz, g.edges.csv.gz

    Output: a directory containing a WebGraph compressed graph

    Compression steps are: (1) mph, (2) bv, (3) bv_obl, (4) bfs, (5) permute,
    (6) permute_obl, (7) stats, (8) transpose, (9) transpose_obl, (10) maps,
    (11) clean_tmp. Compression steps can be selected by name or number using
    --steps, separating them with commas; step ranges (e.g., 3-9, 6-, etc.) are
    also supported.

    """
    from swh.graph import webgraph

    graph_name = graph.name
    in_dir = graph.parent
    try:
        conf = ctx.obj["config"]["graph"]["compress"]
    except KeyError:
        conf = {}  # use defaults

    webgraph.compress(graph_name, in_dir, out_dir, steps, conf)


@graph_cli_group.command(name="cachemount")
@click.option(
    "--graph", "-g", required=True, metavar="GRAPH", help="compressed graph basename"
)
@click.option(
    "--cache",
    "-c",
    default="/dev/shm/swh-graph/default",
    metavar="CACHE",
    type=PathlibPath(),
    help="Memory cache path (defaults to /dev/shm/swh-graph/default)",
)
@click.pass_context
def cachemount(ctx, graph, cache):
    """
    Cache the mmapped files of the compressed graph in a tmpfs.

    This command creates a new directory at the path given by CACHE that has
    the same structure as the compressed graph basename, except it copies the
    files that require mmap access (*.graph) but uses symlinks from the source
    for all the other files (.map, .bin, ...).

    The command outputs the path to the memory cache directory (particularly
    useful when relying on the default value).
    """
    import shutil

    cache.mkdir(parents=True)
    for src in Path(graph).parent.glob("*"):
        dst = cache / src.name
        if src.suffix == ".graph":
            shutil.copy2(src, dst)
        else:
            dst.symlink_to(src.resolve())
    print(cache)


def main():
    return graph_cli_group(auto_envvar_prefix="SWH_GRAPH")


if __name__ == "__main__":
    main()
