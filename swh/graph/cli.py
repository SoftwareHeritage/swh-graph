# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import aiohttp
import click
import sys

from pathlib import Path

from swh.core.cli import CONTEXT_SETTINGS, AliasedGroup
from swh.graph import client, webgraph
from swh.graph.pid import PidToIntMap, IntToPidMap
from swh.graph.server.app import make_app
from swh.graph.backend import Backend


class PathlibPath(click.Path):
    """A Click path argument that returns a pathlib Path, not a string"""
    def convert(self, value, param, ctx):
        return Path(super().convert(value, param, ctx))


@click.group(name='graph', context_settings=CONTEXT_SETTINGS,
             cls=AliasedGroup)
@click.pass_context
def cli(ctx):
    """Software Heritage graph tools."""
    ctx.ensure_object(dict)


@cli.command('api-client')
@click.option('--host', default='localhost', help='Graph server host')
@click.option('--port', default='5009', help='Graph server port')
@click.pass_context
def api_client(ctx, host, port):
    """client for the graph REST service"""
    url = 'http://{}:{}'.format(host, port)
    app = client.RemoteGraphClient(url)

    # TODO: run web app
    print(app.stats())


@cli.group('map')
@click.pass_context
def map(ctx):
    """Manage swh-graph on-disk maps"""
    pass


def dump_pid2int(filename):
    for (pid, int) in PidToIntMap(filename):
        print('{}\t{}'.format(pid, int))


def dump_int2pid(filename):
    for (int, pid) in IntToPidMap(filename):
        print('{}\t{}'.format(int, pid))


def restore_pid2int(filename):
    """read a textual PID->int map from stdin and write its binary version to
    filename

    """
    with open(filename, 'wb') as dst:
        for line in sys.stdin:
            (str_pid, str_int) = line.split()
            PidToIntMap.write_record(dst, str_pid, int(str_int))


def restore_int2pid(filename, length):
    """read a textual int->PID map from stdin and write its binary version to
    filename

    """
    int2pid = IntToPidMap(filename, mode='wb', length=length)
    for line in sys.stdin:
        (str_int, str_pid) = line.split()
        int2pid[int(str_int)] = str_pid
    int2pid.close()


@map.command('dump')
@click.option('--type', '-t', 'map_type', required=True,
              type=click.Choice(['pid2int', 'int2pid']),
              help='type of map to dump')
@click.argument('filename', required=True, type=click.Path(exists=True))
@click.pass_context
def dump_map(ctx, map_type, filename):
    """dump a binary PID<->int map to textual format"""
    if map_type == 'pid2int':
        dump_pid2int(filename)
    elif map_type == 'int2pid':
        dump_int2pid(filename)
    else:
        raise ValueError('invalid map type: ' + map_type)
    pass


@map.command('restore')
@click.option('--type', '-t', 'map_type', required=True,
              type=click.Choice(['pid2int', 'int2pid']),
              help='type of map to dump')
@click.option('--length', '-l', type=int,
              help='''map size in number of logical records
              (required for int2pid maps)''')
@click.argument('filename', required=True, type=click.Path())
@click.pass_context
def restore_map(ctx, map_type, length, filename):
    """restore a binary PID<->int map from textual format"""
    if map_type == 'pid2int':
        restore_pid2int(filename)
    elif map_type == 'int2pid':
        if length is None:
            raise click.UsageError(
                'map length is required when restoring {} maps'.format(
                    map_type), ctx)
        restore_int2pid(filename, length)
    else:
        raise ValueError('invalid map type: ' + map_type)


@cli.command(name='rpc-serve')
@click.option('--host', '-h', default='0.0.0.0',
              metavar='IP', show_default=True,
              help='host IP address to bind the server on')
@click.option('--port', '-p', default=5009, type=click.INT,
              metavar='PORT', show_default=True,
              help='port to bind the server on')
@click.option('--graph', '-g', required=True, metavar='GRAPH',
              help='compressed graph basename')
@click.pass_context
def serve(ctx, host, port, graph):
    """run the graph REST service"""
    backend = Backend(graph_path=graph)
    app = make_app(backend=backend)

    with backend:
        aiohttp.web.run_app(app, host=host, port=port)


@cli.command()
@click.option('--graph', '-g', required=True, metavar='GRAPH',
              type=PathlibPath(),
              help='input graph basename')
@click.option('--outdir', '-o', 'out_dir', required=True, metavar='DIR',
              type=PathlibPath(),
              help='directory where to store compressed graph')
@click.option('--steps', '-s', metavar='STEPS', type=webgraph.StepOption(),
              help='run only these compression steps (default: all steps)')
@click.pass_context
def compress(ctx, graph, out_dir, steps):
    """Compress a graph using WebGraph

    Input: a pair of files g.nodes.csv.gz, g.edges.csv.gz

    Output: a directory containing a WebGraph compressed graph

    Compression steps are: (1) mph, (2) bv, (3) bv_obl, (4) bfs, (5) permute,
    (6) permute_obl, (7) stats, (8) transpose, (9) transpose_obl. Compression
    steps can be selected by name or number using --steps, separating them with
    commas; step ranges (e.g., 3-9, 6-, etc.) are also supported.

    """
    graph_name = graph.name
    in_dir = graph.parent
    conf = {}  # use defaults

    webgraph.compress(conf, graph_name, in_dir, out_dir, steps)


def main():
    return cli(auto_envvar_prefix='SWH_GRAPH')


if __name__ == '__main__':
    main()
