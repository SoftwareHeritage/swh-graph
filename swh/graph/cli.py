# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import click
import sys

from swh.core.cli import CONTEXT_SETTINGS, AliasedGroup
from swh.graph import client
from swh.graph.pid import PidToIntMap, IntToPidMap


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
    """Client for the Software Heritage Graph REST service

    """
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
    for (int, pid) in enumerate(IntToPidMap(filename)):
        print('{}\t{}'.format(int, pid))


def restore_pid2int(filename):
    """read a textual PID->int map from stdin and write its binary version to
    filename

    """
    with open(filename, 'wb') as dst:
        for line in sys.stdin:
            (str_pid, str_int) = line.split()
            PidToIntMap.write_record(dst, str_pid, int(str_int))


def restore_int2pid(filename):
    """read a textual int->PID map from stdin and write its binary version to
    filename

    """
    with open(filename, 'wb') as dst:
        for line in sys.stdin:
            (_str_int, str_pid) = line.split()
            IntToPidMap.write_record(dst, str_pid)


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
@click.argument('filename', required=True, type=click.Path())
@click.pass_context
def restore_map(ctx, map_type, filename):
    """restore a binary PID<->int map from textual format"""
    if map_type == 'pid2int':
        restore_pid2int(filename)
    elif map_type == 'int2pid':
        restore_int2pid(filename)
    else:
        raise ValueError('invalid map type: ' + map_type)


def main():
    return cli(auto_envvar_prefix='SWH_GRAPH')


if __name__ == '__main__':
    main()
