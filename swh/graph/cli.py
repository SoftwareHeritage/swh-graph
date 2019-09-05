import click

from swh.core.cli import CONTEXT_SETTINGS, AliasedGroup
from swh.graph import client


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


def main():
    return cli(auto_envvar_prefix='SWH_GRAPH')


if __name__ == '__main__':
    main()
