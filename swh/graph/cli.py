import click

import client


CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])


@click.command(context_settings=CONTEXT_SETTINGS)
@click.option('--host', default='0.0.0.0', help="Host where runs the server")
@click.option('--port', default='5009', help="Binding port of the server")
@click.pass_context
def cli(ctx, host, port):
    """Software Heritage Graph API

    """
    url = 'http://' + host + ':' + port
    app = client.RemoteGraphClient(url)

    # TODO: run web app
    print(app.stats())


def main():
    return cli(auto_envvar_prefix='SWH_GRAPH')


if __name__ == '__main__':
    main()
