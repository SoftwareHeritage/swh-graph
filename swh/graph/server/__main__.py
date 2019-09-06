# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


import argparse
import aiohttp.web

from swh.core.api.asynchronous import RPCServerApp
from swh.graph.server.backend import Backend


async def index(request):
    return aiohttp.web.Response(body="SWH Graph API server")


async def visit(request):
    node_id = int(request.match_info['id'])
    response = aiohttp.web.StreamResponse(status=200)
    await response.prepare(request)
    async for node_id in request.app['backend'].visit(node_id):
        await response.write('{}\n'.format(node_id).encode())
    await response.write_eof()
    return response


def make_app(backend, **kwargs):
    app = RPCServerApp(**kwargs)
    app.router.add_route('GET', '/', index)

    # Endpoints used by the web API
    app.router.add_route('GET', '/visit/{id}', visit)

    app['backend'] = backend
    return app


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', default='0.0.0.0')
    parser.add_argument('--port', type=int, default=5009)
    parser.add_argument('--graph', required=True)
    args = parser.parse_args()

    backend = Backend(graph_path=args.graph)
    app = make_app(backend=backend)

    with backend:
        aiohttp.web.run_app(app, host=args.host, port=args.port)


if __name__ == '__main__':
    main()
