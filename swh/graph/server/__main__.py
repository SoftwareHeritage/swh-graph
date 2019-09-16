# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


import argparse
import contextlib
import aiohttp.web

from swh.core.api.asynchronous import RPCServerApp
from swh.graph.server.backend import Backend


@contextlib.asynccontextmanager
async def stream_response(request, *args, **kwargs):
    response = aiohttp.web.StreamResponse(*args, **kwargs)
    await response.prepare(request)
    yield response
    await response.write_eof()


async def index(request):
    return aiohttp.web.Response(body="SWH Graph API server")


async def stats(request):
    stats = request.app['backend'].stats()
    return aiohttp.web.Response(body=stats, content_type='application/json')


async def _simple_traversal(request, ttype):
    assert ttype in ('leaves', 'neighbors', 'visit_nodes')
    method = getattr(request.app['backend'], ttype)

    src = request.match_info['src']
    edges = request.query.get('edges', '*')
    direction = request.query.get('direction', 'forward')

    async with stream_response(request) as response:
        async for res_pid in method(direction, edges, src):
            await response.write('{}\n'.format(res_pid).encode())
        return response


async def leaves(request):
    return (await _simple_traversal(request, 'leaves'))


async def neighbors(request):
    return (await _simple_traversal(request, 'neighbors'))


async def visit(request):
    return (await _simple_traversal(request, 'visit_nodes'))


async def walk(request):
    src = request.match_info['src']
    dst = request.match_info['dst']
    edges = request.query.get('edges', '*')
    direction = request.query.get('direction', 'forward')
    algo = request.query.get('traversal', 'dfs')

    it = request.app['backend'].walk(direction, edges, algo, src, dst)
    async with stream_response(request) as response:
        async for res_pid in it:
            await response.write('{}\n'.format(res_pid).encode())
        return response


def make_app(backend, **kwargs):
    app = RPCServerApp(**kwargs)
    app.router.add_route('GET', '/', index)
    app.router.add_route('GET', '/graph/stats', stats)
    app.router.add_route('GET', '/graph/leaves/{src}', leaves)
    app.router.add_route('GET', '/graph/neighbors/{src}', neighbors)
    app.router.add_route('GET', '/graph/walk/{src}/{dst}', walk)
    app.router.add_route('GET', '/graph/visit/nodes/{src}', visit)
    # TODO: graph/visit/paths/ ?

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
