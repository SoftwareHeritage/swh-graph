# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""
A proxy HTTP server for swh-graph, talking to the Java code via py4j, and using
FIFO as a transport to stream integers between the two languages.
"""

import contextlib
import aiohttp.web

from swh.core.api.asynchronous import RPCServerApp


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


def get_simple_traversal_handler(ttype):
    async def simple_traversal(request):
        src = request.match_info['src']
        edges = request.query.get('edges', '*')
        direction = request.query.get('direction', 'forward')

        async with stream_response(request) as response:
            async for res_pid in request.app['backend'].simple_traversal(
                ttype, direction, edges, src
            ):
                await response.write('{}\n'.format(res_pid).encode())
            return response

    return simple_traversal


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


async def visit_paths(request):
    src = request.match_info['src']
    edges = request.query.get('edges', '*')
    direction = request.query.get('direction', 'forward')

    it = request.app['backend'].visit_paths(direction, edges, src)
    async with stream_response(request) as response:
        async for res_pid in it:
            await response.write('{}\n'.format(res_pid).encode())
        return response


def make_app(backend, **kwargs):
    app = RPCServerApp(**kwargs)
    app.router.add_route('GET', '/', index)
    app.router.add_route('GET', '/graph/stats', stats)
    app.router.add_route('GET', '/graph/leaves/{src}',
                         get_simple_traversal_handler('leaves'))
    app.router.add_route('GET', '/graph/neighbors/{src}',
                         get_simple_traversal_handler('neighbors'))
    app.router.add_route('GET', '/graph/visit/nodes/{src}',
                         get_simple_traversal_handler('visit_nodes'))
    app.router.add_route('GET', '/graph/visit/paths/{src}', visit_paths)
    app.router.add_route('GET', '/graph/walk/{src}/{dst}', walk)

    app['backend'] = backend
    return app
