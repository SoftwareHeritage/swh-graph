# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""
A proxy HTTP server for swh-graph, talking to the Java code via py4j, and using
FIFO as a transport to stream integers between the two languages.
"""

import asyncio
import json
import aiohttp.web

from swh.core.api.asynchronous import RPCServerApp
from swh.model.identifiers import PID_TYPES

try:
    from contextlib import asynccontextmanager
except ImportError:
    # Compatibility with 3.6 backport
    from async_generator import asynccontextmanager

@asynccontextmanager
async def stream_response(request, *args, **kwargs):
    response = aiohttp.web.StreamResponse(*args, **kwargs)
    await response.prepare(request)
    yield response
    await response.write_eof()


async def index(request):
    return aiohttp.web.Response(
        content_type='text/html',
        body="""<html>
<head><title>Software Heritage storage server</title></head>
<body>
<p>You have reached the <a href="https://www.softwareheritage.org/">
Software Heritage</a> graph API server.</p>

<p>See its
<a href="https://docs.softwareheritage.org/devel/swh-graph/api.html">API
documentation</a> for more information.</p>
</body>
</html>""")


async def stats(request):
    stats = request.app['backend'].stats()
    return aiohttp.web.Response(body=stats, content_type='application/json')


def get_simple_traversal_handler(ttype):
    async def simple_traversal(request):
        backend = request.app['backend']

        src = request.match_info['src']
        edges = request.query.get('edges', '*')
        direction = request.query.get('direction', 'forward')

        src_node = backend.pid2node[src]
        async with stream_response(request) as response:
            async for res_node in backend.simple_traversal(
                ttype, direction, edges, src_node
            ):
                res_pid = backend.node2pid[res_node]
                await response.write('{}\n'.format(res_pid).encode())
            return response

    return simple_traversal


async def walk(request):
    backend = request.app['backend']

    src = request.match_info['src']
    dst = request.match_info['dst']
    edges = request.query.get('edges', '*')
    direction = request.query.get('direction', 'forward')
    algo = request.query.get('traversal', 'dfs')

    src_node = backend.pid2node[src]
    if dst not in PID_TYPES:
        dst = backend.pid2node[dst]
    async with stream_response(request) as response:
        async for res_node in backend.walk(
                direction, edges, algo, src_node, dst
        ):
            res_pid = backend.node2pid[res_node]
            await response.write('{}\n'.format(res_pid).encode())
        return response


async def visit_paths(request):
    backend = request.app['backend']

    src = request.match_info['src']
    edges = request.query.get('edges', '*')
    direction = request.query.get('direction', 'forward')

    src_node = backend.pid2node[src]
    it = backend.visit_paths(direction, edges, src_node)
    async with stream_response(request) as response:
        async for res_path in it:
            res_path_pid = [backend.node2pid[n] for n in res_path]
            line = json.dumps(res_path_pid)
            await response.write('{}\n'.format(line).encode())
        return response


def get_count_handler(ttype):
    async def count(request):
        loop = asyncio.get_event_loop()
        backend = request.app['backend']

        src = request.match_info['src']
        edges = request.query.get('edges', '*')
        direction = request.query.get('direction', 'forward')

        src_node = backend.pid2node[src]
        cnt = await loop.run_in_executor(
            None, backend.count, ttype, direction, edges, src_node)
        return aiohttp.web.Response(body=str(cnt),
                                    content_type='application/json')

    return count


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

    app.router.add_route('GET', '/graph/neighbors/count/{src}',
                         get_count_handler('neighbors'))
    app.router.add_route('GET', '/graph/leaves/count/{src}',
                         get_count_handler('leaves'))
    app.router.add_route('GET', '/graph/visit/nodes/count/{src}',
                         get_count_handler('visit_nodes'))

    app['backend'] = backend
    return app
