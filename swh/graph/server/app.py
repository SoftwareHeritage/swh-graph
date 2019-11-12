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
from swh.model.exceptions import ValidationError

try:
    from contextlib import asynccontextmanager
except ImportError:
    # Compatibility with 3.6 backport
    from async_generator import asynccontextmanager  # type: ignore


@asynccontextmanager
async def stream_response(request, content_type='text/plain',
                          *args, **kwargs):
    response = aiohttp.web.StreamResponse(*args, **kwargs)
    response.content_type = content_type
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


def get_direction(request):
    """validate HTTP query parameter `direction`"""
    s = request.query.get('direction', 'forward')
    if s not in ('forward', 'backward'):
        raise aiohttp.web.HTTPBadRequest(body=f'invalid direction: {s}')
    return s


def get_edges(request):
    """validate HTTP query parameter `edges`, i.e., edge restrictions"""
    s = request.query.get('edges', '*')
    if any([node_type != '*' and node_type not in PID_TYPES
           for edge in s.split(':')
           for node_type in edge.split(',', maxsplit=1)]):
        raise aiohttp.web.HTTPBadRequest(body=f'invalid edge restriction: {s}')
    return s


def get_traversal(request):
    """validate HTTP query parameter `traversal`, i.e., visit order"""
    s = request.query.get('traversal', 'dfs')
    if s not in ('bfs', 'dfs'):
        raise aiohttp.web.HTTPBadRequest(body=f'invalid traversal order: {s}')
    return s


def node_of_pid(pid, backend):
    """lookup a PID in a pid2node map, failing in an HTTP-nice way if needed"""
    try:
        return backend.pid2node[pid]
    except KeyError:
        raise aiohttp.web.HTTPNotFound(body=f'PID not found: {pid}')
    except ValidationError:
        raise aiohttp.web.HTTPBadRequest(body=f'malformed PID: {pid}')


def pid_of_node(node, backend):
    """lookup a node in a node2pid map, failing in an HTTP-nice way if needed

    """
    try:
        return backend.node2pid[node]
    except KeyError:
        raise aiohttp.web.HTTPInternalServerError(
            body=f'reverse lookup failed for node id: {node}')


def get_simple_traversal_handler(ttype):
    async def simple_traversal(request):
        backend = request.app['backend']

        src = request.match_info['src']
        edges = get_edges(request)
        direction = get_direction(request)

        src_node = node_of_pid(src, backend)
        async with stream_response(request) as response:
            async for res_node in backend.simple_traversal(
                ttype, direction, edges, src_node
            ):
                res_pid = pid_of_node(res_node, backend)
                await response.write('{}\n'.format(res_pid).encode())
            return response

    return simple_traversal


async def walk(request):
    backend = request.app['backend']

    src = request.match_info['src']
    dst = request.match_info['dst']
    edges = get_edges(request)
    direction = get_direction(request)
    algo = get_traversal(request)

    src_node = node_of_pid(src, backend)
    if dst not in PID_TYPES:
        dst = backend.node_of_pid(dst, backend)
    async with stream_response(request) as response:
        async for res_node in backend.walk(
                direction, edges, algo, src_node, dst
        ):
            res_pid = pid_of_node(res_node, backend)
            await response.write('{}\n'.format(res_pid).encode())
        return response


async def visit_paths(request):
    backend = request.app['backend']

    src = request.match_info['src']
    edges = get_edges(request)
    direction = get_direction(request)

    src_node = node_of_pid(src, backend)
    it = backend.visit_paths(direction, edges, src_node)
    async with stream_response(request, content_type='application/x-ndjson') \
            as response:
        async for res_path in it:
            res_path_pid = [pid_of_node(n, backend) for n in res_path]
            line = json.dumps(res_path_pid)
            await response.write('{}\n'.format(line).encode())
        return response


def get_count_handler(ttype):
    async def count(request):
        loop = asyncio.get_event_loop()
        backend = request.app['backend']

        src = request.match_info['src']
        edges = get_edges(request)
        direction = get_direction(request)

        src_node = node_of_pid(src, backend)
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
