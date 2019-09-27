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


async def _simple_traversal(request, ttype):
    assert ttype in ('leaves', 'neighbors', 'visit_nodes', 'visit_paths')
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


async def visit_nodes(request):
    return (await _simple_traversal(request, 'visit_nodes'))


async def visit_paths(request):
    return (await _simple_traversal(request, 'visit_paths'))


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
    app.router.add_route('GET', '/graph/visit/nodes/{src}', visit_nodes)
    app.router.add_route('GET', '/graph/visit/paths/{src}', visit_paths)

    app['backend'] = backend
    return app
