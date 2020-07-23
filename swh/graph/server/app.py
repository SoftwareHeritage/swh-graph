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
from collections import deque
from typing import Optional

from swh.core.api.asynchronous import RPCServerApp
from swh.model.identifiers import PID_TYPES
from swh.model.exceptions import ValidationError

try:
    from contextlib import asynccontextmanager
except ImportError:
    # Compatibility with 3.6 backport
    from async_generator import asynccontextmanager  # type: ignore


# maximum number of retries for random walks
RANDOM_RETRIES = 5  # TODO make this configurable via rpc-serve configuration


async def index(request):
    return aiohttp.web.Response(
        content_type="text/html",
        body="""<html>
<head><title>Software Heritage storage server</title></head>
<body>
<p>You have reached the <a href="https://www.softwareheritage.org/">
Software Heritage</a> graph API server.</p>

<p>See its
<a href="https://docs.softwareheritage.org/devel/swh-graph/api.html">API
documentation</a> for more information.</p>
</body>
</html>""",
    )


class GraphView(aiohttp.web.View):
    """Base class for views working on the graph, with utility functions"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.backend = self.request.app["backend"]

    def node_of_pid(self, pid):
        """Lookup a PID in a pid2node map, failing in an HTTP-nice way if needed."""
        try:
            return self.backend.pid2node[pid]
        except KeyError:
            raise aiohttp.web.HTTPNotFound(body=f"PID not found: {pid}")
        except ValidationError:
            raise aiohttp.web.HTTPBadRequest(body=f"malformed PID: {pid}")

    def pid_of_node(self, node):
        """Lookup a node in a node2pid map, failing in an HTTP-nice way if needed."""
        try:
            return self.backend.node2pid[node]
        except KeyError:
            raise aiohttp.web.HTTPInternalServerError(
                body=f"reverse lookup failed for node id: {node}"
            )

    def get_direction(self):
        """Validate HTTP query parameter `direction`"""
        s = self.request.query.get("direction", "forward")
        if s not in ("forward", "backward"):
            raise aiohttp.web.HTTPBadRequest(body=f"invalid direction: {s}")
        return s

    def get_edges(self):
        """Validate HTTP query parameter `edges`, i.e., edge restrictions"""
        s = self.request.query.get("edges", "*")
        if any(
            [
                node_type != "*" and node_type not in PID_TYPES
                for edge in s.split(":")
                for node_type in edge.split(",", maxsplit=1)
            ]
        ):
            raise aiohttp.web.HTTPBadRequest(body=f"invalid edge restriction: {s}")
        return s

    def get_traversal(self):
        """Validate HTTP query parameter `traversal`, i.e., visit order"""
        s = self.request.query.get("traversal", "dfs")
        if s not in ("bfs", "dfs"):
            raise aiohttp.web.HTTPBadRequest(body=f"invalid traversal order: {s}")
        return s

    def get_limit(self):
        """Validate HTTP query parameter `limit`, i.e., number of results"""
        s = self.request.query.get("limit", "0")
        try:
            return int(s)
        except ValueError:
            raise aiohttp.web.HTTPBadRequest(body=f"invalid limit value: {s}")


class StreamingGraphView(GraphView):
    """Base class for views streaming their response line by line."""

    content_type = "text/plain"

    @asynccontextmanager
    async def response_streamer(self, *args, **kwargs):
        """Context manager to prepare then close a StreamResponse"""
        response = aiohttp.web.StreamResponse(*args, **kwargs)
        response.content_type = self.content_type
        await response.prepare(self.request)
        yield response
        await response.write_eof()

    async def get(self):
        await self.prepare_response()
        async with self.response_streamer() as self.response_stream:
            await self.stream_response()
            return self.response_stream

    async def prepare_response(self):
        """This can be overridden with some setup to be run before the response
        actually starts streaming.
        """
        pass

    async def stream_response(self):
        """Override this to perform the response streaming. Implementations of
        this should await self.stream_line(line) to write each line.
        """
        raise NotImplementedError

    async def stream_line(self, line):
        """Write a line in the response stream."""
        await self.response_stream.write((line + "\n").encode())


class StatsView(GraphView):
    """View showing some statistics on the graph"""

    async def get(self):
        stats = self.backend.stats()
        return aiohttp.web.Response(body=stats, content_type="application/json")


class SimpleTraversalView(StreamingGraphView):
    """Base class for views of simple traversals"""

    simple_traversal_type: Optional[str] = None

    async def prepare_response(self):
        src = self.request.match_info["src"]
        self.src_node = self.node_of_pid(src)

        self.edges = self.get_edges()
        self.direction = self.get_direction()

    async def stream_response(self):
        async for res_node in self.backend.simple_traversal(
            self.simple_traversal_type, self.direction, self.edges, self.src_node
        ):
            res_pid = self.pid_of_node(res_node)
            await self.stream_line(res_pid)


class LeavesView(SimpleTraversalView):
    simple_traversal_type = "leaves"


class NeighborsView(SimpleTraversalView):
    simple_traversal_type = "neighbors"


class VisitNodesView(SimpleTraversalView):
    simple_traversal_type = "visit_nodes"


class WalkView(StreamingGraphView):
    async def prepare_response(self):
        src = self.request.match_info["src"]
        dst = self.request.match_info["dst"]
        self.src_node = self.node_of_pid(src)
        if dst not in PID_TYPES:
            self.dst_thing = self.node_of_pid(dst)
        else:
            self.dst_thing = dst

        self.edges = self.get_edges()
        self.direction = self.get_direction()
        self.algo = self.get_traversal()
        self.limit = self.get_limit()

    async def get_walk_iterator(self):
        return self.backend.walk(
            self.direction, self.edges, self.algo, self.src_node, self.dst_thing
        )

    async def stream_response(self):
        it = self.get_walk_iterator()
        if self.limit < 0:
            queue = deque(maxlen=-self.limit)
            async for res_node in it:
                res_pid = self.pid_of_node(res_node)
                queue.append(res_pid)
            while queue:
                await self.stream_line(queue.popleft())
        else:
            count = 0
            async for res_node in it:
                if self.limit == 0 or count < self.limit:
                    res_pid = self.pid_of_node(res_node)
                    await self.stream_line(res_pid)
                    count += 1
                else:
                    break


class RandomWalkView(WalkView):
    def get_walk_iterator(self):
        return self.backend.random_walk(
            self.direction, self.edges, RANDOM_RETRIES, self.src_node, self.dst_thing
        )


class VisitEdgesView(SimpleTraversalView):
    async def stream_response(self):
        it = self.backend.visit_edges(self.direction, self.edges, self.src_node)
        async for (res_src, res_dst) in it:
            res_src_pid = self.pid_of_node(res_src)
            res_dst_pid = self.pid_of_node(res_dst)
            await self.stream_line("{} {}".format(res_src_pid, res_dst_pid))


class VisitPathsView(SimpleTraversalView):
    content_type = "application/x-ndjson"

    async def stream_response(self):
        it = self.backend.visit_paths(self.direction, self.edges, self.src_node)
        async for res_path in it:
            res_path_pid = [self.pid_of_node(n) for n in res_path]
            line = json.dumps(res_path_pid)
            await self.stream_line(line)


class CountView(GraphView):
    """Base class for counting views."""

    count_type: Optional[str] = None

    async def get(self):
        src = self.request.match_info["src"]
        self.src_node = self.node_of_pid(src)

        self.edges = self.get_edges()
        self.direction = self.get_direction()

        loop = asyncio.get_event_loop()
        cnt = await loop.run_in_executor(
            None,
            self.backend.count,
            self.count_type,
            self.direction,
            self.edges,
            self.src_node,
        )
        return aiohttp.web.Response(body=str(cnt), content_type="application/json")


class CountNeighborsView(CountView):
    count_type = "neighbors"


class CountLeavesView(CountView):
    count_type = "leaves"


class CountVisitNodesView(CountView):
    count_type = "visit_nodes"


def make_app(backend, **kwargs):
    app = RPCServerApp(**kwargs)
    app.add_routes(
        [
            aiohttp.web.get("/", index),
            aiohttp.web.get("/graph", index),
            aiohttp.web.view("/graph/stats", StatsView),
            aiohttp.web.view("/graph/leaves/{src}", LeavesView),
            aiohttp.web.view("/graph/neighbors/{src}", NeighborsView),
            aiohttp.web.view("/graph/visit/nodes/{src}", VisitNodesView),
            aiohttp.web.view("/graph/visit/edges/{src}", VisitEdgesView),
            aiohttp.web.view("/graph/visit/paths/{src}", VisitPathsView),
            # temporarily disabled in wait of a proper fix for T1969
            # aiohttp.web.view("/graph/walk/{src}/{dst}", WalkView)
            aiohttp.web.view("/graph/randomwalk/{src}/{dst}", RandomWalkView),
            aiohttp.web.view("/graph/neighbors/count/{src}", CountNeighborsView),
            aiohttp.web.view("/graph/leaves/count/{src}", CountLeavesView),
            aiohttp.web.view("/graph/visit/nodes/count/{src}", CountVisitNodesView),
        ]
    )

    app["backend"] = backend
    return app
