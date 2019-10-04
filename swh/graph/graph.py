# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import asyncio
import contextlib
from swh.graph.backend import Backend
from swh.graph.dot import dot_to_svg, graph_dot


KIND_TO_SHAPE = {
    'ori': 'egg',
    'snp': 'doubleoctagon',
    'rel': 'octagon',
    'rev': 'diamond',
    'dir': 'folder',
    'cnt': 'oval',
}


KIND_TO_URL = {
    'ori': 'https://archive.softwareheritage.org/browse/origin/{}',
    'snp': 'https://archive.softwareheritage.org/browse/snapshot/{}',
    'rel': 'https://archive.softwareheritage.org/browse/release/{}',
    'rev': 'https://archive.softwareheritage.org/browse/revision/{}',
    'dir': 'https://archive.softwareheritage.org/browse/directory/{}',
    'cnt': 'https://archive.softwareheritage.org/browse/content/sha1_git:{}/',
}


def call_async_gen(generator, *args, **kwargs):
    loop = asyncio.get_event_loop()
    it = generator(*args, **kwargs).__aiter__()
    while True:
        try:
            res = loop.run_until_complete(it.__anext__())
            yield res
        except StopAsyncIteration:
            break


class Neighbors:
    """Neighbor iterator with custom O(1) length method"""
    def __init__(self, parent_graph, iterator, length_func):
        self.parent_graph = parent_graph
        self.iterator = iterator
        self.length_func = length_func

    def __iter__(self):
        return self

    def __next__(self):
        succ = self.iterator.nextLong()
        if succ == -1:
            raise StopIteration
        return GraphNode(self.parent_graph, succ)

    def __len__(self):
        return self.length_func()


class GraphNode:
    """Node in the SWH graph"""

    def __init__(self, parent_graph, node_id):
        self.parent_graph = parent_graph
        self.id = node_id

    def children(self):
        return Neighbors(
            self.parent_graph,
            self.parent_graph.java_graph.successors(self.id),
            lambda: self.parent_graph.java_graph.outdegree(self.id))

    def parents(self):
        return Neighbors(
            self.parent_graph,
            self.parent_graph.java_graph.predecessors(self.id),
            lambda: self.parent_graph.java_graph.indegree(self.id))

    def simple_traversal(self, ttype, direction='forward', edges='*'):
        for node in call_async_gen(
            self.parent_graph.backend.simple_traversal,
            ttype, direction, edges, self.id
        ):
            yield self.parent_graph[node]

    def leaves(self, *args, **kwargs):
        yield from self.simple_traversal('leaves', *args, **kwargs)

    def visit_nodes(self, *args, **kwargs):
        yield from self.simple_traversal('visit_nodes', *args, **kwargs)

    def visit_paths(self, direction='forward', edges='*'):
        for path in call_async_gen(
                self.parent_graph.backend.visit_paths,
                direction, edges, self.id
        ):
            yield [self.parent_graph[node] for node in path]

    def walk(self, dst, direction='forward', edges='*', traversal='dfs'):
        for node in call_async_gen(
            self.parent_graph.backend.walk,
            direction, edges, traversal, self.id, dst
        ):
            yield self.parent_graph[node]

    @property
    def pid(self):
        return self.parent_graph.node2pid[self.id]

    @property
    def kind(self):
        return self.pid.split(':')[2]

    def __str__(self):
        return self.pid

    def __repr__(self):
        return '<{}>'.format(self.pid)

    def dot_fragment(self):
        swh, version, kind, hash = self.pid.split(':')
        label = '{}:{}..{}'.format(kind, hash[0:2], hash[-2:])
        url = KIND_TO_URL[kind].format(hash)
        shape = KIND_TO_SHAPE[kind]
        return ('{} [label="{}", href="{}", target="_blank", shape="{}"];'
                .format(self.id, label, url, shape))

    def _repr_svg_(self):
        nodes = [self, *list(self.children()), *list(self.parents())]
        dot = graph_dot(nodes)
        svg = dot_to_svg(dot)
        return svg


class Graph:
    def __init__(self, backend, node2pid, pid2node):
        self.backend = backend
        self.java_graph = backend.entry.get_graph()
        self.node2pid = node2pid
        self.pid2node = pid2node

    def stats(self):
        return self.backend.stats()

    @property
    def path(self):
        return self.java_graph.getPath()

    def __len__(self):
        return self.java_graph.getNbNodes()

    def __getitem__(self, node_id):
        if isinstance(node_id, int):
            self.node2pid[node_id]  # check existence
            return GraphNode(self, node_id)
        elif isinstance(node_id, str):
            node_id = self.pid2node[node_id]
            return GraphNode(self, node_id)


@contextlib.contextmanager
def load(graph_path):
    with Backend(graph_path) as backend:
        yield Graph(backend, backend.node2pid, backend.pid2node)
