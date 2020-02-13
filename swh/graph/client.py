# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import json

from swh.core.api import RPCClient


class GraphAPIError(Exception):
    """Graph API Error"""
    def __str__(self):
        return ('An unexpected error occurred in the Graph backend: {}'
                .format(self.args))


class RemoteGraphClient(RPCClient):
    """Client to the Software Heritage Graph."""

    def __init__(self, url, timeout=None):
        super().__init__(
            api_exception=GraphAPIError, url=url, timeout=timeout)

    def raw_verb_lines(self, verb, endpoint, **kwargs):
        response = self.raw_verb(verb, endpoint, stream=True, **kwargs)
        self.raise_for_status(response)
        for line in response.iter_lines():
            yield line.decode().lstrip('\n')

    def get_lines(self, endpoint, **kwargs):
        yield from self.raw_verb_lines('get', endpoint, **kwargs)

    # Web API endpoints

    def stats(self):
        return self.get('stats')

    def leaves(self, src, edges="*", direction="forward"):
        return self.get_lines(
            'leaves/{}'.format(src),
            params={
                'edges': edges,
                'direction': direction
            })

    def neighbors(self, src, edges="*", direction="forward"):
        return self.get_lines(
            'neighbors/{}'.format(src),
            params={
                'edges': edges,
                'direction': direction
            })

    def visit_nodes(self, src, edges="*", direction="forward"):
        return self.get_lines(
            'visit/nodes/{}'.format(src),
            params={
                'edges': edges,
                'direction': direction
            })

    def visit_paths(self, src, edges="*", direction="forward"):
        def decode_path_wrapper(it):
            for e in it:
                yield json.loads(e)

        return decode_path_wrapper(
            self.get_lines(
                'visit/paths/{}'.format(src),
                params={
                    'edges': edges,
                    'direction': direction
                }))

    def walk(self, src, dst, edges="*", traversal="dfs",
             direction="forward", limit=None):
        endpoint = 'walk/{}/{}'
        return self.get_lines(
            endpoint.format(src, dst),
            params={
                'edges': edges,
                'traversal': traversal,
                'direction': direction,
                'limit': limit
            })

    def random_walk(self, src, dst,
                    edges="*", direction="forward", limit=None):
        endpoint = 'randomwalk/{}/{}'
        return self.get_lines(
            endpoint.format(src, dst),
            params={
                'edges': edges,
                'direction': direction,
                'limit': limit
            })

    def count_leaves(self, src, edges="*", direction="forward"):
        return self.get(
            'leaves/count/{}'.format(src),
            params={
                'edges': edges,
                'direction': direction
            })

    def count_neighbors(self, src, edges="*", direction="forward"):
        return self.get(
            'neighbors/count/{}'.format(src),
            params={
                'edges': edges,
                'direction': direction
            })

    def count_visit_nodes(self, src, edges="*", direction="forward"):
        return self.get(
            'visit/nodes/count/{}'.format(src),
            params={
                'edges': edges,
                'direction': direction
            })
