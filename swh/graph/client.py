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

    # Web API endpoints

    def leaves(self, src, edges="*", direction="forward"):
        return self.get('leaves/{}'.format(src),
                        params={
                            'edges': edges,
                            'direction': direction
                        },
                        stream_lines=True)

    def neighbors(self, src, edges="*", direction="forward"):
        return self.get('neighbors/{}'.format(src),
                        params={
                            'edges': edges,
                            'direction': direction
                        },
                        stream_lines=True)

    def stats(self):
        return self.get('stats')

    def visit_nodes(self, src, edges="*", direction="forward"):
        return self.get('visit/nodes/{}'.format(src),
                        params={
                            'edges': edges,
                            'direction': direction
                        },
                        stream_lines=True)

    def visit_paths(self, src, edges="*", direction="forward"):
        def decode_path_wrapper(it):
            for e in it:
                yield json.loads(e)

        return decode_path_wrapper(
            self.get('visit/paths/{}'.format(src),
                     params={
                         'edges': edges,
                         'direction': direction
                     }, stream_lines=True))

    def walk(self, src, dst, edges="*", traversal="dfs", direction="forward"):
        return self.get('walk/{}/{}'.format(src, dst),
                        params={
                            'edges': edges,
                            'traversal': traversal,
                            'direction': direction
                        },
                        stream_lines=True)
