# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from enum import Enum

from swh.core.api import SWHRemoteAPI


class GraphAPIError(Exception):
    """Graph API Error"""
    def __str__(self):
        return ('An unexpected error occurred in the Graph backend: {}'
                .format(self.args))


class RemoteGraphClient(SWHRemoteAPI):
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
                        })

    def neighbors(self, src, edges="*", direction="forward"):
        return self.get('neighbors/{}'.format(src),
                        params={
                            'edges': edges,
                            'direction': direction
                        })

    def stats(self):
        return self.get('stats')

    def visit_nodes(self, src, edges="*", direction="forward"):
        return self.get('visit/nodes/{}'.format(src),
                        params={
                            'edges': edges,
                            'direction': direction
                        })

    def visit_paths(self, src, edges="*", direction="forward"):
        return self.get('visit/paths/{}'.format(src),
                        params={
                            'edges': edges,
                            'direction': direction
                        })

    def walk(self, src, dst, edges="*", traversal="dfs", direction="forward"):
        return self.get('walk/{}/{}'.format(src, dst),
                        params={
                            'edges': edges,
                            'traversal': traversal,
                            'direction': direction
                        })
