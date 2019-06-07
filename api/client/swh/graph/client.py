# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

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

    def visit(self, swh_id, edges=None, traversal="dfs", direction="forward"):
        return self.get('visit/{}'.format(swh_id),
                        params={
                            'edges': edges,
                            'traversal': traversal,
                            'direction': direction
                        })

    def stats(self, src_type, dst_type):
        return self.get('stats/{}/{}'.format(src_type, dst_type))
