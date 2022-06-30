#  Copyright (c) 2022 The Software Heritage developers
#  See the AUTHORS file at the top-level directory of this distribution
#  License: GNU General Public License version 3, or any later version
#  See top-level LICENSE file for more information


import warnings

from .http_client import *  # noqa

warnings.warn(
    "the swh.graph.client module is deprecated, use swh.graph.http_client instead",
    DeprecationWarning,
    stacklevel=2,
)
