# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


import argparse
import aiohttp.web

from swh.graph.server.app import make_app
from swh.graph.server.backend import Backend


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', default='0.0.0.0')
    parser.add_argument('--port', type=int, default=5009)
    parser.add_argument('--graph', required=True)
    args = parser.parse_args()

    backend = Backend(graph_path=args.graph)
    app = make_app(backend=backend)

    with backend:
        aiohttp.web.run_app(app, host=args.host, port=args.port)


if __name__ == '__main__':
    main()
