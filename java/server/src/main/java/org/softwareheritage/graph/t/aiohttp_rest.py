#!/usr/bin/env python3

# Copyright (C) 2018 Antoine Pietri
# SPDX-License-Identifier: MIT

import argparse
import aiohttp
import aiohttp.web
import hashutil


async def hello(request):
    return aiohttp.web.json_response(
        {'hi': 'hello'}, headers={'Access-Control-Allow-Origin': '*'})


async def make_app():
    app = aiohttp.web.Application()
    app.add_routes([
        aiohttp.web.get('/hello', hello)])
    return app


async def get_stream(request):  # from objstorage
    hex_id = request.match_info['hex_id']
    obj_id = hashutil.hash_to_bytes(hex_id)
    response = aiohttp.web.StreamResponse()
    await response.prepare(request)
    for chunk in request.app['objstorage'].get_stream(obj_id, 2 << 20):
        await response.write(chunk)
    await response.write_eof()
    return response


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Test')
    parser.add_argument('--host', default='127.0.0.1', help='Bind address')
    parser.add_argument('--port', default=9012, help='Bind port')

    args = parser.parse_args()

    aiohttp.web.run_app(make_app(), host=args.host, port=args.port)
