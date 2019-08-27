#!/usr/bin/python3

import os
import struct
import sys
import tempfile

from concurrent.futures import ThreadPoolExecutor

from py4j.java_gateway import JavaGateway, GatewayParameters


GATEWAY_SERVER_PORT = 25333

BUF_SIZE = 64*1024
BIN_FMT = '>q'  # 64 bit integer, big endian


def print_node_ids(fname):
    with open(fname, 'rb') as f:
        data = f.read(BUF_SIZE)
        while(data):
            for data in struct.iter_unpack(BIN_FMT, data):
                print(data[0])
            data = f.read(BUF_SIZE)


if __name__ == '__main__':
    try:
        node_id = int(sys.argv[1])
    except IndexError:
        print('Usage: py4jcli NODE_ID')
        sys.exit(1)

    gw_params = GatewayParameters(port=GATEWAY_SERVER_PORT)
    gateway = JavaGateway(gateway_parameters=gw_params)

    with tempfile.TemporaryDirectory() as tmpdirname:
        cli_fifo = os.path.join(tmpdirname, 'swh-graph.fifo')
        os.mkfifo(cli_fifo)

        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(print_node_ids, cli_fifo)
            gateway.entry_point.visit(node_id, 'forward', '*', cli_fifo)
            _result = future.result()

    gateway.shutdown()
