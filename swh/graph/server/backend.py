import asyncio
import os
import struct
import sys
import tempfile

from py4j.java_gateway import JavaGateway

GATEWAY_SERVER_PORT = 25335

BUF_SIZE = 64*1024
BIN_FMT = '>q'  # 64 bit integer, big endian


async def read_node_ids(fname):
    loop = asyncio.get_event_loop()
    with open(fname, 'rb') as f:
        while True:
            data = await loop.run_in_executor(None, f.read, BUF_SIZE)
            if not data:
                break
            for data in struct.iter_unpack(BIN_FMT, data):
                yield data[0]


class Backend:
    def __init__(self, graph_path):
        self.gateway = None
        self.entry = None
        self.graph_path = graph_path

    def __enter__(self):
        self.gateway = JavaGateway.launch_gateway(
            port=GATEWAY_SERVER_PORT,
            classpath='java/server/target/swh-graph-0.0.2-jar-with-dependencies.jar',
            die_on_exit=True,
            redirect_stdout=sys.stdout,
            redirect_stderr=sys.stderr,
        )
        self.entry = self.gateway.jvm.org.softwareheritage.graph.Entry()
        self.entry.load_graph(self.graph_path)
        # "/home/seirl/swh-graph/sample/big/compressed/swh-graph")

    def __exit__(self):
        self.gateway.shutdown()

    async def visit(self, node_id):
        loop = asyncio.get_event_loop()

        with tempfile.TemporaryDirectory() as tmpdirname:
            cli_fifo = os.path.join(tmpdirname, 'swh-graph.fifo')
            os.mkfifo(cli_fifo)

            def _visit():
                return self.entry.visit(node_id, 'forward', '*', cli_fifo)

            java_call = loop.run_in_executor(None, _visit)
            async for node_id in read_node_ids(cli_fifo):
                yield node_id
            await java_call
