import asyncio
import argparse
import logging
from multiprocessing import Queue

from servidor.asyncio_server import correr_servidor

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

queue = Queue()
args = argparse.Namespace(host="0.0.0.0", port_tcp=9000, port_udp=9001)

asyncio.run(correr_servidor(queue, args))
