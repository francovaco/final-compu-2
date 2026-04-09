import asyncio
import json
import logging
import multiprocessing
from argparse import Namespace


class HeartbeatProtocol(asyncio.DatagramProtocol):
    # Protocolo UDP para recibir heartbeats de los clientes

    def __init__(self, queue: multiprocessing.Queue) -> None:
        self.queue = queue

    def datagram_received(self, data: bytes, addr: tuple) -> None:
        # Decodifica el heartbeat y lo pone en la Queue
        try:
            mensaje = json.loads(data.decode())
            mensaje["tipo"] = "heartbeat"
            self.queue.put(mensaje)
            logging.debug("Heartbeat recibido de %s:%d", *addr)
        except json.JSONDecodeError:
            logging.warning("Heartbeat inválido de %s:%d", *addr)