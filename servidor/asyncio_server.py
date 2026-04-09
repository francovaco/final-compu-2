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


async def manejar_cliente_tcp(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    queue: multiprocessing.Queue,
) -> None:
    # Maneja una conexión TCP: lee métricas línea a línea y las pone en la Queue
    addr = writer.get_extra_info("peername")
    logging.info("Cliente conectado: %s:%d", *addr)
    try:
        while True:
            linea = await reader.readline()
            if not linea:
                break
            try:
                metrica = json.loads(linea.decode())
                metrica["tipo"] = "metrica"
                queue.put(metrica)
                logging.debug("Métrica recibida de %s", metrica.get("nodo", addr))
            except json.JSONDecodeError:
                logging.warning("Métrica inválida de %s:%d", *addr)
    except asyncio.IncompleteReadError:
        pass
    finally:
        logging.info("Cliente desconectado: %s:%d", *addr)
        writer.close()