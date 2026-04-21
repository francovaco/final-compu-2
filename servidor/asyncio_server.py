import asyncio
import json
import logging
import multiprocessing
import socket
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
            ip, *_ = addr
            version = "IPv6" if ":" in ip else "IPv4"
            logging.debug("Heartbeat recibido de %s (%s)", mensaje.get("nodo", ip), version)
        except json.JSONDecodeError:
            logging.warning("Heartbeat inválido de %s", addr)


async def manejar_cliente_tcp(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    queue: multiprocessing.Queue,
) -> None:
    # Maneja una conexión TCP: lee métricas línea a línea y las pone en la Queue
    addr = writer.get_extra_info("peername")
    ip, *_ = addr
    version = "IPv6" if ":" in ip else "IPv4"
    nodo = None
    try:
        while True:
            linea = await reader.readline()
            if not linea:
                break
            try:
                metrica = json.loads(linea.decode())
                metrica["tipo"] = "metrica"
                queue.put(metrica)
                if nodo is None:
                    nodo = metrica.get("nodo", ip)
                    logging.info("Cliente conectado: %s (%s)", nodo, version)
                logging.debug("Métrica recibida de %s (%s)", nodo, version)
            except json.JSONDecodeError:
                logging.warning("Métrica inválida de %s", addr)
    except asyncio.IncompleteReadError:
        pass
    finally:
        logging.info("Cliente desconectado: %s", nodo or ip)
        writer.close()
        await writer.wait_closed()


def _detectar_familias() -> tuple[bool, bool]:
    # Detecta qué familias IP soporta el SO probando crear un socket de cada tipo
    ipv4 = False
    ipv6 = False
    for family in (socket.AF_INET, socket.AF_INET6):
        try:
            s = socket.socket(family, socket.SOCK_STREAM)
            s.close()
            if family == socket.AF_INET:
                ipv4 = True
            else:
                ipv6 = True
        except OSError:
            pass
    return ipv4, ipv6


async def correr_servidor(queue: multiprocessing.Queue, args: Namespace) -> None:
    ipv4, ipv6 = _detectar_familias()
    hosts_tcp = []
    if ipv4:
        hosts_tcp.append(args.host)
    if ipv6:
        hosts_tcp.append("::")

    servidor_tcp = await asyncio.start_server(
        lambda r, w: manejar_cliente_tcp(r, w, queue),
        host=hosts_tcp,
        port=args.port_tcp,
    )
    logging.info("Servidor TCP escuchando en %s:%d", "/".join(hosts_tcp), args.port_tcp)

    loop = asyncio.get_running_loop()
    transportes_udp = []

    if ipv4:
        try:
            transporte_udp_v4, _ = await loop.create_datagram_endpoint(
                lambda: HeartbeatProtocol(queue),
                local_addr=(args.host, args.port_udp),
                family=socket.AF_INET,
            )
            transportes_udp.append(transporte_udp_v4)
            logging.info("Servidor UDP IPv4 escuchando en %s:%d", args.host, args.port_udp)
        except OSError as e:
            logging.warning("UDP IPv4 no disponible: %s", e)

    if ipv6:
        try:
            transporte_udp_v6, _ = await loop.create_datagram_endpoint(
                lambda: HeartbeatProtocol(queue),
                local_addr=("::", args.port_udp),
                family=socket.AF_INET6,
            )
            transportes_udp.append(transporte_udp_v6)
            logging.info("Servidor UDP IPv6 escuchando en [::]:%d", args.port_udp)
        except OSError as e:
            logging.warning("UDP IPv6 no disponible: %s", e)

    try:
        async with servidor_tcp:
            await servidor_tcp.serve_forever()
    finally:
        for transporte in transportes_udp:
            transporte.close()
