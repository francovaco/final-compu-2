import argparse
import json
import logging
import socket
import time
from datetime import datetime

import psutil


def configurar_logging(nivel: str) -> None:
    # Configura el formato y nivel de logs
    logging.basicConfig(
        level=getattr(logging, nivel.upper()),
        format="%(asctime)s [%(levelname)s] %(message)s",
    )


def obtener_temperatura() -> float | None:
    # Retorna la temperatura de la CPU o None si no está disponible
    try:
        temps = psutil.sensors_temperatures()
        if temps:
            for entradas in temps.values():
                if entradas:
                    return entradas[0].current
    except (AttributeError, NotImplementedError):
        pass
    return None


def recolectar_metricas() -> dict:
    # Lee CPU, RAM, disco y temperatura con psutil
    return {
        "nodo": socket.gethostname(),
        "cpu": psutil.cpu_percent(interval=1),
        "ram": psutil.virtual_memory().percent,
        "disco": psutil.disk_usage("/").percent,
        "temperatura": obtener_temperatura(),
        "timestamp": datetime.now().isoformat(),
    }


def enviar_metricas(sock: socket.socket, metricas: dict) -> None:
    # Serializa las métricas a JSON y las envía por TCP
    datos = json.dumps(metricas).encode() + b"\n"
    sock.sendall(datos)


def enviar_heartbeat(sock_udp: socket.socket, nodo: str, servidor: str, port_udp: int) -> None:
    # Envía un paquete UDP para indicar que el nodo sigue activo
    datos = json.dumps({"nodo": nodo, "tipo": "heartbeat"}).encode()
    sock_udp.sendto(datos, (servidor, port_udp))