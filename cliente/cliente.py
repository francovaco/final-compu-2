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


def conectar_tcp(servidor: str, port_tcp: int) -> socket.socket:
    # Crea y conecta un socket TCP al servidor
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((servidor, port_tcp))
    logging.info("Conectado al servidor %s:%d", servidor, port_tcp)
    return sock


def correr(args: argparse.Namespace) -> None:
    # Loop principal: recolecta métricas, las envía por TCP y manda heartbeats por UDP
    nodo = socket.gethostname()
    sock_udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    ultimo_heartbeat = 0.0
    sock_tcp = None

    while True:
        # Conectar o reconectar TCP
        if sock_tcp is None:
            try:
                sock_tcp = conectar_tcp(args.servidor, args.port_tcp)
            except OSError as e:
                logging.warning("No se pudo conectar al servidor: %s. Reintentando en %.0fs...", e, args.intervalo)
                time.sleep(args.intervalo)
                continue

        # Recolectar y enviar métricas por TCP
        try:
            metricas = recolectar_metricas()
            enviar_metricas(sock_tcp, metricas)
            logging.info("Métricas enviadas: CPU=%.1f%% RAM=%.1f%% Disco=%.1f%%",
                         metricas["cpu"], metricas["ram"], metricas["disco"])
        except OSError as e:
            logging.warning("Error enviando métricas: %s. Reconectando...", e)
            sock_tcp.close()
            sock_tcp = None
            continue

        # Enviar heartbeat por UDP si corresponde
        ahora = time.monotonic()
        if ahora - ultimo_heartbeat >= args.intervalo_heartbeat:
            try:
                enviar_heartbeat(sock_udp, nodo, args.servidor, args.port_udp)
                logging.debug("Heartbeat enviado")
                ultimo_heartbeat = ahora
            except OSError as e:
                logging.warning("Error enviando heartbeat: %s", e)

        time.sleep(args.intervalo)


def main() -> None:
    # Parsea argumentos, configura logging e inicia el agente
    parser = argparse.ArgumentParser(description="Agente de monitoreo de servidor")
    parser.add_argument("--servidor", default="localhost", help="Host del servidor central")
    parser.add_argument("--port-tcp", type=int, default=9000, help="Puerto TCP del servidor")
    parser.add_argument("--port-udp", type=int, default=9001, help="Puerto UDP del servidor")
    parser.add_argument("--intervalo", type=float, default=10, help="Segundos entre envíos de métricas")
    parser.add_argument("--intervalo-heartbeat", type=float, default=5, help="Segundos entre heartbeats")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    args = parser.parse_args()

    configurar_logging(args.log_level)
    logging.info("Iniciando agente en nodo '%s'", socket.gethostname())

    try:
        correr(args)
    except KeyboardInterrupt:
        logging.info("Agente detenido")


if __name__ == "__main__":
    main()
