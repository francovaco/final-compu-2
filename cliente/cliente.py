import argparse
import json
import logging
import random
import socket
import time
from datetime import datetime


def configurar_logging(nivel: str) -> None:
    # Configura el formato y nivel de logs
    logging.basicConfig(
        level=getattr(logging, nivel.upper()),
        format="%(asctime)s [%(levelname)s] %(message)s",
    )


def recolectar_metricas() -> dict:
    # Genera métricas aleatorias simuladas
    return {
        "nodo": socket.gethostname(),
        "cpu": round(random.uniform(10, 95), 1),
        "ram": round(random.uniform(20, 90), 1),
        "disco": round(random.uniform(30, 85), 1),
        "temperatura": round(random.uniform(40, 95), 1),
        "timestamp": datetime.now().isoformat(),
    }


def enviar_metricas(sock: socket.socket, metricas: dict) -> None:
    # Serializa las métricas a JSON y las envía por TCP
    datos = json.dumps(metricas).encode() + b"\n"
    sock.sendall(datos)


def enviar_heartbeat(sock_udp: socket.socket, nodo: str, udp_addr: tuple) -> None:
    # Envía un paquete UDP para indicar que el nodo sigue activo
    datos = json.dumps({"nodo": nodo, "tipo": "heartbeat"}).encode()
    sock_udp.sendto(datos, udp_addr)


def conectar_tcp(servidor: str, port_tcp: int) -> socket.socket:
    # Prueba todas las familias disponibles (IPv4/IPv6) y conecta con la primera que funcione
    infos = socket.getaddrinfo(servidor, port_tcp, type=socket.SOCK_STREAM)
    ultimo_error = None
    for family, _, _, _, sockaddr in infos:
        try:
            sock = socket.socket(family, socket.SOCK_STREAM)
            sock.connect(sockaddr)
            logging.info("Conectado al servidor %s (IPv%s)", sockaddr, "6" if family == socket.AF_INET6 else "4")
            return sock
        except OSError as e:
            sock.close()
            ultimo_error = e
    raise OSError(f"No se pudo conectar a {servidor}:{port_tcp}: {ultimo_error}")


def resolver_udp(servidor: str, port_udp: int) -> tuple[socket.socket, tuple]:
    # Resuelve la dirección UDP y crea el socket con la familia correcta (IPv4/IPv6)
    info = socket.getaddrinfo(servidor, port_udp, type=socket.SOCK_DGRAM)[0]
    family, _, _, _, udp_addr = info
    sock = socket.socket(family, socket.SOCK_DGRAM)
    logging.debug("UDP resuelto a %s (IPv%s)", udp_addr, "6" if family == socket.AF_INET6 else "4")
    return sock, udp_addr


def correr(args: argparse.Namespace) -> None:
    # Loop principal: recolecta métricas, las envía por TCP y manda heartbeats por UDP
    nodo = socket.gethostname()
    sock_udp, udp_addr = resolver_udp(args.servidor, args.port_udp)

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
                enviar_heartbeat(sock_udp, nodo, udp_addr)
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
