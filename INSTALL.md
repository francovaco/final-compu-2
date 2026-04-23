# Instalación y puesta en marcha

## Requisitos

- Docker
- Docker Compose

## Pasos

**1. Clonar el repositorio**

```bash
git clone https://github.com/francovaco/final-compu-2.git
cd final-compu-2
```

**2. Configurar el archivo `.env`**

Copiá el archivo de ejemplo y completá las credenciales de email:

```bash
cp .env.example .env
```

Editá el `.env` con tus datos:

```
EMAIL_REMITENTE=tu-cuenta@gmail.com
EMAIL_PASSWORD=tu-app-password
EMAIL_DESTINATARIO=destinatario@gmail.com
```

> Si no configurás `EMAIL_DESTINATARIO`, el sistema funciona igual pero no envía emails — las alertas solo se loguean en consola.

> Para Gmail, `EMAIL_PASSWORD` debe ser una **App Password** generada desde: Cuenta de Google → Seguridad → Verificación en dos pasos → Contraseñas de aplicaciones.

**3. Levantar el sistema**

```bash
docker compose up --build --scale cliente=N
```

Esto levanta el servidor, Redis, el worker Celery y N clientes simulados.

## Comandos útiles

```bash
# Ver logs del servidor
docker compose logs -f servidor

# Ver logs de los clientes
docker compose logs -f cliente

# Ver logs de Celery
docker compose logs -f celery

# Abrir terminal dentro del servidor
docker compose exec servidor bash

# Bajar todo
docker compose down

# Bajar todo y eliminar las bases de datos
docker compose down -v
```
