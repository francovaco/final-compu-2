## Descripción de la aplicación

La idea es armar una aplicación cliente-servidor para monitorear el estado de múltiples servidores en tiempo real. Cada servidor tiene un agente corriendo que recolecta métricas del sistema (CPU, RAM, disco) y las envía por TCP a un servidor central además de enviar un "heartbeat" por UDP para saber si el servidor esta vivo o no.

El servidor central tiene dos procesos internos: uno basado en asyncio que recibe las conexiones de los agentes de forma concurrente, y un proceso analizador separado que evalúa las métricas y genera alertas cuando algo supera un umbral configurable. Además de las métricas por TCP, cada agente envía un heartbeat periódico por UDP para indicar que sigue activo. Si el analizador deja de recibir heartbeats de un nodo durante un tiempo determinado, genera una alerta de nodo caído. Estos dos procesos se comunican mediante un FIFO. El analizador también persiste todas las métricas en una base de datos SQLite para tener historial.

Las tareas pesadas como generar reportes históricos o calcular tendencias se delegan a workers de Celery que corren en background, consultando SQLite cuando necesitan datos. Esto evita que el proceso principal se trabe con trabajo costoso.

La concurrencia se usa en asyncio para manejar múltiples agentes conectados al mismo tiempo sin crear un proceso por cada uno. El paralelismo se usa en los workers de Celery que pueden ejecutar múltiples reportes en simultáneo. La comunicación entre el proceso asyncio y el analizador es asincrónica mediante un FIFO. Tanto el agente como el servidor se configuran completamente por línea de comandos con argparse.

---

## Funcionalidades de cada entidad

**Agente (cliente)**
- Recolecta métricas del sistema: CPU, RAM, disco y timestamp
- Se conecta al servidor central por TCP
- Envía las métricas cada N segundos (configurable por argparse)
- Envía un heartbeat por UDP cada N segundos para indicar que sigue activo
- Se reconecta automáticamente si pierde la conexión

**Servidor — proceso asyncio**
- Escucha conexiones TCP entrantes de múltiples agentes en simultáneo
- Escucha heartbeats UDP de los agentes
- Lee las métricas que llegan de forma no bloqueante
- Escribe las métricas y los heartbeats en el FIFO para que el analizador los procese

**Servidor — proceso analizador**
- Lee métricas y heartbeats del FIFO
- Compara cada métrica contra los umbrales configurados
- Genera una alerta en consola o log si se supera algún umbral
- Detecta nodos caídos cuando dejan de llegar heartbeats durante un tiempo determinado
- Persiste todas las métricas en SQLite con timestamp y nombre del nodo
- Encola tareas en Celery cuando corresponde generar reportes

**Celery workers**
- Consultan SQLite para obtener el historial de un nodo
- Calculan promedios, máximos y mínimos del período
- Generan reportes históricos
- Limpian registros viejos de la base de datos periódicamente

**SQLite**
- Almacena el historial completo de métricas de todos los nodos
- Persiste los datos aunque el servidor se reinicie
