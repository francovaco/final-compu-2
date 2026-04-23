# Decisiones de diseño

**Franco Vaccarezza — Legajo 63179**

---

## asyncio para el servidor TCP/UDP

El servidor recibe conexiones de múltiples agentes simultáneamente. Esta tarea es 100% I/O bound: el proceso pasa la mayor parte del tiempo esperando datos de la red, no ejecutando cómputo. Maneja miles de conexiones concurrentes con un solo hilo mediante un event loop, sin el overhead de crear un thread por cliente. Multithreading agregaría complejidad y contención por el GIL sin ningún beneficio real en tareas I/O bound.

## multiprocessing.Queue entre asyncio y el analizador

El servidor asyncio y el analizador corren en procesos separados. Para comunicarlos se usa `multiprocessing.Queue`, que es thread-safe, process-safe y soporta múltiples productores. Se eligió sobre pipes porque la Queue puede tener múltiples escritores concurrentes (varios clientes enviando métricas al mismo tiempo) sin coordinación adicional. Los pipes de Unix están diseñados para comunicación entre dos procesos, no para este patrón de múltiples productores.

## ProcessPoolExecutor en el analizador

El análisis de métricas (comparar umbrales, escribir en SQLite) es CPU bound. Para procesar métricas de múltiples agentes en paralelo real se usa `ProcessPoolExecutor`, que crea workers en procesos separados con su propio GIL. Esto permite analizar varias métricas simultáneamente sin que el GIL las serialice. Multithreading no sería suficiente porque el GIL limita la ejecución paralela de código Python puro.

## multiprocessing.Lock para escrituras en SQLite

El `ProcessPoolExecutor` tiene múltiples workers que pueden intentar escribir en `metricas.db` al mismo tiempo. SQLite no garantiza seguridad ante escrituras concurrentes desde múltiples procesos. Se usa un `multiprocessing.Lock` creado en `main.py` y pasado al analizador para serializar las escrituras: cada worker adquiere el lock solo al momento de escribir, permitiendo que el análisis en sí corra en paralelo.

## Celery + Redis para tareas en background

El envío de emails y la generación de reportes son tareas que no deben bloquear el flujo principal de análisis de métricas. Celery permite encolar estas tareas y ejecutarlas de forma asíncrona en un worker separado. Redis actúa como broker, almacenando la cola de tareas pendientes. Esta separación garantiza que un fallo en el envío de email no afecte la recepción ni el análisis de métricas.

## Docker para el despliegue

Docker garantiza que todos los componentes del sistema corren en entornos idénticos y aislados, independientemente del SO del host. Permite levantar el servidor, Redis, Celery y múltiples clientes con un solo comando, sin instalar dependencias manualmente. El escalado de clientes para simular múltiples agentes se resuelve con `--scale cliente=N`. Además, los volúmenes compartidos entre contenedores permiten que el analizador y Celery accedan a los mismos archivos SQLite sin configuración adicional.
