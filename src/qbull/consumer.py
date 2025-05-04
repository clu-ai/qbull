import asyncio
import redis.asyncio as aioredis
import uuid
import json
from datetime import datetime, timezone
import redis.exceptions
import logging
import signal
import socket # For default consumer name
from typing import Optional, Dict, Any, List, Callable, Coroutine # Añadido para type hints

# Assuming other modules are in the same package or PYTHONPATH
from .lock import RedisLock
from .partition_allocator import PartitionAllocator
from .config import (
    DEFAULT_JOB_LOCK_TTL_MS,
    DEFAULT_PARTITION_LOCK_TTL_SEC,
    PARTITION_REFRESH_INTERVAL_SEC,
    DEFAULT_CONSUMER_GROUP_NAME,
    DEFAULT_READ_BLOCK_MS,
    PARTITIONS as TOTAL_PARTITIONS, # Import total partitions config
    PARTITIONS_PER_POD, # Import partitions per pod config
)

# Logger para la librería
logger = logging.getLogger(__name__) # Usar __name__ es estándar

# Type Alias para Handlers para claridad
JobData = Dict[str, Any]
JobHandler = Callable[[JobData], Coroutine[Any, Any, None]]

class Consumer:
    """
    Manages consuming jobs from multiple stream partitions concurrently.
    Designed for integration into async applications or standalone execution.

    Handles automatic partition acquisition, locking, refreshing, and release.
    Allows registering handlers for specific stream names.
    Provides methods for standalone execution (`run`) or background execution
    integration (`run_in_background`, `stop`, `close`).
    """
    def __init__(
        self,
        redis_url: str,
        group_name: str = DEFAULT_CONSUMER_GROUP_NAME,
        total_partitions: int = TOTAL_PARTITIONS,
        partitions_to_acquire: int = PARTITIONS_PER_POD,
        job_lock_ttl_ms: int = DEFAULT_JOB_LOCK_TTL_MS,
        partition_lock_ttl_sec: int = DEFAULT_PARTITION_LOCK_TTL_SEC,
        partition_refresh_interval_sec: int = PARTITION_REFRESH_INTERVAL_SEC,
        read_block_ms: int = DEFAULT_READ_BLOCK_MS,
    ):
        # --- Validación de Configuración ---
        if partition_refresh_interval_sec >= partition_lock_ttl_sec:
            raise ValueError("partition_refresh_interval_sec must be less than partition_lock_ttl_sec")

        self.redis_url = redis_url
        self.group_name = group_name

        # Validar y convertir tipos de partición de forma robusta
        try:
            self.total_partitions = int(total_partitions)
            self.partitions_to_acquire = int(partitions_to_acquire)
            if self.total_partitions <= 0:
                raise ValueError("total_partitions must be positive")
            if self.partitions_to_acquire <= 0:
                 raise ValueError("partitions_to_acquire must be positive.")
            if self.partitions_to_acquire > self.total_partitions:
                 logger.warning(f"partitions_to_acquire ({self.partitions_to_acquire}) > total_partitions ({self.total_partitions}). Clamping to {self.total_partitions}.")
                 self.partitions_to_acquire = self.total_partitions
        except (ValueError, TypeError) as e:
            logger.error(f"Invalid integer value for partitions config: total='{total_partitions}', acquire='{partitions_to_acquire}'. Error: {e}")
            raise ValueError("total_partitions and partitions_to_acquire must be positive integers.") from e

        self.job_lock_ttl_ms = int(job_lock_ttl_ms)
        self.partition_lock_ttl_sec = int(partition_lock_ttl_sec)
        self.partition_refresh_interval_sec = int(partition_refresh_interval_sec)
        self.read_block_ms = int(read_block_ms)

        # Identificadores y Tokens
        self.consumer_base_name = f"consumer-{socket.gethostname()}-{uuid.uuid4().hex[:6]}"
        self.token = str(uuid.uuid4()) # Token para bloqueos de trabajo

        # Componentes Internos
        self.redis: Optional[aioredis.Redis] = None
        self.locker = RedisLock(redis_url)
        self.partition_allocator = PartitionAllocator(
            redis_url=redis_url,
            total_partitions=self.total_partitions,
            ttl_sec=self.partition_lock_ttl_sec
        )

        # Estado Interno
        self._connected = False
        self._running = False # Indica si las tareas de fondo están activas/deberían estarlo
        self.handlers: Dict[str, JobHandler] = {}
        self.acquired_partitions: List[int] = []
        self._listener_tasks: Dict[int, asyncio.Task] = {}
        self._refresh_task: Optional[asyncio.Task] = None
        self._main_loop_task: Optional[asyncio.Task] = None # Tarea para el bucle de espera
        self._stop_event = asyncio.Event()
        self._shutdown_signals_handled = False

        logger.info(f"Consumer '{self.consumer_base_name}' initialized. Group: '{group_name}', Target: {self.partitions_to_acquire}/{self.total_partitions} partitions, Job Lock: {job_lock_ttl_ms}ms, Part Lock: {partition_lock_ttl_sec}s")

    # --- REGISTRO DE HANDLERS ---
    def handler(self, stream_name: str) -> Callable[[JobHandler], JobHandler]:
        """Decorator para registrar un handler para un nombre de stream base específico."""
        if not isinstance(stream_name, str) or not stream_name:
             raise ValueError("stream_name for handler must be a non-empty string.")

        def decorator(func: JobHandler) -> JobHandler:
            if not asyncio.iscoroutinefunction(func):
                 raise TypeError(f"Handler for '{stream_name}' must be an async function (coroutine).")
            if stream_name in self.handlers:
                logger.warning(f"Overwriting handler for stream '{stream_name}' with function {func.__name__}.")
            self.handlers[stream_name] = func
            logger.info(f"Registered handler for stream '{stream_name}': {func.__name__}")
            return func
        return decorator

    # --- CONEXIÓN ---
    async def connect(self):
        """Conecta componentes internos (Redis client, Locker, Allocator) si no están conectados."""
        if self._connected:
            logger.debug(f"Consumer '{self.consumer_base_name}' already connected.")
            return
        logger.info(f"Consumer '{self.consumer_base_name}' connecting internal components...")
        try:
            # Conectar principal, locker, y allocator concurrentemente
            results = await asyncio.gather(
                aioredis.from_url(self.redis_url, decode_responses=True),
                self.locker.connect(),
                self.partition_allocator.connect(),
                return_exceptions=True
            )

            # Revisar resultados
            redis_conn, locker_res, allocator_res = results
            exceptions = []
            if isinstance(redis_conn, Exception):
                 exceptions.append(f"Main Redis connection failed: {redis_conn}")
            else:
                 self.redis = redis_conn
                 try:
                      await self.redis.ping()
                      logger.info("Main Redis connection successful.")
                 except (redis.exceptions.RedisError, OSError) as e:
                      exceptions.append(f"Main Redis ping failed: {e}")
                      self.redis = None

            if isinstance(locker_res, Exception): exceptions.append(f"Locker connection failed: {locker_res}")
            if isinstance(allocator_res, Exception): exceptions.append(f"Partition Allocator connection failed: {allocator_res}")

            if exceptions or not self.redis:
                 error_msg = f"Consumer '{self.consumer_base_name}' connection failed. Errors: " + "; ".join(exceptions)
                 logger.error(error_msg)
                 await self._close_internal_resources() # Intenta limpiar
                 raise ConnectionError(error_msg)

            self._connected = True
            logger.info(f"Consumer '{self.consumer_base_name}' connection established successfully.")

        except ConnectionError: # Re-lanzar ConnectionError específicamente
            raise
        except Exception as e:
             logger.exception(f"Unexpected error during consumer '{self.consumer_base_name}' connection")
             await self._close_internal_resources()
             self._connected = False
             raise # Re-lanzar para indicar fallo

    # --- LÓGICA DE GRUPOS ---
    async def _ensure_groups_for_partition(self, partition_num: int) -> bool:
        """Asegura que los grupos existan para todos los handlers en una partición."""
        if not self.redis or not self._connected:
             logger.error(f"[P{partition_num}] Cannot ensure groups: Consumer not connected.")
             return False
        if not self.handlers:
             logger.debug(f"[P{partition_num}] No handlers registered, skipping group creation.")
             return True

        logger.debug(f"[P{partition_num}] Ensuring consumer groups exist for registered streams...")
        group_creation_tasks = []
        stream_names = list(self.handlers.keys()) # Get keys once
        for stream_name in stream_names:
            stream_partition_key = f"{stream_name}:{partition_num}"
            group_creation_tasks.append(
                self._create_group_if_not_exists(stream_partition_key, self.group_name)
            )

        results = await asyncio.gather(*group_creation_tasks, return_exceptions=True)

        all_ok = True
        for i, result in enumerate(results):
             stream_partition_key = f"{stream_names[i]}:{partition_num}"
             if isinstance(result, Exception):
                  logger.error(f"[P{partition_num}] Failed to ensure group '{self.group_name}' for stream '{stream_partition_key}': {result}")
                  all_ok = False
             elif result is False:
                  logger.error(f"[P{partition_num}] Failed to ensure group '{self.group_name}' for stream '{stream_partition_key}' (creation/check error).")
                  all_ok = False
        if all_ok:
             logger.debug(f"[P{partition_num}] Consumer groups ensured.")
        return all_ok

    async def _create_group_if_not_exists(self, stream_key: str, group_name: str) -> bool:
        """Crea grupo XGROUP CREATE con MKSTREAM, ignora error si ya existe."""
        try:
            await self.redis.xgroup_create(stream_key, group_name, id='0', mkstream=True)
            logger.info(f"Created consumer group '{group_name}' for stream '{stream_key}'.")
            return True
        except redis.exceptions.ResponseError as e:
            if "BUSYGROUP Consumer Group name already exists" in str(e):
                logger.debug(f"Group '{group_name}' already exists for stream '{stream_key}'.")
                return True
            else:
                logger.error(f"Failed to create/verify group '{group_name}' for stream '{stream_key}': {e}")
                return False
        except redis.exceptions.RedisError as e:
            logger.error(f"Redis error ensuring group '{group_name}' for stream '{stream_key}': {e}")
            return False

    # --- LÓGICA DE PROCESAMIENTO DE MENSAJES ---
    async def _listen_on_partition(self, partition_num: int):
        """Tarea de fondo: Escucha mensajes en una partición específica."""
        consumer_name_partition = f"{self.consumer_base_name}-{partition_num}"
        logger.info(f"[P{partition_num}] Listener task started. Consumer name: '{consumer_name_partition}', Group: '{self.group_name}'")

        # Asegurar grupos ANTES del bucle principal
        if not await self._ensure_groups_for_partition(partition_num):
             logger.error(f"[P{partition_num}] Could not ensure groups. Listener task stopping.")
             # Considerar si necesitamos reintentar o señalar error globalmente
             self._running = False # Marcar como no corriendo si falla aquí? O dejar que el monitor lo reinicie?
             return

        streams_to_read = { f"{stream_name}:{partition_num}": ">" for stream_name in self.handlers.keys() }
        if not streams_to_read:
            logger.warning(f"[P{partition_num}] No streams to read (no handlers registered?). Listener task exiting.")
            return
        logger.info(f"[P{partition_num}] Listening on streams: {list(streams_to_read.keys())}")

        while not self._stop_event.is_set():
            response = None # Definir fuera del try
            try:
                response = await self.redis.xreadgroup(
                    groupname=self.group_name,
                    consumername=consumer_name_partition,
                    streams=streams_to_read,
                    count=1,
                    block=self.read_block_ms,
                )

                if not response: continue # Timeout

                for stream_key, messages in response:
                    try:
                         base_stream_name, p_str = stream_key.rsplit(':', 1)
                         if int(p_str) != partition_num: continue # Sanity check
                    except (ValueError, IndexError):
                         logger.warning(f"[P{partition_num}] Could not parse stream key '{stream_key}'. Skipping message.")
                         continue

                    if base_stream_name not in self.handlers: continue # Ya no debería pasar por la lógica de groups_ok

                    for message_id, fields in messages:
                        logger.debug(f"[P{partition_num}] Received message {message_id} from '{stream_key}'")
                        await self.process_message(message_id, fields, base_stream_name, partition_num, consumer_name_partition)
                        await asyncio.sleep(0.001) # Pequeño yield

            except redis.exceptions.ConnectionError as e:
                logger.error(f"[P{partition_num}] Redis connection error in listener: {e}. Attempting recovery/reconnect...")
                await asyncio.sleep(5)
                # Reintentar conexión principal (o asumir que aioredis lo hace?)
                try:
                    if not self.redis or not await self.redis.ping():
                         logger.warning(f"[P{partition_num}] Main Redis connection lost, attempting explicit reconnect...")
                         # No llamar a self.connect() completo para evitar relanzar todo
                         try:
                              new_conn = await aioredis.from_url(self.redis_url, decode_responses=True)
                              await new_conn.ping()
                              self.redis = new_conn # Reemplazar conexión si funciona
                              logger.info(f"[P{partition_num}] Main Redis reconnected.")
                         except Exception as recon_e:
                              logger.error(f"[P{partition_num}] Explicit reconnect failed: {recon_e}")
                              # Seguir reintentando en el próximo ciclo
                except Exception as ping_e:
                     logger.error(f"[P{partition_num}] Error during ping check after connection error: {ping_e}")

            except redis.exceptions.ResponseError as e:
                 if "NOGROUP" in str(e):
                     logger.warning(f"[P{partition_num}] Group '{self.group_name}' not found for a stream key in {list(streams_to_read.keys())}. Re-ensuring groups...")
                     await asyncio.sleep(2)
                     await self._ensure_groups_for_partition(partition_num) # Reintentar crear grupo
                 else:
                     logger.error(f"[P{partition_num}] Redis response error in listener: {e}")
                     await asyncio.sleep(5)
            except asyncio.CancelledError:
                logger.info(f"[P{partition_num}] Listener task cancelled.")
                break # Salir del bucle while si se cancela
            except Exception as e:
                logger.exception(f"[P{partition_num}] Unexpected error in listener loop: {e}")
                await asyncio.sleep(5)

        logger.info(f"[P{partition_num}] Listener task stopped.")

    async def process_message(self, message_id: str, fields: dict, stream_name: str, partition_num: int, consumer_name: str):
        """Procesa un mensaje: lock, handler, ack/release."""
        job_data = None
        job_id = None
        lock_key = None
        acquired = False
        stream_partition_key = f"{stream_name}:{partition_num}"

        try:
            job_raw = fields.get("job")
            if not job_raw:
                # ... (log y ack como antes) ...
                return

            try: job_data = json.loads(job_raw)
            except json.JSONDecodeError as e:
                # ... (log y ack como antes) ...
                return

            job_id = job_data.get("job_id")
            if not job_id:
                # ... (log y ack como antes) ...
                return

            lock_key = f"qbull:job_lock:{job_id}"
            acquired = await self.locker.acquire_or_extend(lock_key, self.token, self.job_lock_ttl_ms)

            if not acquired:
                # ... (log y return como antes) ...
                return

            logger.info(f"[{consumer_name}] Processing job {job_id} ({message_id} from '{stream_partition_key}')...")
            handler = self.handlers.get(stream_name)
            if not handler: # Should not happen if logic is correct
                # ... (log y ack como antes) ...
                return

            # Ejecutar handler
            await handler(job_data)
            logger.info(f"[{consumer_name}] Job {job_id} ({message_id} from '{stream_partition_key}') processed successfully.")

            # Ack DESPUÉS de procesar
            ack_result = await self.redis.xack(stream_partition_key, self.group_name, message_id)
            # ... (log de ack_result como antes) ...

        except Exception as e:
            # Error en el handler o en este método
            logger.exception(f"[{consumer_name}] Error processing job {job_id} ({message_id} from '{stream_partition_key}'): {e}")
            # NO HACER ACK
        finally:
            # Liberar lock si se adquirió
            if acquired and lock_key:
                released = await self.locker.release(lock_key, self.token)
                # ... (log si no se libera, como antes) ...

    async def _refresh_partition_locks(self):
        """Tarea de fondo: Refresca periódicamente los locks de partición."""
        logger.info(f"Partition lock refresh task started for '{self.consumer_base_name}'.")
        while not self._stop_event.is_set():
            # Calcular tiempo de espera preciso hasta el próximo refresco
            wait_time = self.partition_refresh_interval_sec
            try:
                # Esperar la mayor parte del tiempo, pero despertar si stop_event se activa
                await asyncio.wait_for(self._stop_event.wait(), timeout=wait_time)
                # Si wait_for no lanza TimeoutError, significa que _stop_event se activó
                break # Salir del bucle si el evento de parada se activó durante la espera
            except asyncio.TimeoutError:
                # Es el caso normal, el tiempo de espera terminó sin que se activara stop_event
                pass
            except asyncio.CancelledError:
                 logger.info("Refresh task wait cancelled.")
                 break # Salir si la tarea se cancela

            # Proceder con el refresco si todavía no debemos parar
            if self._stop_event.is_set(): break # Doble chequeo por si acaso
            if not self.acquired_partitions: continue # No hacer nada si no hay particiones
            if not self.partition_allocator or not self.partition_allocator._connected:
                 logger.warning("Partition allocator not connected, cannot refresh locks.")
                 continue

            logger.debug(f"Refreshing locks for partitions: {self.acquired_partitions}")
            try:
                success = await self.partition_allocator.refresh_n(self.acquired_partitions)
                if not success:
                    logger.warning("Potential issue refreshing partition locks (check allocator logs). Some locks might be lost.")
                    # Aquí podrías añadir lógica para verificar qué particiones se perdieron
                    # y quizás intentar readquirirlas o detener el listener correspondiente.
            except Exception as e:
                 logger.exception(f"Unexpected error during partition lock refresh: {e}")

        logger.info(f"Partition lock refresh task stopped for '{self.consumer_base_name}'.")

    # --- MÉTODOS DE CICLO DE VIDA REFACTORIZADOS ---

    async def start_listeners(self):
        """
        Adquiere particiones e inicia las tareas de escucha y refresco SIN bloquear.
        Llama a connect() implícitamente si es necesario.
        """
        if self._running:
            logger.warning(f"Consumer '{self.consumer_base_name}' background tasks may already be running.")
            return
        if not self._connected:
            # Asegurar conexión primero; connect() maneja su propia lógica de ya conectado
            await self.connect()

        if not self.handlers:
             logger.warning("No handlers registered. Consumer starting but will be idle regarding message processing.")
             # Permitir iniciar sin handlers, podrían registrarse después

        logger.info(f"Starting consumer background tasks '{self.consumer_base_name}'...")
        self._stop_event.clear() # Asegurar que el evento de parada no esté activo

        # Adquirir Particiones
        logger.info(f"Attempting to acquire {self.partitions_to_acquire} partitions of {self.total_partitions} total...")
        try:
            # Usar un timeout razonable para la adquisición inicial? O dejar que espere indefinidamente?
            # Por ahora, usa el wait_interval interno del allocator.
            self.acquired_partitions = await self.partition_allocator.acquire_n(self.partitions_to_acquire)
        except Exception as e:
            logger.exception("Failed to acquire partitions during startup.")
            await self._close_internal_resources() # Intenta limpiar conexiones
            raise ConnectionError("Failed to acquire initial partitions.") from e

        if not self.acquired_partitions:
            logger.warning(f"Failed to acquire any partitions after attempt. Consumer '{self.consumer_base_name}' running idle.")
            self._running = True # Marcar como corriendo (en estado idle)
            # No iniciar tareas de escucha/refresco si no hay particiones
            return

        logger.info(f"Successfully acquired partitions: {self.acquired_partitions}")

        # Iniciar Listener Tasks
        logger.info("Starting listener tasks...")
        self._listener_tasks = {}
        partitions_with_listeners = []
        for p_num in self.acquired_partitions:
             # Asegurar grupos antes de lanzar listener
             if not await self._ensure_groups_for_partition(p_num):
                 logger.error(f"Skipping listener task for partition {p_num} due to group creation/check failure.")
                 continue # No lanzar tarea para esta partición

             # Solo iniciar si no está ya corriendo (aunque stop() debería limpiarlos)
             if p_num not in self._listener_tasks or self._listener_tasks[p_num].done():
                 task_name = f"qbull_listener_{self.consumer_base_name}_{p_num}"
                 task = asyncio.create_task(self._listen_on_partition(p_num), name=task_name)
                 self._listener_tasks[p_num] = task
                 partitions_with_listeners.append(p_num)
                 logger.info(f"Listener task '{task_name}' created for partition {p_num}.")
             else:
                 logger.warning(f"Listener task for partition {p_num} seems to be already running (state unexpected).")
                 partitions_with_listeners.append(p_num) # Asumir que sigue corriendo

        if not partitions_with_listeners:
             logger.warning("No listener tasks were started (e.g., group creation failed for all acquired partitions). Consumer running idle.")
             self._running = True # Corriendo pero idle
             # No iniciar refresh si no hay listeners activos
             return

        # Iniciar Refresh Task (solo si tenemos listeners corriendo)
        self._refresh_task = None
        # Refrescar solo las particiones para las que SÍ iniciamos listener
        partitions_to_refresh = partitions_with_listeners
        if partitions_to_refresh:
             task_name = f"qbull_refresher_{self.consumer_base_name}"
             # Pasar las particiones a refrescar explícitamente (o modificar _refresh_partition_locks para usar self.acquired_partitions)
             # Modifiquemos _refresh_partition_locks para que use self.acquired_partitions (más simple)
             self._refresh_task = asyncio.create_task(self._refresh_partition_locks(), name=task_name)
             logger.info(f"Partition lock refresh task '{task_name}' started for partitions {partitions_to_refresh}.")
        else:
             logger.info("No partitions have active listeners, refresh task not started.")

        self._running = True # Marcar como activo (con tareas)
        logger.info(f"Consumer background tasks for '{self.consumer_base_name}' started successfully.")

    async def _wait_for_stop(self):
        """Bucle de espera principal. Bloquea hasta que stop() ponga el evento."""
        if not self._running:
            logger.debug(f"_wait_for_stop called but consumer '{self.consumer_base_name}' is not marked as running.")
            return # No esperar si no estamos corriendo
        logger.debug(f"Consumer '{self.consumer_base_name}' main loop waiting for stop event...")
        try:
            await self._stop_event.wait()
        except asyncio.CancelledError:
            logger.info(f"Consumer '{self.consumer_base_name}' wait loop cancelled externally.")
            # Asegurar que el evento se ponga para que stop() funcione bien
            self._stop_event.set()
        finally:
            logger.debug(f"Consumer '{self.consumer_base_name}' main loop finished waiting.")

    async def run_in_background(self) -> asyncio.Task | None:
        """
        Inicia el consumidor completo (conexión, listeners) en una tarea de fondo.
        Retorna la tarea principal del loop del consumidor si el inicio es exitoso,
        o None si falla. Ideal para integración con frameworks (FastAPI lifespan).
        """
        self.logger.info(f"Attempting to run consumer '{self.consumer_base_name}' in background...")
        self._main_loop_task = None # Reiniciar referencia a la tarea principal
        try:
            # Conectar y lanzar listeners/refresh. start_listeners ya llama a connect.
            await self.start_listeners()

            # Crear la tarea para el bucle de espera bloqueante, SOLO si estamos 'running'
            # (lo que implica que al menos la adquisición de particiones no falló críticamente)
            if self._running:
                task_name = f"qbull_wait_loop_{self.consumer_base_name}"
                self._main_loop_task = asyncio.create_task(
                    self._wait_for_stop(),
                    name=task_name
                )
                self.logger.info(f"Consumer main loop running in background task: {self._main_loop_task.get_name()}")
                return self._main_loop_task # Devolver tarea
            else:
                 # No se pudo iniciar correctamente (ej: fallo al adquirir particiones y se marcó idle)
                 self.logger.warning(f"Consumer '{self.consumer_base_name}' did not start background tasks properly (state is not running).")
                 # Devolver None porque la tarea principal de espera no se creó
                 return None

        except Exception as e:
            # Error durante connect() o start_listeners() que no fue ConnectionError
            self.logger.exception(f"CRITICAL: Failed to start consumer '{self.consumer_base_name}' in background!")
            await self._close_internal_resources() # Intentar limpiar conexiones
            self._running = False # Asegurar estado no corriendo
            return None # Indicar fallo

    async def stop(self, signal_name: str = "programmatic"):
        """Señaliza al consumidor para detenerse ordenadamente, cancela tareas hijas."""
        # Chequeo idempotencia / estado no corriendo
        if not self._running and self._stop_event.is_set():
             logger.info(f"Stop requested ({signal_name}) for '{self.consumer_base_name}', but already stopped or stopping.")
             return
        if not self._running:
             logger.info(f"Stop requested ({signal_name}) for '{self.consumer_base_name}', but consumer wasn't running. Setting stop event.")
             self._stop_event.set() # Poner evento por si algo esperaba
             return

        logger.info(f"Stop requested ({signal_name}) for '{self.consumer_base_name}'. Initiating graceful shutdown...")
        self._stop_event.set() # <-- Señaliza a _wait_for_stop para que termine

        # Cancelar tareas hijas explícitamente
        tasks_to_cancel: List[asyncio.Task] = []
        task_names_to_cancel: List[str] = []

        logger.info("Cancelling listener and refresh tasks...")
        if self._refresh_task and not self._refresh_task.done():
            self._refresh_task.cancel()
            tasks_to_cancel.append(self._refresh_task)
            try: task_names_to_cancel.append(self._refresh_task.get_name())
            except AttributeError: pass # Older asyncio might not have get_name

        listener_tasks_to_cancel = [task for task in self._listener_tasks.values() if task and not task.done()]
        for task in listener_tasks_to_cancel:
            task.cancel()
            tasks_to_cancel.append(task)
            try: task_names_to_cancel.append(task.get_name())
            except AttributeError: pass

        # Esperar a que las tareas canceladas terminen
        if tasks_to_cancel:
            logger.info(f"Waiting for tasks {task_names_to_cancel} to finish cancellation...")
            results = await asyncio.gather(*tasks_to_cancel, return_exceptions=True)
            logger.info("Listener/refresh tasks finished cancellation.")
            for i, result in enumerate(results):
                 # Loguear errores que no sean CancelledError
                 if isinstance(result, Exception) and not isinstance(result, asyncio.CancelledError):
                     logger.error(f"Error during cancellation of task {task_names_to_cancel[i]}: {result!r}")
        else:
             logger.info("No active listener/refresh tasks found to cancel.")

        # La tarea _main_loop_task (si existe) terminará por sí sola debido al evento.
        # NO la cancelamos aquí. El llamador (lifespan o run()) es responsable de esperarla.
        logger.info("Graceful shutdown signal sent. Main loop should exit.")
        # El estado _running se pone a False explícitamente en close()


    async def _close_internal_resources(self):
         """Cierra conexiones internas (Redis, Allocator, Locker)."""
         logger.debug(f"Closing internal connections for '{self.consumer_base_name}'...")
         close_tasks = []

         # Intentar cerrar allocator y locker primero
         if self.partition_allocator:
             # Usar gather para asegurar que close es llamado incluso si falla uno
             async def safe_close_allocator():
                 try: await self.partition_allocator.close()
                 except Exception as e: logger.error(f"Error closing PartitionAllocator: {e}", exc_info=True); return e
                 return None
             close_tasks.append(safe_close_allocator())
         if self.locker:
             async def safe_close_locker():
                 try: await self.locker.close()
                 except Exception as e: logger.error(f"Error closing RedisLock: {e}", exc_info=True); return e
                 return None
             close_tasks.append(safe_close_locker())

         # Cerrar conexión principal Redis después
         if self.redis:
             async def safe_close_main_redis():
                 try:
                      await self.redis.close()
                      logger.info("Main Redis connection closed.")
                 except redis.exceptions.RedisError as e:
                      logger.error(f"Error closing main Redis connection: {e}", exc_info=True)
                      return e
                 except Exception as e:
                     logger.error(f"Unexpected error closing main Redis connection: {e}", exc_info=True)
                     return e
                 finally:
                      self.redis = None # Asegurar que se ponga a None
                 return None
             close_tasks.append(safe_close_main_redis())

         if close_tasks:
             results = await asyncio.gather(*close_tasks) # return_exceptions=False, handled inside helpers
             logger.info("Attempted closing internal connections.")
             # Loguear errores específicos capturados por los helpers si es necesario
             # for result in results:
             #    if isinstance(result, Exception): # O chequear si no es None
             #       logger.error(f"Captured error during component close: {result!r}")
         self._connected = False # Marcar como desconectado

    async def close(self):
        """Detiene el consumidor, libera particiones y cierra todas las conexiones."""
        logger.info(f"Closing consumer '{self.consumer_base_name}' resources...")

        # 1. Iniciar parada de tareas (pone evento, cancela listeners/refresh)
        # Llamar a stop incluso si _running es False para asegurar el estado _stop_event.set()
        await self.stop()

        # 2. Esperar (opcionalmente) a la tarea principal si existe y fue creada por run_in_background
        # Es mejor que el llamador (lifespan) espere, pero damos una pequeña oportunidad aquí.
        if self._main_loop_task and not self._main_loop_task.done():
             logger.debug("Waiting briefly for main loop task in close()...")
             try:
                  await asyncio.wait_for(self._main_loop_task, timeout=2.0)
             except asyncio.TimeoutError:
                  logger.warning("Main loop task did not finish quickly during close.")
             except asyncio.CancelledError:
                  logger.info("Main loop task was already cancelled during close wait.")
             except Exception:
                 logger.exception("Error waiting for main loop task during close.")

        # 3. Liberar particiones adquiridas (IMPORTANTE)
        if self.acquired_partitions and self.partition_allocator:
            logger.info(f"Releasing acquired partitions: {self.acquired_partitions}")
            try:
                await self.partition_allocator.release_n(self.acquired_partitions)
                logger.info("Partitions released.")
            except Exception as e:
                 logger.exception(f"Error releasing partitions during close: {e}")
            finally:
                 self.acquired_partitions = [] # Limpiar siempre

        # 4. Cerrar conexiones internas restantes (allocator, locker, redis)
        await self._close_internal_resources()

        # 5. Actualizar estado final
        self._running = False # Asegurar estado final
        self._listener_tasks = {}
        self._refresh_task = None
        self._main_loop_task = None
        # No limpiar self.handlers

        logger.info(f"Consumer '{self.consumer_base_name}' closed.")

    # --- MÉTODO run() PARA EJECUCIÓN STANDALONE ---
    async def run(self):
        """Conecta, configura señales, inicia tareas en fondo y espera hasta detenerse."""
        main_task = None
        try:
             # run_in_background maneja connect y start_listeners
             main_task = await self.run_in_background()

             if main_task:
                 # Configurar señales solo si el inicio fue exitoso y estamos corriendo
                 self._setup_signal_handlers()
                 # Esperar a que la tarea principal (_wait_for_stop) termine
                 await main_task # El wait_for real sucede aquí en modo standalone
             else:
                 logger.error(f"Consumer '{self.consumer_base_name}' could not start in background. Exiting run method.")

        except ConnectionError as e:
             logger.error(f"Failed to connect, consumer cannot run: {e}")
        except Exception as e:
             logger.exception(f"Consumer run method failed unexpectedly: {e}")
             # Si la tarea principal fue creada pero falló, intentar cancelarla?
             # if main_task and not main_task.done(): main_task.cancel()
        finally:
             # close() se encarga de llamar a stop() y limpiar todo
             logger.info(f"Consumer '{self.consumer_base_name}' run method finished or failed, closing resources...")
             await self.close()

    # --- MANEJO DE SEÑALES ---
    def _setup_signal_handlers(self):
        """Configura manejadores de señal OS para cierre ordenado."""
        if self._shutdown_signals_handled: return
        try:
             loop = asyncio.get_running_loop()
             for sig in (signal.SIGINT, signal.SIGTERM):
                  # Usar lambda para pasar argumento a la corutina creada por la señal
                  loop.add_signal_handler(sig, lambda s=sig: asyncio.create_task(self.stop(signal_name=s.name)))
             self._shutdown_signals_handled = True
             logger.info("OS signal handlers (SIGINT, SIGTERM) configured.")
        except NotImplementedError:
             logger.warning("Signal handler setup not supported (e.g., Windows ProactorLoop). Graceful shutdown via signals may not work.")
        except Exception as e:
            logger.exception("Failed to set up OS signal handlers.")