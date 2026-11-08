"""Cola durable de tareas en RabbitMQ (AMQP sobre TLS mutuo, puerto 5671).

El líder de metadatos publica tareas de re-replicación y limpieza; los
trabajadores las consumen con ACK manual (sección 8). Si RabbitMQ se cae, las
tareas siguen registradas en metadatos y se vuelven a publicar.
"""
from __future__ import annotations

import json
import logging
import queue
import ssl
import threading
import time

import pika

from .config import env, env_int
from .tls import TLS_ENABLED, client_ssl_context

log = logging.getLogger("mq")

QUEUE = env("DFSHA_MQ_QUEUE", "dfsha.tasks")


def connection_params(cert_name: str) -> pika.ConnectionParameters:
    host = env("DFSHA_MQ_HOST", "127.0.0.1")
    user = env("DFSHA_MQ_USER", "dfsha")
    password = env("DFSHA_MQ_PASSWORD", "")
    kwargs = dict(
        host=host,
        virtual_host=env("DFSHA_MQ_VHOST", "/"),
        credentials=pika.PlainCredentials(user, password),
        heartbeat=30,
        blocked_connection_timeout=30,
        connection_attempts=1,
        socket_timeout=5,
    )
    if TLS_ENABLED:
        ctx = client_ssl_context(cert_name)
        assert isinstance(ctx, ssl.SSLContext)
        kwargs["port"] = env_int("DFSHA_MQ_PORT", 5671)
        kwargs["ssl_options"] = pika.SSLOptions(ctx, server_hostname=env("DFSHA_MQ_TLS_NAME", "rabbitmq"))
    else:
        kwargs["port"] = env_int("DFSHA_MQ_PORT", 5672)
    return pika.ConnectionParameters(**kwargs)


def declare(channel) -> None:
    channel.queue_declare(queue=QUEUE, durable=True, arguments={"x-max-priority": 10})


class Publisher:
    """Hilo dueño de una conexión pika (pika no es seguro entre hilos)."""

    def __init__(self, cert_name: str):
        self.cert_name = cert_name
        self.q: queue.Queue = queue.Queue(maxsize=10000)
        self.conn = None
        self.ch = None
        self.ok = False
        threading.Thread(target=self._run, name="mq-publisher", daemon=True).start()

    def publish(self, task: dict, priority: int = 5) -> bool:
        try:
            self.q.put_nowait((task, priority))
            return True
        except queue.Full:
            return False

    def _connect(self) -> None:
        self.conn = pika.BlockingConnection(connection_params(self.cert_name))
        self.ch = self.conn.channel()
        declare(self.ch)
        self.ok = True
        log.info("conectado a RabbitMQ")

    def _run(self) -> None:
        while True:
            try:
                if self.conn is None or self.conn.is_closed:
                    self._connect()
                try:
                    task, prio = self.q.get(timeout=1.0)
                except queue.Empty:
                    self.conn.process_data_events(time_limit=0)
                    continue
                self.ch.basic_publish(
                    exchange="", routing_key=QUEUE, body=json.dumps(task).encode(),
                    properties=pika.BasicProperties(delivery_mode=2, priority=max(0, min(9, prio)),
                                                    content_type="application/json"),
                )
            except Exception as exc:  # RabbitMQ caído: las tareas se republican luego
                if self.ok:
                    log.warning("RabbitMQ no disponible: %s", exc)
                self.ok = False
                self.conn = None
                time.sleep(3)
