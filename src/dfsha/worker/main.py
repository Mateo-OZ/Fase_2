"""Trabajador de mantenimiento (sección 8).

Consume la cola durable de RabbitMQ con ACK manual. Para cada tarea pide a
metadatos un plan actualizado (fuente, destino y token), lo ejecuta contra
los nodos de datos por el puerto interno (TLS mutuo), confirma en metadatos y
solo entonces hace el ACK. Si se cae a mitad de una tarea, el mensaje vuelve
a la cola y otro trabajador la toma.
"""
from __future__ import annotations

import json
import time

import grpc
import pika

from dfsha.common.audit import setup_logging
from dfsha.common.config import env, parse_peers
from dfsha.common.metaclient import MetaClient
from dfsha.common.mq import QUEUE, connection_params, declare
from dfsha.common.tls import grpc_channel
from dfsha.proto import dfsha_pb2 as pb
from dfsha.proto import dfsha_pb2_grpc as pbg

WORKER_ID = env("DFSHA_WORKER_ID", "worker1")
log = setup_logging(WORKER_ID)


class Worker:
    def __init__(self):
        self.meta = MetaClient(list(parse_peers(env("DFSHA_META_PEERS", required=True)).values()), WORKER_ID)
        self.stubs: dict[str, pbg.DataNodeStub] = {}

    def stub(self, addr: str) -> pbg.DataNodeStub:
        if addr not in self.stubs:
            self.stubs[addr] = pbg.DataNodeStub(grpc_channel(addr, WORKER_ID))
        return self.stubs[addr]

    def handle(self, task: dict) -> bool:
        """Devuelve True si la tarea terminó (ACK) o False para reintentar."""
        status, plan = self.meta.call("task_plan", {"task_id": task["task_id"]})
        if status != 200:
            log.warning("no se obtuvo el plan de %s: %s %s", task["task_id"], status, plan)
            return False
        action = plan.get("action")
        if action == "skip":
            return True
        if action == "retry":
            log.info("tarea %s pospuesta: %s", task["task_id"], plan.get("reason"))
            return False
        receipt = None
        if action == "replicate":
            req = pb.ReplicateBlockRequest(block_id=plan["block_id"], token=plan["token"],
                                           source_addr=plan["source_addr"], sha256=plan["sha256"],
                                           size=plan["size"])
            try:
                r = self.stub(plan["target_addr"]).ReplicateBlock(req, timeout=120)
            except grpc.RpcError as exc:
                log.warning("copia de %s a %s falló: %s %s", plan["block_id"][:14], plan["target"],
                            exc.code().name, exc.details())
                return False
            receipt = {"dn_id": r.dn_id, "block_id": r.block_id, "sha256": r.sha256, "size": r.size, "sig": r.sig}
            log.info("re-replicación: bloque %s copiado de %s a %s", plan["block_id"][:14], plan["source"],
                     plan["target"])
        elif action == "delete":
            for dn, addr in plan.get("targets", []):
                try:
                    self.stub(addr).DeleteBlock(pb.DeleteBlockRequest(block_id=task["block_id"],
                                                                      token=plan["token"]), timeout=20)
                except grpc.RpcError as exc:
                    log.warning("no se pudo borrar %s en %s: %s", task["block_id"][:14], dn, exc.code().name)
                    return False
            log.info("limpieza: bloque %s borrado en %s", task["block_id"][:14],
                     [d for d, _ in plan.get("targets", [])] or "ningún nodo vivo")
        elif action != "complete":
            log.warning("acción desconocida %s", action)
            return True
        status, res = self.meta.call("task_complete", {"task_id": task["task_id"], "receipt": receipt})
        return status == 200

    def run(self) -> None:
        while True:
            try:
                conn = pika.BlockingConnection(connection_params(WORKER_ID))
                ch = conn.channel()
                declare(ch)
                ch.basic_qos(prefetch_count=1)
                log.info("%s conectado a RabbitMQ, esperando tareas en %s", WORKER_ID, QUEUE)

                def on_message(channel, method, props, body):
                    task = json.loads(body)
                    try:
                        done = self.handle(task)
                    except Exception:
                        log.exception("error procesando %s", task)
                        done = False
                    if done:
                        channel.basic_ack(method.delivery_tag)
                    else:
                        time.sleep(2)
                        channel.basic_nack(method.delivery_tag, requeue=True)

                ch.basic_consume(queue=QUEUE, on_message_callback=on_message, auto_ack=False)
                ch.start_consuming()
            except Exception as exc:
                log.warning("RabbitMQ no disponible (%s); reintento en 5 s", exc)
                time.sleep(5)


def main() -> None:
    Worker().run()


if __name__ == "__main__":
    main()
