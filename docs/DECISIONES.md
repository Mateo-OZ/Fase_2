# Del diseño de la Fase I a la implementación de la Fase II

Este documento relaciona cada decisión del diseño individual de **Andrey Murillo Osorio**
(Opción 1, cliente/servidor con servicio distribuido entre servidores) con el código, y
registra **qué cambió y por qué** (requisito de la sección 5.1 del enunciado: "registrar qué
decisiones cambian y por qué").

## 1. Correspondencia diseño → código

| Sección del diseño | Decisión | Dónde está implementada |
|---|---|---|
| 1, 4 | Bloques de 8 MiB (configurable, 1 MiB en pruebas), 3 réplicas, confirmación con 2 | `common/config.py` (`DFSHA_BLOCK_SIZE`, `DFSHA_REPLICATION`, `DFSHA_MIN_REPLICAS`); `--block-size` y `--replication` en `dfs put` |
| 2 | CLI `dfs` y librería Python | `client/cli.py`, `client/api.py` (`DFSClient`, `DFSFile`) |
| 2 | Secuencia de carga: upload_id → bloques en paralelo → cadena → commit | `DFSClient.put`, `metadata/service.py: op_begin_upload / op_commit_upload`, `datanode/service.py: WriteBlock` |
| 2 | Lectura: manifiesto → bloques en paralelo → hash por bloque y del archivo | `DFSClient.get`, `DFSClient.read_block` |
| 2 | Escritura parcial con copia en escritura | `DFSFile.write / fsync` (bloques nuevos, nunca se modifica uno existente) |
| 2 | Borrado lógico + limpieza asíncrona | `state_machine._op_rm`, `service._garbage_scan`, `worker/main.py` |
| 3 | Plano de control separado del plano de datos | La puerta solo mueve JSON; los bytes van cliente ↔ nodos por gRPC |
| 3 | Puerta de entrada REST (FastAPI), 2 instancias + balanceador | `gateway/app.py`; NLB interno en `deploy/aws/04-balanceador-dns.sh` |
| 3 | Autenticación con bcrypt y JWT de 15 min | `auth/app.py`, `auth/admin.py` |
| 3 | Metadatos replicados con Raft (3 nodos) | `metadata/raft.py` (implementación propia sobre gRPC + TLS mutuo) |
| 3 | RabbitMQ durable + 2 trabajadores, ACK manual | `common/mq.py`, `worker/main.py` |
| 3 | Contenedores Docker en EC2 de una subred privada | `docker/Dockerfile`, `deploy/compose/docker-compose.yml`, `deploy/aws/*` |
| 4 | Política de ubicación: vivos, distintos, zonas, espacio/carga, primaria rotativa | `metadata/placement.py` |
| 4 | Si no hay 3 nodos se aceptan 2 y se encola la tercera réplica | `Placement.choose(need=2)`, `service._replication_scan` |
| 4 | Entidades: árbol, manifiesto, bloque, nodo, lock | `metadata/state_machine.py` (docstring) |
| 4 | Bloques inmutables: el commit es cambiar un puntero | `_op_commit_upload` |
| 5 | REST cliente↔puerta; gRPC con streaming cliente↔nodo (trozos de 1 MiB) | `gateway/app.py`, `proto/dfsha.proto` |
| 5 | Raft sobre gRPC; latidos cada 3 s; inventario al arrancar | `proto/dfsha.proto` (servicio `Metadata`), `datanode/main.py` |
| 6 | Close-to-open; versión vieja o nueva completa, nunca mezcla | `DFSFile` (sesión local), `_op_commit_upload` (una sola entrada de log) |
| 6 | Modos r / w / a / rw | `DFSFile.__init__`, `write`, `truncate`, `fsync` |
| 6 | lock(fd, rango, modo, espera): lease 30 s renovado cada 10 s, token de cercado, cola FIFO | `_op_lock_acquire`, `LockQueue`, `DFSFile._renew_loop` |
| 6 | 423 LOCKED y 409 VERSION_CONFLICT por block_id esperado | `_op_lock_acquire`, `_op_commit_upload` |
| 7 | W = 2 de N = 3 con recibos verificados; R = 1 | `_op_commit_upload` (recibos firmados por cada nodo) |
| 7 | Temporal + renombrado atómico; scrubber cada 24 h | `datanode/store.py` (`BlockWriter.commit`, `verify_at_rest`), `datanode/main.py: _scrub_loop` |
| 7 | Cargas vencen a la hora; commit idempotente | `_op_expire`, `_op_commit_upload` (devuelve el mismo resultado) |
| 7 | Log de Raft en disco con snapshots | `metadata/storage.py`, `RaftNode._maybe_snapshot`, `InstallSnapshot` |
| 8 | 6 s sospechoso, 10 s caído; re-replicación priorizada; máx. 2 copias por nodo | `service._detect_failures`, `_replication_scan`, `SharedState.maint` |
| 8 | Inventario al volver: aceptar, borrar sobrantes y huérfanos | `MetadataService.BlockReport`, `_replication_scan` (trim) |
| 8 | Partición: el lado sin mayoría rechaza (CP) | `RaftNode.read_barrier` y `propose` → 503 NO_QUORUM |
| 9.1–9.4 | Escenarios obligatorios | pruebas `03`, `05`, `04-E` y `01` (ver README) |
| 10 | Token de bloque HMAC (block_id, operación, 5 min) validado sin consultar metadatos | `common/tokens.py`, `DataNodeService._check_token` |
| 10 | TLS en todo; TLS mutuo entre servidores con CA interna | `common/tls.py`, `scripts/gen-certs.sh` |
| 10 | AES-256-GCM por bloque con clave por archivo envuelta con clave maestra | `common/crypto.py`, `datanode/store.py` |
| 10 | Auditoría | `common/audit.py` (login, login_failed, access_denied, commit, lock, node_dead...) |
| 11 | Métricas y experimentos con 5 repeticiones, mediana y p95 | `tests/bench.py`, `tests/07_rendimiento.sh` |

## 2. Cambios y precisiones respecto al diseño

| # | En el diseño | En la implementación | Por qué |
|---|---|---|---|
| 1 | Un puerto gRPC por nodo de datos (`dn1:7001`) | Dos puertos: **7001** (clientes, TLS) y **7002** (servidores, TLS mutuo obligatorio) | gRPC en Python no permite exigir certificado de cliente solo a algunos llamadores del mismo puerto. Separarlos permite que la cadena de réplicas, las copias y los borrados solo los puedan pedir servidores con certificado de la CA, y que los grupos de seguridad lo reflejen. |
| 2 | Puerta → metadatos "con contrato tipado en .proto" | Raft, latidos, inventarios y todo el plano de datos son mensajes tipados; las ~20 operaciones de la puerta usan un sobre tipado `MetaRequest{op, user, role, payload JSON}` | Evita duplicar en protobuf el mismo JSON del Anexo A; el sobre conserva los códigos HTTP del Anexo A.4 y añade 421 (no soy el líder). |
| 3 | Servicio de autenticación con sus cuentas | Las cuentas se guardan en metadatos (replicadas con Raft); el servicio de autenticación no tiene estado | Así las cuentas también toleran la caída de un nodo. Se mantiene 1 instancia de auth en ctrl1 como dice el diseño. Si ctrl1 cae, los tokens ya emitidos siguen valiendo (las puertas los validan con la clave pública) pero no hay nuevos inicios de sesión hasta que vuelva; como auth no tiene estado, una segunda instancia solo requeriría emitir su certificado y añadir su URL a `DFSHA_AUTH_URLS`. |
| 4 | "No se borran mientras haya un lease de lectura activo (máximo 10 min)" | Las versiones reemplazadas se conservan exactamente 10 min (`DFSHA_HISTORY_RETENTION`) y el lector puede pedir el manifiesto de esa versión | Es la misma cota superior del diseño sin llevar la cuenta de cada lector. |
| 5 | La primaria responde cuando hay 2 réplicas | Cada salto de la cadena espera a su siguiente nodo con el límite de 5 s por trozo; si el siguiente se atrasa más, se corta y se confirma con los que respondieron | Mantiene W = 2 y "tolera un nodo lento" sin perder el recibo firmado de la tercera réplica cuando sí responde a tiempo. |
| 6 | El revisor periódico recalcula el SHA-256 | Recalcula el SHA-256 del archivo **cifrado** (guardado al escribir); el SHA-256 del contenido original se verifica en cada lectura | El nodo no guarda claves de archivo (solo le llegan dentro de los tokens), así que no puede descifrar por su cuenta. |
| 7 | Cliente pide "un nodo de reemplazo" (9.3) | Nueva ruta `POST /v1/files/uploads/{id}/blocks/{index}/replace` | El Anexo A no tenía la ruta para ese paso. También se añadieron `GET /v1/fs/stat`, `GET /v1/auth/whoami`, `POST /v1/files/replica-report` (aviso de réplica dañada, sección 4) y `GET /v1/admin/cluster`. |
| 8 | Raft clásico | Raft con **PreVote** (tesis de Ongaro, §9.6) | En las pruebas, un nodo de metadatos que volvía tras una caída subía el término y derrocaba a un líder sano. PreVote lo evita. |
| 9 | Clave maestra en KMS | Clave maestra como secreto en `cluster.env` (generada por `gen-certs.sh`) | El Learner Lab limita KMS; el código solo necesita la clave en `DFSHA_MASTER_KEY`, así que pasar a KMS es cambiar de dónde se lee. Los discos EBS además se crean cifrados. |
| 10 | Puerta en el puerto 443 | NLB escucha en 443 y reenvía a las puertas en 8443 | Los contenedores corren como usuario sin privilegios (uid 10001). |
| 11 | Token de cercado "que solo crece" | Contador global del clúster; se guarda por rango de bloques del archivo | Un único contador replicado garantiza el orden total entre todos los locks. |
| 12 | — | Inyección de fallas con archivos `fault_crash_after_bytes` y `fault_delay_ms` en el nodo | Para reproducir los escenarios 9.2 y 9.3 de forma repetible en las pruebas. |

## 3. Semántica declarada (RF3)

* `write()` admite **creación, reemplazo completo, anexado y modificación parcial por bloque**.
* La unidad de cambio es el bloque: cambiar 10 bytes crea una copia nueva de ese bloque.
* Visibilidad **close-to-open**: los cambios se publican en `fsync()`/`close()` con un único
  commit atómico; cada `open()` lee la última versión confirmada. El escritor ve sus propios
  cambios sin confirmar.
* `lock(rango, modo, espera)`: compartido o exclusivo por rango de bloques; lease de 30 s
  renovado cada 10 s; espera 0 → 423 inmediato; espera > 0 → cola FIFO del rango.
* Una escritura está **confirmada** cuando el commit responde 200. Desde ahí cualquier `open`
  o `ls` devuelve esa versión o una posterior, y ningún lector ve bytes no confirmados.
* No hay enlaces, permisos de grupo, mmap ni archivos compartidos entre usuarios.
