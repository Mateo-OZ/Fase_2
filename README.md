# DFSha — Sistema de archivos distribuido con alta disponibilidad

Guía para **replicar desde cero** el sistema DFSha en AWS y **ejecutar las pruebas** de
funcionamiento: carga y recuperación, acceso a archivos, concurrencia, fallos, consistencia,
seguridad y rendimiento.

## Contenido

1. [Qué es DFSha](#1-qué-es-dfsha)
2. [Qué se va a construir](#2-qué-se-va-a-construir)
3. [Requisitos](#3-requisitos)
4. [Preparar la nube en AWS (consola)](#4-preparar-la-nube-en-aws-consola)
5. [Preparar el cliente (bastión)](#5-preparar-el-cliente-bastión)
6. [Instalar DFSha en los nodos](#6-instalar-dfsha-en-los-nodos)
7. [Crear cuentas y verificar el clúster](#7-crear-cuentas-y-verificar-el-clúster)
8. [Usar el sistema](#8-usar-el-sistema)
9. [Ejecutar las pruebas](#9-ejecutar-las-pruebas)
10. [Operación, problemas frecuentes y limpieza](#10-operación-problemas-frecuentes-y-limpieza)
11. [Referencia técnica](#11-referencia-técnica)

---

## 1. Qué es DFSha

DFSha es un sistema de archivos distribuido de tipo cliente/servidor. El cliente ve un árbol de
directorios propio (`/home/<usuario>`) y puede subir, bajar, leer y modificar archivos. Por
debajo:

* Cada archivo se parte en **bloques de 8 MiB**.
* Cada bloque se guarda en **3 nodos de datos distintos**. Para eso, los nodos se lo pasan en
  cadena mientras lo reciben.
* Un **servicio de metadatos replicado con Raft** (3 réplicas) sabe qué bloques forman cada
  versión de cada archivo y dónde está cada bloque.
* Una escritura se vuelve visible solo cuando **al menos 2 réplicas** guardaron y verificaron
  cada bloque (SHA-256) y metadatos registró la nueva versión.
* Todo el tráfico va cifrado con TLS (TLS mutuo entre servidores) y los bloques se guardan
  cifrados con AES-256-GCM.

![Componentes](docs/img/image1.png)

El plano de control (línea continua) va del cliente a la puerta de entrada y a metadatos. El
plano de datos (línea punteada) va directo del cliente a los nodos de datos.

| Componente | Qué hace | Instancias |
|---|---|---|
| Cliente `dfs` | CLI y librería Python; parte y une archivos, calcula hashes | en el bastión |
| Puerta de entrada (*gateway*) | API REST sin estado; valida el usuario y reenvía al líder de metadatos | 2, detrás de un balanceador |
| Autenticación | contraseñas con bcrypt y tokens JWT de 15 min | 1 |
| Metadatos | árbol, versiones, ubicación de bloques, locks, estado de los nodos (Raft) | 3 |
| Nodos de datos | guardan los bloques cifrados y los replican | 4 |
| RabbitMQ + trabajadores | cola de tareas para volver a copiar bloques perdidos y borrar los que sobran | 1 + 2 |

---

## 2. Qué se va a construir

Una VPC con dos subredes en una sola zona de disponibilidad:

* **Subred pública:** el **bastión/cliente**, que es la única máquina con IP pública, y el **NAT
  Gateway**, por el que los nodos privados descargan paquetes.
* **Subred privada:** todos los nodos del sistema y un **balanceador de red interno (NLB)**
  publicado con el nombre privado `dfs.internal`.

```
                Internet
                   │  SSH (solo desde su IP)
  ┌────────────────┼─────────────────── VPC dfsha-vpc 10.20.0.0/16 ──────────────────────┐
  │  Subred pública 10.20.1.0/24                                                         │
  │    [cliente 10.20.1.10]                     [NAT Gateway]                            │
  │        │ https://dfs.internal (443)        ▲ salida a Internet de la subred privada  │
  │  ──────┼───────────────────────────────────┼──────────────────────────────────────── │
  │  Subred privada 10.20.2.0/24 (sin IP pública)                                        │
  │    [NLB 10.20.2.100 = dfs.internal] ──► ctrl1:8443 , ctrl2:8443                       │
  │    ctrl1 10.20.2.11  meta1 · gateway1 · auth                                          │
  │    ctrl2 10.20.2.12  meta2 · gateway2                                                 │
  │    ctrl3 10.20.2.13  meta3 · rabbitmq · worker1 · worker2                             │
  │    dn1 10.20.2.21   dn2 10.20.2.22   dn3 10.20.2.23   dn4 10.20.2.24   (datos)        │
  └──────────────────────────────────────────────────────────────────────────────────────┘
```

**Las IP privadas son fijas** y deben ser exactamente las de la tabla, porque los
certificados TLS y la configuración se generan con ellas (`deploy/inventory/aws.env`). Si
necesita otras IP, cambie ese archivo antes de generar los certificados (paso 5.3).

| Nombre de la instancia | IP privada | Subred | Tipo | Disco | Grupo de seguridad | Contenedores |
|---|---|---|---|---|---|---|
| `dfsha-cliente` | 10.20.1.10 (+ IP pública) | pública | t3.small | 30 GiB | `dfsha-client` | — (CLI `dfs`) |
| `dfsha-ctrl1` | 10.20.2.11 | privada | t3.medium | 30 GiB | `dfsha-ctrl` | meta1, gateway1, auth |
| `dfsha-ctrl2` | 10.20.2.12 | privada | t3.medium | 30 GiB | `dfsha-ctrl` | meta2, gateway2 |
| `dfsha-ctrl3` | 10.20.2.13 | privada | t3.medium | 30 GiB | `dfsha-ctrl` | meta3, rabbitmq, worker1, worker2 |
| `dfsha-dn1` | 10.20.2.21 | privada | t3.small | 30 GiB | `dfsha-data` | datanode dn1 |
| `dfsha-dn2` | 10.20.2.22 | privada | t3.small | 30 GiB | `dfsha-data` | datanode dn2 |
| `dfsha-dn3` | 10.20.2.23 | privada | t3.small | 30 GiB | `dfsha-data` | datanode dn3 |
| `dfsha-dn4` | 10.20.2.24 | privada | t3.small | 30 GiB | `dfsha-data` | datanode dn4 |
| NLB `dfsha-nlb` | 10.20.2.100 | privada | interno | — | `dfsha-nlb` | reenvía 443 → 8443 |

Puertos que usa el sistema:

| Puerto | Servicio | Quién se conecta | Seguridad |
|---|---|---|---|
| 22 | SSH | usted → bastión; bastión → nodos | llave `.pem` |
| 443 | NLB `dfs.internal` | cliente | TLS (termina en la puerta) |
| 8443 | puertas de entrada (REST) | NLB | TLS |
| 8500 | autenticación | puertas | TLS mutuo |
| 7100 | metadatos (Raft y operaciones) | metadatos, puertas, auth, trabajadores, nodos de datos | TLS mutuo |
| 5671 | RabbitMQ (AMQP) | metadatos y trabajadores | TLS mutuo |
| 7001 | nodos de datos, puerto de clientes | cliente | TLS + token por bloque |
| 7002 | nodos de datos, puerto interno | nodos de datos y trabajadores | TLS mutuo |

---

## 3. Requisitos

* **Una cuenta de AWS** en la que pueda crear VPC, EC2, Elastic Load Balancing y, de
  preferencia, Route 53. Sirve el **AWS Academy Learner Lab**, que tiene estas condiciones:
  * región `us-east-1`;
  * máximo 9 instancias a la vez (aquí se usan 8);
  * par de llaves `vockey`;
  * perfil de instancia `LabInstanceProfile`;
  * la sesión dura 4 horas y al terminar se apagan las instancias.
* **En su computador:** un navegador y un cliente SSH. En Windows sirve Git Bash o PowerShell
  (OpenSSH viene incluido); en macOS o Linux, la terminal.
* **El repositorio:** `https://github.com/Mateo-OZ/Fase_2`. Es público, así que los nodos lo
  clonan sin contraseña.
* **Costo:** el NAT Gateway, el balanceador y las instancias cobran por hora. Al terminar,
  borre todo (sección 10.3).

> Hay dos caminos para el paso 4. **(A) la consola web**, descrito a continuación clic a clic,
> o **(B) los scripts de AWS CLI** de `deploy/aws/`, que crean exactamente lo mismo
> (sección 4.9). Use uno de los dos, no ambos.

---

## 4. Preparar la nube en AWS (consola)

Todo se crea en la región **us-east-1 (N. Virginia)** y en la zona **us-east-1a**. Si usa otra
región, elija una sola zona y úsela en todos los pasos.

En el Learner Lab: **Start Lab** → espere el círculo verde → clic en **AWS** para abrir la
consola. En **AWS Details → Download PEM** descargue `labsuser.pem`, que es la llave privada
del par `vockey`. En una cuenta normal, cree un par de llaves en **EC2 → Key pairs → Create key
pair** (tipo RSA, formato `.pem`) y úselo donde esta guía dice `vockey`.

### 4.1 VPC, subredes, Internet Gateway y NAT Gateway

**VPC → Your VPCs → Create VPC**, opción **VPC and more**:

| Campo | Valor |
|---|---|
| Name tag auto-generation | `dfsha` |
| IPv4 CIDR block | `10.20.0.0/16` |
| IPv6 CIDR block | No IPv6 CIDR block |
| Tenancy | Default |
| Number of Availability Zones | **1** (despliegue *Customize AZs* → `us-east-1a`) |
| Number of public subnets | **1** |
| Number of private subnets | **1** |
| Customize subnets CIDR blocks | pública `10.20.1.0/24`, privada `10.20.2.0/24` |
| NAT gateways | **In 1 AZ** |
| VPC endpoints | None |
| DNS options | ✔ Enable DNS hostnames, ✔ Enable DNS resolution |

**Create VPC**. El asistente crea la VPC `dfsha-vpc`, las subredes
`dfsha-subnet-public1-us-east-1a` y `dfsha-subnet-private1-us-east-1a`, el Internet Gateway, el
NAT Gateway con su IP elástica y las dos tablas de rutas:

* pública: `0.0.0.0/0 → IGW`;
* privada: `0.0.0.0/0 → NAT`.

El NAT tarda 1 a 2 minutos en quedar *Available*.

Compruebe en **Subnets** que la subred pública tenga **Auto-assign public IPv4 address = Yes**.
Si no, selecciónela → **Actions → Edit subnet settings** → marque *Enable auto-assign public
IPv4 address*.

### 4.2 Grupos de seguridad

Las reglas de unos grupos apuntan a otros grupos, no a direcciones IP. Por eso primero se
crean **los cuatro grupos vacíos** y después se les agregan las reglas.

**EC2 → Security Groups → Create security group**, cuatro veces, todos en la VPC `dfsha-vpc`,
**sin reglas de entrada** y con la regla de salida por defecto (*All traffic → 0.0.0.0/0*):

| Nombre | Descripción |
|---|---|
| `dfsha-client` | Bastion y cliente DFSha |
| `dfsha-nlb` | Balanceador interno de las puertas |
| `dfsha-ctrl` | Maquinas de control DFSha |
| `dfsha-data` | Nodos de datos DFSha |

Después, en cada grupo: **Inbound rules → Edit inbound rules → Add rule**. Todas son *Custom
TCP*. En *Source*, escriba el nombre del grupo y elíjalo de la lista. Para la regla de SSH del
bastión elija *My IP*.

**`dfsha-client`**

| Puerto | Origen | Para qué |
|---|---|---|
| 22 | My IP (su IP pública /32) | SSH del administrador al bastión |

**`dfsha-nlb`**

| Puerto | Origen | Para qué |
|---|---|---|
| 443 | `dfsha-client` | el cliente llega a `dfs.internal` |

**`dfsha-ctrl`**

| Puerto | Origen | Para qué |
|---|---|---|
| 8443 | `dfsha-nlb` | el balanceador llega a las puertas (incluye sus *health checks*) |
| 8443 | `dfsha-client` | acceso directo a una puerta si no hay balanceador (plan B) |
| 7100 | `dfsha-ctrl` | Raft entre metadatos; puertas, auth y trabajadores hablan con metadatos |
| 7100 | `dfsha-data` | latidos e inventarios de los nodos de datos |
| 8500 | `dfsha-ctrl` | las puertas llaman a autenticación |
| 5671 | `dfsha-ctrl` | metadatos y trabajadores usan RabbitMQ |
| 22 | `dfsha-client` | SSH desde el bastión |

**`dfsha-data`**

| Puerto | Origen | Para qué |
|---|---|---|
| 7001 | `dfsha-client` | el cliente sube y baja bloques |
| 7002 | `dfsha-data` | replicación en cadena entre nodos de datos |
| 7002 | `dfsha-ctrl` | los trabajadores copian y borran bloques |
| 22 | `dfsha-client` | SSH desde el bastión |

> Nada más queda abierto. En particular, los nodos de datos no aceptan conexiones al puerto
> interno 7002 desde el cliente, y ningún nodo privado tiene IP pública.

### 4.3 Instancias EC2 (8)

**EC2 → Instances → Launch instances**, una vez por cada fila de la tabla de la sección 2. En
cada una:

1. **Name:** `dfsha-cliente`, `dfsha-ctrl1`, … , `dfsha-dn4`.
2. **Application and OS Images:** *Quick Start* → **Ubuntu** → **Ubuntu Server 26.04 LTS (HVM),
   SSD Volume Type**, o la LTS más reciente que aparezca; Architecture **64-bit (x86)**.
3. **Instance type:** `t3.small` para el cliente y los dn; `t3.medium` para ctrl1, ctrl2 y ctrl3.
4. **Key pair:** `vockey` (o el suyo).
5. **Network settings → Edit:**
   * VPC: `dfsha-vpc`.
   * Subnet: la **pública** solo para `dfsha-cliente`; la **privada** para todas las demás.
   * Auto-assign public IP: **Enable** solo para `dfsha-cliente`; **Disable** para las demás.
   * Firewall → *Select existing security group*: `dfsha-client`, `dfsha-ctrl` o `dfsha-data`,
     según la tabla.
   * Despliegue **Advanced network configuration** → **Primary IP**: la IP de la tabla (por
     ejemplo `10.20.2.11` para ctrl1).
6. **Configure storage:** 30 GiB, `gp3`. En *Advanced*, marque **Encrypted**.
7. **Advanced details:**
   * IAM instance profile: `LabInstanceProfile` (en el Learner Lab; en otra cuenta puede
     dejarse vacío).
   * Metadata version: **V2 only (token required)**.
8. **Launch instance**.

Al final deben aparecer las 8 en estado *Running*. Anote la **IP pública de
`dfsha-cliente`**.

### 4.4 Grupo de destino de las puertas

**EC2 → Target Groups → Create target group:**

| Campo | Valor |
|---|---|
| Target type | Instances |
| Name | `dfsha-gateways` |
| Protocol : Port | **TCP : 8443** |
| VPC | `dfsha-vpc` |
| Health check protocol | **HTTPS** |
| Health check path | `/healthz` |
| Advanced: healthy / unhealthy threshold | 2 / 2 |
| Advanced: interval | 10 s |

**Next** → seleccione `dfsha-ctrl1` y `dfsha-ctrl2`, puerto **8443** → **Include as pending
below** → **Create target group**.

Los dos destinos aparecerán como *unhealthy* hasta que se instalen las puertas (paso 6). Es
normal.

### 4.5 Balanceador de red interno

**EC2 → Load Balancers → Create load balancer → Network Load Balancer:**

| Campo | Valor |
|---|---|
| Name | `dfsha-nlb` |
| Scheme | **Internal** |
| Load balancer IP address type | IPv4 |
| VPC | `dfsha-vpc` |
| Mappings | `us-east-1a` → subred **privada**; *Private IPv4 address*: **Assign from CIDR** `10.20.2.100` |
| Security groups | solo `dfsha-nlb` (quite el *default*) |
| Listeners | **TCP : 443** → forward to `dfsha-gateways` |

**Create load balancer**. Tarda unos minutos en quedar *Active*.

El NLB no descifra nada. Reenvía la conexión TCP y el TLS termina en las puertas, con el
certificado de la CA interna.

### 4.6 Nombre privado `dfs.internal` (Route 53)

El cliente solo conoce el nombre `dfs.internal`, nunca la dirección de un nodo.

1. **Route 53 → Hosted zones → Create hosted zone:**
   * Domain name: `dfs.internal`.
   * Type: **Private hosted zone**.
   * VPC: `us-east-1` / `dfsha-vpc`.
2. Dentro de la zona: **Create record:**
   * Record name: vacío (el registro es el propio `dfs.internal`).
   * Type: **A**.
   * **Alias** activado → *Route traffic to*: **Alias to Network Load Balancer**, región
     `us-east-1`, `dfsha-nlb`.
   * **Create records**.

> **Si Route 53 no está permitido en su cuenta** (pasa en algunos laboratorios), sáltese este
> paso. En el bastión se usará la IP fija del NLB en `/etc/hosts` (paso 5.1).

### 4.7 Lista de comprobación antes de seguir

| Comprobar | Dónde | Correcto si… |
|---|---|---|
| NAT Gateway | VPC → NAT gateways | *Available*, en la subred pública |
| Rutas | VPC → Route tables | la privada tiene `0.0.0.0/0 → nat-…`; la pública `0.0.0.0/0 → igw-…` |
| Instancias | EC2 → Instances | 8 en *Running*, con las IP privadas de la tabla; solo el cliente tiene IP pública |
| Grupos de seguridad | EC2 → Security Groups | reglas exactamente como en 4.2 |
| Balanceador | EC2 → Load Balancers | `dfsha-nlb` *Active*, interno, IP 10.20.2.100, listener TCP 443 |
| DNS | Route 53 | zona privada `dfs.internal` asociada a `dfsha-vpc` con un alias al NLB |

### 4.8 Si algo no coincide

* **¿IP privada distinta?** No se puede cambiar en una instancia ya creada. Termínela y vuelva
  a crearla, o ajuste `deploy/inventory/aws.env` antes del paso 5.3.
* **¿El cliente no recibió IP pública?** Asígnele una IP elástica: **EC2 → Elastic IPs →
  Allocate → Associate** con `dfsha-cliente`.

### 4.9 Alternativa: crear lo mismo con la AWS CLI

Desde la terminal del Learner Lab (o AWS CloudShell), con la AWS CLI ya configurada:

```bash
git clone https://github.com/Mateo-OZ/Fase_2.git
cd Fase_2
export MY_IP=<su_ip_publica>/32          # la de su computador: https://checkip.amazonaws.com
./deploy/aws/01-red.sh                    # VPC, subredes, IGW, NAT, rutas (≈ 2 min)
./deploy/aws/02-seguridad.sh              # los 4 grupos de seguridad y sus reglas
./deploy/aws/03-instancias.sh             # 8 EC2 Ubuntu (AMI más reciente de Canonical vía SSM)
./deploy/aws/04-balanceador-dns.sh        # grupo de destino, NLB interno y zona dfs.internal
cat deploy/aws/state.env                  # identificadores creados; CLIENT_PUBLIC_IP es la del bastión
```

---

## 5. Preparar el cliente (bastión)

### 5.1 Entrar al bastión

Desde su computador, en la carpeta donde está `labsuser.pem`:

```bash
# macOS / Linux / Git Bash
chmod 400 labsuser.pem
scp -i labsuser.pem labsuser.pem ubuntu@<IP_PUBLICA_CLIENTE>:~/.ssh/labsuser.pem
ssh -i labsuser.pem ubuntu@<IP_PUBLICA_CLIENTE>
```

En PowerShell (Windows), si `ssh` se queja de los permisos de la llave:

```powershell
icacls labsuser.pem /inheritance:r /grant:r "$($env:USERNAME):(R)"
```

La llave se copia al bastión para que desde allí pueda entrar a los nodos privados.

Ya dentro del bastión:

```bash
chmod 400 ~/.ssh/labsuser.pem
sudo apt-get update
sudo apt-get install -y git openssl python3-venv python3-pip tcpdump curl

# ¿Resuelve dfs.internal? Debe responder 10.20.2.100.
getent hosts dfs.internal || echo "10.20.2.100 dfs.internal" | sudo tee -a /etc/hosts   # plan B sin Route 53

# ¿Llega a los 7 nodos privados?
for ip in 10.20.2.11 10.20.2.12 10.20.2.13 10.20.2.21 10.20.2.22 10.20.2.23 10.20.2.24; do
  ssh -i ~/.ssh/labsuser.pem -o StrictHostKeyChecking=accept-new -o ConnectTimeout=5 ubuntu@$ip hostname
done
```

Deben aparecer 7 nombres de host. Si alguno no responde, revise el grupo de seguridad de ese
nodo (regla 22 desde `dfsha-client`).

### 5.2 Clonar el repositorio e instalar la CLI

```bash
cd ~
git clone https://github.com/Mateo-OZ/Fase_2.git
cd Fase_2
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
dfs --help
```

> En cada sesión nueva del bastión, vuelva a activar el entorno con
> `cd ~/Fase_2 && source .venv/bin/activate`.

### 5.3 Generar la CA interna, los certificados y los secretos

```bash
cd ~/Fase_2
cat deploy/inventory/aws.env                          # IPs que se usarán (deben coincidir con AWS)
./scripts/gen-certs.sh deploy/inventory/aws.env cluster
ls cluster/bundles                                    # client ctrl1 ctrl2 ctrl3 dn1 dn2 dn3 dn4
```

Qué genera el script:

* **CA interna** (`cluster/ca/`), ECDSA P-256. Su clave privada nunca sale del bastión.
* **Un certificado por servicio:** `meta1..3`, `gateway1..2`, `auth`, `rabbitmq`, `worker1..2`
  y `dn1..4`. El CN es el nombre del servicio y los SAN incluyen su IP privada. Las puertas
  además incluyen `dfs.internal` y la IP del NLB. Cada servicio solo acepta peticiones de los
  roles que le corresponden según el CN.
* **Llaves RS256 para firmar los JWT.**
* **`cluster.env`:** direcciones del clúster y tres secretos aleatorios:
  * HMAC de los tokens de bloque;
  * clave maestra AES-256 que envuelve la clave de cada archivo;
  * contraseña de RabbitMQ.
* **`cluster/bundles/<máquina>/`:** solo lo que necesita cada máquina.

`cluster/` está en `.gitignore`: **nunca lo suba al repositorio**. Para comprobar los
certificados:

```bash
openssl x509 -in cluster/certs/gateway1.crt -noout -subject -ext subjectAltName
openssl verify -CAfile cluster/ca/ca.crt cluster/certs/dn1.crt
```

---

## 6. Instalar DFSha en los nodos

Cada nodo necesita su paquete de certificados, una copia del repositorio y la ejecución de
`scripts/node-setup.sh <rol>`. El script hace esto:

1. Instala `chrony` sincronizado con *Amazon Time Sync* (169.254.169.123). Los leases de los
   locks dependen del reloj.
2. Instala **Docker Engine + Compose** desde el repositorio oficial de Docker para Ubuntu.
3. Instala los certificados en `/etc/dfsha/certs` y `cluster.env` en `/opt/dfsha`, con
   permisos mínimos.
4. Crea `/var/lib/dfsha/{meta,dn,audit}` y escribe `deploy/compose/.env` con el rol, la IP y
   la zona.
5. Construye la imagen `dfsha:2.0` y levanta los contenedores de ese rol con
   `restart: unless-stopped`.

**Orden obligatorio:** primero **ctrl1, ctrl2 y ctrl3**, porque Raft necesita 2 de 3
metadatos para elegir líder. Después **dn1 a dn4**.

### 6.1 Opción automática (recomendada)

En el bastión:

```bash
cd ~/Fase_2 && source .venv/bin/activate
./deploy/aws/05-distribuir.sh          # ctrl1 ctrl2 ctrl3 dn1 dn2 dn3 dn4, en ese orden (≈ 3-5 min por nodo)
```

Para repetir una sola máquina: `./deploy/aws/05-distribuir.sh dn3`.

### 6.2 Opción manual (máquina por máquina)

Desde el bastión, en `~/Fase_2`. El patrón es el mismo para las 7 máquinas; cambia solo la IP
y el rol:

| Rol | IP |
|---|---|
| ctrl1 | 10.20.2.11 |
| ctrl2 | 10.20.2.12 |
| ctrl3 | 10.20.2.13 |
| dn1 | 10.20.2.21 |
| dn2 | 10.20.2.22 |
| dn3 | 10.20.2.23 |
| dn4 | 10.20.2.24 |

```bash
KEY=~/.ssh/labsuser.pem
ROL=ctrl1; IP=10.20.2.11                       # cambie estos dos valores en cada máquina

scp -i $KEY -r cluster/bundles/$ROL ubuntu@$IP:~/bundle
ssh -i $KEY ubuntu@$IP
#   ----- dentro del nodo -----
sudo apt-get update && sudo apt-get install -y git
git clone https://github.com/Mateo-OZ/Fase_2.git ~/Fase_2
sudo ~/Fase_2/scripts/node-setup.sh ctrl1 ~/bundle          # el mismo rol de arriba
sudo docker ps --format 'table {{.Names}}\t{{.Status}}'
exit
```

Contenedores que deben quedar en cada máquina:

| Máquina | `docker ps` debe mostrar | Registro que confirma que arrancó |
|---|---|---|
| ctrl1 | `dfsha-meta`, `dfsha-gateway`, `dfsha-auth` | `sudo docker logs dfsha-meta` → `metadatos meta1 escuchando…` |
| ctrl2 | `dfsha-meta`, `dfsha-gateway` | en alguna de ctrl1..3: `soy el LÍDER` |
| ctrl3 | `dfsha-meta`, `dfsha-rabbitmq`, `dfsha-worker1`, `dfsha-worker2` | `sudo docker logs dfsha-worker1` → `conectado a RabbitMQ` |
| dn1..dn4 | `dfsha-datanode` | `sudo docker logs dfsha-datanode` → `inventario enviado` |

---

## 7. Crear cuentas y verificar el clúster

### 7.1 Cuentas

Las crea el administrador en **ctrl1**. Cada usuario recibe su espacio `/home/<usuario>`.

```bash
ssh -i ~/.ssh/labsuser.pem ubuntu@10.20.2.11
sudo docker exec -it dfsha-auth python -m dfsha.auth.admin create-user admin --admin
sudo docker exec -it dfsha-auth python -m dfsha.auth.admin create-user ana
sudo docker exec -it dfsha-auth python -m dfsha.auth.admin create-user beto
sudo docker exec dfsha-auth python -m dfsha.auth.admin list-users
exit
```

Cada comando pide la contraseña dos veces (mínimo 8 caracteres). Las pruebas usan tres
cuentas: `admin` (rol administrador), `ana` (usuaria principal) y `beto` (otro usuario, para
probar que no puede ver lo de `ana`).

### 7.2 Sesiones en el cliente

En el bastión:

```bash
cd ~/Fase_2 && source .venv/bin/activate
for d in ~/.dfsha ~/.dfsha-admin ~/.dfsha-beto; do mkdir -p $d && cp cluster/bundles/client/ca.crt $d/ca.crt; done
export DFS_GATEWAY=https://dfs.internal      # valor por defecto: lo único que conoce el cliente

dfs login -u ana
DFS_HOME=~/.dfsha-admin dfs login -u admin
DFS_HOME=~/.dfsha-beto  dfs login -u beto
dfs whoami                                    # ana (user) — atendido por gateway1 o gateway2
```

> Si `dfs.internal` no resuelve, puede ir directo a una puerta con
> `export DFS_GATEWAY=https://10.20.2.11:8443`.

### 7.3 Verificación

```bash
DFS_HOME=~/.dfsha-admin dfs admin status
```

Debe mostrar:

* un líder de metadatos (`meta1`, `meta2` o `meta3`);
* `rabbitmq=ok`;
* `sub-replicados=0` y `sin-réplica-viva=0`;
* los 4 nodos `dn1..dn4` en estado **active**, con su IP `10.20.2.2x:7001` y un latido de
  hace menos de 3 s.

Otras comprobaciones:

```bash
# TLS de la puerta, validado con la CA interna
openssl s_client -connect dfs.internal:443 -CAfile ~/.dfsha/ca.crt -brief </dev/null

# Estado de Raft en las 3 réplicas: misma huella de estado si tienen el mismo índice aplicado
ssh -i ~/.ssh/labsuser.pem ubuntu@10.20.2.11 "sudo docker exec dfsha-meta python /app/scripts/meta-status.py meta1"
```

En la consola de AWS, **EC2 → Target Groups → `dfsha-gateways` → Targets** debe mostrar
ctrl1 y ctrl2 como **healthy**.

---

## 8. Usar el sistema

### 8.1 CLI `dfs`

```bash
dfs pwd ; dfs ls ; dfs mkdir -p docs/2026 ; dfs cd docs ; dfs mv a.txt b.txt ; dfs rm b.txt ; dfs rmdir 2026
dfs put video.mp4                          # sube un archivo completo (verifica SHA-256)
dfs get video.mp4 copia.mp4                # lo baja en paralelo y verifica el hash
dfs stat --blocks video.mp4                # bloques, réplicas y locks
dfs cat notas.txt --offset 100 --length 50 # lee un rango sin bajar el archivo
dfs write notas.txt --offset 10 --data "hola"      # modificación parcial (pide lock exclusivo)
dfs write notas.txt --mode w --data "todo nuevo"   # reemplazo completo
dfs append notas.txt --data "fin"                  # anexar al final
dfs truncate notas.txt 100
dfs lock notas.txt --blocks 3-3 --hold 20  # lock explícito sobre el bloque 3 durante 20 s
dfs shell                                  # modo interactivo
```

Opciones útiles:

* `put --block-size 1` usa bloques de 1 MiB.
* `put --replication 1` cambia el factor de replicación.
* `write --wait 30` espera hasta 30 s en la cola del lock.
* `write --no-lock` hace la escritura sin lock, solo con control de versión.
* `-v` muestra el detalle por bloque.
* `--json` da la salida en JSON.

### 8.2 Semántica

| Modo | Qué hace |
|---|---|
| `r` | Lee la versión que estaba confirmada al abrir. |
| `w` | Crea o reemplaza el archivo completo. |
| `a` | Anexa al final. |
| `rw` | Modifica cualquier posición: cada bloque tocado se copia y se reescribe (copia en escritura). |

* Los cambios se publican al cerrar (`close`/`fsync`) en un solo commit atómico; todo `open`
  posterior ve esa versión (*close-to-open*). Un lector que ya tenía el archivo abierto sigue
  viendo su versión completa; nunca ve una mezcla.
* **Locks:** compartidos o exclusivos por rango de bloques, con lease de 30 s que el cliente
  renueva cada 10 s y token de cercado.
* **Conflictos:**
  * `423 LOCKED`: otro cliente tiene el lock.
  * `409 VERSION_CONFLICT`: el bloque cambió desde que lo leyó.
  * `409 FENCED`: su lease venció y otro cliente tomó el lock.

### 8.3 API REST

```bash
CA=~/.dfsha/ca.crt
TOKEN=$(curl -s --cacert $CA https://dfs.internal/v1/auth/login -H 'content-type: application/json' \
  -d '{"username":"ana","password":"<clave>"}' | python3 -c 'import json,sys; print(json.load(sys.stdin)["token"])')
curl -s --cacert $CA -H "Authorization: Bearer $TOKEN" "https://dfs.internal/v1/fs/ls?path=/home/ana"
```

| Método y ruta | Uso |
|---|---|
| `POST /v1/auth/login` | iniciar sesión → `{token, expires_in}` |
| `GET /v1/fs/ls?path=` | listar un directorio |
| `GET /v1/fs/stat?path=` | detalles de un archivo o directorio |
| `POST /v1/fs/mkdir` | crear directorio |
| `DELETE /v1/fs/rmdir?path=` | borrar directorio vacío |
| `DELETE /v1/fs/files?path=` | borrar archivo |
| `POST /v1/fs/mv` | mover o renombrar |
| `GET /v1/files/manifest?path=[&version=]` | bloques, réplicas vivas y tokens de lectura |
| `POST /v1/files/uploads` | iniciar una escritura (modo `create`, `replace`, `append` o `modify`) |
| `POST /v1/files/uploads/{id}/blocks/{i}/replace` | pedir nodos de reemplazo para un bloque |
| `POST /v1/files/uploads/{id}/commit` | confirmar la escritura |
| `DELETE /v1/files/uploads/{id}` | cancelar la escritura |
| `POST /v1/locks` | pedir un lock |
| `PUT /v1/locks/{id}` | renovar un lock |
| `DELETE /v1/locks/{id}` | soltar un lock |
| `GET /v1/admin/cluster` | estado del clúster (solo administrador) |

---

## 9. Ejecutar las pruebas

### 9.1 Preparación (una vez por sesión del bastión)

```bash
cd ~/Fase_2 && source .venv/bin/activate
export DFSHA_ENV=aws SSH_KEY=~/.ssh/labsuser.pem
export ADMIN_DFS_HOME=~/.dfsha-admin OTHER_DFS_HOME=~/.dfsha-beto
export DFS_PASSWORD='<clave de ana>'     # renueva el JWT de 15 min durante pruebas largas
mkdir -p ~/evidencias
```

* Las pruebas corren en el bastión con la sesión de `ana`.
* Para simular fallos actúan sobre los nodos por SSH + Docker (`docker kill`, `start`,
  `pause`, `unpause`).
* Los archivos temporales quedan en `/tmp/dfsha-pruebas`.
* Cada script imprime `PASA:` o `FALLA:` en cada verificación y termina con **"Todas las
  verificaciones pasaron"**, o con el número de verificaciones fallidas y código de salida 1.
* Guarde la salida de cada prueba con `| tee ~/evidencias/<n>.txt`.

| # | Prueba | Comando | Duración aprox. |
|---|---|---|---|
| 1 | Carga y recuperación | `./tests/01_carga_recuperacion.sh \| tee ~/evidencias/01.txt` | 1–2 min |
| 2 | Acceso (read/write/append/truncate) | `./tests/02_rf3.sh \| tee ~/evidencias/02.txt` | 1–2 min |
| 3 | Concurrencia | `./tests/03_concurrencia.sh \| tee ~/evidencias/03.txt` | ≈ 1 min |
| 4 | Replicación y fallos | `./tests/04_fallos_replicacion.sh \| tee ~/evidencias/04.txt` | 2–3 min |
| 5 | Consistencia | `./tests/05_consistencia.sh \| tee ~/evidencias/05.txt` | 2–3 min |
| 6 | Seguridad | `./tests/06_seguridad.sh \| tee ~/evidencias/06.txt` | ≈ 1 min (≈ 6 min con `ESPERAR_VENCIMIENTO=310`) |
| 7 | Rendimiento | `./tests/07_rendimiento.sh \| tee ~/evidencias/07.txt` | 15–30 min |

Ejecútelas en orden. Cada una deja el clúster como lo encontró: vuelve a levantar los nodos que
detuvo.

### 9.2 Qué hace cada prueba y qué se espera

**Prueba 1 – Carga y recuperación.**
* **Qué hace:** sube un archivo aleatorio de 256 MiB (o `SIZE_MB=1024` para 1 GiB), muestra en
  qué nodos quedó cada bloque, lo baja y compara hashes.
* **Se espera:**
  * el archivo queda repartido en los 4 nodos, cada uno con una cantidad parecida de réplicas
    y de bloques como primario;
  * el SHA-256 del archivo recuperado es igual al original.

```text
# ejemplo con SIZE_MB=128 (16 bloques x 3 réplicas = 48 réplicas)
  dn1: 12 réplicas (4 como primaria)   dn2: 12 (4)   dn3: 12 (4)   dn4: 12 (4)
PASA: el archivo quedó repartido en 4 nodos
PASA: archivo recuperado idéntico (SHA-256)
```

**Prueba 2 – Acceso a archivos.**
* **Qué hace:** sobre un archivo de 4 bloques de 64 KiB:
  * lee un rango;
  * modifica 22 bytes en el bloque 2;
  * anexa, trunca y reemplaza el archivo;
  * compara 12 operaciones aleatorias contra un modelo local.
* **Se espera:**
  * la modificación parcial crea **un solo bloque nuevo** y los otros conservan su
    identificador;
  * todas las lecturas coinciden byte a byte con el modelo.

**Prueba 3 – Concurrencia.** Dos procesos escriben el mismo bloque. Se espera:

| Caso | Resultado esperado |
|---|---|
| A. Con lock y espera | El segundo proceso espera en la cola, relee y aplica su cambio después del primero. Resultado final: `P1Pp2p2p2`. |
| B. Con lock y sin espera | El segundo recibe `423 LOCKED`. |
| C. Sin lock | El segundo commit recibe `409 VERSION_CONFLICT`. |
| D. Proceso congelado | Se pausa con `SIGSTOP` más de 30 s, su lease vence, otro proceso escribe y, al despertar, el primero recibe `409 FENCED`. |
| E. Bloques distintos | Dos escritores en bloques distintos del mismo archivo avanzan en paralelo y ambos cambios quedan. |

**Prueba 4 – Replicación y fallos.** Se espera:

| Paso | Resultado esperado |
|---|---|
| A. Se detiene dn2 | La lectura sigue funcionando con las otras réplicas. |
| B. Escritura con dn2 caído | Los bloques nuevos van a otros nodos. |
| C. Detección | dn2 queda **caído** unos 10 s después (sin latidos). Los trabajadores re-replican y los archivos vuelven a 3 réplicas en segundos. |
| D. dn2 vuelve | Envía su inventario y metadatos borra las copias que sobran. |
| E. Nodo que se apaga a mitad de un bloque | dn3 se apaga al recibir 5 MiB. El cliente pide un nodo de reemplazo y el archivo queda completo y correcto. Al reiniciar, dn3 descarta los temporales. |
| F. Cae el líder de metadatos | Raft elige otro en 1–2 s. Las operaciones del cliente siguen sin errores, con una pausa máxima de ≈ 2 s. |

**Prueba 5 – Consistencia.** Se espera:

| Paso | Resultado esperado |
|---|---|
| A. dn3 se desconecta justo antes de una escritura (`docker pause`) | La escritura se confirma con 2 réplicas y dn3 no aparece en los bloques nuevos. Las lecturas son correctas y metadatos crea la tercera copia en otro nodo. |
| B. dn4 se vuelve lento (6 s por trozo de 1 MiB) | La escritura no lo espera indefinidamente: se corta a los 5 s y se confirma con los nodos que respondieron. |
| C. *Close-to-open* | Un lector que abrió la versión 1 sigue leyendo la versión 1 después de que otro cliente confirma la 2. Al reabrir ve la 2, y dos clientes que listan ven la misma versión. |
| D. Réplicas de metadatos | Las 3 tienen la misma huella SHA-256 de estado. |

**Prueba 6 – Seguridad.** Se espera:

| Paso | Resultado esperado |
|---|---|
| A. `beto` intenta leer, listar y borrar archivos de `ana` | `403 FORBIDDEN` en las tres operaciones. Un usuario normal no ve el estado del clúster. |
| B. Contraseña errada; petición sin token | `401` en ambos casos. |
| C. Tokens de bloque | Un token alterado, uno de otro bloque o uno vencido dan `PERMISSION_DENIED` en el nodo. El vencido se prueba con `ESPERAR_VENCIMIENTO=310`. |
| D. Puerto interno 7002 | Sin certificado de la CA no se puede usar. En AWS el grupo de seguridad ni siquiera deja conectar. |
| E. `tcpdump` en un nodo de datos durante una carga | El texto del archivo aparece **0 veces** en la captura. |
| F. Discos de los nodos | El texto aparece en 0 archivos. Los bloques empiezan con la cabecera `DFSHA1E` y siguen cifrados. |
| G. Auditoría | Muestra inicios de sesión fallidos, accesos denegados y commits. |

**Prueba 7 – Rendimiento.**
* **Qué hace:** repite cada medición 5 veces (`REPS=5`) y reporta mediana y percentil 95 de
  latencia y de MiB/s en estos experimentos:
  * tamaño de archivo: 10 MiB, 100 MiB y, con `BIG=1`, 1 GiB;
  * factor de replicación 1 frente a 3;
  * bloques de 1 MiB frente a 8 MiB;
  * 1, 4 y 16 clientes simultáneos, sobre archivos distintos y sobre el mismo bloque;
  * latencia de una lectura de 4 KiB, de `ls` y de un commit.
* **Resultado:** los datos crudos quedan en `/tmp/dfsha-pruebas/resultados_rendimiento.csv`.
* **Experimentos adicionales:**
  * **Escalabilidad:** detenga nodos de datos y repita `EXPERIMENT=size` con 1, 2 y 4 nodos
    activos.
  * **Costo del cifrado:** agregue `DFSHA_ENCRYPT_AT_REST=false` al `/opt/dfsha/cluster.env`
    de los nodos de datos y reinícielos (sección 10.1).

### 9.3 Pruebas unitarias (sin AWS)

En cualquier máquina con Python 3.11 o superior:

```bash
pip install -e ".[dev]" && pytest -q tests/unit     # 11 pruebas de la máquina de estados, tokens y cifrado
```

### 9.4 Ensayar todo en una sola máquina Linux (opcional)

`scripts/run-local.sh` levanta el clúster completo como procesos en una sola máquina: 3
metadatos, 2 puertas, 4 nodos y 2 trabajadores. Sirve para ensayar las pruebas antes de pagar
AWS.

```bash
sudo apt-get install -y rabbitmq-server && sudo ./scripts/local-rabbitmq.sh
./scripts/run-local.sh start
echo 'ClaveAna2026!'   | ./scripts/run-local.sh admin create-user ana --password-stdin
echo 'ClaveBeto2026!'  | ./scripts/run-local.sh admin create-user beto --password-stdin
echo 'ClaveAdmin2026!' | ./scripts/run-local.sh admin create-user admin --admin --password-stdin
export DFS_GATEWAY=https://127.0.0.1:8443 DFS_CA_FILE=$PWD/.local-cluster/cluster/certs/ca.crt
dfs login -u ana     # y las otras dos sesiones como en 7.2, con DFS_HOME
DFSHA_ENV=local ADMIN_DFS_HOME=~/.dfsha-admin OTHER_DFS_HOME=~/.dfsha-beto ./tests/01_carga_recuperacion.sh
./scripts/run-local.sh stop
```

---

## 10. Operación, problemas frecuentes y limpieza

### 10.1 Operación

| Tarea | Comando (en la máquina correspondiente) |
|---|---|
| Ver registros | `sudo docker logs -f dfsha-meta` (o `dfsha-gateway`, `dfsha-auth`, `dfsha-datanode`, `dfsha-worker1`, `dfsha-rabbitmq`) |
| Auditoría | ctrl: `sudo tail -f /var/lib/dfsha/meta/audit.jsonl`, `/var/lib/dfsha/audit/*.jsonl`; dn: `/var/lib/dfsha/dn/audit.jsonl` |
| Reiniciar un servicio | `cd ~/Fase_2/deploy/compose && sudo docker compose restart <meta\|gateway\|auth\|datanode\|worker1\|…>` |
| Cambiar configuración | edite `/opt/dfsha/cluster.env` y luego `cd ~/Fase_2/deploy/compose && sudo docker compose up -d` |
| Actualizar el código | `cd ~/Fase_2 && git pull && cd deploy/compose && sudo docker compose up -d --build` |

**Volver después de que se cerró la sesión del Learner Lab:**

* Las instancias arrancan con las mismas IP privadas y los contenedores se levantan solos.
* La IP pública del bastión cambia: búsquela en EC2 → Instances.
* Los tokens JWT habrán vencido: vuelva a ejecutar `dfs login`.

### 10.2 Problemas frecuentes

| Síntoma | Causa probable y solución |
|---|---|
| `ssh` al bastión no responde | Su IP pública cambió: actualice la regla 22 de `dfsha-client` (*My IP*). |
| Los nodos no instalan paquetes | La tabla de rutas privada no apunta al NAT o el NAT no está *Available* (4.7). |
| `dfs.internal` no resuelve | Falta la zona privada de Route 53 o no está asociada a la VPC; use el plan B de `/etc/hosts` (5.1). |
| Destinos del NLB *unhealthy* | Las puertas no están arriba: `curl -k https://10.20.2.11:8443/healthz` desde el bastión y `sudo docker logs dfsha-gateway` en ctrl1. |
| `503 NO_QUORUM` | Hay menos de 2 metadatos vivos o menos de 2 nodos de datos: `dfs admin status`. |
| `401 TOKEN_EXPIRED` | El JWT dura 15 min: `dfs login`, o exporte `DFS_PASSWORD`. |
| `rabbitmq=NO` en `admin status` | `sudo docker logs dfsha-rabbitmq` en ctrl3. Si regeneró los secretos, vuelva a ejecutar `node-setup.sh ctrl3`. |
| Error de certificado (`CERTIFICATE_VERIFY_FAILED`) | Las IP reales no coinciden con `deploy/inventory/aws.env`: corrija el inventario, regenere `cluster/` (borre la carpeta) y vuelva a distribuir. |

### 10.3 Limpieza (para no seguir pagando)

**Si usó la consola**, borre en este orden:

1. Route 53: el registro A y la zona `dfs.internal`.
2. EC2 → Load Balancers: `dfsha-nlb`.
3. Target Groups: `dfsha-gateways`.
4. EC2 → Instances: termine las 8 instancias.
5. VPC → NAT gateways: borre el NAT y espere a que quede *Deleted*.
6. Elastic IPs: libere la IP del NAT.
7. VPC → Your VPCs: borre `dfsha-vpc`. Esto elimina subredes, rutas, IGW y grupos de seguridad.

**Si usó la CLI**:

```bash
./deploy/aws/99-limpieza.sh
```

---

## 11. Referencia técnica

### 11.1 Garantías

| Garantía | Mecanismo | Prueba |
|---|---|---|
| Una escritura confirmada (commit 200) es visible para todo `open`/`ls` posterior | Commit = una entrada del log de Raft confirmada por mayoría; lecturas en el líder | 5-C |
| Una lectura nunca mezcla versiones | Bloques inmutables y cambio completo del manifiesto | 2, 5-C |
| Cada bloque confirmado está en ≥ 2 nodos | Recibos firmados por cada nodo (W = 2 de N = 3) | 4, 5-A |
| Dos escritores no se pisan | Lock exclusivo con lease, token de cercado y commit condicionado | 3 |
| Un archivo nunca queda publicado a medias | Temporal + renombrado atómico; el commit exige hash y 2 recibos | 4-E |
| Con un nodo de datos caído se sigue leyendo y escribiendo; se recupera solo | Latidos, detección a los 10 s, re-replicación con RabbitMQ | 4 |
| Con un metadato caído el sistema sigue (pausa de 1–2 s) | Raft con 3 réplicas | 4-F |
| El cliente no conoce direcciones de nodos | Solo usa `dfs.internal`; la ubicación llega en el manifiesto | 1 |
| Ante una partición, el lado sin mayoría rechaza escrituras | `503 NO_QUORUM` | — |

### 11.2 Estructura del repositorio

```
Fase_2/
├── README.md                      esta guía
├── docs/DECISIONES.md             decisiones de diseño y cómo se implementaron
├── proto/dfsha.proto              contratos gRPC (nodos de datos, metadatos y Raft)
├── src/dfsha/
│   ├── common/                    configuración, TLS, tokens HMAC, AES-GCM, auditoría, clientes de metadatos y RabbitMQ
│   ├── metadata/                  Raft (PreVote, snapshots), máquina de estados, servicio, ubicación de réplicas
│   ├── datanode/                  almacén cifrado, replicación en cadena, latidos, revisor periódico
│   ├── gateway/  auth/  worker/   puerta REST, autenticación, trabajadores
│   └── client/                    librería (DFSClient, DFSFile) y CLI dfs
├── docker/Dockerfile              imagen única dfsha:2.0
├── deploy/
│   ├── aws/                       01-red … 05-distribuir, 99-limpieza (AWS CLI)
│   ├── compose/docker-compose.yml servicios por rol (perfiles ctrl1, ctrl2, ctrl3, data)
│   ├── inventory/aws.env          IPs del clúster (usadas para los certificados)
│   └── rabbitmq/                  configuración de RabbitMQ (solo TLS mutuo)
├── scripts/                       gen-certs.sh, node-setup.sh, meta-status.py, run-local.sh
└── tests/                         pruebas 01–07, bench.py, rf3_check.py, unit/
```

### 11.3 Parámetros principales

Se configuran en `/opt/dfsha/cluster.env`, y se aplican al volver a ejecutar
`docker compose up -d`.

| Variable | Valor por defecto | Significado |
|---|---|---|
| `DFSHA_BLOCK_SIZE` | 8388608 | tamaño de bloque (8 MiB) |
| `DFSHA_REPLICATION` | 3 | réplicas por bloque |
| `DFSHA_MIN_REPLICAS` | 2 | réplicas necesarias para confirmar |
| `DFSHA_HEARTBEAT_INTERVAL` / `SUSPECT_AFTER` / `DEAD_AFTER` | 3 / 6 / 10 s | latidos y detección de caídas |
| `DFSHA_CHUNK_TIMEOUT` | 5 s | límite por trozo de 1 MiB |
| `DFSHA_LOCK_LEASE` | 30 s | duración del lease de un lock |
| `DFSHA_READ_TOKEN_TTL` | 300 s | vigencia de los tokens de lectura |
| `DFSHA_HISTORY_RETENTION` | 600 s | tiempo que se conserva una versión reemplazada |
| `DFSHA_ENCRYPT_AT_REST` | true | cifrado AES-256-GCM en los nodos |
| `DFSHA_TLS` | true | TLS en todos los canales (solo se apaga para medir su costo) |

### 11.4 Referencias

Ghemawat et al., *The Google File System* (SOSP 2003); Apache Hadoop, *HDFS Architecture*;
Ongaro y Ousterhout, *In Search of an Understandable Consensus Algorithm* (USENIX ATC 2014) y
tesis de Ongaro (PreVote, §9.6); van Steen y Tanenbaum, *Distributed Systems* (4.ª ed.);
Kleppmann, *How to do distributed locking*.
