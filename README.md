# DFSha en AWS Academy Learner Lab: guía práctica y registro de despliegue

Esta guía reúne el despliegue de DFSha en AWS Academy Learner Lab y las pruebas realizadas
durante la sesión. Está escrita para repetir el procedimiento en una cuenta nueva y para
entender qué quedó completado en el despliegue documentado.

> **Estado del despliegue documentado:** el clúster quedó operativo y las pruebas 1 a 6
> terminaron con `Todas las verificaciones pasaron`. La prueba 7 (rendimiento) no se ejecutó.
> No se eliminaron los recursos de AWS; consulta la sección [Limpieza](#12-limpieza-y-costos)
> antes de terminar el laboratorio.

## 1. Arquitectura desplegada

El despliegue utilizó ocho instancias EC2 en `us-east-1`:

| Instancia/rol | IP privada | Servicios |
|---|---:|---|
| Bastión/cliente | `10.20.1.10` | Cliente `dfs`, acceso SSH desde el equipo administrador |
| `ctrl1` | `10.20.2.11` | metadatos `meta1`, `gateway1`, autenticación |
| `ctrl2` | `10.20.2.12` | metadatos `meta2`, `gateway2` |
| `ctrl3` | `10.20.2.13` | metadatos `meta3`, RabbitMQ y trabajadores |
| `dn1` | `10.20.2.21` | nodo de datos |
| `dn2` | `10.20.2.22` | nodo de datos |
| `dn3` | `10.20.2.23` | nodo de datos |
| `dn4` | `10.20.2.24` | nodo de datos |

La VPC es `10.20.0.0/16`, con subred pública `10.20.1.0/24` y privada `10.20.2.0/24`.
Los servidores y nodos de datos no tienen IP pública. El NLB interno usa `10.20.2.100` y el
nombre privado del servicio es `dfs.internal`.

## 2. Antes de empezar

Se necesita:

- AWS Academy Learner Lab con créditos activos y región `us-east-1`.
- Una computadora con navegador y SSH (PowerShell/OpenSSH funciona en Windows).
- La llave privada del laboratorio `labsuser.pem`. No la compartas ni la subas al repositorio.
- El repositorio público: `https://github.com/Mateo-OZ/Fase_2`.

El laboratorio permite hasta nueve instancias simultáneas; este despliegue usa ocho. El NAT
Gateway, balanceador e instancias pueden consumir créditos mientras existan.

## 3. Crear la infraestructura de red e instancias

1. En Learner Lab, pulsa **Start Lab**, espera el indicador verde y entra a AWS con el botón
   **AWS**. Trabaja en `us-east-1`.
2. Descarga `labsuser.pem` desde **AWS Details → Download PEM**. Conserva la llave en un lugar
   seguro en tu equipo.
3. En **VPC → Create VPC → VPC and more**, crea la red `dfsha-vpc` con:
   - CIDR de VPC: `10.20.0.0/16`.
   - Una zona de disponibilidad (`us-east-1a`).
   - Subred pública: `10.20.1.0/24`.
   - Subred privada: `10.20.2.0/24`.
   - Un NAT Gateway en la zona pública.
   - DNS hostnames y DNS resolution habilitados.
4. Crea los grupos de seguridad `dfsha-client`, `dfsha-nlb`, `dfsha-ctrl` y `dfsha-data`.
   Añade solo las reglas que requiere el sistema:

   | Grupo | Puerto | Origen |
   |---|---:|---|
   | `dfsha-client` | 22 | La IP pública del equipo administrador (`/32`) |
   | `dfsha-nlb` | 443 | `dfsha-client` |
   | `dfsha-ctrl` | 8443 | `dfsha-nlb` y `dfsha-client` |
   | `dfsha-ctrl` | 7100, 8500, 5671 | `dfsha-ctrl`; el 7100 también desde `dfsha-data` |
   | `dfsha-ctrl` | 22 | `dfsha-client` |
   | `dfsha-data` | 7001 | `dfsha-client` |
   | `dfsha-data` | 7002 | `dfsha-data` y `dfsha-ctrl` |
   | `dfsha-data` | 22 | `dfsha-client` |

   Deja las salidas permitidas según el valor predeterminado del grupo. No abras los puertos de
   los nodos a Internet.
5. Lanza las ocho instancias Ubuntu x86_64, cada una con disco `gp3` de 30 GiB:
   - Bastión y `dn1..dn4`: `t3.small`.
   - `ctrl1..ctrl3`: `t3.medium`.
   - Solo el bastión va en la subred pública y recibe IP pública.
   - Los siete nodos restantes van en la subred privada, sin IP pública.
   - Asigna las IP privadas fijas de la tabla de la sección 1 y el grupo de seguridad
     correspondiente. Usa `vockey`/la llave SSH del laboratorio y el perfil
     `LabInstanceProfile` si la consola lo exige.
6. Crea un grupo de destino `dfsha-gateways` para instancias, TCP puerto 8443, con health check
   HTTPS `/healthz`; registra `dfsha-ctrl1` y `dfsha-ctrl2`.
7. Crea el NLB `dfsha-nlb` con esquema **Internal**, en la subred privada, IP privada
   `10.20.2.100`, grupo `dfsha-nlb` y listener TCP 443 al grupo `dfsha-gateways`.
8. Si Route 53 está disponible, crea la zona privada `dfs.internal`, asociada a la VPC, y un
   registro A alias al NLB. Si no está disponible, se puede usar el plan B de `/etc/hosts`
   descrito más abajo.
9. Comprueba que las ocho instancias estén en `Running`, que el NAT esté `Available`, que el
   NLB esté `Active` y que las IP privadas coincidan con el inventario.

> El proyecto también incluye scripts de aprovisionamiento AWS CLI (`deploy/aws/01-red.sh` a
> `04-balanceador-dns.sh`). Usa esos scripts **en vez de** los pasos manuales anteriores, no
> además de ellos. El despliegue realizado para esta guía se preparó desde la consola AWS.

## 4. Entrar al bastión desde Windows

En PowerShell, ve a la carpeta donde descargaste la llave. Si SSH informa que los permisos de
la llave son demasiado abiertos, aplica:

```powershell
icacls .\labsuser.pem /inheritance:r /grant:r "$($env:USERNAME):(R)"
```

Conecta usando la IP pública actual del bastión, visible en **EC2 → Instances**:

```powershell
ssh -i .\labsuser.pem ubuntu@<IP_PUBLICA_DEL_BASTION>
```

La IP pública puede cambiar cuando se reinicia el Learner Lab. Si eso ocurre, consulta la IP
nueva en EC2 y vuelve a conectar. No publiques capturas que muestren información sensible.

En el bastión, copia la llave para que pueda acceder a las máquinas privadas:

```bash
mkdir -p ~/.ssh
# Copia labsuser.pem desde tu computadora al bastión con scp, o usa el método seguro
# disponible en tu entorno. No pegues el contenido de la llave en chats.
chmod 400 ~/.ssh/labsuser.pem
```

Desde Windows/Git Bash, la copia se puede hacer antes de entrar al bastión:

```bash
scp -i labsuser.pem labsuser.pem ubuntu@<IP_PUBLICA_DEL_BASTION>:~/.ssh/labsuser.pem
```

## 5. Preparar el bastión y el cliente DFSha

Ejecuta en el bastión:

```bash
sudo apt-get update
sudo apt-get install -y git openssl python3-venv python3-pip tcpdump curl
```

Si no configuraste Route 53, añade la dirección del NLB al archivo hosts:

```bash
echo "10.20.2.100 dfs.internal" | sudo tee -a /etc/hosts
getent hosts dfs.internal
```

Clona el proyecto e instala el cliente:

```bash
cd ~
git clone https://github.com/Mateo-OZ/Fase_2.git
cd ~/Fase_2
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
dfs --help
```

Cada vez que abras una sesión nueva en el bastión, activa el entorno:

```bash
cd ~/Fase_2
source .venv/bin/activate
```

Comprueba SSH a los siete nodos privados:

```bash
for ip in 10.20.2.11 10.20.2.12 10.20.2.13 10.20.2.21 10.20.2.22 10.20.2.23 10.20.2.24; do
  ssh -i ~/.ssh/labsuser.pem -o StrictHostKeyChecking=accept-new -o ConnectTimeout=5 "ubuntu@$ip" hostname
done
```

Si un nodo no responde, verifica que esté encendido y que su regla SSH permita conexiones
desde `dfsha-client`.

## 6. Crear certificados y distribuir el sistema

Desde `~/Fase_2`, revisa que `deploy/inventory/aws.env` coincida con las IP privadas reales.
Luego genera la CA, certificados y secretos:

```bash
./scripts/gen-certs.sh deploy/inventory/aws.env cluster
ls cluster/bundles
```

La carpeta `cluster/` contiene certificados y secretos privados. No la compartas, no la
adjuntes a reportes y no la subas a GitHub.

Instala el sistema en los siete nodos, primero los tres controladores y luego los cuatro nodos
de datos:

```bash
./deploy/aws/05-distribuir.sh
```

El despliegue puede tardar varios minutos por nodo. Si hace falta repetir una sola máquina:

```bash
./deploy/aws/05-distribuir.sh dn3
```

Al finalizar, puedes comprobar los contenedores en cada host. Por ejemplo:

```bash
ssh -i ~/.ssh/labsuser.pem ubuntu@10.20.2.21 'sudo docker ps'
```

`ctrl1` debe ejecutar metadatos, gateway y auth; `ctrl2`, metadatos y gateway; `ctrl3`,
metadatos, RabbitMQ y trabajadores; cada `dn` ejecuta un contenedor de datanode.

## 7. Crear usuarios, iniciar sesión y verificar el clúster

En el bastión, crea los usuarios `admin`, `ana` y `beto` desde el contenedor de autenticación
en `ctrl1`. Cada comando solicita la contraseña de forma interactiva:

```bash
ssh -i ~/.ssh/labsuser.pem ubuntu@10.20.2.11
sudo docker exec -it dfsha-auth python -m dfsha.auth.admin create-user admin --admin
sudo docker exec -it dfsha-auth python -m dfsha.auth.admin create-user ana
sudo docker exec -it dfsha-auth python -m dfsha.auth.admin create-user beto
sudo docker exec dfsha-auth python -m dfsha.auth.admin list-users
exit
```

Prepara las sesiones cliente y copia el certificado público de la CA a los directorios locales:

```bash
cd ~/Fase_2
source .venv/bin/activate
for d in ~/.dfsha ~/.dfsha-admin ~/.dfsha-beto; do
  mkdir -p "$d"
  cp cluster/bundles/client/ca.crt "$d/ca.crt"
done
export DFS_GATEWAY=https://dfs.internal
dfs login -u ana
DFS_HOME=~/.dfsha-admin dfs login -u admin
DFS_HOME=~/.dfsha-beto dfs login -u beto
```

Las contraseñas se introducen localmente en el prompt. No las escribas en documentación,
comandos compartidos ni conversaciones. Los tokens duran 15 minutos.

Verifica el estado con la sesión de administrador:

```bash
DFS_HOME=~/.dfsha-admin dfs admin status
```

Debe haber un líder de metadatos, `rabbitmq=ok`, los cuatro nodos de datos activos y cero
bloques sub-replicados. La prueba funcional manual realizada fue subir y bajar `prueba.txt` y
comparar su SHA-256; los hashes coincidieron.

## 8. Preparar las pruebas y guardar evidencia

Las pruebas se ejecutan desde el bastión y desde la raíz del repositorio. Antes de empezar:

```bash
cd ~/Fase_2
source .venv/bin/activate
export DFSHA_ENV=aws
export SSH_KEY=~/.ssh/labsuser.pem
export ADMIN_DFS_HOME=~/.dfsha-admin
export OTHER_DFS_HOME=~/.dfsha-beto
mkdir -p ~/evidencias
df -h /tmp
```

Los scripts usan `/tmp/dfsha-pruebas` para archivos temporales. En este laboratorio, `/tmp`
puede ser un disco temporal con cuota limitada. Si el espacio se agota, revisa `df -h /tmp` y
borra solamente temporales de las pruebas en `/tmp/dfsha-pruebas`; no borres `~/evidencias`.
Las evidencias bajo `~/evidencias` están en el disco del bastión.

Las pruebas se recomiendan en orden. Cada salida se guarda con `tee`:

```bash
./tests/01_carga_recuperacion.sh 2>&1 | tee ~/evidencias/01.txt
./tests/02_rf3.sh 2>&1 | tee ~/evidencias/02.txt
./tests/03_concurrencia.sh 2>&1 | tee ~/evidencias/03.txt
./tests/04_fallos_replicacion.sh 2>&1 | tee ~/evidencias/04.txt
./tests/05_consistencia.sh 2>&1 | tee ~/evidencias/05.txt
```

Comprueba el final de cada evidencia antes de continuar:

```bash
tail -n 20 ~/evidencias/01.txt
```

Se considera aprobada si termina con `Todas las verificaciones pasaron`. En una prueba larga,
si aparece `401 TOKEN_EXPIRED`, renueva la sesión ejecutando `dfs login -u ana`. El token de
admin es independiente; vuelve a iniciar esa sesión con `DFS_HOME=~/.dfsha-admin dfs login -u
admin` si hace falta. Para pruebas largas se puede configurar `DFS_PASSWORD` localmente para
que el cliente renueve el token, pero no incluyas una contraseña literal en un script o en este
README.

En el despliegue documentado, las pruebas 1 a 5 pasaron. Durante la prueba 5 se llenó `/tmp`;
se limpiaron los archivos temporales de la prueba, se renovaron las sesiones y se volvió a
ejecutar. La segunda ejecución pasó. La prueba 4 requiere que las consultas administrativas
usen la sesión de `admin`, no la de `ana`.

## 9. Prueba 6: seguridad

La prueba verifica autorización, credenciales incorrectas, tokens de bloque, TLS mutuo,
cifrado en tránsito y en disco, y auditoría. Antes de ejecutarla, asegúrate de tener sesiones
vigentes para `ana` y `beto`.

El script crea `DFS_HOME=/tmp/dfsha-pruebas/x` para probar una contraseña incorrecta. Ese
directorio también debe contener el certificado público de la CA para que el cliente alcance
el gateway:

```bash
mkdir -p /tmp/dfsha-pruebas/x
cp ~/.dfsha/ca.crt /tmp/dfsha-pruebas/x/ca.crt
```

En la versión del script usada durante esta sesión, la búsqueda de bloques para revisar la
cabecera en disco no encontraba archivos porque el patrón de ruta esperaba otro nombre de
bloque. La ruta real organiza los bloques en subdirectorios y guarda metadatos `.json` junto
al archivo cifrado. Para localizar un bloque real en `dn1`:

```bash
source deploy/inventory/aws.env
ssh -i ~/.ssh/labsuser.pem "ubuntu@$DN1_HOST" \
  "sudo find /var/lib/dfsha/dn/blocks -type f -name 'b-*' ! -name '*.json' -print -quit | xargs -r sudo head -c 8 | od -An -tc"
```

La cabecera esperada es `DFSHA1E`. La corrección aplicada en el script de pruebas fue buscar
un archivo `b-*` que no terminara en `.json`, validar que se encontrara y verificar esa
cabecera. Si se repite el despliegue en un clon limpio y la comprobación sigue usando el
patrón antiguo, actualiza el bloque **F) Cifrado en reposo** de `tests/06_seguridad.sh` antes
de ejecutar la prueba:

```bash
python3 - <<'PY'
from pathlib import Path

path = Path("tests/06_seguridad.sh")
text = path.read_text()
old = 'on_host "$DN1" "f=\\$(sudo ls -t $(dn_data_dir "$DN1")/blocks/*/b-* | grep -v json | head -1); sudo head -c 48 \\$f | od -c | head -3"'
new = '''on_host "$DN1" "f=\\$(sudo find $(dn_data_dir "$DN1")/blocks -type f -name 'b-*' ! -name '*.json' -print -quit); test -n \\"\\$f\\" || { echo 'No se encontro bloque cifrado' >&2; exit 1; }; sudo head -c 8 \\"\\$f\\" | grep -a -q '^DFSHA1E' || { echo 'Cabecera DFSHA1E no encontrada' >&2; exit 1; }; echo \\"Cabecera DFSHA1E confirmada: \\$f\\""'''
if text.count(old) != 1:
    raise SystemExit("No se encontró exactamente una línea esperada; no se modificó el archivo.")
path.write_text(text.replace(old, new))
print("Comprobación de cifrado en reposo actualizada.")
PY
```

Luego ejecuta y guarda la evidencia:

```bash
bash tests/06_seguridad.sh 2>&1 | tee ~/evidencias/06.txt
```

La salida debe confirmar los `403` de acceso no autorizado, `401` para autenticación
incorrecta/sin token, rechazo de tokens inválidos, ausencia del texto en claro en la captura y
en los discos, cabecera `DFSHA1E`, auditoría y finalmente `Todas las verificaciones pasaron`.

Durante esta sesión la prueba 6 pasó después de copiar la CA al directorio temporal y corregir
la búsqueda del bloque. En AWS, el intento de conectar sin certificado al puerto interno
7002 puede dar `DEADLINE_EXCEEDED` por el grupo de seguridad; eso es compatible con que el
puerto no sea accesible desde el bastión. La comprobación de disco confirmó la cabecera
`DFSHA1E` y cero archivos con el marcador de texto claro en los cuatro nodos.

## 10. Prueba 7: rendimiento (pendiente en este despliegue)

Esta prueba puede tardar aproximadamente 15–30 minutos y produce datos CSV en
`/tmp/dfsha-pruebas`. Solo ejecútala si el laboratorio sigue activo y tienes tiempo y espacio
temporal suficientes:

```bash
cd ~/Fase_2
source .venv/bin/activate
export DFSHA_ENV=aws SSH_KEY=~/.ssh/labsuser.pem
export ADMIN_DFS_HOME=~/.dfsha-admin OTHER_DFS_HOME=~/.dfsha-beto
./tests/07_rendimiento.sh 2>&1 | tee ~/evidencias/07.txt
```

Por defecto hace cinco repeticiones por condición y escribe los datos crudos en
`/tmp/dfsha-pruebas/resultados_rendimiento.csv`. En el trabajo documentado no se ejecutó esta
prueba, por lo que no se debe reportar como aprobada.

## 11. Consultar las evidencias

Revisa los resultados guardados en el bastión:

```bash
ls -lh ~/evidencias
tail -n 20 ~/evidencias/01.txt
tail -n 20 ~/evidencias/02.txt
tail -n 20 ~/evidencias/03.txt
tail -n 20 ~/evidencias/04.txt
tail -n 20 ~/evidencias/05.txt
tail -n 30 ~/evidencias/06.txt
```

Las evidencias de las pruebas 1 a 6 deben terminar con `Todas las verificaciones pasaron`.
Guárdalas en un lugar seguro si las necesitas después de cerrar el Learner Lab. No incluyas
contraseñas, tokens, llaves privadas ni archivos de `cluster/`.

## 12. Limpieza y costos

No se ejecutó limpieza al finalizar las pruebas descritas. Mientras existan, algunos recursos
siguen consumiendo créditos. Si ya no necesitas el clúster ni sus datos, elimina los recursos
de forma controlada:

- Si se creó la infraestructura desde la consola, sigue el orden de limpieza de la guía
  principal `README.md`, sección 10.3: zona/registro de Route 53, NLB y grupo de destino,
  instancias, NAT Gateway, IP elástica y VPC.
- Si la infraestructura se creó con los scripts AWS CLI y tienes un `deploy/aws/state.env`
  válido, revisa primero los identificadores y luego usa `./deploy/aws/99-limpieza.sh`.
  Ese script termina instancias y elimina recursos de red; no lo ejecutes si aún necesitas
  el clúster o las evidencias que están guardadas solo en el bastión.

Antes de terminar, descarga o copia de forma segura `~/evidencias/` si debes conservarla. No
copies al repositorio certificados privados, credenciales, llaves `.pem` o secretos de
`cluster/`.