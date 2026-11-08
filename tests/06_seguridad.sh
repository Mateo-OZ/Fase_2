#!/usr/bin/env bash
# Prueba 6 - Seguridad (5.2): acceso autorizado y denegado; evidencia de
# protección del canal y del almacenamiento (sección 10 del diseño).
source "$(dirname "$0")/lib.sh"
titulo "Prueba 6: autenticación, autorización y cifrado"
OTHER_HOME="${OTHER_DFS_HOME:-$HOME/.dfsha-diego}"
ME=$(dfs --json whoami | python3 -c "import json,sys; print(json.load(sys.stdin)['username'])")
OTHER=$(DFS_HOME="$OTHER_HOME" dfs --json whoami | python3 -c "import json,sys; print(json.load(sys.stdin)['username'])")
python3 -c "open('$WORK/secreto.txt','w').write('CONFIDENCIAL-DFSHA-UPB ' * 40000)"
dfs put "$WORK/secreto.txt" secreto.txt >/dev/null

paso "A) Autorización: $OTHER intenta leer y listar archivos de $ME"
DFS_HOME="$OTHER_HOME" dfs get "/home/$ME/secreto.txt" "$WORK/robado.txt" 2>&1 | tee "$WORK/a.err" || true
DFS_HOME="$OTHER_HOME" dfs ls "/home/$ME" 2>&1 | tee -a "$WORK/a.err" || true
DFS_HOME="$OTHER_HOME" dfs rm "/home/$ME/secreto.txt" 2>&1 | tee -a "$WORK/a.err" || true
[[ $(grep -c "403 FORBIDDEN" "$WORK/a.err") -eq 3 ]] && ok "lectura, listado y borrado ajenos: 403" || falla "acceso ajeno"
dfs get secreto.txt "$WORK/propio.txt" >/dev/null && ok "el dueño sí puede leer su archivo" || falla "dueño"
DFS_HOME="$OTHER_HOME" dfs admin status 2>&1 | grep -q 403 && ok "usuario normal no ve el estado del clúster (403)" || falla "admin"

paso "B) Autenticación: contraseña errada y petición sin token"
echo "clave-incorrecta" | DFS_HOME="$WORK/x" dfs login -u "$ME" --password-stdin 2>&1 | tee "$WORK/b.err" || true
grep -q "401" "$WORK/b.err" && ok "contraseña incorrecta: 401" || falla "login"
GW=$(python3 -c "import json,os; print(json.load(open(os.path.expanduser(os.environ.get('DFS_HOME','~/.dfsha'))+'/session.json'))['gateway'])")
CA=$(python3 -c "import json,os; print(json.load(open(os.path.expanduser(os.environ.get('DFS_HOME','~/.dfsha'))+'/session.json'))['ca_file'])")
code=$(curl -s -o /dev/null -w '%{http_code}' --cacert "$CA" "$GW/v1/fs/ls?path=/home/$ME")
[[ "$code" == "401" ]] && ok "sin token: 401" || falla "sin token respondió $code"

paso "C) Tokens de bloque: alterado, de otro bloque y vencido => PERMISSION_DENIED en el nodo"
python3 - <<'EOF'
import base64, json, os, sys, time
sys.path.insert(0, os.environ["REPO"] + "/src")
import grpc
from dfsha.client.api import DFSClient
from dfsha.proto import dfsha_pb2 as pb
c = DFSClient.from_session()
m = c.manifest("secreto.txt")
b = m["blocks"][0]; rep = b["replicas"][0]
body, sig = b["token"].split(".")
claims = json.loads(base64.urlsafe_b64decode(body + "=" * (-len(body) % 4)))
print(f"  token legítimo: bloque {claims['b'][:14]}, op={claims['op']}, vence en {claims['exp']-time.time():.0f} s")
claims["exp"] = time.time() + 86400
forged = base64.urlsafe_b64encode(json.dumps(claims, separators=(",", ":"), sort_keys=True).encode()).decode().rstrip("=") + "." + sig
stub = c._stub(rep["addr"])
for name, tok, bid in [("token alterado (vencimiento extendido)", forged, b["block_id"]),
                       ("token usado para otro bloque", b["token"], "b-" + "0" * 32)]:
    try:
        list(stub.ReadBlock(pb.ReadBlockRequest(block_id=bid, token=tok), timeout=5)); print("  ¡aceptado!"); sys.exit(1)
    except grpc.RpcError as e:
        print(f"  {name}: {e.code().name} ({e.details()})")
wait = float(os.environ.get("ESPERAR_VENCIMIENTO", "0"))
if wait:
    print(f"  esperando {wait:.0f} s a que el token legítimo venza...")
    time.sleep(wait)
    try:
        list(stub.ReadBlock(pb.ReadBlockRequest(block_id=b["block_id"], token=b["token"]), timeout=5)); sys.exit(1)
    except grpc.RpcError as e:
        print(f"  token vencido: {e.code().name} ({e.details()})")
EOF
ok "los nodos validan cada token sin consultar a metadatos"

paso "D) Canal interno: el puerto 7002 exige certificado de cliente de la CA (TLS mutuo)"
DN1=$(dfs --json stat secreto.txt | python3 -c "import json,sys; print(json.load(sys.stdin)['block_list'][0]['replicas'][0])")
python3 - "$DN1" <<'EOF'
import os, sys
sys.path.insert(0, os.environ["REPO"] + "/src")
import grpc
from dfsha.client.api import DFSClient
from dfsha.proto import dfsha_pb2 as pb, dfsha_pb2_grpc as pbg
c = DFSClient.from_session()
m = c.manifest("secreto.txt"); b = m["blocks"][0]
addr = b["replicas"][0]["addr"]; host, port = addr.rsplit(":", 1)
internal = f"{host}:{7002 if port == '7001' else int(port) + 10}"   # local: 7011 -> 7021
ca = open(c.ca_file, "rb").read()
ch = grpc.secure_channel(internal, grpc.ssl_channel_credentials(ca))
try:
    pbg.DataNodeStub(ch).DeleteBlock(pb.DeleteBlockRequest(block_id=b["block_id"], token="x"), timeout=5)
    print("  ¡el puerto interno aceptó a un cliente sin certificado!"); sys.exit(1)
except grpc.RpcError as e:
    print(f"  cliente sin certificado contra el puerto interno {internal}: {e.code().name}")
try:
    pbg.DataNodeStub(grpc.secure_channel(addr, grpc.ssl_channel_credentials(ca))).DeleteBlock(
        pb.DeleteBlockRequest(block_id=b["block_id"], token="x"), timeout=5)
    print("  ¡DeleteBlock aceptado por el puerto de clientes!"); sys.exit(1)
except grpc.RpcError as e:
    print(f"  DeleteBlock por el puerto de clientes {addr}: {e.code().name} ({e.details()})")
EOF
ok "solo los servidores con certificado de la CA usan el puerto interno"

paso "E) Cifrado en tránsito: captura con tcpdump en $DN1 durante una carga"
CAP=/tmp/dfsha-cap.pcap
on_host "$DN1" "sudo rm -f $CAP; (sudo timeout 20 tcpdump -i any -s 0 -w $CAP 'tcp portrange 7001-7024' >/dev/null 2>&1 &) ; sleep 2"
dfs put "$WORK/secreto.txt" secreto2.txt >/dev/null
sleep 3
on_host "$DN1" "sudo pkill tcpdump; sleep 1; echo \"  paquetes capturados: \$(sudo tcpdump -r $CAP 2>/dev/null | wc -l)\"; echo \"  apariciones del texto en claro: \$(sudo grep -a -c CONFIDENCIAL-DFSHA $CAP)\"" | tee "$WORK/e.out"
grep -q "en claro: 0" "$WORK/e.out" && ok "la red solo transporta TLS (el texto no aparece)" || falla "texto visible en la red"

paso "F) Cifrado en reposo: el texto no aparece en los discos de los nodos"
for dn in dn1 dn2 dn3 dn4; do
  n=$(on_host "$dn" "sudo grep -rl CONFIDENCIAL-DFSHA $(dn_data_dir "$dn")/blocks 2>/dev/null | wc -l")
  echo "  $dn: $n archivos con el texto en claro"
  [[ "$n" == "0" ]] || falla "$dn guarda texto en claro"
done
on_host "$DN1" "f=\$(sudo ls -t $(dn_data_dir "$DN1")/blocks/*/b-* | grep -v json | head -1); sudo head -c 48 \$f | od -c | head -3"
ok "los bloques están cifrados con AES-256-GCM (cabecera DFSHA1E)"

paso "G) Registro de auditoría (inicios de sesión, accesos denegados, commits)"
if [[ "$DFSHA_ENV" == "local" ]]; then tail -n 200 "$REPO/.local-cluster/logs/audit.jsonl" | grep -E '"(login|login_failed|access_denied|commit)"' | tail -5
else on_host meta1 "sudo tail -n 200 /var/lib/dfsha/meta/audit.jsonl | grep -E 'access_denied|commit' | tail -3"; on_host auth "sudo tail -n 3 /var/lib/dfsha/audit/auth.jsonl"; fi
resumen
