# Imagen única de DFSha: metadatos, nodo de datos, puerta, auth, trabajador y cliente.
# El servicio que corre lo elige el comando del contenedor (ver deploy/compose).
FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/src \
    PIP_NO_CACHE_DIR=1

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY src/ src/
COPY scripts/meta-status.py scripts/meta-status.py
COPY tests/ tests/

# Usuario sin privilegios (los puertos internos son > 1024: 7001, 7002, 7100, 8443, 8500)
RUN useradd --system --uid 10001 --home /var/lib/dfsha dfsha \
 && mkdir -p /var/lib/dfsha/meta /var/lib/dfsha/dn /var/lib/dfsha/audit /etc/dfsha/certs \
 && chown -R dfsha:dfsha /var/lib/dfsha
USER dfsha

CMD ["python", "-m", "dfsha.client.cli", "--help"]
