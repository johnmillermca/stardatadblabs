# =============================================================================
# Doris FE with embedded Python Cloud MetaService stub
# Base : 192.168.1.50:30500/apache/doris:fe-4.0.7-ranger
#        (custom build with Ranger auth + Cloud warm-up grammar)
# Adds : python3, grpcio, protobuf, cloud_meta_service.py,
#        doris_fe_entrypoint.py
#
# The MetaService stub (cloud_meta_service.py) runs as a daemon thread on
# localhost:5000.  It answers only the two FE bootstrap RPCs (getCluster,
# getInstance) and returns OK for all other MetaService calls.  This activates
# CloudEnv so WARM UP CLUSTER … WITH TABLE, SHOW WARM UP JOB, and COLD_DOWN
# work without an external MetaService pod.
#
# Performance impact: none.  The query/scan/load data path goes FE→BE over
# Thrift and never touches the MetaService stub.
#
# Build:
#   podman build --tls-verify=false \
#     -t 192.168.1.50:30500/apache/doris:fe-4.0.7-ranger-ms \
#     -f manifests/doris/fe/Dockerfile .
# =============================================================================

FROM 192.168.1.50:30500/apache/doris:fe-4.0.7-ranger

# ── Python runtime ────────────────────────────────────────────────────────────
# The base image (Debian-based) ships python3.  We only need grpcio + protobuf.
# Pin exact versions to match the proto wire format used in the FE JAR
# (grpcio 1.65.x matches grpc-netty-1.65.1.jar bundled in the FE).
RUN apt-get update -qq && \
    apt-get install -y --no-install-recommends python3 python3-pip && \
    pip3 install --no-cache-dir \
        grpcio==1.65.5 \
        protobuf==4.25.8 && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# ── MetaService stub + entrypoint ─────────────────────────────────────────────
COPY manifests/doris/fe/cloud_meta_service.py  /app/cloud_meta_service.py
COPY manifests/doris/fe/doris_fe_entrypoint.py /app/doris_fe_entrypoint.py

# ── Default entrypoint ────────────────────────────────────────────────────────
# Arguments after the script are forwarded to start_fe.sh (the FE master FQDN).
# The K8s StatefulSet spec overrides these via the `command:` field.
CMD ["python3", "/app/doris_fe_entrypoint.py"]
