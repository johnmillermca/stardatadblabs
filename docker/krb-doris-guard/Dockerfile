# =============================================================================
# krb-doris-guard — Kerberos enforcement proxy for Apache Doris MySQL port
#
# Intercepts MySQL connections on port 19030.
# Extracts the connecting username from the MySQL HandshakeResponse.
# Runs: kinit -V -n <username>@REALM </dev/null
#   → principal found  : forward to Doris (127.0.0.1:9030)
#   → principal absent : return MySQL Access Denied (1045) and close
#
# Build:
#   podman build --tls-verify=false \
#     -t 192.168.1.50:30500/krb-doris-guard:1.0.0 \
#     docker/krb-doris-guard/
#   podman push --tls-verify=false 192.168.1.50:30500/krb-doris-guard:1.0.0
# =============================================================================
FROM 192.168.1.50:30500/rockylinux:9

# Install Kerberos client (kinit) — baked in, no runtime dnf
RUN dnf install -y \
      krb5-libs \
      krb5-workstation \
      python3 \
    && dnf clean all \
    && rm -rf /var/cache/dnf

WORKDIR /app
COPY guard.py /app/guard.py

EXPOSE 19030

ENV KDC_REALM=STARDATADBLABS.LOCAL \
    KRB5_CONFIG=/etc/krb5.conf.d/cluster.conf \
    DORIS_HOST=127.0.0.1 \
    DORIS_PORT=9030 \
    LISTEN_PORT=19030 \
    EXEMPT_USERS=root \
    LOG_LEVEL=INFO

CMD ["python3", "/app/guard.py"]
