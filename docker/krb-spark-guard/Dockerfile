# =============================================================================
# krb-spark-guard — Kerberos enforcement proxy for Spark standalone (port 7077)
#
# Two servers in one process:
#   :7078 — HTTP auth API:  POST /auth {username, keytab_b64} → {token}
#            Verifies the keytab against the KDC (kinit -kt).
#   :7077 — RPC TCP proxy:  expects "X-Krb-Token: <token>\n" as the first line,
#            validates the token, then forwards to the real Spark master (:17077).
#
# spark-submit-krb wrapper (bundled) handles auth automatically.
#
# Build:
#   podman build --tls-verify=false \
#     -t 192.168.1.50:30500/krb-spark-guard:1.0.0 \
#     docker/krb-spark-guard/
#   podman push --tls-verify=false 192.168.1.50:30500/krb-spark-guard:1.0.0
# =============================================================================
FROM 192.168.1.50:30500/rockylinux:9

# Install Kerberos client (kinit, kdestroy) + Python — baked in.
# NOTE: Do NOT add 'curl' here — rockylinux:9 ships curl-minimal which
# conflicts with the full curl package (both provide 'curl').
# The spark-submit-krb wrapper uses curl, but it runs on the CLIENT machine
# (where spark-submit is invoked), not inside this sidecar image.
RUN dnf install -y \
      krb5-libs \
      krb5-workstation \
      python3 \
    && dnf clean all \
    && rm -rf /var/cache/dnf

WORKDIR /app
COPY guard.py           /app/guard.py
COPY spark-submit-krb   /usr/local/bin/spark-submit-krb
RUN chmod +x /usr/local/bin/spark-submit-krb

EXPOSE 7077 7078

ENV KDC_REALM=STARDATADBLABS.LOCAL \
    KRB5_CONFIG=/etc/krb5.conf.d/cluster.conf \
    SPARK_HOST=127.0.0.1 \
    SPARK_PORT=17077 \
    LISTEN_RPC_PORT=7077 \
    LISTEN_AUTH_PORT=7078 \
    TOKEN_TTL=300 \
    EXEMPT_USERS="" \
    LOG_LEVEL=INFO

CMD ["python3", "/app/guard.py"]
