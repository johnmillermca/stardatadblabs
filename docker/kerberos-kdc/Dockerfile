FROM 192.168.1.50:30500/rockylinux:9

# Install Kerberos packages at IMAGE BUILD TIME — not at runtime.
#
# CRITICAL: Do NOT use rockylinux:9 directly as the pod image and try to run
# dnf inside the container startup script. That approach requires internet
# access (mirrors.rockylinux.org) at every pod start. On an air-gapped or
# slow-DNS node reboot, dnf fails → startup.sh exits via set -e → pod crash-
# loops indefinitely (observed: 108 restarts before this was fixed).
#
# Build and push:
#   podman build --tls-verify=false \
#     -t 192.168.1.50:30500/kerberos-kdc:1.0.0 \
#     docker/kerberos-kdc/
#   podman push --tls-verify=false 192.168.1.50:30500/kerberos-kdc:1.0.0

RUN dnf install -y \
      krb5-server \
      krb5-libs \
      krb5-workstation \
    && dnf clean all \
    && rm -rf /var/cache/dnf

EXPOSE 88/tcp 88/udp 749/tcp
