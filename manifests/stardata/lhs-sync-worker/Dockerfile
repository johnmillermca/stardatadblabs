# =============================================================================
# Dockerfile — LHS Sync Sidecar Worker
# =============================================================================
# Build:
#   docker build -t 192.168.1.50:30500/lhs-sync-worker:python-v1 .
#   docker push 192.168.1.50:30500/lhs-sync-worker:python-v1
#
# Or from the workspace root:
#   docker build -t 192.168.1.50:30500/lhs-sync-worker:python-v1 \
#     manifests/stardata/lhs-sync-worker/
# =============================================================================

FROM python:3.11-slim

# Non-interactive, deterministic locale
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    LANG=C.UTF-8

WORKDIR /app

# Install PyMySQL — the only runtime dependency.
# Pin to a known-good version for reproducibility.
RUN pip install --no-cache-dir "PyMySQL==1.1.1"

COPY sync_worker.py /app/sync_worker.py

# Run as non-root
RUN useradd -r -u 1001 -g root syncworker
USER 1001

ENTRYPOINT ["python", "/app/sync_worker.py"]
