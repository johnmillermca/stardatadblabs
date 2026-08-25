FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# ── System deps ─────────────────────────────────────────────
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# ── Python deps ─────────────────────────────────────────────
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ── App source ──────────────────────────────────────────────
COPY catalog/ catalog/
COPY migrations/ migrations/

# Non-root user
RUN adduser --disabled-password --gecos "" catalog
USER catalog

EXPOSE 8090

CMD ["uvicorn", "catalog.main:app", "--host", "0.0.0.0", "--port", "8090", "--workers", "2"]
