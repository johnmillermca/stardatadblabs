FROM python:3.12-slim

WORKDIR /app

# ── System deps ────────────────────────────────────────────
RUN apt-get update && apt-get install -y --no-install-recommends \
    krb5-user \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# ── Python deps ────────────────────────────────────────────
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ── App ────────────────────────────────────────────────────
COPY api/ api/
COPY migrations/ migrations/

# Non-root user
RUN adduser --disabled-password --gecos "" rbac
USER rbac

EXPOSE 8080

CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8080", "--workers", "2"]
