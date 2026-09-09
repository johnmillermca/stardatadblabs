"""
polaris_auth_proxy.py
=====================
Auto-refreshing OAuth2 token injector proxy for Apache Polaris REST catalog.

Replaces the nginx hardcoded-token approach.  Listens on two ports:
  :8282  — doris-writer principal  (read + write access)
  :8283  — doris-reader principal  (read-only access)

For every incoming HTTP request the proxy:
  1. Checks whether the current OAuth2 token for that port's principal is
     still valid (at least TOKEN_REFRESH_BUFFER_S seconds remaining).
  2. If not — fetches a fresh token from the Polaris token endpoint using
     client_credentials from OpenBao.
  3. Injects "Authorization: Bearer <token>" into the forwarded request.
  4. Forwards all other headers and the body unchanged to Polaris.
  5. Streams the Polaris response back to the caller.

Token refresh happens in a background thread every TOKEN_REFRESH_BUFFER_S
seconds so the first request after startup is never blocked waiting for a token.

Environment variables
---------------------
  POLARIS_URL           Polaris backend  (default: http://polaris-rest.prod.svc.cluster.local:8181)
  BAO_ADDR              OpenBao address  (default: http://openbao.prod.svc.cluster.local:8200)
  BAO_ROLE              OpenBao K8s auth role  (default: platform-secrets-read)
  TOKEN_REFRESH_BUFFER_S  Refresh token this many seconds before expiry  (default: 300)
  PORT_WRITER           Port for doris-writer principal  (default: 8282)
  PORT_READER           Port for doris-reader principal  (default: 8283)
"""
from __future__ import annotations

import http.server
import json
import logging
import os
import threading
import time
import urllib.request
import urllib.parse
from typing import Optional

# ─────────────────────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s polaris-auth-proxy — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("polaris-auth-proxy")

# ─────────────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────────────
POLARIS_URL            = os.environ.get("POLARIS_URL",
    "http://polaris-rest.prod.svc.cluster.local:8181")
BAO_ADDR               = os.environ.get("BAO_ADDR") or os.environ.get("ADDR",
    "http://openbao.prod.svc.cluster.local:8200")
BAO_ROLE               = os.environ.get("BAO_ROLE", "platform-secrets-read")
TOKEN_REFRESH_BUFFER_S = int(os.environ.get("TOKEN_REFRESH_BUFFER_S", "300"))
PORT_WRITER            = int(os.environ.get("PORT_WRITER", "8282"))
PORT_READER            = int(os.environ.get("PORT_READER", "8283"))

_K8S_SA_JWT_FILE = "/var/run/secrets/kubernetes.io/serviceaccount/token"
_PATH_POLARIS    = "secret/data/platform/polaris"

# ─────────────────────────────────────────────────────────────────────────────
# OpenBao client
# ─────────────────────────────────────────────────────────────────────────────

def _bao_token() -> str:
    """Obtain an OpenBao client token via K8s SA JWT."""
    if tok := os.environ.get("TOKEN") or os.environ.get("BAO_TOKEN"):
        return tok
    with open(_K8S_SA_JWT_FILE) as fh:
        jwt = fh.read().strip()
    payload = json.dumps({"role": BAO_ROLE, "jwt": jwt}).encode()
    req = urllib.request.Request(
        f"{BAO_ADDR}/v1/auth/kubernetes/login",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read())["auth"]["client_token"]


def _bao_read(path: str) -> dict:
    tok = _bao_token()
    req = urllib.request.Request(
        f"{BAO_ADDR}/v1/{path}",
        headers={"X-Vault-Token": tok},
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        data = json.loads(resp.read())
    outer = data.get("data", {})
    return outer.get("data", outer)


# ─────────────────────────────────────────────────────────────────────────────
# Token manager — one instance per principal
# ─────────────────────────────────────────────────────────────────────────────

class _TokenManager:
    """
    Manages a single OAuth2 token for one Polaris principal.
    Automatically refreshes before expiry.
    Thread-safe via a lock.
    """

    def __init__(self, name: str, client_id_key: str, client_secret_key: str) -> None:
        self._name             = name
        self._client_id_key    = client_id_key
        self._client_secret_key = client_secret_key
        self._token: Optional[str] = None
        self._expires_at: float    = 0.0
        self._lock                 = threading.Lock()

    def _fetch(self) -> None:
        """Fetch a fresh token from Polaris. Called under lock."""
        logger.info("TokenManager[%s]: fetching fresh token from Polaris.", self._name)
        secret = _bao_read(_PATH_POLARIS)
        client_id     = secret[self._client_id_key]
        client_secret = secret[self._client_secret_key]

        token_url = f"{POLARIS_URL}/api/catalog/v1/oauth/tokens"
        body = urllib.parse.urlencode({
            "grant_type":    "client_credentials",
            "client_id":     client_id,
            "client_secret": client_secret,
            "scope":         "PRINCIPAL_ROLE:ALL",
        }).encode()
        req = urllib.request.Request(
            token_url,
            data=body,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())

        self._token      = data["access_token"]
        expires_in       = int(data.get("expires_in", 3600))
        self._expires_at = time.monotonic() + expires_in
        logger.info(
            "TokenManager[%s]: token obtained, valid for %ds.", self._name, expires_in
        )

    def get_token(self) -> str:
        """Return a valid Bearer token, refreshing if needed."""
        with self._lock:
            if (
                self._token is None
                or time.monotonic() >= self._expires_at - TOKEN_REFRESH_BUFFER_S
            ):
                self._fetch()
            return self._token  # type: ignore[return-value]

    def start_background_refresh(self) -> None:
        """Start a daemon thread that proactively refreshes the token."""
        def _loop() -> None:
            while True:
                try:
                    with self._lock:
                        remaining = self._expires_at - time.monotonic()
                    sleep_for = max(remaining - TOKEN_REFRESH_BUFFER_S, 30)
                    time.sleep(sleep_for)
                    with self._lock:
                        self._fetch()
                except Exception as exc:
                    logger.warning(
                        "TokenManager[%s]: background refresh failed (%s) — retrying in 30s.",
                        self._name, exc,
                    )
                    time.sleep(30)

        t = threading.Thread(target=_loop, name=f"token-refresh-{self._name}", daemon=True)
        t.start()
        logger.info("TokenManager[%s]: background refresh thread started.", self._name)


# ─────────────────────────────────────────────────────────────────────────────
# One global token manager per principal
# ─────────────────────────────────────────────────────────────────────────────

_WRITER_TOKENS = _TokenManager("writer", "doris_writer_id", "doris_writer_secret")
_READER_TOKENS = _TokenManager("reader", "doris_reader_id", "doris_reader_secret")

# Maps listen port → token manager
_PORT_TOKEN_MAP: dict[int, _TokenManager] = {
    PORT_WRITER: _WRITER_TOKENS,
    PORT_READER: _READER_TOKENS,
}

# ─────────────────────────────────────────────────────────────────────────────
# HTTP proxy handler
# ─────────────────────────────────────────────────────────────────────────────

# Hop-by-hop headers that must not be forwarded
_HOP_BY_HOP = {
    "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
    "te", "trailers", "transfer-encoding", "upgrade",
}

class _ProxyHandler(http.server.BaseHTTPRequestHandler):
    """
    Forwards every request to Polaris with a fresh Authorization header.
    The token manager for this port is injected via the class attribute
    `token_manager` before the server starts.
    """

    token_manager: _TokenManager  # set per-server instance

    def log_message(self, fmt: str, *args: object) -> None:
        logger.debug("Proxy[%d]: " + fmt, self.server.server_address[1], *args)

    def _forward(self) -> None:
        token  = self.token_manager.get_token()
        length = int(self.headers.get("Content-Length", 0))
        body   = self.rfile.read(length) if length else None

        # Build target URL
        target = f"{POLARIS_URL}{self.path}"

        # Copy headers, strip hop-by-hop, inject Authorization
        headers = {}
        for key, val in self.headers.items():
            if key.lower() not in _HOP_BY_HOP and key.lower() != "authorization":
                headers[key] = val
        headers["Authorization"]              = f"Bearer {token}"
        headers["X-Iceberg-Access-Delegation"] = "false"
        if body and "Content-Length" not in headers:
            headers["Content-Length"] = str(len(body))

        req = urllib.request.Request(
            target, data=body, headers=headers, method=self.command
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                self.send_response(resp.status)
                for key, val in resp.headers.items():
                    if key.lower() not in _HOP_BY_HOP:
                        self.send_header(key, val)
                self.end_headers()
                self.wfile.write(resp.read())
        except urllib.request.HTTPError as exc:
            self.send_response(exc.code)
            for key, val in exc.headers.items():
                if key.lower() not in _HOP_BY_HOP:
                    self.send_header(key, val)
            self.end_headers()
            self.wfile.write(exc.read())

    do_GET    = _forward
    do_POST   = _forward
    do_PUT    = _forward
    do_DELETE = _forward
    do_HEAD   = _forward
    do_PATCH  = _forward


# ─────────────────────────────────────────────────────────────────────────────
# Per-port server factory
# ─────────────────────────────────────────────────────────────────────────────

def _make_handler(token_mgr: _TokenManager) -> type:
    """Return a handler class bound to the given token manager."""
    class _Handler(_ProxyHandler):
        token_manager = token_mgr
    return _Handler


def _start_server(port: int, token_mgr: _TokenManager) -> None:
    handler = _make_handler(token_mgr)
    server  = http.server.HTTPServer(("0.0.0.0", port), handler)
    logger.info("polaris-auth-proxy: listening on port %d (%s).", port, token_mgr._name)
    t = threading.Thread(target=server.serve_forever, name=f"proxy-{port}", daemon=True)
    t.start()


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    logger.info(
        "Polaris Auth Proxy starting. backend=%s  writer_port=%d  reader_port=%d",
        POLARIS_URL, PORT_WRITER, PORT_READER,
    )

    # Pre-fetch tokens on startup so first request is never blocked
    for mgr in (_WRITER_TOKENS, _READER_TOKENS):
        try:
            mgr.get_token()
            mgr.start_background_refresh()
        except Exception as exc:
            logger.error("Startup token fetch failed for %s: %s — will retry on first request.", mgr._name, exc)

    _start_server(PORT_WRITER, _WRITER_TOKENS)
    _start_server(PORT_READER, _READER_TOKENS)

    logger.info("polaris-auth-proxy: both servers running.")

    # Keep main thread alive
    while True:
        time.sleep(60)


if __name__ == "__main__":
    main()
