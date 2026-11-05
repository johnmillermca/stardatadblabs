"""
polaris_auth_proxy.py
=====================
Auto-refreshing OAuth2 token injector proxy for Apache Polaris REST catalog.
Credentials are read from environment variables (injected from a Kubernetes
Secret — no OpenBao / Vault dependency).

Listens on two ports:
  :8282  — stargraph-writer principal  (read + write access)
  :8283  — stargraph-reader principal  (read-only access)

For every incoming HTTP request the proxy:
  1. Checks whether the current OAuth2 token is still valid.
  2. If not — fetches a fresh token from Polaris using client_credentials.
  3. Injects "Authorization: Bearer <token>" into the forwarded request.
  4. Forwards all other headers and body unchanged to Polaris.
  5. Streams the Polaris response back to the caller.

Environment variables
---------------------
  POLARIS_URL             Polaris backend  (default: http://polaris-rest.prod.svc.cluster.local:8181)
  POLARIS_READER_ID       stargraph-reader client_id      (from polaris-stargraph-credentials secret)
  POLARIS_READER_SECRET   stargraph-reader client_secret
  POLARIS_WRITER_ID       stargraph-writer client_id
  POLARIS_WRITER_SECRET   stargraph-writer client_secret
  TOKEN_REFRESH_BUFFER_S  Refresh token this many seconds before expiry  (default: 300)
  PORT_WRITER             Port for writer principal  (default: 8282)
  PORT_READER             Port for reader principal  (default: 8283)
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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s polaris-auth-proxy — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("polaris-auth-proxy")

# ── Config ────────────────────────────────────────────────────────────────────
POLARIS_URL            = os.environ.get("POLARIS_URL",
    "http://polaris-rest.prod.svc.cluster.local:8181")
TOKEN_REFRESH_BUFFER_S = int(os.environ.get("TOKEN_REFRESH_BUFFER_S", "300"))
PORT_WRITER            = int(os.environ.get("PORT_WRITER", "8282"))
PORT_READER            = int(os.environ.get("PORT_READER", "8283"))

# Credentials injected from polaris-stargraph-credentials Kubernetes Secret
POLARIS_READER_ID      = os.environ["POLARIS_READER_ID"]
POLARIS_READER_SECRET  = os.environ["POLARIS_READER_SECRET"]
POLARIS_WRITER_ID      = os.environ["POLARIS_WRITER_ID"]
POLARIS_WRITER_SECRET  = os.environ["POLARIS_WRITER_SECRET"]


# ── Token manager ─────────────────────────────────────────────────────────────

class _TokenManager:
    def __init__(self, name: str, client_id: str, client_secret: str) -> None:
        self._name          = name
        self._client_id     = client_id
        self._client_secret = client_secret
        self._token: Optional[str] = None
        self._expires_at: float    = 0.0
        self._lock                 = threading.Lock()

    def _fetch(self) -> None:
        logger.info("TokenManager[%s]: fetching fresh token from Polaris.", self._name)
        token_url = f"{POLARIS_URL}/api/catalog/v1/oauth/tokens"
        body = urllib.parse.urlencode({
            "grant_type":    "client_credentials",
            "client_id":     self._client_id,
            "client_secret": self._client_secret,
            "scope":         "PRINCIPAL_ROLE:ALL",
        }).encode()
        req = urllib.request.Request(
            token_url, data=body,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
        self._token      = data["access_token"]
        expires_in       = int(data.get("expires_in", 3600))
        self._expires_at = time.monotonic() + expires_in
        logger.info("TokenManager[%s]: token valid for %ds.", self._name, expires_in)

    def get_token(self) -> str:
        with self._lock:
            if (self._token is None
                    or time.monotonic() >= self._expires_at - TOKEN_REFRESH_BUFFER_S):
                self._fetch()
            return self._token  # type: ignore[return-value]

    def start_background_refresh(self) -> None:
        def _loop() -> None:
            while True:
                try:
                    with self._lock:
                        remaining = self._expires_at - time.monotonic()
                    time.sleep(max(remaining - TOKEN_REFRESH_BUFFER_S, 30))
                    with self._lock:
                        self._fetch()
                except Exception as exc:
                    logger.warning("TokenManager[%s]: refresh failed (%s) — retrying in 30s.",
                                   self._name, exc)
                    time.sleep(30)
        threading.Thread(target=_loop, name=f"refresh-{self._name}", daemon=True).start()
        logger.info("TokenManager[%s]: background refresh started.", self._name)


# ── Global token managers (credentials from K8s Secret env vars) ─────────────

_READER_TOKENS = _TokenManager("reader", POLARIS_READER_ID, POLARIS_READER_SECRET)
_WRITER_TOKENS = _TokenManager("writer", POLARIS_WRITER_ID, POLARIS_WRITER_SECRET)

_PORT_TOKEN_MAP: dict[int, _TokenManager] = {
    PORT_WRITER: _WRITER_TOKENS,
    PORT_READER: _READER_TOKENS,
}


# ── HTTP proxy handler ────────────────────────────────────────────────────────

_HOP_BY_HOP = {
    "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
    "te", "trailers", "transfer-encoding", "upgrade",
}


class _ProxyHandler(http.server.BaseHTTPRequestHandler):
    token_manager: _TokenManager

    def log_message(self, fmt: str, *args: object) -> None:
        logger.debug("Proxy[%d]: " + fmt, self.server.server_address[1], *args)

    def _forward(self) -> None:
        token  = self.token_manager.get_token()
        length = int(self.headers.get("Content-Length", 0))
        body   = self.rfile.read(length) if length else None
        target = f"{POLARIS_URL}{self.path}"
        headers = {
            k: v for k, v in self.headers.items()
            if k.lower() not in _HOP_BY_HOP and k.lower() != "authorization"
        }
        headers["Authorization"]               = f"Bearer {token}"
        headers["X-Iceberg-Access-Delegation"] = "false"
        if body and "Content-Length" not in headers:
            headers["Content-Length"] = str(len(body))
        req = urllib.request.Request(target, data=body, headers=headers, method=self.command)
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                self.send_response(resp.status)
                for k, v in resp.headers.items():
                    if k.lower() not in _HOP_BY_HOP:
                        self.send_header(k, v)
                self.end_headers()
                self.wfile.write(resp.read())
        except urllib.request.HTTPError as exc:
            self.send_response(exc.code)
            for k, v in exc.headers.items():
                if k.lower() not in _HOP_BY_HOP:
                    self.send_header(k, v)
            self.end_headers()
            self.wfile.write(exc.read())

    do_GET = do_POST = do_PUT = do_DELETE = do_HEAD = do_PATCH = _forward


def _make_handler(token_mgr: _TokenManager) -> type:
    class _H(_ProxyHandler):
        token_manager = token_mgr
    return _H


def _start_server(port: int, token_mgr: _TokenManager) -> None:
    server = http.server.HTTPServer(("0.0.0.0", port), _make_handler(token_mgr))
    logger.info("polaris-auth-proxy: listening on :%d (%s).", port, token_mgr._name)
    threading.Thread(target=server.serve_forever, name=f"proxy-{port}", daemon=True).start()


def main() -> None:
    logger.info("Polaris Auth Proxy starting. backend=%s  writer=%d  reader=%d",
                POLARIS_URL, PORT_WRITER, PORT_READER)
    for mgr in (_READER_TOKENS, _WRITER_TOKENS):
        try:
            mgr.get_token()
            mgr.start_background_refresh()
        except Exception as exc:
            logger.error("Startup token fetch failed for %s: %s", mgr._name, exc)
    _start_server(PORT_WRITER, _WRITER_TOKENS)
    _start_server(PORT_READER, _READER_TOKENS)
    logger.info("polaris-auth-proxy: both ports live.")
    while True:
        time.sleep(60)


if __name__ == "__main__":
    main()
