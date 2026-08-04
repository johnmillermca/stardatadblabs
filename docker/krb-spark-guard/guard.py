#!/usr/bin/env python3
"""
krb-spark-guard — Kerberos enforcement proxy for Spark standalone master (port 7077).

Spark standalone uses a custom binary protocol (Akka/Netty frames).
This guard takes a different, more reliable approach than deep protocol parsing:

STRATEGY — Connection-level username negotiation via a lightweight HTTP pre-auth API.
Rather than parsing Spark's internal Akka wire protocol (which is undocumented and
changes between versions), the guard exposes a small REST endpoint:

  POST /auth
  Body: {"username": "alice", "keytab_b64": "<base64-encoded keytab bytes>"}
  Response 200: {"token": "<short-lived token>", "expires_in": 300}
  Response 403: {"error": "principal alice@REALM not found in KDC"}

Spark jobs that want to connect to port 7077 must first call this API.
The token is then passed as spark.authenticate.secret which the guard validates
before forwarding the TCP connection.

For jobs submitted via spark-submit the guard is transparently used via
a wrapper script (spark-submit-krb) that handles the pre-auth automatically.

ARCHITECTURE:
  Client (spark-submit-krb)
    │
    ├─1─► POST /auth  (HTTP :17077)  ←─ guard validates KDC, returns token
    │
    └─2─► TCP :7077 with CONNECT header: X-Krb-Token: <token>
               │
               └─► guard validates token → forwards to spark-master :17077

  The real spark-master listens on 17077 (internal), the guard listens on 7077
  (the port exposed by the Service).

Environment variables:
  KDC_REALM        Kerberos realm  (default: STARDATADBLABS.LOCAL)
  KRB5_CONFIG      Path to krb5.conf (default: /etc/krb5.conf.d/cluster.conf)
  SPARK_HOST       Upstream Spark master host (default: 127.0.0.1)
  SPARK_PORT       Upstream Spark master port (default: 17077)
  LISTEN_RPC_PORT  Guard RPC proxy port (default: 7077) — exposed by K8s Service
  LISTEN_AUTH_PORT Guard HTTP auth port (default: 7078) — internal only
  TOKEN_TTL        Token validity in seconds (default: 300)
  EXEMPT_USERS     Comma-separated users that bypass KDC check (default: )
  LOG_LEVEL        DEBUG / INFO / WARNING (default: INFO)
"""

import asyncio
import base64
import hashlib
import json
import logging
import os
import secrets
import struct
import subprocess
import sys
import tempfile
import time
from typing import Optional

# ── Configuration ──────────────────────────────────────────────────────────────
KDC_REALM        = os.getenv("KDC_REALM",        "STARDATADBLABS.LOCAL")
KRB5_CONFIG      = os.getenv("KRB5_CONFIG",       "/etc/krb5.conf.d/cluster.conf")
SPARK_HOST       = os.getenv("SPARK_HOST",        "127.0.0.1")
SPARK_PORT       = int(os.getenv("SPARK_PORT",    "17077"))
LISTEN_RPC_PORT  = int(os.getenv("LISTEN_RPC_PORT", "7077"))
LISTEN_AUTH_PORT = int(os.getenv("LISTEN_AUTH_PORT", "7078"))
TOKEN_TTL        = int(os.getenv("TOKEN_TTL",     "300"))
EXEMPT_USERS     = set(filter(None, os.getenv("EXEMPT_USERS", "").split(",")))
LOG_LEVEL        = os.getenv("LOG_LEVEL",         "INFO")

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s [krb-spark-guard] %(levelname)s %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger(__name__)

# ── In-memory token store ──────────────────────────────────────────────────────
# token → {"username": str, "expires": float}
_tokens: dict[str, dict] = {}
_tokens_lock = asyncio.Lock()


def _purge_expired() -> None:
    now = time.time()
    expired = [t for t, v in _tokens.items() if v["expires"] < now]
    for t in expired:
        del _tokens[t]


async def issue_token(username: str) -> str:
    token = secrets.token_urlsafe(32)
    async with _tokens_lock:
        _purge_expired()
        _tokens[token] = {"username": username, "expires": time.time() + TOKEN_TTL}
    return token


async def validate_token(token: str) -> Optional[str]:
    async with _tokens_lock:
        _purge_expired()
        entry = _tokens.get(token)
        if entry and entry["expires"] > time.time():
            return entry["username"]
    return None


# ── KDC principal check via keytab ─────────────────────────────────────────────

def verify_keytab(username: str, keytab_bytes: bytes) -> tuple[bool, str]:
    """
    Verify that a keytab is valid for <username>@REALM by running:
      kinit -kt <keytab_file> <username>@REALM
    This actually authenticates to the KDC:
      - If the principal doesn't exist → KDC error (False)
      - If the keytab is wrong for the principal → KDC error (False)
      - If both principal and keytab are correct → TGT issued (True)
    The TGT is immediately destroyed (kdestroy) after the check.
    """
    principal = f"{username}@{KDC_REALM}"
    env = {
        **os.environ,
        "KRB5_CONFIG": KRB5_CONFIG,
        "HOME": "/tmp",
    }
    # Use a unique ccache so concurrent checks don't interfere
    ccache = f"/tmp/krb5cc_guard_{hashlib.md5(principal.encode()).hexdigest()[:8]}_{int(time.time()*1000)}"
    env["KRB5CCNAME"] = f"FILE:{ccache}"

    with tempfile.NamedTemporaryFile(suffix=".keytab", delete=True) as kt_file:
        kt_file.write(keytab_bytes)
        kt_file.flush()
        keytab_path = kt_file.name

        # Run kinit with the provided keytab
        result = subprocess.run(
            ["kinit", "-kt", keytab_path, principal],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=10,
            env=env,
        )
        output = result.stdout.decode(errors="replace").strip()
        log.debug("kinit result for %s: exit=%d output=%s", principal, result.returncode, output)

        # Clean up any TGT that was issued
        subprocess.run(
            ["kdestroy", "-c", f"FILE:{ccache}"],
            env=env, capture_output=True
        )
        try:
            os.unlink(ccache)
        except FileNotFoundError:
            pass

        if result.returncode == 0:
            return True, "OK"

        if "not found in Kerberos database" in output:
            return False, f"Principal {principal} does not exist in KDC"
        if "Keytab contains no suitable keys" in output or "Cannot find" in output:
            return False, f"Keytab is not valid for principal {principal}"
        if "KDC" in output and ("unreachable" in output.lower() or "timeout" in output.lower()):
            return False, f"KDC unreachable — cannot verify {principal}"

        return False, f"kinit failed (exit {result.returncode}): {output}"


# ── HTTP auth server ───────────────────────────────────────────────────────────

async def handle_auth_request(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
) -> None:
    """
    Minimal HTTP/1.1 server that handles POST /auth.
    Accepts:  {"username": "alice", "keytab_b64": "<base64>"}
    Returns:  {"token": "...", "expires_in": 300}
    Or:       {"error": "..."}  with HTTP 403
    """
    peer = writer.get_extra_info("peername")
    try:
        # Read HTTP request (enough to get headers + body)
        raw = await asyncio.wait_for(reader.read(65536), timeout=15)
        text = raw.decode(errors="replace")

        if not text.startswith("POST /auth"):
            _send_http(writer, 404, {"error": "Not found. Use POST /auth"})
            return

        # Extract JSON body (after blank line separating headers from body)
        if "\r\n\r\n" in text:
            body = text.split("\r\n\r\n", 1)[1].strip()
        elif "\n\n" in text:
            body = text.split("\n\n", 1)[1].strip()
        else:
            _send_http(writer, 400, {"error": "Could not parse body"})
            return

        try:
            req = json.loads(body)
        except json.JSONDecodeError as e:
            _send_http(writer, 400, {"error": f"Invalid JSON: {e}"})
            return

        username   = req.get("username", "").strip()
        keytab_b64 = req.get("keytab_b64", "")

        if not username:
            _send_http(writer, 400, {"error": "username is required"})
            return
        if not keytab_b64:
            _send_http(writer, 400, {"error": "keytab_b64 is required"})
            return

        # Decode keytab
        try:
            keytab_bytes = base64.b64decode(keytab_b64)
        except Exception:
            _send_http(writer, 400, {"error": "keytab_b64 is not valid base64"})
            return

        # Exempt users bypass KDC check
        if username in EXEMPT_USERS:
            token = await issue_token(username)
            log.info("AUTH exempt user=%r from %s — token issued", username, peer)
            _send_http(writer, 200, {"token": token, "expires_in": TOKEN_TTL})
            return

        # Verify keytab against KDC
        log.info("AUTH attempt user=%r from %s — checking KDC", username, peer)
        ok, reason = verify_keytab(username, keytab_bytes)

        if not ok:
            log.warning("AUTH DENIED user=%r from %s: %s", username, peer, reason)
            _send_http(writer, 403, {
                "error": reason,
                "hint": (
                    f"Create the principal: "
                    f"kadmin.local -q \"addprinc {username}@{KDC_REALM}\"\n"
                    f"Then export keytab: "
                    f"kadmin.local -q \"ktadd -k /tmp/{username}.keytab {username}@{KDC_REALM}\""
                )
            })
            return

        token = await issue_token(username)
        log.info("AUTH ALLOWED user=%r from %s — token issued (TTL %ds)", username, peer, TOKEN_TTL)
        _send_http(writer, 200, {"token": token, "expires_in": TOKEN_TTL})

    except asyncio.TimeoutError:
        _send_http(writer, 408, {"error": "Request timeout"})
    except Exception as exc:
        log.exception("Error in auth handler for %s: %s", peer, exc)
        _send_http(writer, 500, {"error": "Internal server error"})
    finally:
        writer.close()


def _send_http(writer: asyncio.StreamWriter, status: int, body: dict) -> None:
    status_text = {200: "OK", 400: "Bad Request", 403: "Forbidden",
                   404: "Not Found", 408: "Request Timeout", 500: "Internal Server Error"}.get(status, "Unknown")
    body_bytes = json.dumps(body).encode()
    response = (
        f"HTTP/1.1 {status} {status_text}\r\n"
        f"Content-Type: application/json\r\n"
        f"Content-Length: {len(body_bytes)}\r\n"
        f"Connection: close\r\n"
        f"\r\n"
    ).encode() + body_bytes
    writer.write(response)


# ── RPC proxy server ───────────────────────────────────────────────────────────

_GUARD_HEADER_PREFIX = b"X-Krb-Token: "
_CONNECT_LINE_MAX    = 256   # max bytes to read looking for the guard header

async def handle_rpc_connection(
    client_reader: asyncio.StreamReader,
    client_writer: asyncio.StreamWriter,
) -> None:
    """
    Accept a Spark RPC connection.
    Clients using spark-submit-krb prepend a single line:
      X-Krb-Token: <token>\n
    before the actual Spark bytes. This guard reads that line, validates the token,
    then forwards all remaining bytes (including any already-read Spark bytes) to
    the upstream Spark master.

    Clients NOT using spark-submit-krb (raw connections) send no such line.
    The guard reads up to _CONNECT_LINE_MAX bytes looking for the header.
    If not found → reject with a human-readable message embedded in the stream.
    """
    peer = client_writer.get_extra_info("peername")
    log.debug("RPC connection from %s", peer)
    spark_writer = spark_reader = None

    try:
        # Try to read the guard header line (up to 256 bytes or \n)
        try:
            first_line = await asyncio.wait_for(
                client_reader.readuntil(b"\n"),
                timeout=5.0
            )
        except asyncio.TimeoutError:
            _close_with_msg(client_writer,
                            "REJECTED: no X-Krb-Token header received within 5s. "
                            "Use spark-submit-krb instead of spark-submit directly.")
            return
        except asyncio.LimitOverrunError:
            _close_with_msg(client_writer,
                            "REJECTED: first line too long — expected X-Krb-Token header.")
            return

        if not first_line.startswith(_GUARD_HEADER_PREFIX):
            _close_with_msg(client_writer,
                            "REJECTED: first bytes are not an X-Krb-Token header. "
                            "Authenticate via POST /auth on port 7078 first, "
                            "then use spark-submit-krb.")
            log.warning("BLOCKED raw Spark connection from %s — no auth header", peer)
            return

        token = first_line[len(_GUARD_HEADER_PREFIX):].strip().decode(errors="replace")
        username = await validate_token(token)

        if username is None:
            _close_with_msg(client_writer,
                            "REJECTED: token invalid or expired. "
                            "Re-authenticate via POST /auth on port 7078.")
            log.warning("BLOCKED expired/invalid token from %s", peer)
            return

        log.info("RPC ALLOWED user=%r from %s — forwarding to Spark", username, peer)

        # Connect to upstream Spark master
        spark_reader, spark_writer = await asyncio.open_connection(SPARK_HOST, SPARK_PORT)

        # Proxy bidirectionally (the guard header line has been consumed)
        await asyncio.gather(
            _pipe(client_reader, spark_writer, f"{username}@{peer}→spark"),
            _pipe(spark_reader,  client_writer, f"spark→{username}@{peer}"),
            return_exceptions=True,
        )

    except ConnectionRefusedError:
        log.error("Cannot connect to Spark master at %s:%d", SPARK_HOST, SPARK_PORT)
    except Exception as exc:
        log.exception("RPC handler error from %s: %s", peer, exc)
    finally:
        if spark_writer:
            spark_writer.close()
        client_writer.close()


def _close_with_msg(writer: asyncio.StreamWriter, msg: str) -> None:
    try:
        writer.write(f"\n[krb-spark-guard] {msg}\n".encode())
    except Exception:
        pass
    writer.close()


async def _pipe(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    label: str,
) -> None:
    try:
        while True:
            chunk = await reader.read(65536)
            if not chunk:
                break
            writer.write(chunk)
            await writer.drain()
    except (asyncio.IncompleteReadError, ConnectionResetError, BrokenPipeError):
        pass
    finally:
        try:
            writer.close()
        except Exception:
            pass
    log.debug("Pipe closed: %s", label)


# ── Main ───────────────────────────────────────────────────────────────────────

async def main() -> None:
    rpc_server = await asyncio.start_server(
        handle_rpc_connection,
        host="0.0.0.0",
        port=LISTEN_RPC_PORT,
        limit=1 * 1024 * 1024,  # 1 MiB read buffer
    )
    auth_server = await asyncio.start_server(
        handle_auth_request,
        host="0.0.0.0",
        port=LISTEN_AUTH_PORT,
    )
    log.info(
        "krb-spark-guard RPC proxy on :%d → spark master %s:%d",
        LISTEN_RPC_PORT, SPARK_HOST, SPARK_PORT,
    )
    log.info(
        "krb-spark-guard HTTP auth API on :%d — realm %s — token TTL %ds",
        LISTEN_AUTH_PORT, KDC_REALM, TOKEN_TTL,
    )
    if EXEMPT_USERS:
        log.info("Exempt users (bypass KDC check): %s", EXEMPT_USERS)

    async with rpc_server, auth_server:
        await asyncio.gather(
            rpc_server.serve_forever(),
            auth_server.serve_forever(),
        )


if __name__ == "__main__":
    asyncio.run(main())
