#!/usr/bin/env python3
"""
krb-doris-guard — Kerberos enforcement proxy for Apache Doris MySQL port.

Sits between clients and Doris FE (port 9030).
Listens on port 19030 (the guard port, exposed by the Service).
For every new TCP connection it:
  1. Reads the MySQL handshake from Doris (InitialHandshakePacket).
  2. Forwards it to the client unchanged.
  3. Reads the client's HandshakeResponse (login request) which contains the username.
  4. Checks whether username@REALM exists as a principal in the KDC
     by running:  kadmin -kt <KADMIN_KEYTAB> -p <KADMIN_PRINCIPAL> getprinc <user>@REALM
     This is a direct kadmin lookup — no password required, no TGS round-trip,
     no dependency on allow_anonymous in the KDC config.
     The kadmin-keytab must contain kadmin/admin@REALM and be mounted at
     KADMIN_KEYTAB (default: /etc/security/keytabs/kadmin.keytab).
  5. If the principal does NOT exist → send MySQL error packet (Access denied) and
     close the connection. The client sees:
       ERROR 1045 (28000): Access denied — principal bob@REALM not found in KDC.
  6. If the principal DOES exist → forward the HandshakeResponse to Doris and
     transparently proxy all subsequent bytes in both directions.
     Doris still validates the password; Doris enforces authorization natively.

Environment variables:
  KDC_REALM          Kerberos realm  (default: STARDATADBLABS.LOCAL)
  KRB5_CONFIG        Path to krb5.conf (default: /etc/krb5.conf.d/cluster.conf)
  KADMIN_KEYTAB      Path to kadmin keytab for principal lookup
                     (default: /etc/security/keytabs/kadmin.keytab)
  KADMIN_PRINCIPAL   kadmin principal in the keytab
                     (default: kadmin/admin@STARDATADBLABS.LOCAL)
  DORIS_HOST         Upstream Doris host (default: 127.0.0.1)
  DORIS_PORT         Upstream Doris port (default: 9030)
  LISTEN_PORT        Port this proxy listens on (default: 19030)
  EXEMPT_USERS       Comma-separated usernames that bypass KDC check (default: root)
  LOG_LEVEL          DEBUG / INFO / WARNING (default: INFO)
"""

import asyncio
import logging
import os
import struct
import subprocess
import sys

# ── Configuration ──────────────────────────────────────────────────────────────
KDC_REALM          = os.getenv("KDC_REALM",          "STARDATADBLABS.LOCAL")
KRB5_CONFIG        = os.getenv("KRB5_CONFIG",        "/etc/krb5.conf.d/cluster.conf")
KADMIN_KEYTAB      = os.getenv("KADMIN_KEYTAB",      "/etc/security/keytabs/kadmin.keytab")
KADMIN_PRINCIPAL   = os.getenv("KADMIN_PRINCIPAL",   "kadmin/admin@STARDATADBLABS.LOCAL")
DORIS_HOST         = os.getenv("DORIS_HOST",         "127.0.0.1")
DORIS_PORT         = int(os.getenv("DORIS_PORT",     "9030"))
LISTEN_PORT        = int(os.getenv("LISTEN_PORT",    "19030"))
EXEMPT_USERS       = set(os.getenv("EXEMPT_USERS",   "root").split(","))
LOG_LEVEL          = os.getenv("LOG_LEVEL",          "INFO")

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s [krb-doris-guard] %(levelname)s %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger(__name__)

# ── KDC principal check ────────────────────────────────────────────────────────

def principal_exists(username: str) -> bool:
    """
    Ask the KDC whether <username>@REALM is a valid principal.

    Method: kadmin -kt <keytab> -p kadmin/admin@REALM getprinc <user>@REALM
    This is a direct administrative lookup — no password exchange, no TGS
    round-trip, no dependency on allow_anonymous in the KDC configuration.

    Output for an existing principal contains "Principal: <name>@REALM".
    Output for a missing principal contains "Principal does not exist".

    The kadmin-keytab must be mounted at KADMIN_KEYTAB and must contain
    KADMIN_PRINCIPAL (kadmin/admin@REALM by default).

    On KDC unreachable or keytab errors we DENY by default (fail-closed).
    """
    principal = f"{username}@{KDC_REALM}"
    env = {
        **os.environ,
        "KRB5_CONFIG": KRB5_CONFIG,
        "HOME": "/tmp",
    }

    try:
        result = subprocess.run(
            [
                "kadmin",
                "-kt", KADMIN_KEYTAB,
                "-p", KADMIN_PRINCIPAL,
                "-q", f"getprinc {principal}",
            ],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=5,
            env=env,
        )
        output = result.stdout.decode(errors="replace")
        log.debug("kadmin getprinc %s: exit=%d output=%s",
                  principal, result.returncode, output.strip())

        # Successful lookup — principal exists
        if f"Principal: {principal}" in output:
            log.debug("KDC confirmed principal exists: %s", principal)
            return True

        # Explicit "does not exist" response from kadmin
        if "Principal does not exist" in output or \
           "does not exist" in output.lower():
            log.info("KDC check DENIED: principal %s does not exist", principal)
            return False

        # Any other failure (KDC unreachable, keytab invalid, etc.) → fail-closed
        log.warning("KDC check FAILED for %s (exit %d): %s — denying",
                    principal, result.returncode, output.strip())
        return False

    except subprocess.TimeoutExpired:
        log.warning("KDC check TIMEOUT for %s — denying", principal)
        return False
    except FileNotFoundError as exc:
        log.error("Required binary not found (%s) — denying all.", exc)
        return False


# ── MySQL packet helpers ───────────────────────────────────────────────────────
# MySQL wire protocol: every packet is prefixed with a 4-byte header:
#   [payload_length: 3 bytes little-endian][sequence_id: 1 byte]

async def read_packet(reader: asyncio.StreamReader) -> tuple[int, bytes]:
    """Read one MySQL packet. Returns (sequence_id, payload)."""
    header = await reader.readexactly(4)
    length = struct.unpack_from("<I", header + b"\x00")[0] & 0xFFFFFF
    seq    = header[3]
    payload = await reader.readexactly(length)
    return seq, payload


def make_packet(seq: int, payload: bytes) -> bytes:
    """Encode payload as a MySQL packet with given sequence id."""
    length = len(payload)
    header = struct.pack("<I", length)[:3] + bytes([seq])
    return header + payload


def make_error_packet(seq: int, code: int, msg: str) -> bytes:
    """
    Build MySQL ERR_Packet:
      0xFF [error_code: 2] '#' [sqlstate: 5] [message]
    """
    payload = (
        b"\xff"
        + struct.pack("<H", code)
        + b"#28000"
        + msg.encode()
    )
    return make_packet(seq, payload)


def parse_username(handshake_response: bytes) -> str:
    """
    Extract username from MySQL HandshakeResponse41 payload.
    Layout (capability flags CapLongPassword is always set in modern clients):
      4 bytes  capability flags
      4 bytes  max packet size
      1 byte   character set
      23 bytes reserved (zeros)
      then:    null-terminated username string
    """
    try:
        offset = 4 + 4 + 1 + 23   # skip cap_flags + max_pkt + charset + reserved
        if offset >= len(handshake_response):
            return ""
        null_pos = handshake_response.index(b"\x00", offset)
        return handshake_response[offset:null_pos].decode(errors="replace")
    except (ValueError, IndexError):
        return ""


# ── Per-connection handler ─────────────────────────────────────────────────────

async def handle_client(
    client_reader: asyncio.StreamReader,
    client_writer: asyncio.StreamWriter,
) -> None:
    peer = client_writer.get_extra_info("peername")
    log.debug("New connection from %s", peer)

    doris_reader = doris_writer = None
    try:
        # Connect to upstream Doris
        doris_reader, doris_writer = await asyncio.open_connection(
            DORIS_HOST, DORIS_PORT
        )

        # Step 1: Read Initial Handshake from Doris
        seq, handshake = await read_packet(doris_reader)
        log.debug("Got InitialHandshake seq=%d len=%d from Doris", seq, len(handshake))

        # Step 2: Forward it to the client
        client_writer.write(make_packet(seq, handshake))
        await client_writer.drain()

        # Step 3: Read HandshakeResponse from client (contains username)
        resp_seq, resp_payload = await read_packet(client_reader)
        username = parse_username(resp_payload)
        log.info("Login attempt: user=%r from %s", username, peer)

        # Step 4: KDC principal check (skip exempt users like root)
        if username and username not in EXEMPT_USERS:
            if not principal_exists(username):
                # Step 5: Send MySQL Access Denied and close
                deny_msg = (
                    f"Access denied for user '{username}'@'%': "
                    f"principal {username}@{KDC_REALM} not found in Kerberos KDC. "
                    f"Create the principal first: "
                    f"kadmin.local -q \"addprinc {username}@{KDC_REALM}\""
                )
                client_writer.write(make_error_packet(resp_seq + 1, 1045, deny_msg))
                await client_writer.drain()
                log.warning("BLOCKED user=%r from %s — not in KDC", username, peer)
                return
        elif not username:
            log.warning("Could not parse username from handshake — denying %s", peer)
            client_writer.write(
                make_error_packet(resp_seq + 1, 1045,
                                  "Access denied: could not parse username")
            )
            await client_writer.drain()
            return

        log.info("ALLOWED user=%r from %s — forwarding to Doris", username, peer)

        # Step 6: Forward HandshakeResponse to Doris, then splice bidirectionally
        doris_writer.write(make_packet(resp_seq, resp_payload))
        await doris_writer.drain()

        # Bidirectional transparent proxy
        await asyncio.gather(
            _pipe(client_reader, doris_writer,  f"{username}@{peer}→doris"),
            _pipe(doris_reader,  client_writer,  f"doris→{username}@{peer}"),
            return_exceptions=True,
        )

    except asyncio.IncompleteReadError:
        log.debug("Connection closed early from %s", peer)
    except ConnectionRefusedError:
        log.error("Cannot connect to Doris at %s:%d", DORIS_HOST, DORIS_PORT)
    except Exception as exc:
        log.exception("Error handling connection from %s: %s", peer, exc)
    finally:
        if doris_writer:
            doris_writer.close()
        client_writer.close()


async def _pipe(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    label: str,
) -> None:
    """Forward bytes from reader to writer until EOF."""
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
    server = await asyncio.start_server(
        handle_client,
        host="0.0.0.0",
        port=LISTEN_PORT,
    )
    addrs = [str(s.getsockname()) for s in server.sockets]
    log.info(
        "krb-doris-guard listening on %s — upstream Doris %s:%d — realm %s",
        addrs, DORIS_HOST, DORIS_PORT, KDC_REALM,
    )
    log.info("Exempt users (bypass KDC check): %s", EXEMPT_USERS)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
