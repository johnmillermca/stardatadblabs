"""
Doris RBAC Adapter
==================
Uses the MySQL-compatible Doris FE protocol (aiomysql / asyncmy) to apply
GRANT/REVOKE statements based on the resolved permission set.

Permission-to-SQL mapping:
  SELECT       → GRANT SELECT_PRIV  ON <db>.<table> TO '<user>'@'%'
  INSERT       → GRANT LOAD_PRIV    ON <db>.<table>
  UPDATE       → GRANT LOAD_PRIV    ON <db>.<table>   (Doris merges INSERT+UPDATE into LOAD_PRIV)
  DELETE       → GRANT LOAD_PRIV    ON <db>.<table>
  LOAD         → GRANT LOAD_PRIV    ON <db>.<table>
  CREATE       → GRANT CREATE_PRIV  ON <db>.*
  DROP         → GRANT DROP_PRIV    ON <db>.*
  ALTER        → GRANT ALTER_PRIV   ON <db>.<table>
  GRANT        → GRANT GRANT_PRIV   ON *.*
  ADMIN        → GRANT ADMIN ON *.*

resource_scope: {"database": "sales", "table": "orders"}
  database defaults to "*"  (all databases)
  table    defaults to "*"  (all tables in that database)
"""
from __future__ import annotations

import logging
from typing import Any

import aiomysql

from ..config import get_settings

log = logging.getLogger(__name__)

# Doris privilege names
_PERM_MAP = {
    "SELECT": "SELECT_PRIV",
    "INSERT": "LOAD_PRIV",
    "UPDATE": "LOAD_PRIV",
    "DELETE": "LOAD_PRIV",
    "LOAD":   "LOAD_PRIV",
    "CREATE": "CREATE_PRIV",
    "DROP":   "DROP_PRIV",
    "ALTER":  "ALTER_PRIV",
    "GRANT":  "GRANT_PRIV",
    "ADMIN":  "ADMIN",
}


class DorisAdapter:
    """Apply Doris GRANT/REVOKE SQL to match a desired permission set."""

    async def _conn(self) -> aiomysql.Connection:
        s = get_settings()
        return await aiomysql.connect(
            host=s.doris_host,
            port=s.doris_port,
            user=s.doris_admin_user,
            password=s.doris_admin_password,
            db="information_schema",
            connect_timeout=5,
            charset="utf8mb4",
        )

    async def _user_exists(self, cursor, username: str) -> bool:
        await cursor.execute(
            "SELECT COUNT(*) FROM information_schema.user_privileges "
            "WHERE grantee = %s",
            (f"'{username}'@'%'",),
        )
        row = await cursor.fetchone()
        return bool(row and row[0] > 0)

    async def sync_user(self, username: str, perms: list[dict]) -> None:
        """
        Idempotent: compute desired state, revoke all current grants,
        then re-grant from scratch.  This guarantees convergence even when
        external manual grants exist.
        """
        conn = await self._conn()
        try:
            async with conn.cursor() as cur:
                # Ensure user exists (no password — auth via KDC guard)
                if not await self._user_exists(cur, username):
                    await cur.execute(
                        f"CREATE USER IF NOT EXISTS '{username}'@'%';"
                    )
                    log.info("Doris: created user %s", username)

                # Revoke all existing privileges
                await cur.execute(
                    f"REVOKE ALL ON *.* FROM '{username}'@'%';"
                )

                if not perms:
                    log.info("Doris: user %s → no permissions (revoke-only)", username)
                    return

                # Build GRANT statements
                grants: dict[str, set[str]] = {}   # resource_key → set of privs
                for p in perms:
                    perm_name = p["permission"]
                    scope = p.get("resource_scope") or {}
                    db    = scope.get("database", "*")
                    table = scope.get("table", "*")
                    resource = f"{db}.{table}"

                    doris_priv = _PERM_MAP.get(perm_name)
                    if doris_priv is None:
                        log.warning("Doris: unknown permission '%s' — skipping", perm_name)
                        continue

                    if perm_name == "ADMIN":
                        resource = "*.*"

                    grants.setdefault(resource, set()).add(doris_priv)

                for resource, privs in grants.items():
                    priv_str = ", ".join(sorted(privs))
                    sql = f"GRANT {priv_str} ON {resource} TO '{username}'@'%';"
                    log.debug("Doris: %s", sql)
                    await cur.execute(sql)

            await conn.commit()
            log.info("Doris: synced user %s (%d permission groups)", username, len(grants))
        finally:
            conn.close()

    async def delete_user(self, username: str) -> None:
        conn = await self._conn()
        try:
            async with conn.cursor() as cur:
                await cur.execute(f"DROP USER IF EXISTS '{username}'@'%';")
            await conn.commit()
            log.info("Doris: dropped user %s", username)
        finally:
            conn.close()
