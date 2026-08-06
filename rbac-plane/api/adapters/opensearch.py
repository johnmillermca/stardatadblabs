"""
OpenSearch RBAC Adapter
=======================
Uses the OpenSearch Security REST API (/_plugins/_security/api/) to:
  1. Create/update an internal user.
  2. Map the user to a role that matches the permission set.
  3. Create the role if it doesn't already exist.

Permission-to-role mapping:
  INDEX_READ    → indices:data/read/*, indices:admin/mappings/get
  INDEX_WRITE   → indices:data/write/*
  INDEX_ADMIN   → indices:admin/*
  CLUSTER_READ  → cluster:monitor/*
  CLUSTER_ADMIN → cluster:admin/* + cluster:monitor/*

resource_scope:
  {"index": "logs-*"}  — if present, the role is scoped to that index pattern.
  {}                   — applies to all indices ("*")

Role naming convention: rbac_<permission>_<index_sanitized>
  e.g. rbac_index_read_logs_all  (for index pattern "logs-*")

User is mapped to the appropriate roles via /_plugins/_security/api/rolesmapping/.
"""
from __future__ import annotations

import logging
import re
from typing import Any

import httpx

from ..config import get_settings

log = logging.getLogger(__name__)

_PERM_TO_ACTIONS: dict[str, list[str]] = {
    "INDEX_READ":    ["indices:data/read/*", "indices:admin/mappings/get"],
    "INDEX_WRITE":   ["indices:data/write/*"],
    "INDEX_ADMIN":   ["indices:admin/*", "indices:data/write/*", "indices:data/read/*"],
    "CLUSTER_READ":  ["cluster:monitor/*"],
    "CLUSTER_ADMIN": ["cluster:admin/*", "cluster:monitor/*"],
}


def _sanitize(name: str) -> str:
    return re.sub(r"[^a-z0-9]", "_", name.lower())


def _role_name(perm: str, index: str) -> str:
    return f"rbac_{_sanitize(perm)}_{_sanitize(index or 'all')}"


class OpenSearchAdapter:
    """Push user/role state to the OpenSearch Security plugin."""

    def _client(self) -> httpx.AsyncClient:
        s = get_settings()
        return httpx.AsyncClient(
            base_url=f"https://{s.opensearch_host}:{s.opensearch_port}",
            auth=(s.opensearch_admin_user, s.opensearch_admin_password),
            verify=s.opensearch_verify_ssl,
            timeout=10.0,
        )

    async def _ensure_role(
        self, client: httpx.AsyncClient, role_name: str, perm: str, index: str
    ) -> None:
        actions = _PERM_TO_ACTIONS.get(perm, [])
        if not actions:
            return

        index_pattern = index if index else "*"
        body: dict = {
            "description": f"RBAC managed role: {perm} on {index_pattern}",
            "cluster_permissions": [],
            "index_permissions": [],
        }

        cluster_actions = [a for a in actions if a.startswith("cluster:")]
        index_actions   = [a for a in actions if not a.startswith("cluster:")]

        if cluster_actions:
            body["cluster_permissions"] = cluster_actions
        if index_actions:
            body["index_permissions"] = [
                {
                    "index_patterns": [index_pattern],
                    "allowed_actions": index_actions,
                }
            ]

        resp = await client.put(
            f"/_plugins/_security/api/roles/{role_name}",
            json=body,
        )
        if resp.status_code not in (200, 201):
            log.warning("OpenSearch: could not create role %s: %s", role_name, resp.text)

    async def _upsert_user(self, client: httpx.AsyncClient, username: str) -> None:
        """Create the internal user if it doesn't exist.
        Password is set to a random value — auth is via Kerberos SPNEGO,
        not password, so the password is irrelevant but required by the API."""
        import secrets
        resp = await client.get(
            f"/_plugins/_security/api/internalusers/{username}"
        )
        if resp.status_code == 200:
            return  # already exists
        rand_pass = secrets.token_urlsafe(20)
        await client.put(
            f"/_plugins/_security/api/internalusers/{username}",
            json={"password": rand_pass, "backend_roles": [], "attributes": {}},
        )
        log.info("OpenSearch: created internal user %s", username)

    async def sync_user(self, username: str, perms: list[dict]) -> None:
        async with self._client() as client:
            await self._upsert_user(client, username)

            desired_roles: list[str] = []
            for p in perms:
                perm_name = p["permission"]
                scope     = p.get("resource_scope") or {}
                index     = scope.get("index", "")
                rname = _role_name(perm_name, index)
                await self._ensure_role(client, rname, perm_name, index)
                desired_roles.append(rname)

            if not desired_roles:
                # Map user to an empty role list (effective revoke)
                desired_roles = []

            # Retrieve current mapping for each desired role and add user
            # Strategy: set the users list in the rolesmapping for each rbac role
            # Revoke: get all rbac_* rolesmappings and remove the user from any
            # not in the desired set.
            all_mappings_resp = await client.get(
                "/_plugins/_security/api/rolesmapping"
            )
            existing: dict = all_mappings_resp.json() if all_mappings_resp.status_code == 200 else {}

            # Remove user from all rbac roles not in desired set
            for role_name, mapping in existing.items():
                if not role_name.startswith("rbac_"):
                    continue
                users_in_role = mapping.get("users", [])
                if username in users_in_role and role_name not in desired_roles:
                    users_in_role.remove(username)
                    await client.patch(
                        f"/_plugins/_security/api/rolesmapping/{role_name}",
                        json=[{"op": "replace", "path": "/users",
                               "value": users_in_role}],
                    )

            # Add user to desired roles
            for rname in desired_roles:
                mapping = existing.get(rname, {})
                users_in_role = list(set(mapping.get("users", []) + [username]))
                await client.patch(
                    f"/_plugins/_security/api/rolesmapping/{rname}",
                    json=[{"op": "replace", "path": "/users", "value": users_in_role}],
                )

            log.info("OpenSearch: synced user %s → %d roles", username, len(desired_roles))

    async def delete_user(self, username: str) -> None:
        async with self._client() as client:
            resp = await client.delete(
                f"/_plugins/_security/api/internalusers/{username}"
            )
            log.info("OpenSearch: deleted user %s (status %d)", username, resp.status_code)
