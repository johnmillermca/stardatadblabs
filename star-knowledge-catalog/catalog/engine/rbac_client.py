"""
Star Knowledge Catalog — RBAC Control Plane client.

Used by the masking engine to resolve a user's roles and to determine
whether they hold a masking exception for a given classification.
"""
from __future__ import annotations

import logging
from typing import Optional

import httpx

from ..config import get_settings

log = logging.getLogger(__name__)


class RBACClient:
    """Lightweight async HTTP client for the RBAC Control Plane API."""

    def __init__(self):
        s = get_settings()
        self._base_url = s.rbac_plane_url.rstrip("/")
        self._headers = {
            "Authorization": f"Bearer {s.rbac_plane_token}",
            "Content-Type": "application/json",
        }

    async def get_user_roles(self, username: str) -> dict:
        """
        Fetch the effective roles and permissions for *username* from the
        RBAC Control Plane.  Returns a dict like:
          {
            "username": "alice",
            "roles": ["analyst"],
            "permissions": [{"service": "doris", "permission": "MASKED_SELECT", ...}],
            "cached": true
          }
        Returns an empty structure on error so callers get safe defaults.
        """
        url = f"{self._base_url}/api/v1/users/{username}/roles"
        try:
            async with httpx.AsyncClient(timeout=3.0) as client:
                resp = await client.get(url, headers=self._headers)
                if resp.status_code == 200:
                    return resp.json()
                log.warning(
                    "RBACClient: GET %s returned HTTP %d", url, resp.status_code
                )
        except Exception as exc:
            log.warning("RBACClient: failed to fetch roles for %s: %s", username, exc)
        return {"username": username, "roles": [], "permissions": [], "cached": False}

    async def role_names(self, username: str) -> list[str]:
        """Return just the list of role names for *username*."""
        data = await self.get_user_roles(username)
        return data.get("roles", [])


_client: Optional[RBACClient] = None


def get_rbac_client() -> RBACClient:
    global _client
    if _client is None:
        _client = RBACClient()
    return _client
