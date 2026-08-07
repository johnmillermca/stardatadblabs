"""
Spark RBAC Adapter
==================
Spark standalone has no built-in RBAC.  The `krb-spark-guard` sidecar
(already deployed) proxies the RPC port and validates Kerberos tokens.

For job-level authorization we use an allowlist ConfigMap:
  - ConfigMap `spark-rbac-allowlist` in the `prod` namespace
  - Data key: `allowlist.json`  (JSON: {"username": {"can_submit": bool, "can_kill_any": bool}})

The guard reads this ConfigMap at startup and periodically re-reads it
(watch-loop), so there is no restart needed when RBAC changes.

Permission mapping:
  SUBMIT_JOB   → allowed: true
  KILL_OWN_JOB → (implicit — everyone who can submit can kill their own)
  KILL_ANY_JOB → can_kill_any: true
  VIEW_UI      → view_ui: true  (guard doesn't gate the UI — informational)
"""
from __future__ import annotations

import json
import logging
from typing import Any

from ..config import get_settings

log = logging.getLogger(__name__)


class SparkAdapter:
    """Update the spark-rbac-allowlist ConfigMap via the Kubernetes API."""

    async def _client(self):
        try:
            from kubernetes_asyncio import client, config
            try:
                config.load_incluster_config()
            except Exception:
                await config.load_kube_config()
            return client.CoreV1Api()
        except ImportError:
            raise RuntimeError("kubernetes_asyncio not installed")

    async def _read_allowlist(self, api) -> dict:
        s = get_settings()
        try:
            cm = await api.read_namespaced_config_map(
                name=s.spark_allowlist_cm, namespace=s.spark_namespace
            )
            raw = (cm.data or {}).get("allowlist.json", "{}")
            return json.loads(raw)
        except Exception:
            return {}

    async def _write_allowlist(self, api, allowlist: dict) -> None:
        s = get_settings()
        payload = json.dumps(allowlist, indent=2, sort_keys=True)
        from kubernetes_asyncio.client import V1ConfigMap, V1ObjectMeta
        body = V1ConfigMap(
            api_version="v1",
            kind="ConfigMap",
            metadata=V1ObjectMeta(
                name=s.spark_allowlist_cm,
                namespace=s.spark_namespace,
                labels={"rbac-managed": "true"},
            ),
            data={"allowlist.json": payload},
        )
        try:
            await api.read_namespaced_config_map(
                name=s.spark_allowlist_cm, namespace=s.spark_namespace
            )
            await api.replace_namespaced_config_map(
                name=s.spark_allowlist_cm,
                namespace=s.spark_namespace,
                body=body,
            )
        except Exception as exc:
            if getattr(exc, "status", None) == 404 or "Not Found" in str(exc):
                await api.create_namespaced_config_map(
                    namespace=s.spark_namespace, body=body
                )
            else:
                raise

    async def sync_user(self, username: str, perms: list[dict]) -> None:
        api = await self._client()
        allowlist = await self._read_allowlist(api)

        perm_names = {p["permission"] for p in perms}
        if not perm_names:
            # Remove user from allowlist
            allowlist.pop(username, None)
        else:
            allowlist[username] = {
                "can_submit":       "SUBMIT_JOB"    in perm_names,
                "can_kill_any":     "KILL_ANY_JOB"  in perm_names,
                "view_ui":          "VIEW_UI"        in perm_names,
                # Iceberg / Polaris catalog permissions
                "can_use_catalog":  "USE_CATALOG"   in perm_names,
                "can_write_iceberg":"WRITE_ICEBERG"  in perm_names,
                "can_admin_catalog":"ADMIN_CATALOG"  in perm_names,
            }

        await self._write_allowlist(api, allowlist)
        log.info("Spark: updated allowlist for %s → %s", username, allowlist.get(username))

    async def delete_user(self, username: str) -> None:
        api = await self._client()
        allowlist = await self._read_allowlist(api)
        allowlist.pop(username, None)
        await self._write_allowlist(api, allowlist)
        log.info("Spark: removed %s from allowlist", username)
