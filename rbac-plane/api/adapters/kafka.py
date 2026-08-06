"""
Kafka RBAC Adapter
==================
Uses two mechanisms:
  1. Strimzi KafkaUser CR  — creates SCRAM-SHA-512 credentials for the user.
     (Required for the user to authenticate at all via the SCRAM listener.)
  2. Strimzi AclRule list   — sets topic/cluster ACLs inside the KafkaUser CR.

resource_scope:
  topic  resources → {"topic": "orders-*"}   (supports wildcard prefix)
  cluster resources → {}

Permission mapping:
  PRODUCE      → acl type=allow, operation=Write,   resource=topic
  CONSUME      → acl type=allow, operation=Read,    resource=topic
                 + acl type=allow, operation=Describe, resource=topic
                 + acl type=allow, operation=Read,   resource=group (ConsumerGroup *)
  DESCRIBE     → acl type=allow, operation=Describe, resource=topic
  CREATE_TOPIC → acl type=allow, operation=Create,  resource=cluster
  DELETE_TOPIC → acl type=allow, operation=Delete,  resource=topic
  ADMIN        → acl type=allow, operation=All,      resource=cluster

The KafkaUser CR is applied via the Kubernetes API (in-cluster service account).
We use the `kubernetes_asyncio` client — no kubectl subprocess needed.
"""
from __future__ import annotations

import logging
from typing import Any

from ..config import get_settings

log = logging.getLogger(__name__)


def _build_acl_rules(perms: list[dict]) -> list[dict]:
    """Translate logical permissions into Strimzi AclRule dicts."""
    rules = []

    for p in perms:
        perm = p["permission"]
        scope = p.get("resource_scope") or {}
        topic = scope.get("topic", "*")

        if perm == "PRODUCE":
            rules.append(_topic_rule("Write", topic))
            rules.append(_topic_rule("Describe", topic))

        elif perm == "CONSUME":
            rules.append(_topic_rule("Read", topic))
            rules.append(_topic_rule("Describe", topic))
            # Allow joining any consumer group
            rules.append(_group_rule("Read", "*"))

        elif perm == "DESCRIBE":
            rules.append(_topic_rule("Describe", topic))

        elif perm == "CREATE_TOPIC":
            rules.append(_cluster_rule("Create"))
            rules.append(_topic_rule("Create", topic))

        elif perm == "DELETE_TOPIC":
            rules.append(_topic_rule("Delete", topic))

        elif perm == "ADMIN":
            rules.append(_cluster_rule("All"))

    # De-duplicate
    seen = set()
    unique = []
    for r in rules:
        key = str(sorted(r.items()))
        if key not in seen:
            seen.add(key)
            unique.append(r)
    return unique


def _topic_rule(operation: str, topic: str) -> dict:
    pattern_type = "prefix" if topic.endswith("*") and len(topic) > 1 else "literal"
    name = topic.rstrip("*") if pattern_type == "prefix" else topic
    return {
        "type": "allow",
        "resource": {
            "type": "topic",
            "name": name if name else "*",
            "patternType": pattern_type if name else "literal",
        },
        "operations": [operation],
    }


def _group_rule(operation: str, group: str) -> dict:
    return {
        "type": "allow",
        "resource": {"type": "group", "name": group, "patternType": "literal"},
        "operations": [operation],
    }


def _cluster_rule(operation: str) -> dict:
    return {
        "type": "allow",
        "resource": {"type": "cluster", "name": "kafka-cluster", "patternType": "literal"},
        "operations": [operation],
    }


def _kafka_user_manifest(username: str, cluster: str, namespace: str,
                         acl_rules: list[dict], acl_supported: bool = True) -> dict:
    body: dict = {
        "apiVersion": "kafka.strimzi.io/v1",
        "kind": "KafkaUser",
        "metadata": {
            "name": username,
            "namespace": namespace,
            "labels": {
                "strimzi.io/cluster": cluster,
                "rbac-managed": "true",
            },
        },
        "spec": {
            "authentication": {"type": "scram-sha-512"},
        },
    }
    # Only emit ACL rules when the Kafka cluster has authorization configured.
    # When acl_supported=False (allow.everyone.if.no.acl.found=true) writing
    # an authorization block causes Strimzi to reject the KafkaUser CR.
    if acl_rules and acl_supported:
        body["spec"]["authorization"] = {
            "type": "simple",
            "acls": acl_rules,
        }
    return body


class KafkaAdapter:
    """Manage KafkaUser CRs via the Kubernetes API."""

    async def _client(self):
        try:
            from kubernetes_asyncio import client, config
            try:
                config.load_incluster_config()
            except Exception:
                await config.load_kube_config()
            return client.CustomObjectsApi()
        except ImportError:
            raise RuntimeError(
                "kubernetes_asyncio not installed. "
                "Add it to requirements.txt: kubernetes-asyncio>=26.1.0"
            )

    async def sync_user(self, username: str, perms: list[dict]) -> None:
        s = get_settings()
        acl_rules = _build_acl_rules(perms)
        manifest = _kafka_user_manifest(
            username, s.kafka_cluster_name, s.kafka_namespace, acl_rules,
            acl_supported=s.kafka_acl_supported,
        )

        api = await self._client()
        try:
            # Try to get existing resource (need resourceVersion for replace)
            existing = await api.get_namespaced_custom_object(
                group="kafka.strimzi.io",
                version="v1",
                namespace=s.kafka_namespace,
                plural="kafkausers",
                name=username,
            )
            # Carry over resourceVersion so the replace (PUT) is accepted
            manifest.setdefault("metadata", {})["resourceVersion"] = (
                existing.get("metadata", {}).get("resourceVersion", "")
            )
            # Exists → replace (PUT) — merge-patch on CRDs requires extra headers;
            # replace is simpler and always correct for a full desired-state push.
            await api.replace_namespaced_custom_object(
                group="kafka.strimzi.io",
                version="v1",
                namespace=s.kafka_namespace,
                plural="kafkausers",
                name=username,
                body=manifest,
            )
            log.info("Kafka: replaced KafkaUser %s (%d acl rules)", username, len(acl_rules))
        except Exception as exc:
            # 404 → create
            if getattr(exc, "status", None) == 404 or "Not Found" in str(exc):
                await api.create_namespaced_custom_object(
                    group="kafka.strimzi.io",
                    version="v1",
                    namespace=s.kafka_namespace,
                    plural="kafkausers",
                    body=manifest,
                )
                log.info("Kafka: created KafkaUser %s (%d acl rules)", username, len(acl_rules))
            else:
                raise

    async def delete_user(self, username: str) -> None:
        s = get_settings()
        api = await self._client()
        try:
            await api.delete_namespaced_custom_object(
                group="kafka.strimzi.io",
                version="v1",
                namespace=s.kafka_namespace,
                plural="kafkausers",
                name=username,
            )
            log.info("Kafka: deleted KafkaUser %s", username)
        except Exception as exc:
            if getattr(exc, "status", None) == 404:
                pass  # already gone
            else:
                raise
