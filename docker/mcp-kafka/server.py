#!/usr/bin/env python3
"""
Kafka MCP Server
Exposes Apache Kafka management operations as MCP tools.
Supports bitnami/kafka (SASL/PLAIN) and Strimzi KRaft (SCRAM-SHA-512).
Transport: SSE on port 3104.
"""
import os
import json
from typing import Any

from confluent_kafka import Producer, Consumer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic, ConfigResource, ConfigSource

try:
    from mcp.server.fastmcp import FastMCP
except ImportError:
    from mcp import FastMCP  # type: ignore

# ── Configuration ─────────────────────────────────────────────────────────────
# Default: Strimzi bootstrap (SCRAM-SHA-512)
KAFKA_BOOTSTRAP   = os.getenv("KAFKA_BOOTSTRAP",   "strimzi-kafka-kafka-bootstrap.streaming.svc.cluster.local:9092")
KAFKA_SECURITY    = os.getenv("KAFKA_SECURITY",    "SASL_PLAINTEXT")
KAFKA_SASL_MECH   = os.getenv("KAFKA_SASL_MECH",   "SCRAM-SHA-512")
KAFKA_USER        = os.getenv("KAFKA_USER",        "kafka-app-user")
KAFKA_PASS        = os.getenv("KAFKA_PASS",        "")

mcp = FastMCP("kafka-mcp", port=3104)


def _admin_conf() -> dict:
    conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "security.protocol": KAFKA_SECURITY,
        "sasl.mechanism": KAFKA_SASL_MECH,
        "sasl.username": KAFKA_USER,
        "sasl.password": KAFKA_PASS,
        "socket.timeout.ms": 10000,
    }
    return conf


def _admin() -> AdminClient:
    return AdminClient(_admin_conf())


@mcp.tool()
def kafka_list_topics() -> dict:
    """List all Kafka topics in the cluster with partition and replica info."""
    try:
        admin = _admin()
        metadata = admin.list_topics(timeout=10)
        topics = []
        for name, topic in metadata.topics.items():
            if name.startswith("__"):
                continue  # skip internal topics
            topics.append({
                "name": name,
                "partitions": len(topic.partitions),
                "error": str(topic.error) if topic.error else None,
            })
        return {"success": True, "topics": sorted(topics, key=lambda t: t["name"])}
    except Exception as e:
        return {"success": False, "error": str(e)}


@mcp.tool()
def kafka_create_topic(
    topic: str,
    partitions: int = 1,
    replication_factor: int = 1,
) -> dict:
    """
    Create a new Kafka topic.
    Args:
        topic: Topic name
        partitions: Number of partitions (default 1)
        replication_factor: Replication factor (default 1 for single-node lab)
    """
    try:
        admin = _admin()
        new_topic = NewTopic(topic, num_partitions=partitions, replication_factor=replication_factor)
        futures = admin.create_topics([new_topic])
        for t, f in futures.items():
            f.result()  # raises KafkaException on error
        return {"success": True, "created": topic}
    except Exception as e:
        return {"success": False, "error": str(e)}


@mcp.tool()
def kafka_delete_topic(topic: str) -> dict:
    """
    Delete a Kafka topic.
    Args:
        topic: Topic name to delete
    """
    try:
        admin = _admin()
        futures = admin.delete_topics([topic])
        for t, f in futures.items():
            f.result()
        return {"success": True, "deleted": topic}
    except Exception as e:
        return {"success": False, "error": str(e)}


@mcp.tool()
def kafka_produce(topic: str, message: str, key: str = "") -> dict:
    """
    Produce a single message to a Kafka topic.
    Args:
        topic: Target topic name
        message: Message value (string)
        key: Optional message key
    """
    try:
        conf = _admin_conf()
        conf.pop("socket.timeout.ms", None)
        producer = Producer(conf)
        delivered = []
        errors = []

        def on_delivery(err, msg):
            if err:
                errors.append(str(err))
            else:
                delivered.append({"partition": msg.partition(), "offset": msg.offset()})

        producer.produce(
            topic,
            value=message.encode("utf-8"),
            key=key.encode("utf-8") if key else None,
            callback=on_delivery,
        )
        producer.flush(timeout=10)
        return {"success": not errors, "delivered": delivered, "errors": errors}
    except Exception as e:
        return {"success": False, "error": str(e)}


@mcp.tool()
def kafka_consume(
    topic: str,
    group_id: str = "mcp-consumer",
    max_messages: int = 10,
    timeout_seconds: int = 5,
) -> dict:
    """
    Consume messages from the beginning of a Kafka topic (read-only peek).
    Args:
        topic: Topic name
        group_id: Consumer group ID
        max_messages: Maximum number of messages to return
        timeout_seconds: Poll timeout in seconds
    """
    try:
        conf = _admin_conf()
        conf.update({
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        })
        consumer = Consumer(conf)
        consumer.subscribe([topic])
        messages = []
        total = 0
        while total < max_messages:
            msg = consumer.poll(timeout=timeout_seconds)
            if msg is None:
                break
            if msg.error():
                break
            messages.append({
                "partition": msg.partition(),
                "offset": msg.offset(),
                "key": msg.key().decode("utf-8") if msg.key() else None,
                "value": msg.value().decode("utf-8") if msg.value() else None,
                "timestamp": msg.timestamp()[1],
            })
            total += 1
        consumer.close()
        return {"success": True, "messages": messages}
    except Exception as e:
        return {"success": False, "error": str(e)}


@mcp.tool()
def kafka_list_consumer_groups() -> dict:
    """List all Kafka consumer groups and their state."""
    try:
        admin = _admin()
        groups = admin.list_groups(timeout=10)
        return {
            "success": True,
            "groups": [{"id": g.id, "state": str(g.state)} for g in groups],
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


@mcp.tool()
def kafka_cluster_metadata() -> dict:
    """Get Kafka cluster metadata — brokers, controller, and cluster ID."""
    try:
        admin = _admin()
        meta = admin.list_topics(timeout=10)
        brokers = [{"id": b.id, "host": b.host, "port": b.port} for b in meta.brokers.values()]
        return {"success": True, "brokers": brokers, "controller_id": meta.controller_id}
    except Exception as e:
        return {"success": False, "error": str(e)}


if __name__ == "__main__":
    mcp.run(transport="sse")
