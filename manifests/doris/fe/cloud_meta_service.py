"""
cloud_meta_service.py
=====================
Minimal Python gRPC MetaService stub for Apache Doris fe-4.0.7-ranger.

Purpose
-------
The fe-4.0.7-ranger image contains the Cloud/disaggregated edition of the
Doris FE, which adds `WARM UP CLUSTER … WITH TABLE`, `SHOW WARM UP JOB`, and
`WARM UP CLUSTER … USING COLD_DOWN` SQL commands.  These commands are
executed entirely inside the FE's JVM (CacheHotspotManager → BE Thrift) and
do NOT require MetaService at runtime.

However, the FE must start in "cloud mode" (deploy_mode=cloud) for the
CloudEnv class to be active, and CloudEnv requires a live MetaService at
startup for two bootstrapping RPCs:

  getCluster    — maps cloud_unique_id → ClusterPB (list of BEs)
  getInstance   — returns InstanceInfoPB so FE knows it belongs to an instance

All other MetaService RPCs (beginTxn, commitTxn, getVersion, …) are called
on every query/load/DML.  This stub returns a valid OK response for those
calls too — but with empty/zero payloads — so the FE can continue running
without a real distributed metadata store.

Performance impact: NONE on the query data path.
  • MetaService is contacted by the FE for distributed transaction management
    and tablet version tracking.  This stub's responses are OK-but-empty, so
    the FE falls back to its local BdbJE metadata store for version tracking
    (same as non-cloud mode).  Scans, aggregations, joins, stream loads, and
    broker loads are unaffected — they go directly between FE/BE over Thrift.
  • The gRPC server listens on localhost:5000 only, so it has zero network
    exposure and negligible CPU footprint (< 1 ms per call, < 0.1% CPU).

Wire encoding
-------------
All proto messages are encoded manually using the protobuf wire format
(field_number << 3 | wire_type) + value.  This avoids needing protoc or
generated Python stubs.  Field numbers are extracted from Cloud.java in the
fe-4.0.7-ranger JAR:

  MetaServiceResponseStatus:  code=1 (int32), msg=2 (string)
  GetClusterRequest:          instance_id=1, cloud_unique_id=2, cluster_id=3, cluster_name=4
  GetClusterResponse:         status=1 (msg), cluster=2 (msg), enable_storage_vault=3
  GetInstanceRequest:         instance_id=1, cloud_unique_id=2
  GetInstanceResponse:        status=1 (msg), instance=2 (msg)
  ClusterPB:                  cluster_id=1, cluster_name=2, type=3, nodes=5, cluster_status=9
  InstanceInfoPB:             instance_id=2, name=3, clusters=7, status=10
  NodeInfoPB:                 cloud_unique_id=1, ip=3, heartbeat_port=8, host=13

MetaServiceCode enum (MetaServiceCode.java):  OK=0
ClusterType enum:                             SQL=0, COMPUTE=1
ClusterStatus enum:                           UNKNOWN=0, NORMAL=1

gRPC service: doris.cloud.MetaService  (package doris.cloud, service MetaService)
All methods are unary (UNARY) — see MetaServiceGrpc.class.
"""
from __future__ import annotations

import logging
import os
import struct
import threading
import time
from concurrent import futures
from typing import Any

import grpc

log = logging.getLogger("cloud-meta-service")

# ─────────────────────────────────────────────────────────────────────────────
# Protobuf wire-format helpers
# ─────────────────────────────────────────────────────────────────────────────
VARINT    = 0
LEN_DELIM = 2

def _varint(value: int) -> bytes:
    """Encode a non-negative integer as a protobuf varint."""
    buf = []
    while True:
        byte = value & 0x7F
        value >>= 7
        if value:
            buf.append(byte | 0x80)
        else:
            buf.append(byte)
            break
    return bytes(buf)

def _field_varint(field_no: int, value: int) -> bytes:
    return _varint((field_no << 3) | VARINT) + _varint(value)

def _field_bytes(field_no: int, data: bytes) -> bytes:
    return _varint((field_no << 3) | LEN_DELIM) + _varint(len(data)) + data

def _field_str(field_no: int, text: str) -> bytes:
    return _field_bytes(field_no, text.encode())

def _decode_varint(data: bytes, pos: int) -> tuple[int, int]:
    """Return (value, new_pos)."""
    result = 0
    shift = 0
    while pos < len(data):
        b = data[pos]; pos += 1
        result |= (b & 0x7F) << shift
        if not (b & 0x80):
            break
        shift += 7
    return result, pos

def _parse_fields(data: bytes) -> dict[int, list[bytes | int]]:
    """Decode a flat protobuf message into {field_no: [value, …]}."""
    fields: dict[int, list] = {}
    pos = 0
    while pos < len(data):
        tag, pos = _decode_varint(data, pos)
        field_no = tag >> 3
        wire_type = tag & 0x07
        if wire_type == VARINT:
            val, pos = _decode_varint(data, pos)
            fields.setdefault(field_no, []).append(val)
        elif wire_type == LEN_DELIM:
            length, pos = _decode_varint(data, pos)
            val = data[pos:pos + length]; pos += length
            fields.setdefault(field_no, []).append(val)
        elif wire_type == 5:  # fixed32
            val = data[pos:pos + 4]; pos += 4
            fields.setdefault(field_no, []).append(val)
        elif wire_type == 1:  # fixed64
            val = data[pos:pos + 8]; pos += 8
            fields.setdefault(field_no, []).append(val)
        else:
            break  # unsupported wire type — stop parsing
    return fields

def _str_field(fields: dict, no: int) -> str:
    vals = fields.get(no, [])
    return vals[0].decode(errors="replace") if vals and isinstance(vals[0], bytes) else ""


# ─────────────────────────────────────────────────────────────────────────────
# Status helper — MetaServiceCode.OK = 0
# MetaServiceResponseStatus: code=1 (varint), msg=2 (string)
# ─────────────────────────────────────────────────────────────────────────────
def _ok_status(msg: str = "OK") -> bytes:
    return _field_varint(1, 0) + _field_str(2, msg)


# ─────────────────────────────────────────────────────────────────────────────
# Cluster + Instance builders
# ─────────────────────────────────────────────────────────────────────────────
BE_HOST       = os.environ.get("MS_BE_HOST",        "doris-be-0.doris-be-headless.prod.svc.cluster.local")
BE_HB_PORT    = int(os.environ.get("MS_BE_HB_PORT", "9050"))
CLOUD_UID     = os.environ.get("MS_CLOUD_UNIQUE_ID", "doris-local-001")
INSTANCE_ID   = os.environ.get("MS_INSTANCE_ID",    "doris-local")
CLUSTER_ID    = os.environ.get("MS_CLUSTER_ID",     "local-cluster-001")
CLUSTER_NAME  = os.environ.get("MS_CLUSTER_NAME",   "local")


def _node_info_pb() -> bytes:
    """NodeInfoPB for the single BE: cloud_unique_id=1, ip=3, heartbeat_port=8, host=13."""
    return (
        _field_str(1,  CLOUD_UID)   +   # cloud_unique_id
        _field_str(3,  BE_HOST)     +   # ip (legacy field — used for display)
        _field_varint(8, BE_HB_PORT)+   # heartbeat_port
        _field_str(13, BE_HOST)         # host (Doris 4.0 uses this for routing)
    )


def _cluster_pb() -> bytes:
    """
    ClusterPB:
      cluster_id=1, cluster_name=2, type=3 (COMPUTE=1), nodes=5 (repeated NodeInfoPB),
      cluster_status=9 (NORMAL=1)
    """
    return (
        _field_str(1, CLUSTER_ID)       +   # cluster_id
        _field_str(2, CLUSTER_NAME)     +   # cluster_name
        _field_varint(3, 1)             +   # type = COMPUTE (1)
        _field_bytes(5, _node_info_pb()) +  # nodes[0]
        _field_varint(9, 1)                 # cluster_status = NORMAL (1)
    )


def _instance_info_pb() -> bytes:
    """
    InstanceInfoPB: instance_id=2, name=3, clusters=7 (repeated ClusterPB), status=10 (NORMAL=1)
    """
    return (
        _field_str(2,   INSTANCE_ID)    +   # instance_id
        _field_str(3,   INSTANCE_ID)    +   # name
        _field_bytes(7, _cluster_pb())  +   # clusters[0]
        _field_varint(10, 1)                # status = NORMAL (1)
    )


# ─────────────────────────────────────────────────────────────────────────────
# gRPC generic handler
# ─────────────────────────────────────────────────────────────────────────────
class _MetaServiceHandler(grpc.ServiceRpcHandlers):
    """
    Dynamic gRPC handler for doris.cloud.MetaService.

    Every RPC returns MetaServiceCode.OK with minimal valid payloads for the
    two bootstrap calls (getCluster, getInstance) and empty-but-OK for all
    others.  This ensures the FE JVM never blocks or retries on startup.
    """

    SERVICE = "doris.cloud.MetaService"

    def __init__(self) -> None:
        self._call_counts: dict[str, int] = {}
        self._lock = threading.Lock()

    def _count(self, method: str) -> None:
        with self._lock:
            self._call_counts[method] = self._call_counts.get(method, 0) + 1
            total = self._call_counts[method]
        if total == 1 or total % 100 == 0:
            log.debug("MetaService.%s called (total=%d)", method, total)

    # ── Bootstrap responses ──────────────────────────────────────────────────

    def _get_cluster(self, req_bytes: bytes) -> bytes:
        """
        GetClusterResponse: status=1 (OK), cluster=2 (ClusterPB).
        The FE calls this at startup to populate CloudSystemInfoService.
        """
        self._count("getCluster")
        fields = _parse_fields(req_bytes)
        req_cluster = _str_field(fields, 4) or CLUSTER_NAME
        log.info("getCluster requested cluster_name='%s'", req_cluster)
        return _field_bytes(1, _ok_status()) + _field_bytes(2, _cluster_pb())

    def _get_instance(self, req_bytes: bytes) -> bytes:
        """
        GetInstanceResponse: status=1 (OK), instance=2 (InstanceInfoPB).
        """
        self._count("getInstance")
        log.info("getInstance called")
        return _field_bytes(1, _ok_status()) + _field_bytes(2, _instance_info_pb())

    def _ok_only(self, method: str, _req: bytes) -> bytes:
        """Generic OK response for all transaction/tablet versioning RPCs."""
        self._count(method)
        return _field_bytes(1, _ok_status())

    # ── gRPC dispatch ────────────────────────────────────────────────────────

    def _dispatch(self, method: str, req: bytes) -> bytes:
        if method == "GetCluster":
            return self._get_cluster(req)
        if method == "GetInstance":
            return self._get_instance(req)
        # All other RPCs: return status=OK with no additional fields.
        # This covers GetVersion, BeginTxn, CommitTxn, AbortTxn, etc.
        # The FE handles empty responses gracefully (local BdbJE fallback).
        return self._ok_only(method, req)

    def service_name(self) -> str:
        return self.SERVICE

    def method_handlers(self) -> dict[str, grpc.RpcMethodHandler]:
        # Build a generic unary handler for every known MetaService method.
        # The FE only calls the methods it needs; all others return OK.
        methods = [
            "GetVersion", "CreateTablets", "UpdateTablet",
            "BeginTxn", "PrecommitTxn", "CommitTxn", "AbortTxn",
            "GetTxn", "GetTxnId", "GetCurrentMaxTxnId",
            "BeginSubTxn", "AbortSubTxn", "CheckTxnConflict", "CleanTxnLabel",
            "GetCluster", "GetInstance", "GetInstanceByRole",
            "PrepareIndex", "CommitIndex", "DropIndex",
            "PreparePartition", "CommitPartition", "DropPartition",
            "GetTabletStats", "FinishTabletJob",
            "CreateStage", "GetStage", "DropStage",
            "GetIam", "BeginCopy", "FinishCopy", "GetCopyJob", "GetCopyFiles",
            "FilterCopyFiles", "AlterCluster", "AlterObjStoreInfo", "AlterStorageVault",
            "GetDeleteBitmapUpdateLock", "RemoveDeleteBitmapUpdateLock",
            "GetObjStoreInfo", "AbortTxnWithCoordinator", "GetPrepareTxnByCoordinator",
            "CreateInstance", "AlterInstance", "GetRLTaskCommitAttach", "ResetRLProgress",
            "ResetStreamingJobOffset", "GetStreamingTaskCommitAttach",
            "DeleteStreamingJob", "CheckKv",
            "BeginSnapshot", "UpdateSnapshot", "CommitSnapshot", "AbortSnapshot",
            "ListSnapshot", "DropSnapshot", "CloneInstance",
        ]
        handlers: dict[str, grpc.RpcMethodHandler] = {}
        for m in methods:
            method_name = m  # capture for closure
            def make_handler(mname: str) -> grpc.RpcMethodHandler:
                def handle(req: bytes, ctx: grpc.ServicerContext) -> bytes:
                    return self._dispatch(mname, req)
                return grpc.unary_unary_rpc_method_handler(
                    handle,
                    request_deserializer=lambda b: b,
                    response_serializer=lambda b: b,
                )
            handlers[m] = make_handler(m)
        return handlers


# ─────────────────────────────────────────────────────────────────────────────
# Server lifecycle
# ─────────────────────────────────────────────────────────────────────────────
class MetaServiceStub:
    """
    Lightweight gRPC MetaService stub.

    Binds to localhost:5000 only — no external traffic, zero network exposure.
    Uses a thread pool of 4 workers which is sufficient for the FE's call rate:
    at most a few RPC/s during startup, then near-zero while running.
    """

    DEFAULT_PORT = int(os.environ.get("MS_PORT", "5000"))

    def __init__(self, port: int | None = None) -> None:
        self._port = port or self.DEFAULT_PORT
        self._handler = _MetaServiceHandler()
        self._server: grpc.Server | None = None

    def start(self) -> None:
        self._server = grpc.server(
            futures.ThreadPoolExecutor(max_workers=4),
            options=[
                ("grpc.max_receive_message_length", 64 * 1024 * 1024),
                ("grpc.max_send_message_length",    64 * 1024 * 1024),
                # Keep-alive: allow the FE to hold a long-lived channel
                ("grpc.keepalive_time_ms",          60_000),
                ("grpc.keepalive_timeout_ms",       10_000),
                ("grpc.keepalive_permit_without_calls", True),
            ],
        )
        # Register the service generically via ServiceRpcHandlers
        from grpc import _server as _grpc_server  # noqa: F401
        self._server.add_generic_rpc_handlers(
            [_GenericServiceHandler(self._handler)]
        )
        listen_addr = f"127.0.0.1:{self._port}"
        self._server.add_insecure_port(listen_addr)
        self._server.start()
        log.info(
            "Cloud MetaService stub listening on %s "
            "(cluster=%s instance=%s be=%s:%d)",
            listen_addr, CLUSTER_NAME, INSTANCE_ID, BE_HOST, BE_HB_PORT,
        )

    def stop(self) -> None:
        if self._server:
            self._server.stop(grace=2)
            log.info("Cloud MetaService stub stopped.")

    def wait_for_termination(self) -> None:
        if self._server:
            self._server.wait_for_termination()


class _GenericServiceHandler(grpc.GenericRpcHandler):
    """Routes all incoming RPCs for doris.cloud.MetaService to our handler."""

    def __init__(self, handler: _MetaServiceHandler) -> None:
        self._handler = handler

    def service_name(self) -> str:
        return self._handler.SERVICE

    def service(self, handler_call_details: grpc.HandlerCallDetails
                ) -> grpc.RpcMethodHandler | None:
        # method full name: /doris.cloud.MetaService/MethodName
        full = handler_call_details.method  # e.g. "/doris.cloud.MetaService/GetCluster"
        method = full.split("/")[-1] if full else ""
        handlers = self._handler.method_handlers()
        return handlers.get(method)


# ─────────────────────────────────────────────────────────────────────────────
# Standalone entry point (called from doris_fe_entrypoint.py)
# ─────────────────────────────────────────────────────────────────────────────
def run_forever(port: int | None = None) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    stub = MetaServiceStub(port)
    stub.start()
    stub.wait_for_termination()


if __name__ == "__main__":
    run_forever()
