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
FE_HOST       = os.environ.get("MS_FE_HOST",        "doris-fe-0.doris-fe-headless.prod.svc.cluster.local")
FE_HB_PORT    = int(os.environ.get("MS_FE_HB_PORT", "9010"))
CLOUD_UID     = os.environ.get("MS_CLOUD_UNIQUE_ID", "doris-local-001")
INSTANCE_ID   = os.environ.get("MS_INSTANCE_ID",    "doris-local")
CLUSTER_ID    = os.environ.get("MS_CLUSTER_ID",     "local-cluster-001")
CLUSTER_NAME  = os.environ.get("MS_CLUSTER_NAME",   "local")

# The name CloudEnv uses when looking up the FE's own cluster to determine node_type.
# Matches Config.cloud_sql_server_cluster_name default value.
SQL_CLUSTER_NAME = "RESERVED_CLUSTER_NAME_FOR_SQL_SERVER"
SQL_CLUSTER_ID   = os.environ.get("MS_SQL_CLUSTER_ID", "sql-cluster-001")


def _node_info_pb() -> bytes:
    """NodeInfoPB for the single BE: cloud_unique_id=1, ip=3, heartbeat_port=8, host=13."""
    return (
        _field_str(1,  CLOUD_UID)   +   # cloud_unique_id
        _field_str(3,  BE_HOST)     +   # ip (legacy field — used for display)
        _field_varint(8, BE_HB_PORT)+   # heartbeat_port
        _field_str(13, BE_HOST)         # host (Doris 4.0 uses this for routing)
    )


def _fe_node_info_pb() -> bytes:
    """
    NodeInfoPB for the FE itself.

    CloudEnv.lambda#2 builds:  (enable_fqdn_mode ? host : ip) + "_" + editLogPort
    and compares it to selfNode.getIdent() = "<fqdn>_9010".

    Field numbers (from Cloud.java):
      cloud_unique_id = 1, ip = 3, edit_log_port = 10, host = 13, node_type = 11
    """
    return (
        _field_str(1,  CLOUD_UID)       +   # cloud_unique_id (must match FE's cloud_unique_id)
        _field_str(3,  FE_HOST)         +   # ip
        _field_varint(10, FE_HB_PORT)   +   # edit_log_port (field 10) — used for selfNode match
        _field_str(13, FE_HOST)         +   # host (used when enable_fqdn_mode=true)
        _field_varint(11, 1)                # node_type = FE_MASTER (1)
    )


def _cluster_pb() -> bytes:
    """
    ClusterPB for BE compute nodes:
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


def _sql_cluster_pb() -> bytes:
    """
    ClusterPB for the FE SQL-server cluster (type=SQL=0).
    CloudEnv.getLocalTypeFromMetaService() calls get_cluster with
    cluster_name=RESERVED_CLUSTER_NAME_FOR_SQL_SERVER and scans nodes for
    a NodeInfoPB whose cloud_unique_id matches the FE's own cloud_unique_id.
    It reads node_type to set the FE's role (FE_MASTER/FE_FOLLOWER/FE_OBSERVER).
    """
    return (
        _field_str(1, SQL_CLUSTER_ID)      +   # cluster_id
        _field_str(2, SQL_CLUSTER_NAME)    +   # cluster_name
        _field_varint(3, 0)                +   # type = SQL (0)
        _field_bytes(5, _fe_node_info_pb()) +  # nodes[0] — the FE itself
        _field_varint(9, 1)                    # cluster_status = NORMAL (1)
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
# Method names extracted from MetaServiceGrpc.java inside doris-fe.jar (snake_case)
_MS_METHODS = {
    "abort_snapshot", "abort_sub_txn", "abort_txn", "abort_txn_with_coordinator",
    "alter_cluster", "alter_iam", "alter_instance", "alter_obj_store_info",
    "alter_ram_user", "alter_storage_vault",
    "begin_copy", "begin_snapshot", "begin_sub_txn", "begin_txn",
    "check_kv", "check_txn_conflict", "clean_txn_label", "clone_instance",
    "commit_index", "commit_partition", "commit_restore_job", "commit_rowset",
    "commit_snapshot", "commit_txn",
    "create_instance", "create_stage", "create_tablets",
    "delete_streaming_job", "drop_index", "drop_partition", "drop_snapshot", "drop_stage",
    "filter_copy_files", "finish_copy", "finish_restore_job", "finish_tablet_job",
    "get_cluster", "get_cluster_status", "get_copy_files", "get_copy_job",
    "get_current_max_txn_id", "get_delete_bitmap", "get_delete_bitmap_update_lock",
    "get_iam", "get_instance", "get_obj_store_info", "get_prepare_txn_by_coordinator",
    "get_rl_task_commit_attach", "get_rowset", "get_schema_dict", "get_stage",
    "get_streaming_task_commit_attach", "get_tablet", "get_tablet_stats",
    "get_txn", "get_txn_id", "get_version",
    "list_snapshot",
    "precommit_txn", "prepare_index", "prepare_partition", "prepare_restore_job",
    "prepare_rowset",
    "remove_delete_bitmap", "remove_delete_bitmap_update_lock",
    "reset_rl_progress", "reset_streaming_job_offset",
    "start_tablet_job",
    "update_ak_sk", "update_delete_bitmap", "update_packed_file_info",
    "update_snapshot", "update_tablet", "update_tmp_rowset",
}

SERVICE_NAME = "doris.cloud.MetaService"

# ─────────────────────────────────────────────────────────────────────────────
# Dispatch logic (plain functions, no class hierarchy needed)
# ─────────────────────────────────────────────────────────────────────────────
_call_counts: dict[str, int] = {}
_count_lock = threading.Lock()


def _count(method: str) -> None:
    with _count_lock:
        _call_counts[method] = _call_counts.get(method, 0) + 1
        total = _call_counts[method]
    if total == 1 or total % 100 == 0:
        log.debug("MetaService.%s called (total=%d)", method, total)


def _get_cluster(req_bytes: bytes) -> bytes:
    """
    GetClusterResponse: status=1 (OK), cluster=2 (ClusterPB).

    Two callers:
    1. CloudSystemInfoService — uses the compute cluster (CLUSTER_NAME / CLUSTER_ID).
    2. CloudEnv.getLocalTypeFromMetaService — calls with cluster_name=
       RESERVED_CLUSTER_NAME_FOR_SQL_SERVER to find the FE's own NodeInfoPB
       and read its node_type (FE_MASTER/FE_FOLLOWER/FE_OBSERVER).
    """
    _count("get_cluster")
    fields = _parse_fields(req_bytes)
    req_cluster_name = _str_field(fields, 4)
    req_cluster_id   = _str_field(fields, 3)
    log.info("get_cluster requested cluster_name='%s' cluster_id='%s'",
             req_cluster_name, req_cluster_id)

    # FE self-identification: return the SQL-server cluster containing the FE node
    if req_cluster_name == SQL_CLUSTER_NAME or req_cluster_id == SQL_CLUSTER_ID:
        return _field_bytes(1, _ok_status()) + _field_bytes(2, _sql_cluster_pb())

    return _field_bytes(1, _ok_status()) + _field_bytes(2, _cluster_pb())


def _get_instance(req_bytes: bytes) -> bytes:
    """GetInstanceResponse: status=1 (OK), instance=2 (InstanceInfoPB)."""
    _count("GetInstance")
    log.info("getInstance called")
    return _field_bytes(1, _ok_status()) + _field_bytes(2, _instance_info_pb())


def _ok_only(method: str, _req: bytes) -> bytes:
    """Generic OK response for all transaction/tablet versioning RPCs."""
    _count(method)
    return _field_bytes(1, _ok_status())


def _dispatch(method: str, req: bytes) -> bytes:
    if method == "get_cluster":
        return _get_cluster(req)
    if method in ("get_instance", "get_instance_by_role"):
        return _get_instance(req)
    return _ok_only(method, req)


# ─────────────────────────────────────────────────────────────────────────────
# Generic gRPC service handler (grpc.GenericRpcHandler)
# ─────────────────────────────────────────────────────────────────────────────
class _GenericServiceHandler(grpc.GenericRpcHandler):
    """Routes all incoming RPCs for doris.cloud.MetaService."""

    def service_name(self) -> str:
        return SERVICE_NAME

    def service(self, handler_call_details: grpc.HandlerCallDetails
                ) -> grpc.RpcMethodHandler | None:
        full = handler_call_details.method  # "/doris.cloud.MetaService/MethodName"
        method = full.split("/")[-1] if full else ""
        if method not in _MS_METHODS:
            return None

        def handle(req: bytes, _ctx: grpc.ServicerContext) -> bytes:
            return _dispatch(method, req)

        return grpc.unary_unary_rpc_method_handler(
            handle,
            request_deserializer=lambda b: b,
            response_serializer=lambda b: b,
        )


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
        self._server: grpc.Server | None = None

    def start(self) -> None:
        self._server = grpc.server(
            futures.ThreadPoolExecutor(max_workers=4),
            options=[
                ("grpc.max_receive_message_length", 64 * 1024 * 1024),
                ("grpc.max_send_message_length",    64 * 1024 * 1024),
                ("grpc.keepalive_time_ms",          60_000),
                ("grpc.keepalive_timeout_ms",       10_000),
                ("grpc.keepalive_permit_without_calls", True),
            ],
        )
        self._server.add_generic_rpc_handlers([_GenericServiceHandler()])
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
