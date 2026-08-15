"""
spark_iceberg_utils.py
======================
Global Spark Iceberg table-creation wrapper.

Every time an Iceberg table is created through this module two audit columns are
automatically injected into the schema regardless of what the caller provides:

    snap_timestamp  TIMESTAMP   – wall-clock time the snapshot was written
    snap_id         BIGINT      – Iceberg snapshot id (written at commit time)

Usage
-----
from spark_iceberg_utils import IcebergTableBuilder

builder = IcebergTableBuilder(spark)
builder.create_table(
    catalog="polaris",
    namespace="tpcds_sf10tcl",
    table="customer",
    schema=customer_schema,           # StructType – WITHOUT snap columns
    partition_spec=[                  # list[PartitionField] or None
        IcebergTableBuilder.hours("ts_col"),
        IcebergTableBuilder.bucket("ss_sold_date_sk", 4),
    ],
    location="s3://xdatatoiceberg1/iceberg/customer",
    target_file_size_bytes=2_621_440, # 2.5 MB ≈ 2:56 MiB target
    extra_properties={}
)
"""

from __future__ import annotations

import logging
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    LongType,
    StructField,
    StructType,
    TimestampType,
)

logger = logging.getLogger(__name__)

# ── Snap-column constants ──────────────────────────────────────────────────────
_SNAP_COLS: list[StructField] = [
    StructField("snap_timestamp", TimestampType(), nullable=True),
    StructField("snap_id",        LongType(),      nullable=True),
]
_SNAP_COL_NAMES = {f.name for f in _SNAP_COLS}

# ── Target file size (2.5 MB → ≈ 2:56 MiB) ───────────────────────────────────
_DEFAULT_TARGET_FILE_SIZE_BYTES: int = 2_621_440  # 2.5 × 1 048 576


def _inject_snap_cols(schema: StructType) -> StructType:
    """Return a new StructType with snap audit columns appended."""
    existing = {f.name for f in schema.fields}
    extra = [f for f in _SNAP_COLS if f.name not in existing]
    if not extra:
        return schema
    return StructType(schema.fields + extra)


# ─────────────────────────────────────────────────────────────────────────────
class IcebergTableBuilder:
    """
    Wrapper around ``spark.sql`` that creates Iceberg tables with mandatory
    snap audit columns and consistent default properties.
    """

    def __init__(self, spark: SparkSession) -> None:
        self._spark = spark

    # ── Partition helpers ──────────────────────────────────────────────────────
    @staticmethod
    def hours(col: str) -> dict:
        return {"type": "hours", "col": col}

    @staticmethod
    def days(col: str) -> dict:
        return {"type": "days", "col": col}

    @staticmethod
    def months(col: str) -> dict:
        return {"type": "months", "col": col}

    @staticmethod
    def years(col: str) -> dict:
        return {"type": "years", "col": col}

    @staticmethod
    def bucket(col: str, n: int) -> dict:
        return {"type": "bucket", "col": col, "n": n}

    @staticmethod
    def truncate(col: str, w: int) -> dict:
        return {"type": "truncate", "col": col, "w": w}

    @staticmethod
    def identity(col: str) -> dict:
        return {"type": "identity", "col": col}

    # ── DDL builders ──────────────────────────────────────────────────────────
    @staticmethod
    def _schema_ddl(schema: StructType) -> str:
        """Convert StructType → SQL column definitions."""
        parts = []
        for field in schema.fields:
            nullable = "" if field.nullable else " NOT NULL"
            parts.append(f"  `{field.name}` {field.dataType.simpleString()}{nullable}")
        return ",\n".join(parts)

    @staticmethod
    def _partition_ddl(spec: list[dict]) -> str:
        """Convert partition spec list → PARTITIONED BY (...) clause."""
        exprs = []
        for p in spec:
            t = p["type"]
            col = p["col"]
            if t == "hours":
                exprs.append(f"hours(`{col}`)")
            elif t == "days":
                exprs.append(f"days(`{col}`)")
            elif t == "months":
                exprs.append(f"months(`{col}`)")
            elif t == "years":
                exprs.append(f"years(`{col}`)")
            elif t == "bucket":
                exprs.append(f"bucket({p['n']}, `{col}`)")
            elif t == "truncate":
                exprs.append(f"truncate({p['w']}, `{col}`)")
            elif t == "identity":
                exprs.append(f"`{col}`")
            else:
                raise ValueError(f"Unknown partition type: {t!r}")
        return f"PARTITIONED BY ({', '.join(exprs)})" if exprs else ""

    # ── Public API ─────────────────────────────────────────────────────────────
    def create_table(
        self,
        catalog: str,
        namespace: str,
        table: str,
        schema: StructType,
        *,
        partition_spec: list[dict] | None = None,
        location: str | None = None,
        target_file_size_bytes: int = _DEFAULT_TARGET_FILE_SIZE_BYTES,
        extra_properties: dict[str, Any] | None = None,
        if_not_exists: bool = True,
    ) -> str:
        """
        Create an Iceberg table, always injecting snap audit columns.

        Returns the fully-qualified table name created.
        """
        augmented_schema = _inject_snap_cols(schema)
        fqn = f"`{catalog}`.`{namespace}`.`{table}`"
        exists_clause = "IF NOT EXISTS " if if_not_exists else ""

        col_ddl = self._schema_ddl(augmented_schema)
        partition_clause = self._partition_ddl(partition_spec or [])

        # Build TBLPROPERTIES
        props: dict[str, Any] = {
            "format-version": "2",
            "write.format.default": "parquet",
            "write.parquet.compression-codec": "snappy",
            "write.target-file-size-bytes": str(target_file_size_bytes),
            # snap audit columns metadata
            "platform.snap-columns": "snap_timestamp,snap_id",
        }
        if extra_properties:
            props.update(extra_properties)
        tblprops_ddl = ", ".join(
            f"'{k}' = '{v}'" for k, v in props.items()
        )

        location_clause = f"LOCATION '{location}'" if location else ""

        ddl = (
            f"CREATE TABLE {exists_clause}{fqn} (\n"
            f"{col_ddl}\n"
            f")\n"
            f"USING iceberg\n"
            f"{partition_clause}\n"
            f"{location_clause}\n"
            f"TBLPROPERTIES ({tblprops_ddl})"
        )

        logger.info("Creating Iceberg table %s", fqn)
        logger.debug("DDL:\n%s", ddl)
        self._spark.sql(ddl)
        logger.info("Table %s created (or already existed).", fqn)
        return fqn

    def drop_table(self, catalog: str, namespace: str, table: str) -> None:
        fqn = f"`{catalog}`.`{namespace}`.`{table}`"
        self._spark.sql(f"DROP TABLE IF EXISTS {fqn}")
        logger.info("Dropped table %s", fqn)

    def table_exists(self, catalog: str, namespace: str, table: str) -> bool:
        fqn = f"`{catalog}`.`{namespace}`.`{table}`"
        try:
            self._spark.sql(f"DESCRIBE TABLE {fqn}")
            return True
        except Exception:
            return False
