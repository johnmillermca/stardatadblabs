"""
star_transform.py
=================
Kafka-side transformation library for the CDC → Iceberg pipeline.

Provides Flink-like composable transformation functions that operate on
Spark Structured Streaming DataFrames (micro-batch foreachBatch context).
Call these functions inside the foreachBatch handler in
05_kafka_to_iceberg_streaming.py — or from any Spark Structured Streaming
job — before the data is written to Iceberg.

Design
------
Every function takes a Spark DataFrame and returns a transformed DataFrame.
Functions are pure (no side-effects on Iceberg), composable, and chainable:

    from star_transform import StarTransform as ST

    result = (
        ST.filter_op(df, ops=["c", "u"])
        .transform(ST.deduplicate(pk="id"))
        .transform(ST.add_processing_time())
        .transform(ST.mask_columns(["email", "phone"]))
        .transform(ST.enrich_from_broadcast(dim_df, join_col="product_id"))
    )

Functions
---------
  filter_op          — keep only specific Debezium op codes (c/u/d/r)
  deduplicate        — keep last event per PK within a micro-batch
  add_processing_time— inject proc_time TIMESTAMP column (wall-clock)
  rename_columns     — rename a dict of {old: new} columns
  cast_columns       — cast a dict of {col: spark_type} columns
  drop_columns       — drop a list of column names
  mask_columns       — SHA-256 hash sensitive columns (PII masking)
  add_source_tag     — inject source_system STRING column
  add_op_label       — inject human-readable op_label (INSERT/UPDATE/DELETE)
  flatten_json_col   — expand a JSON string column into top-level columns
  enrich_from_broadcast — left join a streaming batch against a broadcast dim
  aggregate_counts   — count events by (pk_col, op) within the batch
  pivot_before_after — side-by-side before/after columns from Debezium envelope
  filter_columns     — keep only listed columns (projection)
  null_coalesce      — coalesce(col, default_value) for nullable columns
  route_by_topic     — split a multi-topic DataFrame into a dict keyed by topic
  apply_pipeline     — chain a list of (fn, kwargs) tuples sequentially

These functions are intentionally stateless within the batch.  For
cross-batch state (e.g. windowed aggregations), use Spark's native
stateful streaming operators (mapGroupsWithState / flatMapGroupsWithState).

Usage example in foreachBatch
------------------------------
    def my_batch_writer(batch_df: DataFrame, batch_id: int) -> None:
        from star_transform import StarTransform as ST

        # 1. Keep only inserts and updates
        df = ST.filter_op(batch_df, ops=["c", "u"])

        # 2. Deduplicate by id (last event wins within the batch)
        df = df.transform(ST.deduplicate("id"))

        # 3. Mask PII
        df = df.transform(ST.mask_columns(["email", "phone_number"]))

        # 4. Enrich with product dimension
        df = df.transform(ST.enrich_from_broadcast(product_dim, "product_id"))

        # 5. Write to Iceberg
        df.writeTo("catalog.ns.table").append()
"""

from __future__ import annotations

import hashlib
import logging
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, TimestampType

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
class StarTransform:
    """
    Flink-like composable transformation functions for Spark micro-batch DataFrames.

    All methods are @staticmethod — instantiation is never required.
    Use StarTransform as ST for brevity.
    """

    # ── Op filtering ──────────────────────────────────────────────────────────

    @staticmethod
    def filter_op(df: DataFrame, ops: list[str]) -> DataFrame:
        """
        Keep only rows whose Debezium _op code is in *ops*.

        Debezium op codes:
          c = create (INSERT)
          u = update (UPDATE)
          d = delete (DELETE)
          r = read   (snapshot)

        Example:
            df = ST.filter_op(df, ops=["c", "u"])   # inserts + updates only
            df = ST.filter_op(df, ops=["d"])         # deletes only
        """
        return df.filter(F.col("_op").isin(ops))

    # ── Deduplication ─────────────────────────────────────────────────────────

    @staticmethod
    def deduplicate(pk: str, order_col: str = "kafka_ts") -> Any:
        """
        Return a DataFrame.transform-compatible function that keeps the last
        event per primary-key value within a micro-batch.

        When multiple CDC events for the same PK arrive in one micro-batch
        (e.g. INSERT then immediately UPDATE), only the last event (by
        *order_col*, default kafka_ts) is kept — "last-write-wins".

        Example:
            df = df.transform(ST.deduplicate("id"))
            df = df.transform(ST.deduplicate("_id", order_col="ts_ms"))
        """
        from pyspark.sql import Window

        def _dedup(df: DataFrame) -> DataFrame:
            w = (
                Window
                .partitionBy(F.col(pk))
                .orderBy(F.col(order_col).desc())
            )
            return (
                df
                .withColumn("__row_num", F.row_number().over(w))
                .filter(F.col("__row_num") == 1)
                .drop("__row_num")
            )
        return _dedup

    # ── Timestamp injection ───────────────────────────────────────────────────

    @staticmethod
    def add_processing_time(col_name: str = "proc_time") -> Any:
        """
        Inject a *col_name* TIMESTAMP column set to current wall-clock time.

        Different from snap_timestamp (which is injected at Iceberg write time).
        proc_time marks when the Kafka record was processed by this pipeline.

        Example:
            df = df.transform(ST.add_processing_time())
            df = df.transform(ST.add_processing_time("pipeline_ts"))
        """
        def _add(df: DataFrame) -> DataFrame:
            return df.withColumn(col_name, F.current_timestamp())
        return _add

    # ── Column rename ─────────────────────────────────────────────────────────

    @staticmethod
    def rename_columns(mapping: dict[str, str]) -> Any:
        """
        Rename columns according to *mapping* = {old_name: new_name}.

        Example:
            df = df.transform(ST.rename_columns({"cust_id": "customer_id"}))
        """
        def _rename(df: DataFrame) -> DataFrame:
            for old, new in mapping.items():
                if old in df.columns:
                    df = df.withColumnRenamed(old, new)
                else:
                    logger.warning("rename_columns: column %r not found — skipped.", old)
            return df
        return _rename

    # ── Type casting ──────────────────────────────────────────────────────────

    @staticmethod
    def cast_columns(casts: dict[str, Any]) -> Any:
        """
        Cast columns to the given Spark SQL type strings or DataType objects.

        Example:
            from pyspark.sql.types import DoubleType
            df = df.transform(ST.cast_columns({
                "price": "double",
                "quantity": "int",
                "event_ts": TimestampType(),
            }))
        """
        def _cast(df: DataFrame) -> DataFrame:
            for col_name, dtype in casts.items():
                if col_name in df.columns:
                    df = df.withColumn(col_name, F.col(col_name).cast(dtype))
                else:
                    logger.warning("cast_columns: column %r not found — skipped.", col_name)
            return df
        return _cast

    # ── Column drop ───────────────────────────────────────────────────────────

    @staticmethod
    def drop_columns(columns: list[str]) -> Any:
        """
        Drop *columns* from the DataFrame (silently skips missing columns).

        Example:
            df = df.transform(ST.drop_columns(["__internal", "debug_field"]))
        """
        def _drop(df: DataFrame) -> DataFrame:
            existing = [c for c in columns if c in df.columns]
            return df.drop(*existing) if existing else df
        return _drop

    # ── PII masking ───────────────────────────────────────────────────────────

    @staticmethod
    def mask_columns(columns: list[str], algorithm: str = "sha256") -> Any:
        """
        Replace sensitive column values with a deterministic hash.

        The value is converted to a UTF-8 string then hashed with *algorithm*
        (default sha256). NULL values remain NULL.

        Use this for PII fields (email, phone, SSN, etc.) before writing to
        Iceberg so the lakehouse never stores raw PII.

        Example:
            df = df.transform(ST.mask_columns(["email", "phone_number", "ssn"]))
        """
        if algorithm not in ("sha256", "md5", "sha1"):
            raise ValueError(f"Unsupported mask algorithm: {algorithm!r}")

        spark_fn = {
            "sha256": F.sha2,
            "md5":    lambda c, _: F.md5(c),
            "sha1":   lambda c, _: F.sha1(c),
        }[algorithm]

        def _mask(df: DataFrame) -> DataFrame:
            for col_name in columns:
                if col_name in df.columns:
                    if algorithm == "sha256":
                        df = df.withColumn(
                            col_name,
                            F.when(
                                F.col(col_name).isNotNull(),
                                spark_fn(F.col(col_name).cast(StringType()), 256),
                            )
                        )
                    else:
                        df = df.withColumn(
                            col_name,
                            F.when(
                                F.col(col_name).isNotNull(),
                                spark_fn(F.col(col_name).cast(StringType()), None),
                            )
                        )
                else:
                    logger.warning("mask_columns: column %r not found — skipped.", col_name)
            return df
        return _mask

    # ── Source tagging ────────────────────────────────────────────────────────

    @staticmethod
    def add_source_tag(source_system: str, col_name: str = "source_system") -> Any:
        """
        Inject a STRING column *col_name* with the literal value *source_system*.

        Useful when merging streams from multiple sources into one Iceberg table.

        Example:
            df = df.transform(ST.add_source_tag("oracle_tpcds"))
        """
        def _tag(df: DataFrame) -> DataFrame:
            return df.withColumn(col_name, F.lit(source_system))
        return _tag

    # ── Human-readable op label ───────────────────────────────────────────────

    @staticmethod
    def add_op_label(op_col: str = "_op", label_col: str = "op_label") -> Any:
        """
        Add a human-readable *label_col* STRING derived from Debezium *op_col*.

        Mapping: c → INSERT, u → UPDATE, d → DELETE, r → INSERT (snapshot).

        Example:
            df = df.transform(ST.add_op_label())
            # result has new column: op_label in (INSERT, UPDATE, DELETE)
        """
        def _label(df: DataFrame) -> DataFrame:
            return df.withColumn(
                label_col,
                F.when(F.col(op_col) == "c", F.lit("INSERT"))
                 .when(F.col(op_col) == "u", F.lit("UPDATE"))
                 .when(F.col(op_col) == "d", F.lit("DELETE"))
                 .when(F.col(op_col) == "r", F.lit("INSERT"))
                 .otherwise(F.lit("UNKNOWN")),
            )
        return _label

    # ── JSON column flattening ────────────────────────────────────────────────

    @staticmethod
    def flatten_json_col(json_col: str, schema: Any, prefix: str = "") -> Any:
        """
        Parse a JSON string column *json_col* using *schema* (StructType) and
        expand its fields as top-level columns, optionally prefixed with *prefix*.

        The original *json_col* is dropped after expansion.

        Example:
            from pyspark.sql.types import StructType, StructField, StringType, LongType
            addr_schema = StructType([
                StructField("street", StringType()),
                StructField("city",   StringType()),
                StructField("zip",    StringType()),
            ])
            df = df.transform(ST.flatten_json_col("address_json", addr_schema, "addr_"))
            # Result has columns: addr_street, addr_city, addr_zip
        """
        from pyspark.sql.functions import from_json

        def _flatten(df: DataFrame) -> DataFrame:
            parsed = df.withColumn("__parsed", from_json(F.col(json_col), schema))
            for field in schema.fields:
                out_col = f"{prefix}{field.name}" if prefix else field.name
                parsed = parsed.withColumn(out_col, F.col(f"__parsed.{field.name}"))
            return parsed.drop("__parsed", json_col)
        return _flatten

    # ── Broadcast dimension enrich ────────────────────────────────────────────

    @staticmethod
    def enrich_from_broadcast(
        dim_df:      DataFrame,
        join_col:    str,
        select_cols: list[str] | None = None,
        how:         str = "left",
    ) -> Any:
        """
        Left-join the streaming batch against a static dimension DataFrame
        *dim_df* (broadcast hint applied automatically).

        *join_col*    — column present in both DataFrames to join on.
        *select_cols* — columns to bring in from *dim_df* (default: all).
        *how*         — join type (default: left).

        The dim DataFrame is broadcast so no shuffle is triggered — safe for
        large streaming batches with small-to-medium dimension tables.

        Example:
            product_dim = spark.table("polaris.dims.products")
            df = df.transform(
                ST.enrich_from_broadcast(product_dim, "product_id",
                                         select_cols=["category", "brand"])
            )
        """
        dim_broadcast = F.broadcast(dim_df)
        if select_cols:
            dim_broadcast = dim_broadcast.select([join_col] + select_cols)

        def _enrich(df: DataFrame) -> DataFrame:
            # Avoid column name collision: prefix dim columns with "dim_" if needed
            existing = set(df.columns)
            dim_cols = [c for c in dim_broadcast.columns if c != join_col]
            rename_map = {c: f"dim_{c}" for c in dim_cols if c in existing}
            bdim = dim_broadcast
            for old, new in rename_map.items():
                bdim = bdim.withColumnRenamed(old, new)
            return df.join(bdim, on=join_col, how=how)
        return _enrich

    # ── Batch aggregate counts ────────────────────────────────────────────────

    @staticmethod
    def aggregate_counts(
        pk_col: str,
        op_col: str = "_op",
        out_col: str = "event_count",
    ) -> Any:
        """
        Count events per (pk_col, op_col) within the micro-batch.

        Returns a summary DataFrame — not suitable for direct Iceberg write
        of the original rows.  Use this for audit/metrics side-writes.

        Example:
            counts_df = ST.aggregate_counts("id")(batch_df)
            counts_df.writeTo("catalog.audit.event_counts").append()
        """
        def _agg(df: DataFrame) -> DataFrame:
            return (
                df
                .groupBy(F.col(pk_col), F.col(op_col))
                .agg(F.count("*").alias(out_col))
            )
        return _agg

    # ── Before/After side-by-side pivot ───────────────────────────────────────

    @staticmethod
    def pivot_before_after(
        before_col: str = "before",
        after_col:  str = "after",
        schema:     Any = None,
    ) -> Any:
        """
        Parse the raw Debezium *before* and *after* JSON string columns and
        expand them side-by-side as before_<field> and after_<field> columns.

        *schema* must be a StructType matching both before and after payloads.
        If schema is None the columns are left as JSON strings.

        Primarily used in history_tracking mode to preserve the full before
        image alongside the after image for UPDATE events.

        Example:
            df = df.transform(ST.pivot_before_after(schema=my_schema))
            # Columns: before_id, before_name, after_id, after_name, ...
        """
        from pyspark.sql.functions import from_json

        def _pivot(df: DataFrame) -> DataFrame:
            if schema is None:
                return df  # caller handles raw JSON strings
            before_parsed = from_json(F.col(before_col), schema)
            after_parsed  = from_json(F.col(after_col),  schema)
            result = df
            for field in schema.fields:
                result = result.withColumn(
                    f"before_{field.name}", before_parsed[field.name]
                ).withColumn(
                    f"after_{field.name}", after_parsed[field.name]
                )
            return result.drop(before_col, after_col)
        return _pivot

    # ── Column projection ─────────────────────────────────────────────────────

    @staticmethod
    def filter_columns(keep: list[str]) -> Any:
        """
        Keep only *keep* columns, dropping everything else.

        Example:
            df = df.transform(ST.filter_columns(["id", "name", "email", "_op"]))
        """
        def _proj(df: DataFrame) -> DataFrame:
            existing = [c for c in keep if c in df.columns]
            missing  = [c for c in keep if c not in df.columns]
            if missing:
                logger.warning("filter_columns: missing columns %s — skipped.", missing)
            return df.select(existing)
        return _proj

    # ── Null coalescing ───────────────────────────────────────────────────────

    @staticmethod
    def null_coalesce(defaults: dict[str, Any]) -> Any:
        """
        For each column in *defaults*, replace NULL with the given literal value.

        Example:
            df = df.transform(ST.null_coalesce({
                "status": "UNKNOWN",
                "score":  0,
                "active": False,
            }))
        """
        def _coalesce(df: DataFrame) -> DataFrame:
            for col_name, default in defaults.items():
                if col_name in df.columns:
                    df = df.withColumn(
                        col_name,
                        F.coalesce(F.col(col_name), F.lit(default)),
                    )
                else:
                    logger.warning("null_coalesce: column %r not found — skipped.", col_name)
            return df
        return _coalesce

    # ── Topic routing ─────────────────────────────────────────────────────────

    @staticmethod
    def route_by_topic(df: DataFrame) -> dict[str, DataFrame]:
        """
        Split a multi-topic DataFrame into a dict of {topic_name: DataFrame}.

        Collects the distinct topic names in the batch (one small action),
        then filters the DataFrame per topic without additional Kafka reads.

        Example:
            topic_dfs = ST.route_by_topic(batch_df)
            for topic, tdf in topic_dfs.items():
                process_table(topic, tdf)
        """
        try:
            topics = [r[0] for r in df.select("topic").distinct().collect()]
        except Exception as exc:
            logger.warning("route_by_topic: could not collect topics: %s", exc)
            return {}
        return {t: df.filter(F.col("topic") == t) for t in topics}

    # ── Pipeline composition ──────────────────────────────────────────────────

    @staticmethod
    def apply_pipeline(
        df: DataFrame,
        steps: list[tuple[Any, dict]],
    ) -> DataFrame:
        """
        Apply a sequence of transformation steps to *df*.

        *steps* is a list of (transform_fn, kwargs) tuples where:
          - transform_fn is a StarTransform static method that returns a
            DataFrame.transform-compatible callable when called with **kwargs
          - kwargs is the dict of arguments to pass to the method

        Example:
            result = ST.apply_pipeline(batch_df, [
                (ST.filter_op,          {"ops": ["c", "u"]}),
                (ST.deduplicate,        {"pk": "id"}),
                (ST.mask_columns,       {"columns": ["email"]}),
                (ST.add_processing_time, {}),
                (ST.add_source_tag,     {"source_system": "oracle_tpcds"}),
            ])
        """
        for fn, kwargs in steps:
            try:
                # Methods like filter_op take df directly; others return a transform fn
                result = fn(df, **kwargs)
                if isinstance(result, DataFrame):
                    df = result
                elif callable(result):
                    df = df.transform(result)
                else:
                    logger.warning("apply_pipeline: step %r returned unexpected type.", fn)
            except Exception as exc:
                logger.error("apply_pipeline: step %r failed: %s", fn, exc, exc_info=True)
                raise
        return df


# ─────────────────────────────────────────────────────────────────────────────
# Convenience alias
# ─────────────────────────────────────────────────────────────────────────────
ST = StarTransform
