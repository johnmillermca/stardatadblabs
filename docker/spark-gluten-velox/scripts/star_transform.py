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
  filter_op              — keep only specific Debezium op codes (c/u/d/r)
  deduplicate            — keep last event per PK within a micro-batch
  add_processing_time    — inject proc_time TIMESTAMP column (wall-clock)
  rename_columns         — rename a dict of {old: new} columns
  cast_columns           — cast a dict of {col: spark_type} columns
  drop_columns           — drop a list of column names
  mask_columns           — SHA-256 hash sensitive columns (PII masking)
  add_source_tag         — inject source_system STRING column
  add_op_label           — inject human-readable op_label (INSERT/UPDATE/DELETE)
  flatten_json_col       — expand a JSON string column into top-level columns
  enrich_from_broadcast  — left join a streaming batch against a broadcast dim
  aggregate_counts       — count events by (pk_col, op) within the batch
  pivot_before_after     — side-by-side before/after columns from Debezium envelope
  filter_columns         — keep only listed columns (projection)
  null_coalesce          — coalesce(col, default_value) for nullable columns
  route_by_topic         — split a multi-topic DataFrame into a dict keyed by topic
  apply_pipeline         — chain a list of (fn, kwargs) tuples sequentially

  ── Aggregate functions (batch-level summaries → separate Iceberg tables) ──
  windowed_aggregate     — group-by + multi-agg (sum/avg/min/max/count) over any columns
  rolling_sum            — cumulative sum of a numeric column, ordered by order_col
  rolling_avg            — cumulative average of a numeric column, ordered by order_col
  count_distinct_per_key — count distinct values of value_col per group_col
  top_n_per_group        — keep top-N rows per group by a rank column
  event_rate             — events-per-second throughput metric for the current batch

  ── Multi-topic join functions (cross-topic enrichment → Iceberg) ──
  stream_join            — inner/left join two batches on a shared key column
  temporal_join          — join two batches keeping closest-in-time match per key
  multi_topic_union      — UNION ALL multiple DataFrames with a source_topic tag
  join_and_tag_source    — join + add topic-name columns for full provenance

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


    # =========================================================================
    # ── AGGREGATE FUNCTIONS ───────────────────────────────────────────────────
    # =========================================================================

    # ── General windowed / grouped aggregation ────────────────────────────────

    @staticmethod
    def windowed_aggregate(
        group_cols: list[str],
        agg_specs: list[tuple[str, str, str]],
        batch_ts_col: str = "proc_batch_ts",
    ) -> Any:
        """
        Group-by *group_cols* and compute multiple aggregations in one pass.

        *agg_specs* is a list of (column, function, alias) triples.
        Supported functions: ``sum``, ``avg``, ``min``, ``max``, ``count``,
        ``count_distinct``, ``first``, ``last``, ``stddev``, ``variance``.

        A ``proc_batch_ts`` TIMESTAMP column (wall-clock) is added so every
        summary row carries the batch time — essential for time-series queries
        against the Iceberg aggregate table.

        Returns a **summary** DataFrame — write it to a separate Iceberg table,
        not to the original events table.

        Example::

            agg_df = ST.windowed_aggregate(
                ["country", "_op"],
                [("total_amount", "sum", "total_revenue"),
                 ("id",           "count", "event_count"),
                 ("total_amount", "avg",   "avg_order_value")],
            )(batch_df)
            agg_df.writeTo("postgres.e2e_testing.orders_agg_summary").append()
        """
        _FN_MAP = {
            "sum":            lambda c: F.sum(c),
            "avg":            lambda c: F.avg(c),
            "min":            lambda c: F.min(c),
            "max":            lambda c: F.max(c),
            "count":          lambda c: F.count(c),
            "count_distinct": lambda c: F.countDistinct(c),
            "first":          lambda c: F.first(c, ignorenulls=True),
            "last":           lambda c: F.last(c, ignorenulls=True),
            "stddev":         lambda c: F.stddev(c),
            "variance":       lambda c: F.variance(c),
        }

        def _agg(df: DataFrame) -> DataFrame:
            exprs = []
            for src_col, fn_name, alias in agg_specs:
                fn_name_lower = fn_name.lower()
                if fn_name_lower not in _FN_MAP:
                    raise ValueError(
                        f"windowed_aggregate: unsupported function {fn_name!r}. "
                        f"Choose from {sorted(_FN_MAP)}."
                    )
                if src_col not in df.columns:
                    logger.warning(
                        "windowed_aggregate: column %r not found — skipped.", src_col
                    )
                    continue
                exprs.append(_FN_MAP[fn_name_lower](F.col(src_col)).alias(alias))
            if not exprs:
                return df
            return (
                df.groupBy([F.col(c) for c in group_cols])
                  .agg(*exprs)
                  .withColumn(batch_ts_col, F.current_timestamp())
            )
        return _agg

    # ── Rolling (cumulative) sum ───────────────────────────────────────────────

    @staticmethod
    def rolling_sum(
        value_col: str,
        order_col: str = "kafka_ts",
        partition_cols: list[str] | None = None,
        out_col: str | None = None,
    ) -> Any:
        """
        Add a cumulative **sum** of *value_col* ordered by *order_col* using a
        window that spans all preceding rows up to and including the current
        one (``ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW``).

        *partition_cols* — optional list of columns to partition the window by
        (e.g. ``["customer_id"]`` for a per-customer running total).
        *out_col* — name of the new column (default: ``<value_col>_rolling_sum``).

        Example::

            df = df.transform(
                ST.rolling_sum("total_amount", "kafka_ts", ["customer_id"])
            )
            # new column: total_amount_rolling_sum
        """
        from pyspark.sql import Window

        output = out_col or f"{value_col}_rolling_sum"

        def _rsum(df: DataFrame) -> DataFrame:
            spec = Window.orderBy(F.col(order_col)).rowsBetween(
                Window.unboundedPreceding, Window.currentRow
            )
            if partition_cols:
                spec = spec.partitionBy([F.col(c) for c in partition_cols])
            return df.withColumn(output, F.sum(F.col(value_col)).over(spec))
        return _rsum

    # ── Rolling (cumulative) average ──────────────────────────────────────────

    @staticmethod
    def rolling_avg(
        value_col: str,
        order_col: str = "kafka_ts",
        partition_cols: list[str] | None = None,
        out_col: str | None = None,
    ) -> Any:
        """
        Add a cumulative **average** of *value_col* ordered by *order_col*.

        Uses the same unbounded-preceding window as :py:meth:`rolling_sum`.
        *partition_cols* and *out_col* behave identically.

        Example::

            df = df.transform(
                ST.rolling_avg("total_amount", "kafka_ts", ["customer_id"])
            )
            # new column: total_amount_rolling_avg
        """
        from pyspark.sql import Window

        output = out_col or f"{value_col}_rolling_avg"

        def _ravg(df: DataFrame) -> DataFrame:
            spec = Window.orderBy(F.col(order_col)).rowsBetween(
                Window.unboundedPreceding, Window.currentRow
            )
            if partition_cols:
                spec = spec.partitionBy([F.col(c) for c in partition_cols])
            return df.withColumn(output, F.avg(F.col(value_col)).over(spec))
        return _ravg

    # ── Count distinct values per group key ───────────────────────────────────

    @staticmethod
    def count_distinct_per_key(
        group_col: str,
        value_col: str,
        out_col: str | None = None,
        batch_ts_col: str = "proc_batch_ts",
    ) -> Any:
        """
        For each distinct value of *group_col*, count the number of **distinct**
        values of *value_col* within the micro-batch.

        Returns a **summary** DataFrame with columns:
        ``(group_col, out_col, proc_batch_ts)``.

        Write the result to a dedicated audit/metrics Iceberg table.

        Example::

            distinct_df = ST.count_distinct_per_key("country", "id")(batch_df)
            # columns: country, id_distinct_count, proc_batch_ts
            distinct_df.writeTo("postgres.e2e_testing.customers_country_stats").append()
        """
        output = out_col or f"{value_col}_distinct_count"

        def _cdpk(df: DataFrame) -> DataFrame:
            return (
                df.groupBy(F.col(group_col))
                  .agg(F.countDistinct(F.col(value_col)).alias(output))
                  .withColumn(batch_ts_col, F.current_timestamp())
            )
        return _cdpk

    # ── Top-N rows per group ───────────────────────────────────────────────────

    @staticmethod
    def top_n_per_group(
        group_col: str,
        rank_col: str,
        n: int = 5,
        ascending: bool = False,
    ) -> Any:
        """
        Within each value of *group_col*, keep the top *n* rows ranked by
        *rank_col* (descending by default — set ``ascending=True`` for bottom-N).

        Uses a window ``row_number()`` so ties are broken deterministically by
        Spark's row ordering rather than discarding rows arbitrarily.

        Useful for "top 5 highest-value orders per country" type queries.

        Example::

            df = df.transform(ST.top_n_per_group("country", "total_amount", n=3))
            # result contains at most 3 rows per country (highest total_amount)
        """
        from pyspark.sql import Window

        def _top_n(df: DataFrame) -> DataFrame:
            order_expr = (
                F.col(rank_col).asc() if ascending else F.col(rank_col).desc()
            )
            w = Window.partitionBy(F.col(group_col)).orderBy(order_expr)
            return (
                df.withColumn("__rn", F.row_number().over(w))
                  .filter(F.col("__rn") <= n)
                  .drop("__rn")
            )
        return _top_n

    # ── Event-rate metric ─────────────────────────────────────────────────────

    @staticmethod
    def event_rate(
        ts_col: str = "kafka_ts",
        out_col: str = "events_per_second",
        batch_ts_col: str = "proc_batch_ts",
    ) -> Any:
        """
        Compute the overall **events-per-second** throughput for the current
        micro-batch from the timestamps in *ts_col*.

        The result is a single-row summary DataFrame with columns:
        ``(event_count, batch_duration_seconds, events_per_second, min_ts,
        max_ts, proc_batch_ts)``.

        Intended for side-writes to a monitoring/metrics Iceberg table.

        Example::

            rate_df = ST.event_rate("kafka_ts")(batch_df)
            rate_df.writeTo("postgres.e2e_testing.pipeline_event_rate").append()
        """
        def _rate(df: DataFrame) -> DataFrame:
            agg_df = df.agg(
                F.count("*").alias("event_count"),
                F.min(F.col(ts_col)).alias("min_ts"),
                F.max(F.col(ts_col)).alias("max_ts"),
            )
            # batch_duration_seconds = (max_ts – min_ts) in seconds; guard div/0
            return (
                agg_df
                .withColumn(
                    "batch_duration_seconds",
                    F.greatest(
                        (
                            F.col("max_ts").cast("double")
                            - F.col("min_ts").cast("double")
                        ),
                        F.lit(1.0),
                    ),
                )
                .withColumn(
                    out_col,
                    F.col("event_count") / F.col("batch_duration_seconds"),
                )
                .withColumn(batch_ts_col, F.current_timestamp())
            )
        return _rate

    # =========================================================================
    # ── MULTI-TOPIC JOIN FUNCTIONS ────────────────────────────────────────────
    # =========================================================================

    # ── Direct batch-to-batch join ────────────────────────────────────────────

    @staticmethod
    def stream_join(
        right_df: DataFrame,
        join_col: str | list[str],
        how: str = "inner",
        left_prefix: str = "",
        right_prefix: str = "right_",
    ) -> Any:
        """
        Join the current micro-batch DataFrame against *right_df* (another
        Kafka topic batch or any DataFrame) on *join_col*.

        Column name collisions (excluding *join_col* itself) are resolved by
        prefixing the right side's columns with *right_prefix* (default
        ``'right_'``).  Set *left_prefix* to also prefix the left side.

        Both DataFrames must have been routed from their respective Kafka topics
        using :py:meth:`route_by_topic` before calling this function.

        *how* supports the full Spark join-type vocabulary: ``inner``,
        ``left``, ``right``, ``outer``, ``left_semi``, ``left_anti``.

        Example::

            topic_dfs = ST.route_by_topic(batch_df)
            orders_df  = topic_dfs.get("postgres.public.orders", spark.createDataFrame([], orders_schema))
            products_df = topic_dfs.get("postgres.public.products", spark.createDataFrame([], products_schema))

            joined = ST.stream_join(products_df, "product_id", how="left")(orders_df)
            joined.writeTo("postgres.e2e_testing.orders_products_joined").append()
        """
        join_keys = [join_col] if isinstance(join_col, str) else list(join_col)

        def _join(left: DataFrame) -> DataFrame:
            # Determine overlapping non-key columns
            left_non_key  = [c for c in left.columns     if c not in join_keys]
            right_non_key = [c for c in right_df.columns if c not in join_keys]
            overlap = set(left_non_key) & set(right_non_key)

            # Build renamed right DataFrame
            right_renamed = right_df
            for col_name in overlap:
                right_renamed = right_renamed.withColumnRenamed(
                    col_name, f"{right_prefix}{col_name}"
                )
            # Optionally prefix left side
            left_renamed = left
            if left_prefix:
                for col_name in overlap:
                    left_renamed = left_renamed.withColumnRenamed(
                        col_name, f"{left_prefix}{col_name}"
                    )

            return left_renamed.join(right_renamed, on=join_keys, how=how)
        return _join

    # ── Temporal (nearest-in-time) join ───────────────────────────────────────

    @staticmethod
    def temporal_join(
        right_df: DataFrame,
        key_col: str,
        left_ts_col: str  = "kafka_ts",
        right_ts_col: str = "kafka_ts",
        tolerance_ms: int | None = None,
        right_prefix: str = "right_",
    ) -> Any:
        """
        For each row in the left batch, find the **closest-in-time** matching
        row in *right_df* sharing the same *key_col* value.

        Steps:
        1. Cross-join-free: broadcast *right_df* and join on *key_col*.
        2. Compute absolute timestamp delta ``|left_ts – right_ts|`` in ms.
        3. If *tolerance_ms* is set, drop matches whose delta exceeds it.
        4. Keep only the closest right-side match per left row (min delta wins).

        This is the micro-batch equivalent of a Flink event-time temporal join —
        it does **not** maintain cross-batch state; use it for same-batch
        enrichment where both sides arrive in the same micro-batch.

        Overlapping non-key, non-ts columns in *right_df* are prefixed with
        *right_prefix*.

        Example::

            enriched = ST.temporal_join(
                right_df=payments_df,
                key_col="order_id",
                left_ts_col="kafka_ts",
                right_ts_col="kafka_ts",
                tolerance_ms=5000,
            )(orders_df)
            enriched.writeTo("postgres.e2e_testing.orders_payments_temporal").append()
        """
        from pyspark.sql import Window

        def _temporal(left: DataFrame) -> DataFrame:
            # Resolve column collisions on the right side
            right_renamed = right_df
            overlap = (
                set(right_df.columns)
                - {key_col, right_ts_col}
            ) & set(left.columns)
            for col_name in overlap:
                right_renamed = right_renamed.withColumnRenamed(
                    col_name, f"{right_prefix}{col_name}"
                )
            # Rename right ts col to avoid ambiguity
            right_ts_alias = f"__right_{right_ts_col}"
            right_renamed = right_renamed.withColumnRenamed(right_ts_col, right_ts_alias)

            joined = left.join(F.broadcast(right_renamed), on=key_col, how="left")

            # Compute delta in milliseconds (timestamps stored as LongType ms)
            joined = joined.withColumn(
                "__ts_delta_ms",
                F.abs(
                    F.col(left_ts_col).cast("long")
                    - F.col(right_ts_alias).cast("long")
                ),
            )

            if tolerance_ms is not None:
                joined = joined.filter(
                    F.col("__ts_delta_ms").isNull()
                    | (F.col("__ts_delta_ms") <= tolerance_ms)
                )

            # Keep only the single closest right-side match per left row
            # Use a synthetic row identity to partition over
            w = (
                Window
                .partitionBy(F.col(key_col), F.col(left_ts_col))
                .orderBy(F.col("__ts_delta_ms").asc_nulls_last())
            )
            return (
                joined
                .withColumn("__closest_rn", F.row_number().over(w))
                .filter(F.col("__closest_rn") == 1)
                .drop("__closest_rn", "__ts_delta_ms", right_ts_alias)
            )
        return _temporal

    # ── UNION ALL multiple topic DataFrames ───────────────────────────────────

    @staticmethod
    def multi_topic_union(
        topic_dfs: dict[str, DataFrame],
        tag_col: str = "source_topic",
        harmonise_schema: bool = True,
    ) -> DataFrame:
        """
        UNION ALL multiple DataFrames (typically from :py:meth:`route_by_topic`)
        into a single DataFrame and inject a *tag_col* STRING column carrying
        the original topic name.

        When *harmonise_schema* is ``True`` (default), missing columns are
        added as ``NULL`` casts so all DataFrames share the same schema before
        the union — required when topics have slightly different column sets.

        Pass the result directly to an Iceberg ``append()`` write to build a
        unified multi-source events table.

        Example::

            topic_dfs = ST.route_by_topic(batch_df)
            unified = ST.multi_topic_union(topic_dfs)
            unified.writeTo("postgres.e2e_testing.all_topics_union").append()
        """
        if not topic_dfs:
            raise ValueError("multi_topic_union: topic_dfs dict is empty.")

        tagged: list[DataFrame] = []
        for topic_name, tdf in topic_dfs.items():
            tagged.append(tdf.withColumn(tag_col, F.lit(topic_name)))

        if not harmonise_schema:
            result = tagged[0]
            for tdf in tagged[1:]:
                result = result.unionByName(tdf, allowMissingColumns=True)
            return result

        # Collect the full superset of column names (preserving first-seen order)
        all_cols: list[str] = []
        seen: set[str] = set()
        for tdf in tagged:
            for c in tdf.columns:
                if c not in seen:
                    all_cols.append(c)
                    seen.add(c)

        harmonised: list[DataFrame] = []
        for tdf in tagged:
            missing = [c for c in all_cols if c not in tdf.columns]
            for c in missing:
                tdf = tdf.withColumn(c, F.lit(None).cast(StringType()))
            harmonised.append(tdf.select(all_cols))

        result = harmonised[0]
        for tdf in harmonised[1:]:
            result = result.union(tdf)
        return result

    # ── Join two topic batches and tag both sides with provenance ─────────────

    @staticmethod
    def join_and_tag_source(
        right_df: DataFrame,
        join_col: str | list[str],
        left_topic: str,
        right_topic: str,
        how: str = "inner",
        right_prefix: str = "right_",
    ) -> Any:
        """
        Join two topic DataFrames on *join_col* and add ``left_topic`` and
        ``right_topic`` STRING columns that carry the names of the originating
        Kafka topics for full data-lineage provenance in the Iceberg table.

        This is a thin wrapper around :py:meth:`stream_join` that adds the
        provenance columns after the join.

        Example::

            result = ST.join_and_tag_source(
                right_df=inventory_df,
                join_col="product_id",
                left_topic="postgres.public.orders",
                right_topic="postgres.public.inventory",
                how="left",
            )(orders_df)
            result.writeTo("postgres.e2e_testing.orders_inventory_joined").append()
        """
        _join_fn = StarTransform.stream_join(
            right_df, join_col, how=how, right_prefix=right_prefix
        )

        def _tag_join(df: DataFrame) -> DataFrame:
            joined = _join_fn(df)
            return (
                joined
                .withColumn("left_topic",  F.lit(left_topic))
                .withColumn("right_topic", F.lit(right_topic))
            )
        return _tag_join


# ─────────────────────────────────────────────────────────────────────────────
# Convenience alias
# ─────────────────────────────────────────────────────────────────────────────
ST = StarTransform
