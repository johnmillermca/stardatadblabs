-- =============================================================================
-- platform_audit — Doris table + Routine Load from Kafka platform.audit
-- Retention: 10 days (dynamic partitioning drops older daily partitions)
-- Read-only console user: audit_reader  (no DELETE, no DROP)
-- =============================================================================

-- Run as Doris admin on the StarData cluster

USE platform;

-- ── Audit table ───────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS platform.platform_audit (
  ts           DATETIME(3)   NOT NULL,
  event_id     VARCHAR(40)   NOT NULL,
  source       VARCHAR(16)   NOT NULL,          -- 'zitadel' | 'console'
  actor_id     VARCHAR(40),
  actor_name   VARCHAR(200),
  target_type  VARCHAR(20),
  target_id    VARCHAR(40),
  target_name  VARCHAR(200),
  component    VARCHAR(20),
  action       VARCHAR(60)   NOT NULL,
  before_value JSON,
  after_value  JSON,
  result       VARCHAR(10)   NOT NULL,          -- 'ok' | 'denied' | 'waiting' | 'blocked'
  reason       STRING,
  ip           VARCHAR(45),
  user_agent   STRING,
  request_id   VARCHAR(40)
)
DUPLICATE KEY (ts, event_id)
PARTITION BY RANGE (ts) ()
DISTRIBUTED BY HASH (event_id) BUCKETS 8
PROPERTIES (
  "dynamic_partition.enable"     = "true",
  "dynamic_partition.time_unit"  = "DAY",
  "dynamic_partition.start"      = "-10",   -- keep 10 days; older partitions auto-dropped
  "dynamic_partition.end"        = "3",
  "dynamic_partition.prefix"     = "p",
  "replication_num"              = "3"
);

-- ── Read-only audit user ───────────────────────────────────────────────────────
-- Create user and grant SELECT only (no INSERT, no DELETE, no DROP)
-- Run once; skip if user already exists.
-- CREATE USER 'audit_reader'@'%' IDENTIFIED BY 'REPLACE_WITH_AUDIT_READER_PASS';
-- GRANT SELECT ON platform.platform_audit TO 'audit_reader'@'%';

-- ── Routine Load job — reads from Kafka platform.audit ───────────────────────
-- Kafka brokers (SASL_SSL, SCRAM-SHA-512) from the prod cluster
-- Replace KAFKA_USERNAME / KAFKA_PASSWORD with the actual values

CREATE ROUTINE LOAD platform.audit_load ON platform_audit
COLUMNS TERMINATED BY ","
PROPERTIES (
  "format"    = "json",
  "jsonpaths" = "[\"$.ts\",\"$.event_id\",\"$.source\",\"$.actor_id\",\"$.actor_name\","
                "\"$.target_type\",\"$.target_id\",\"$.target_name\",\"$.component\","
                "\"$.action\",\"$.before\",\"$.after\",\"$.result\",\"$.reason\","
                "\"$.ip\",\"$.user_agent\",\"$.request_id\"]",
  "strip_outer_array" = "false",
  "max_batch_interval" = "10",
  "max_batch_rows"     = "20000",
  "max_batch_size"     = "104857600",
  "max_error_number"   = "10"
)
FROM KAFKA (
  "kafka_broker_list"         = "kafka-bootstrap.prod.svc.cluster.local:9093",
  "kafka_topic"               = "platform.audit",
  "kafka_partitions"          = "0,1,2,3,4,5,6,7",
  "property.security.protocol"= "SASL_SSL",
  "property.sasl.mechanism"   = "SCRAM-SHA-512",
  "property.sasl.username"    = "REPLACE_WITH_KAFKA_USERNAME",
  "property.sasl.password"    = "REPLACE_WITH_KAFKA_PASSWORD",
  "property.ssl.ca.location"  = "/etc/ssl/certs/ca-certificates.crt"
);

-- ── Verify load status ────────────────────────────────────────────────────────
-- SHOW ROUTINE LOAD FOR platform.audit_load;
-- SHOW ROUTINE LOAD TASK WHERE JobName = 'audit_load';
