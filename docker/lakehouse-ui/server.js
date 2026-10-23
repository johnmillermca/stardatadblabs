#!/usr/bin/env node
'use strict';

const express = require('express');
const fetch   = require('node-fetch');
const path    = require('path');
const WebSocket = require('ws');
const http    = require('http');
const oracledb   = require('oracledb');
// oracledb v6+ defaults to thin mode (pure JS, no Instant Client) when
// initOracleClient() is never called — no extra configuration needed.
const { MongoClient, ObjectId } = require('mongodb');

// ── Oracle JDBC-style query helper ───────────────────────────────────────────
// Connects directly to Oracle over TCP using credentials from the connector
// config + OpenBao.  Works for any Oracle instance (in-cluster or external).
// connCfg: { hostname, port, dbname, user, password }
// sql:     SELECT statement (single result set)
// returns: string[][] — rows as arrays of string values
async function oracleQuery(connCfg, sql, timeoutMs = 10000) {
  const { hostname, port, dbname, user, password } = connCfg;
  const connectString = `${hostname}:${port}/${dbname}`;
  let conn;
  try {
    conn = await Promise.race([
      oracledb.getConnection({ user, password, connectString }),
      new Promise((_, reject) => setTimeout(() => reject(new Error('Oracle connect timed out')), timeoutMs)),
    ]);
    const result = await conn.execute(sql, [], { outFormat: oracledb.OUT_FORMAT_ARRAY, fetchArraySize: 200 });
    return (result.rows || []).map(r => r.map(v => (v == null ? '' : String(v))));
  } finally {
    if (conn) await conn.close().catch(() => {});
  }
}

// Build Oracle connCfg from a Debezium connector config object + OpenBao creds.
// Falls back to defaults from the template when fields are absent.
async function oracleConnCfg(cfg) {
  const ora = await baoRead(BAO_PATH.oracle);
  return {
    hostname: cfg['database.hostname'] || 'oracle-xe.prod.svc.cluster.local',
    port:     parseInt(cfg['database.port'] || '1521', 10),
    dbname:   cfg['database.pdb.name'] || cfg['database.dbname'] || 'XEPDB1',
    user:     cfg['database.user']     || 'c##dbzcdc',
    password: ora.cdc_password ?? ora.password,
  };
}

// ── MongoDB query helper ─────────────────────────────────────────────────────
// Builds a connection string from the Debezium connector config + OpenBao creds.
// Works for any MongoDB instance (in-cluster or external).
async function mongoConnString(cfg) {
  const mg = await baoRead(BAO_PATH.mongodb);
  // If the connector already has a full connection string use it as the base,
  // replacing the password placeholder with the live value from OpenBao.
  const existing = cfg['mongodb.connection.string'] || '';
  if (existing) {
    return existing.replace(/^(mongodb(?:\+srv)?:\/\/[^:]+:)[^@]*(@)/, `$1${encodeURIComponent(mg.password)}$2`);
  }
  // Fall back to constructing from individual fields (matches Debezium defaults)
  const host = cfg['mongodb.hosts'] || 'mongodb.prod.svc.cluster.local:27017';
  return `mongodb://debezium:${encodeURIComponent(mg.password)}@${host}/?authSource=admin&replicaSet=rs0`;
}

// Run a callback with a connected MongoClient — handles connect + cleanup.
async function withMongo(cfg, fn, timeoutMs = 10000) {
  const uri    = await mongoConnString(cfg);
  const client = new MongoClient(uri, { serverSelectionTimeoutMS: timeoutMs, connectTimeoutMS: timeoutMs });
  try {
    await client.connect();
    return await fn(client);
  } finally {
    await client.close().catch(() => {});
  }
}

const app = express();
app.use(express.json());
// Serve static files with no-cache so browsers always pick up re-deployed HTML/JS
app.use(express.static(path.join(__dirname, 'public'), {
  etag: false,
  lastModified: false,
  setHeaders: (res) => {
    res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate');
    res.setHeader('Pragma', 'no-cache');
  },
}));

// ─── Environment / config ──────────────────────────────────────────────────
const DEBEZIUM_ENDPOINTS = {
  postgres: process.env.DEBEZIUM_POSTGRES_URL || 'http://debezium-connect-postgres.prod.svc.cluster.local:8083',
  oracle:   process.env.DEBEZIUM_ORACLE_URL   || 'http://debezium-connect-oracle.prod.svc.cluster.local:8083',
  mongodb:  process.env.DEBEZIUM_MONGODB_URL  || 'http://debezium-connect-mongodb.prod.svc.cluster.local:8083',
};
const KAFKA_BOOTSTRAP    = process.env.KAFKA_BOOTSTRAP    || 'strimzi-kafka-kafka-bootstrap.prod.svc.cluster.local:9092';
const KSQLDB_URL         = process.env.KSQLDB_URL         || 'http://ksqldb.prod.svc.cluster.local:8088';
const K8S_API            = 'https://kubernetes.default.svc';
const K8S_TOKEN_FILE     = '/var/run/secrets/kubernetes.io/serviceaccount/token';
const K8S_CA_FILE        = '/var/run/secrets/kubernetes.io/serviceaccount/ca.crt';
const K8S_NAMESPACE      = process.env.K8S_NAMESPACE || 'prod';

const fs = require('fs');

function k8sToken() {
  try { return fs.readFileSync(K8S_TOKEN_FILE, 'utf8').trim(); } catch { return ''; }
}

// ═══════════════════════════════════════════════════════════════════════════
//  OPENBAO CLIENT
//  All DB credentials are read directly from OpenBao — K8s secrets are never
//  used for passwords.  The SA JWT is used to authenticate (role:
//  platform-secrets-read, same as every other in-cluster service).
// ═══════════════════════════════════════════════════════════════════════════
const BAO_ADDR      = process.env.BAO_ADDR || `http://openbao.${K8S_NAMESPACE}.svc.cluster.local:8200`;
const BAO_ROLE      = process.env.BAO_ROLE || 'platform-secrets-read';
const BAO_AUTH_PATH = 'auth/kubernetes/login';

// Token cache — re-authenticate only when the cached token is absent/expired.
let _baoToken     = null;
let _baoTokenExp  = 0;   // epoch-ms of expiry (0 = not set)

async function baoToken() {
  if (_baoToken && Date.now() < _baoTokenExp) return _baoToken;
  const jwt = fs.readFileSync(K8S_TOKEN_FILE, 'utf8').trim();
  const res = await fetch(`${BAO_ADDR}/v1/${BAO_AUTH_PATH}`, {
    method:  'POST',
    headers: { 'Content-Type': 'application/json' },
    body:    JSON.stringify({ role: BAO_ROLE, jwt }),
  });
  if (!res.ok) throw new Error(`OpenBao auth failed: HTTP ${res.status}`);
  const data  = await res.json();
  _baoToken   = data.auth.client_token;
  // lease_duration is in seconds; renew 60 s before expiry
  _baoTokenExp = Date.now() + (data.auth.lease_duration - 60) * 1000;
  return _baoToken;
}

async function baoRead(secretPath) {
  const token = await baoToken();
  const res   = await fetch(`${BAO_ADDR}/v1/${secretPath}`, {
    headers: { 'X-Vault-Token': token },
  });
  if (!res.ok) throw new Error(`OpenBao read failed for "${secretPath}": HTTP ${res.status}`);
  const data = await res.json();
  // KV v2: data.data.data  — KV v1: data.data
  return data.data?.data ?? data.data;
}

// Paths in OpenBao KV — mirrors bao_spark_init.py
const BAO_PATH = {
  postgres: 'secret/data/platform/postgres',
  oracle:   'secret/data/platform/oracle',
  mongodb:  'secret/data/platform/mongodb',
  kafka:    'secret/data/platform/kafka',
};

async function k8sFetch(path, opts = {}) {
  const https = require('https');
  let ca;
  try { ca = fs.readFileSync(K8S_CA_FILE); } catch { ca = undefined; }
  const agent = new https.Agent({ ca });
  const token = k8sToken();
  const res = await fetch(`${K8S_API}${path}`, {
    ...opts,
    agent,
    headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json', ...(opts.headers || {}) }
  });
  return res;
}

// ─── Helper: proxy to debezium ──────────────────────────────────────────────
async function dbzRequest(source, method, path, body) {
  const base = DEBEZIUM_ENDPOINTS[source];
  if (!base) throw new Error(`Unknown source: ${source}`);
  const opts = { method, headers: { 'Content-Type': 'application/json' } };
  if (body) opts.body = JSON.stringify(body);
  let res;
  try {
    res = await fetch(`${base}${path}`, opts);
  } catch (e) {
    // EPERM on connect() = kube-proxy iptables rules are stale or CNI dropped the
    // ClusterIP SNAT rule. Fix: kubectl rollout restart -n kube-system daemonset/kube-proxy
    // then verify: kubectl get svc debezium-connect-postgres -n prod
    const hint = (e.message || '').includes('EPERM')
      ? ' (EPERM = kube-proxy/CNI issue — run: kubectl rollout restart -n kube-system daemonset/kube-proxy)'
      : '';
    throw new Error(e.message + hint);
  }
  const text = await res.text();
  let json;
  try { json = JSON.parse(text); } catch { json = { raw: text }; }
  return { status: res.status, body: json };
}

// ═══════════════════════════════════════════════════════════════════════════
//  DEBEZIUM CONNECTOR API
// ═══════════════════════════════════════════════════════════════════════════

// List connectors for a source
app.get('/api/connectors/:source', async (req, res) => {
  try {
    const { source } = req.params;
    const { status: s, body } = await dbzRequest(source, 'GET', '/connectors?expand=status&expand=info');
    res.status(s).json(body);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Get single connector config
app.get('/api/connectors/:source/:name/config', async (req, res) => {
  try {
    const { source, name } = req.params;
    const { status: s, body } = await dbzRequest(source, 'GET', `/connectors/${name}/config`);
    res.status(s).json(body);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Get connector status
app.get('/api/connectors/:source/:name/status', async (req, res) => {
  try {
    const { source, name } = req.params;
    const { status: s, body } = await dbzRequest(source, 'GET', `/connectors/${name}/status`);
    res.status(s).json(body);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── Helper: inject real credentials from OpenBao ──────────────────────────────
// Reads the live password directly from OpenBao KV and stamps it into the
// connector config before forwarding to Debezium.  K8s secrets are never used.
// Called by both POST (create) and PUT (update/edit) connector endpoints.
async function injectCredentials(source, cfg) {
  const patched = { ...cfg };
  try {
    if (source === 'postgres') {
      const pg = await baoRead(BAO_PATH.postgres);
      patched['database.password'] = pg.password;
    } else if (source === 'oracle') {
      const ora = await baoRead(BAO_PATH.oracle);
      // Oracle CDC connector uses the dedicated CDC user password
      patched['database.password'] = ora.cdc_password ?? ora.password;
    } else if (source === 'mongodb') {
      const mg  = await baoRead(BAO_PATH.mongodb);
      const existing = patched['mongodb.connection.string'] || '';
      patched['mongodb.connection.string'] = existing
        ? existing.replace(/^(mongodb:\/\/[^:]+:)[^@]*(@)/, `$1${encodeURIComponent(mg.password)}$2`)
        : `mongodb://debezium:${encodeURIComponent(mg.password)}@mongodb.prod.svc.cluster.local:27017/?authSource=admin&replicaSet=rs0`;
    }
  } catch (e) {
    // Surface the error — never silently forward empty credentials
    throw new Error(`OpenBao credential inject failed for ${source}: ${e.message}`);
  }
  return patched;
}

// Create or update connector (PUT — edit existing)
app.put('/api/connectors/:source/:name', async (req, res) => {
  try {
    const { source, name } = req.params;
    const cfg = await injectCredentials(source, req.body);
    const { status: s, body } = await dbzRequest(source, 'PUT', `/connectors/${name}/config`, cfg);
    res.status(s).json(body);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Create new connector (POST)
// After a successful create the connector is immediately paused so it sits in
// PAUSED state with no tables until the user explicitly picks tables via
// "Assign Tables & Start".  tasks.max=0 is unreliable across Debezium versions
// (some silently start 1 task anyway) so a hard PUT /pause is the only safe
// mechanism to guarantee a stopped initial state.
app.post('/api/connectors/:source', async (req, res) => {
  try {
    const { source } = req.params;
    const payload = { ...req.body };
    payload.config = await injectCredentials(source, payload.config || {});

    // ── Validate schema / database exists in the source DB ──────────────────
    // Reject immediately with HTTP 422 if the user typed a schema/db that does
    // not exist — prevents a connector that will never capture any data.
    const cfgForValidation = payload.config;
    if (source === 'postgres') {
      const schemaList = (cfgForValidation['schema.include.list'] || '').split(',').map(s => s.trim()).filter(Boolean);
      if (schemaList.length) {
        const pod = await getSourcePod('postgresql', 'postgres');
        const db  = cfgForValidation['database.dbname'] || 'cache_testing';
        const { stdout } = await execFileAsync('kubectl', [
          'exec', '-n', K8S_NAMESPACE, pod, '--',
          'psql', '-U', 'postgres', '-d', db, '-tAc',
          `SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT IN ('pg_catalog','information_schema','pg_toast')`,
        ], { timeout: 10000 });
        const validSchemas = stdout.trim().split('\n').map(l => l.trim()).filter(Boolean);
        const invalid = schemaList.filter(s => !validSchemas.includes(s));
        if (invalid.length) {
          return res.status(422).json({
            error: `schema.include.list contains schema(s) that do not exist in database "${db}": ${invalid.join(', ')}`,
            valid: validSchemas,
            hint: `Valid schemas in "${db}": ${validSchemas.join(', ')}`,
          });
        }
      }
    } else if (source === 'oracle') {
      const schemaList = (cfgForValidation['schema.include.list'] || '').split(',').map(s => s.trim().toUpperCase()).filter(Boolean);
      if (schemaList.length) {
        // Connect directly via JDBC — works for in-cluster and external Oracle.
        const connCfg = await oracleConnCfg(cfgForValidation);
        const inList  = schemaList.map(s => `'${s}'`).join(',');
        const rows    = await oracleQuery(connCfg,
          `SELECT USERNAME FROM DBA_USERS WHERE USERNAME IN (${inList})`);
        const foundSchemas = rows.map(r => r[0]);
        const invalid = schemaList.filter(s => !foundSchemas.includes(s));
        if (invalid.length) {
          return res.status(422).json({
            error: `schema.include.list contains schema(s) that do not exist in Oracle: ${invalid.join(', ')}`,
            hint: `Schema(s) not found: ${invalid.join(', ')}`,
          });
        }
      }
    } else if (source === 'mongodb') {
      const dbList = (cfgForValidation['database.include.list'] || '').split(',').map(s => s.trim()).filter(Boolean);
      if (dbList.length) {
        // Connect directly via MongoDB driver — works for in-cluster and external MongoDB.
        const validDbs = await withMongo(cfgForValidation, async (client) => {
          const result = await client.db('admin').command({ listDatabases: 1, nameOnly: true });
          return result.databases.map(d => d.name).filter(n => !['admin','config','local'].includes(n));
        });
        const invalid = dbList.filter(d => !validDbs.includes(d));
        if (invalid.length) {
          return res.status(422).json({
            error: `database.include.list contains database(s) that do not exist in MongoDB: ${invalid.join(', ')}`,
            valid: validDbs,
            hint: `Valid databases: ${validDbs.join(', ')}`,
          });
        }
      }
    }

    // Always strip table/collection include lists on creation — tables are assigned
    // later via "Assign Tables & Start" after supplemental logging is verified.
    delete payload.config['table.include.list'];
    delete payload.config['collection.include.list'];

    // Debezium rejects tasks.max < 1. The connector is paused immediately after
    // creation, so tasks.max=0 is never needed — clamp to 1 as a safety net.
    if (parseInt(payload.config['tasks.max'], 10) < 1) {
      payload.config['tasks.max'] = '1';
    }

    const { status: s, body } = await dbzRequest(source, 'POST', '/connectors', payload);
    if (s >= 300) return res.status(s).json(body);

    // Pause immediately — small delay lets Kafka Connect finish task initialisation
    // before the pause takes effect (avoids a race where pause arrives before the
    // task thread has started and is silently ignored).
    const connName = payload.name || body.name;
    await new Promise(r => setTimeout(r, 2000));
    await dbzRequest(source, 'PUT', `/connectors/${connName}/pause`);

    res.status(s).json(body);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Delete connector
app.delete('/api/connectors/:source/:name', async (req, res) => {
  try {
    const { source, name } = req.params;
    const { status: s, body } = await dbzRequest(source, 'DELETE', `/connectors/${name}`);
    res.status(s).json(body);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Pause connector
app.post('/api/connectors/:source/:name/pause', async (req, res) => {
  try {
    const { source, name } = req.params;
    const { status: s, body } = await dbzRequest(source, 'PUT', `/connectors/${name}/pause`);
    res.status(s).json(body);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Resume connector
app.post('/api/connectors/:source/:name/resume', async (req, res) => {
  try {
    const { source, name } = req.params;
    const { status: s, body } = await dbzRequest(source, 'PUT', `/connectors/${name}/resume`);
    res.status(s).json(body);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Restart connector
app.post('/api/connectors/:source/:name/restart', async (req, res) => {
  try {
    const { source, name } = req.params;
    const { status: s, body } = await dbzRequest(source, 'POST', `/connectors/${name}/restart`);
    res.status(s).json(body);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Restart a single task — POST /api/connectors/:source/:name/tasks/:taskId/restart
// Calls Debezium POST /connectors/{name}/tasks/{taskId}/restart
// Used by "Start Task" when mode=resume (no position change needed).
app.post('/api/connectors/:source/:name/tasks/:taskId/restart', async (req, res) => {
  try {
    const { source, name, taskId } = req.params;
    const { status: s, body } = await dbzRequest(source, 'POST', `/connectors/${name}/tasks/${taskId}/restart`);
    res.status(s).json(body);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// GET /api/connectors/:source/logs
// Returns the last 300 lines from the Debezium Connect pod for the given source.
const DBZ_POD_LABEL = {
  postgres: 'app=debezium-connect-postgres',
  oracle:   'app=debezium-connect-oracle',
  mongodb:  'app=debezium-connect-mongodb',
};
app.get('/api/connectors/:source/logs', async (req, res) => {
  const { source } = req.params;
  const tailLines = parseInt(req.query.tail || '300', 10);
  const labelSelector = DBZ_POD_LABEL[source];
  if (!labelSelector) return res.status(400).json({ error: `Unknown source: ${source}` });
  try {
    const pr = await k8sFetch(`/api/v1/namespaces/${K8S_NAMESPACE}/pods?labelSelector=${encodeURIComponent(labelSelector)}`);
    const body = await pr.json();
    const pod = body.items && body.items[0];
    if (!pod) return res.status(404).json({ error: `No running pod found for source: ${source}` });
    const podName = pod.metadata.name;
    const lr = await k8sFetch(`/api/v1/namespaces/${K8S_NAMESPACE}/pods/${podName}/log?tailLines=${tailLines}&timestamps=true&container=debezium-connect`);
    const logs = await lr.text();
    res.json({ logs, podName });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ═══════════════════════════════════════════════════════════════════════════
//  START CDC — fetch live offset position from source DB
// ═══════════════════════════════════════════════════════════════════════════

// GET /api/connectors/:source/:name/offset-position
// Returns the current SCN (Oracle) / LSN (Postgres) / timestamp (MongoDB)
// by exec-ing into the appropriate source pod via kubectl.
app.get('/api/connectors/:source/:name/offset-position', async (req, res) => {
  const { source } = req.params;
  try {
    if (source === 'oracle') {
      // Query Oracle for current SCN via JDBC — works for any Oracle instance.
      const connCfg = await oracleConnCfg({});   // use defaults from template/OpenBao
      const rows = await oracleQuery(connCfg, 'SELECT current_scn FROM v$database', 15000);
      const scn  = rows?.[0]?.[0];
      if (!scn) return res.status(502).json({ error: 'Could not read SCN from Oracle' });
      return res.json({ source, type: 'SCN', position: scn });

    } else if (source === 'postgres') {
      // Query Postgres for current WAL LSN
      const pod = await getSourcePod('postgresql', 'postgres');
      const { stdout } = await execFileAsync('kubectl', [
        'exec', '-n', K8S_NAMESPACE, pod, '--',
        'psql', '-U', 'postgres', '-d', 'cache_testing', '-tAc',
        "SELECT pg_current_wal_lsn()::text",
      ], { timeout: 10000 });
      const lsn = stdout.trim().split('\n').map(l => l.trim()).find(l => /^[0-9A-F]+\/[0-9A-F]+$/i.test(l));
      if (!lsn) return res.status(502).json({ error: 'Could not parse LSN from Postgres output', raw: stdout.slice(0, 300) });
      return res.json({ source, type: 'LSN', position: lsn });

    } else if (source === 'mongodb') {
      // Return current timestamp as a proxy for resume-token starting point.
      // Connect directly via MongoDB driver — no pod exec needed.
      const ts = Math.floor(Date.now() / 1000);
      return res.json({ source, type: 'Timestamp', position: JSON.stringify({ ts: { $timestamp: { t: ts, i: 1 } } }) });

    } else {
      return res.status(400).json({ error: `Unknown source: ${source}` });
    }
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ═══════════════════════════════════════════════════════════════════════════
//  TABLE / COLLECTION BROWSER
//  POST /api/schema/:source  — body: connector config object from the UI
//  Derives host/port/user/password/db dynamically from the connector config.
// ═══════════════════════════════════════════════════════════════════════════

// ── GET /api/schemas/:source ─────────────────────────────────────────────────
// Lightweight endpoint — returns only schema/database names (no tables).
// Used by the schema picker on the New Connector page so the user selects
// from real values instead of typing free text.
//   postgres → schemas in database.dbname  (query param: db=<name>)
//   oracle   → schema owners with tables
//   mongodb  → database names
app.get('/api/schemas/:source', async (req, res) => {
  const { source } = req.params;
  const db = req.query.db || 'cache_testing';
  try {
    if (source === 'postgres') {
      const pod = await getSourcePod('postgresql', 'postgres');
      const { stdout } = await execFileAsync('kubectl', [
        'exec', '-n', K8S_NAMESPACE, pod, '--',
        'psql', '-U', 'postgres', '-d', db, '-tAc',
        `SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT IN ('pg_catalog','information_schema','pg_toast') ORDER BY schema_name`,
      ], { timeout: 10000 });
      const schemas = stdout.trim().split('\n').map(l => l.trim()).filter(Boolean);
      return res.json({ schemas });

    } else if (source === 'oracle') {
      // Connect directly via JDBC — works for in-cluster and external Oracle.
      const connCfg = await oracleConnCfg({});
      const rows    = await oracleQuery(connCfg,
        `SELECT USERNAME FROM DBA_USERS WHERE ORACLE_MAINTAINED='N' ORDER BY USERNAME`);
      const schemas = rows.map(r => r[0]);
      return res.json({ schemas });

    } else if (source === 'mongodb') {
      // Connect directly via MongoDB driver — works for in-cluster and external MongoDB.
      const schemas = await withMongo({}, async (client) => {
        const result = await client.db('admin').command({ listDatabases: 1, nameOnly: true });
        return result.databases.map(d => d.name).filter(n => !['admin','config','local'].includes(n));
      });
      return res.json({ schemas });

    } else {
      return res.status(400).json({ error: `Unknown source: ${source}` });
    }
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/schema/:source', async (req, res) => {
  const { source } = req.params;
  const cfg = req.body || {};   // full connector config key/value object from the UI

  try {
    if (source === 'postgres') {
      // Use local peer-auth as postgres superuser inside the pod — no password needed.
      // The debezium user credentials in cfg are for the connector, not for schema browsing.
      const db  = cfg['database.dbname'] || '';
      const pod = await getSourcePod('postgresql', 'postgres');

      // 1. List all non-system databases
      const { stdout: dbOut } = await execFileAsync('kubectl', [
        'exec', '-n', K8S_NAMESPACE, pod, '--',
        'psql', '-U', 'postgres', '-tAc',
        `SELECT datname FROM pg_database WHERE datistemplate = false AND datname NOT IN ('template0','template1') ORDER BY datname`,
      ], { timeout: 10000 });

      const allDbs = dbOut.trim().split('\n').map(l => l.trim()).filter(Boolean);
      // If a specific db is configured show it first; otherwise show all
      const databases = db && allDbs.includes(db) ? [db, ...allDbs.filter(d => d !== db)] : allDbs;

      // 2. For each database list tables grouped by schema
      const result = {};
      for (const database of databases) {
        const { stdout } = await execFileAsync('kubectl', [
          'exec', '-n', K8S_NAMESPACE, pod, '--',
          'psql', '-U', 'postgres', '-d', database, '-tAc',
          `SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema NOT IN ('pg_catalog','information_schema') AND table_type = 'BASE TABLE' ORDER BY table_schema, table_name`,
        ], { timeout: 10000 });

        const schemas = {};
        stdout.trim().split('\n').filter(l => l.includes('|')).forEach(l => {
          const [schema, table] = l.split('|');
          if (!schemas[schema.trim()]) schemas[schema.trim()] = [];
          schemas[schema.trim()].push(table.trim());
        });
        if (Object.keys(schemas).length) result[database] = schemas;
      }
      res.json({ source: 'postgres', databases: result });

    } else if (source === 'mongodb') {
      // Connect directly via MongoDB driver — works for in-cluster and external MongoDB.
      const result = await withMongo(cfg, async (client) => {
        const dbList = await client.db('admin').command({ listDatabases: 1, nameOnly: true });
        const dbs = dbList.databases.map(d => d.name).filter(n => !['admin','config','local'].includes(n));
        const out = {};
        for (const database of dbs) {
          const cols = await client.db(database).listCollections().toArray();
          const names = cols.map(c => c.name).filter(n => !n.startsWith('system.'));
          if (names.length) out[database] = names;
        }
        return out;
      });
      res.json({ source: 'mongodb', databases: result });

    } else if (source === 'oracle') {
      // Connect directly via JDBC — works for in-cluster and external Oracle.
      const connCfg = await oracleConnCfg({});

      // 1. List non-system schemas
      const schemaRows = await oracleQuery(connCfg,
        `SELECT USERNAME FROM DBA_USERS WHERE ORACLE_MAINTAINED='N' ORDER BY USERNAME`);
      const schemas = schemaRows.map(r => r[0]);

      // 2. For each schema list tables
      const result = {};
      for (const schema of schemas) {
        const tableRows = await oracleQuery(connCfg,
          `SELECT TABLE_NAME FROM ALL_TABLES WHERE OWNER='${schema}' ORDER BY TABLE_NAME`);
        const tables = tableRows.map(r => r[0]);
        if (tables.length) result[schema] = tables;
      }
      res.json({ source: 'oracle', databases: result });

    } else {
      res.status(400).json({ error: `Unknown source: ${source}` });
    }
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Cache of source pod names (30s).
// Returns null when no pod is found — callers must handle null to support
// external (non-Kubernetes) databases where kubectl exec is not available.
const _srcPodCache = {};
async function getSourcePod(appLabel, source) {
  const key = appLabel;
  if (_srcPodCache[key] && Date.now() < _srcPodCache[key].exp) return _srcPodCache[key].name;
  const { stdout } = await execFileAsync('kubectl', [
    'get', 'pod', '-n', K8S_NAMESPACE,
    '-l', `app=${appLabel}`,
    '-o', 'jsonpath={.items[0].metadata.name}',
  ], { timeout: 10000 });
  const name = stdout.trim() || null;
  _srcPodCache[key] = { name, exp: Date.now() + 30000 };
  return name;   // null = no in-cluster pod (external DB)
}

// POST /api/connectors/:source/:name/start-from
// Deletes the connector and recreates it with snapshot.mode adjusted to
// the chosen start position so CDC begins exactly where requested.
app.post('/api/connectors/:source/:name/start-from', async (req, res) => {
  const { source, name } = req.params;
  const { mode, position } = req.body;   // mode: 'now' | 'live' | 'manual'

  try {
    // 1. Read current config
    const cfgR = await dbzRequest(source, 'GET', `/connectors/${name}/config`);
    if (cfgR.status >= 300) return res.status(cfgR.status).json({ error: 'Could not read connector config', detail: cfgR.body });
    const cfg = { ...cfgR.body };

    // 2. Patch config based on chosen mode
    if (mode === 'now') {
      // snapshot.mode=no_data: skip full snapshot, stream only new changes.
      // 'never' was renamed to 'no_data' in Debezium 2.x — use the current name.
      if (source === 'postgres') {
        cfg['snapshot.mode'] = 'no_data';
        delete cfg['slot.drop.on.stop'];
      } else if (source === 'oracle') {
        cfg['snapshot.mode'] = 'schema_only';
        cfg['log.mining.strategy'] = cfg['log.mining.strategy'] || 'online_catalog';
      } else if (source === 'mongodb') {
        cfg['snapshot.mode'] = 'no_data';
      }
    } else if (mode === 'live' || mode === 'manual') {
      // Pin to a specific SCN / LSN
      if (source === 'oracle') {
        cfg['snapshot.mode'] = 'schema_only';
        cfg['log.mining.start.scn'] = String(position);
      } else if (source === 'postgres') {
        // Debezium Postgres does NOT support LSN pinning via connector config.
        // The replication slot already tracks the correct WAL position; setting
        // snapshot.mode=no_data is sufficient — the slot will stream from wherever
        // it last left off, which is the safest recoverable starting point.
        cfg['snapshot.mode'] = 'no_data';
        delete cfg['slot.drop.on.stop'];
        // Remove any stale snapshot.lsn key that may have been set by older versions
        // of this code — it is not a real Debezium property and causes config validation errors.
        delete cfg['snapshot.lsn'];
      } else if (source === 'mongodb') {
        cfg['snapshot.mode'] = 'no_data';
        // MongoDB: use the resume token / timestamp provided
        cfg['initial.sync.max.threads'] = cfg['initial.sync.max.threads'] || '1';
        cfg['snapshot.filter.query.mode'] = cfg['snapshot.filter.query.mode'] || 'none';
        // Store the token so the user can see it was set; actual usage is per connector version
        cfg['mongodb.resume.token'] = String(position);
      }
    } else {
      return res.status(400).json({ error: `Unknown mode: ${mode}` });
    }

    // 3. Delete existing connector
    const delR = await dbzRequest(source, 'DELETE', `/connectors/${name}`);
    if (delR.status >= 300 && delR.status !== 404) {
      return res.status(delR.status).json({ error: 'Delete failed', detail: delR.body });
    }

    // 4. Small delay to allow Kafka Connect to fully release the connector
    await new Promise(r => setTimeout(r, 1500));

    // 5. Recreate connector with patched config
    const createR = await dbzRequest(source, 'POST', '/connectors', { name, config: cfg });
    res.status(createR.status).json({ message: `Connector "${name}" started (mode=${mode})`, config: cfg, response: createR.body });

  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ═══════════════════════════════════════════════════════════════════════════
//  START-WITH-TABLES
//  POST /api/connectors/:source/:name/start-with-tables
//  Body: { tables: ["schema.table", ...] }
//
//  Steps:
//   1. Validate supplemental logging is enabled for every requested table.
//      • Postgres  : checks pg_publication_tables — table must be in the
//                    connector's publication (all columns logged by default).
//      • Oracle    : checks ALL_LOG_GROUPS — table must have ALL COLUMNS
//                    supplemental logging (ADD ALL COLUMNS).
//      • MongoDB   : change-streams capture full documents by default —
//                    no extra logging config needed, check always passes.
//   2. If any table fails → HTTP 409 { logErrors, fixHint }
//   3. On pass → patch connector config (tasks.max=1, table/collection.include.list)
//      then PUT to Debezium.
// ═══════════════════════════════════════════════════════════════════════════
app.post('/api/connectors/:source/:name/start-with-tables', async (req, res) => {
  const { source, name } = req.params;
  // tables: the full desired list the UI wants after the operation.
  // replace=true → use tables as the exact final list (supports removal of unchecked tables).
  // replace=false/absent → merge with existing (legacy first-time assign behaviour).
  const { tables, replace } = req.body;

  if (!Array.isArray(tables) || tables.length === 0) {
    return res.status(400).json({ error: 'tables array required' });
  }

  try {
    // Fetch the live connector config once — used by both the logging check and the patch step.
    const cfgR = await dbzRequest(source, 'GET', `/connectors/${name}/config`);
    if (cfgR.status >= 300) {
      return res.status(cfgR.status).json({ error: 'Could not read connector config', detail: cfgR.body });
    }
    const cfg = { ...cfgR.body };

    // ── 1. Supplemental logging check ──────────────────────────────────────
    const logErrors = [];

    if (source === 'postgres') {
      // Postgres: every selected table must exist in the connector's publication.
      // If the publication was created WITH (publish_via_partition_root = true)
      // or is set to FOR ALL TABLES it covers everything — pass automatically.
      const pod = await getSourcePod('postgresql', 'postgres');

      // Get the publication name from the live connector config
      const pubName = cfg['publication.name'] || 'debezium_pub_cache_testing';

      // Check if publication covers ALL tables
      const { stdout: pubAll } = await execFileAsync('kubectl', [
        'exec', '-n', K8S_NAMESPACE, pod, '--',
        'psql', '-U', 'postgres', '-d', 'cache_testing', '-tAc',
        `SELECT puballtables FROM pg_publication WHERE pubname='${pubName}'`,
      ], { timeout: 10000 });

      if (pubAll.trim() === 't') {
        // Publication covers all tables — no per-table check needed
      } else {
        // Check each table is in the publication
        for (const full of tables) {
          const [schema, table] = full.split('.');
          const { stdout } = await execFileAsync('kubectl', [
            'exec', '-n', K8S_NAMESPACE, pod, '--',
            'psql', '-U', 'postgres', '-d', 'cache_testing', '-tAc',
            `SELECT COUNT(*) FROM pg_publication_tables WHERE pubname='${pubName}' AND schemaname='${schema}' AND tablename='${table}'`,
          ], { timeout: 10000 });
          if (stdout.trim() === '0') {
            logErrors.push({
              table: full,
              reason: `Not in publication "${pubName}" — run: ALTER PUBLICATION ${pubName} ADD TABLE ${full};`,
            });
          }
        }
      }

    } else if (source === 'oracle') {
      // Connect directly via JDBC — works for in-cluster and external Oracle.
      const connCfg = await oracleConnCfg(cfg);

      for (const full of tables) {
        const parts = full.split('.');
        const owner = (parts.length === 2 ? parts[0] : 'CACHE_TESTING').toUpperCase();
        const tbl   = (parts.length === 2 ? parts[1] : parts[0]).toUpperCase();
        const rows  = await oracleQuery(connCfg,
          `SELECT LOG_GROUP_TYPE FROM ALL_LOG_GROUPS WHERE OWNER='${owner}' AND TABLE_NAME='${tbl}' AND LOG_GROUP_TYPE='ALL COLUMN LOGGING'`);
        if (!rows.length) {
          logErrors.push({ table: full, reason: 'ALL COLUMNS supplemental logging not enabled' });
        }
      }

    } else if (source === 'mongodb') {
      // MongoDB change-streams capture full documents — no extra logging config.
      // Check always passes.
    }

    // ── 2. Return errors if any ─────────────────────────────────────────────
    if (logErrors.length > 0) {
      const fixHint = source === 'postgres'
        ? `For each table run: ALTER PUBLICATION <pub_name> ADD TABLE <schema>.<table>;`
        : source === 'oracle'
        ? `For each table run: ALTER TABLE <owner>.<table> ADD SUPPLEMENTAL LOG DATA (ALL COLUMNS);`
        : '';
      return res.status(409).json({ logErrors, fixHint });
    }

    // ── 3. Patch connector config ────────────────────────────────────────────
    const includeKey = source === 'mongodb' ? 'collection.include.list' : 'table.include.list';
    const removeKey  = source === 'mongodb' ? 'table.include.list'       : 'collection.include.list';

    // replace=true: the UI sends the exact final desired list (add + remove).
    // replace=false: merge into existing list (first-time assign, no removal).
    let finalList;
    if (replace) {
      finalList = [...new Set(tables)];
    } else {
      const existing = (cfg[includeKey] || '').split(',').map(t => t.trim()).filter(Boolean);
      finalList = [...new Set([...existing, ...tables])];
    }

    cfg['tasks.max']  = '1';
    cfg[includeKey]   = finalList.join(',');
    delete cfg[removeKey];

    // Inject live credentials from OpenBao before writing
    const patchedCfg = await injectCredentials(source, cfg);

    const putR = await dbzRequest(source, 'PUT', `/connectors/${name}/config`, patchedCfg);
    if (putR.status >= 300) {
      return res.status(putR.status).json({ error: 'Debezium rejected config update', detail: putR.body });
    }

    // ── 4. Resume the connector (it was paused at creation) ─────────────────
    // PUT /config on a paused connector restarts tasks automatically in most
    // Debezium versions, but an explicit resume guarantees CDC starts.
    await new Promise(r => setTimeout(r, 1000));
    await dbzRequest(source, 'PUT', `/connectors/${name}/resume`);

    // ── 5. Rolling-restart all streaming jobs ────────────────────────────────
    // Each deployment runs all sources — restart all of them so they pick up
    // any new/removed Kafka topics after the connector table list changes.
    try {
      const labelSel = `app=kafka-to-iceberg,pipeline.type=streaming`;
      const djR = await k8sFetch(
        `/apis/apps/v1/namespaces/${K8S_NAMESPACE}/deployments?labelSelector=${encodeURIComponent(labelSel)}`
      );
      const djBody = await djR.json();
      const deps = djBody.items || [];
      const restartPatch = {
        spec: {
          template: {
            metadata: {
              annotations: { 'kubectl.kubernetes.io/restartedAt': new Date().toISOString() },
            },
          },
        },
      };
      await Promise.all(deps.map(d =>
        k8sFetch(
          `/apis/apps/v1/namespaces/${K8S_NAMESPACE}/deployments/${d.metadata.name}`,
          { method: 'PATCH', headers: { 'Content-Type': 'application/strategic-merge-patch+json' }, body: JSON.stringify(restartPatch) }
        )
      ));
    } catch (_) { /* non-fatal — connector update already succeeded */ }

    res.json({ ok: true, tables, config: patchedCfg, streamingJobsRestarted: true });

  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ═══════════════════════════════════════════════════════════════════════════
//  KUBERNETES STREAMING JOBS
// ═══════════════════════════════════════════════════════════════════════════

// GET /api/streaming/configmap — read shared kafka-to-iceberg-config ConfigMap
app.get('/api/streaming/configmap', async (req, res) => {
  try {
    const r = await k8sFetch(`/api/v1/namespaces/${K8S_NAMESPACE}/configmaps/kafka-to-iceberg-config`);
    const body = await r.json();
    res.status(r.status).json({ data: body.data || {} });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// PATCH /api/streaming/configmap — update one or more keys in the ConfigMap
app.patch('/api/streaming/configmap', async (req, res) => {
  try {
    const { data } = req.body;
    if (!data || typeof data !== 'object') return res.status(400).json({ error: 'data object required' });
    const patch = { data };
    const r = await k8sFetch(
      `/api/v1/namespaces/${K8S_NAMESPACE}/configmaps/kafka-to-iceberg-config`,
      { method: 'PATCH', headers: { 'Content-Type': 'application/merge-patch+json' }, body: JSON.stringify(patch) }
    );
    const body = await r.json();
    res.status(r.status).json({ data: body.data || {} });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// GET /api/streaming/:name/env — read deployment env vars (container[0])
app.get('/api/streaming/:name/env', async (req, res) => {
  try {
    const r = await k8sFetch(`/apis/apps/v1/namespaces/${K8S_NAMESPACE}/deployments/${req.params.name}`);
    const dep = await r.json();
    if (!r.ok) return res.status(r.status).json(dep);
    const env = dep?.spec?.template?.spec?.containers?.[0]?.env || [];
    res.json({ env });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// PATCH /api/streaming/:name/env — set/add/delete env vars on a deployment
// Body: { set: [{name, value}], delete: ['VAR_NAME'] }
app.patch('/api/streaming/:name/env', async (req, res) => {
  try {
    const { name } = req.params;
    const { set = [], del = [] } = req.body;

    // Read current deployment
    const dr = await k8sFetch(`/apis/apps/v1/namespaces/${K8S_NAMESPACE}/deployments/${name}`);
    const dep = await dr.json();
    if (!dr.ok) return res.status(dr.status).json(dep);

    let env = dep?.spec?.template?.spec?.containers?.[0]?.env || [];

    // Delete vars
    if (del.length) env = env.filter(e => !del.includes(e.name));

    // Set/update vars (only direct value= entries, not configMapKeyRef ones)
    for (const { name: varName, value } of set) {
      const idx = env.findIndex(e => e.name === varName);
      if (idx >= 0) {
        env[idx] = { name: varName, value: String(value) };
      } else {
        env.push({ name: varName, value: String(value) });
      }
    }

    const patch = { spec: { template: { spec: { containers: [{ name: dep.spec.template.spec.containers[0].name, env }] } } } };
    const r = await k8sFetch(
      `/apis/apps/v1/namespaces/${K8S_NAMESPACE}/deployments/${name}`,
      { method: 'PATCH', headers: { 'Content-Type': 'application/strategic-merge-patch+json' }, body: JSON.stringify(patch) }
    );
    const body = await r.json();
    res.status(r.status).json({ ok: r.ok, env: body?.spec?.template?.spec?.containers?.[0]?.env || [] });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// List kafka-to-iceberg deployments
app.get('/api/streaming/deployments', async (req, res) => {
  try {
    const r = await k8sFetch(`/apis/apps/v1/namespaces/${K8S_NAMESPACE}/deployments?labelSelector=app%3Dkafka-to-iceberg`);
    const body = await r.json();
    res.status(r.status).json(body);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Scale deployment (start=1, stop=0)
app.post('/api/streaming/:name/scale', async (req, res) => {
  try {
    const { name } = req.params;
    const { replicas } = req.body;
    const patch = { spec: { replicas: Number(replicas) } };
    const r = await k8sFetch(
      `/apis/apps/v1/namespaces/${K8S_NAMESPACE}/deployments/${name}`,
      { method: 'PATCH', headers: { 'Content-Type': 'application/merge-patch+json' }, body: JSON.stringify(patch) }
    );
    const body = await r.json();
    res.status(r.status).json(body);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Rollout restart
app.post('/api/streaming/:name/restart', async (req, res) => {
  try {
    const { name } = req.params;
    const patch = { spec: { template: { metadata: { annotations: { 'kubectl.kubernetes.io/restartedAt': new Date().toISOString() } } } } };
    const r = await k8sFetch(
      `/apis/apps/v1/namespaces/${K8S_NAMESPACE}/deployments/${name}`,
      { method: 'PATCH', headers: { 'Content-Type': 'application/strategic-merge-patch+json' }, body: JSON.stringify(patch) }
    );
    const body = await r.json();
    res.status(r.status).json(body);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// GET /api/streaming/:name/topics — list Kafka topics relevant to this job's source,
// and return the current TABLE_FILTER env var value so the UI can show active/available.
app.get('/api/streaming/:name/topics', async (req, res) => {
  try {
    const { name } = req.params;

    // 1. Read the deployment env to get SOURCE and TABLE_FILTER
    const dr = await k8sFetch(`/apis/apps/v1/namespaces/${K8S_NAMESPACE}/deployments/${name}`);
    const dep = await dr.json();
    if (!dr.ok) return res.status(dr.status).json(dep);
    const env = dep?.spec?.template?.spec?.containers?.[0]?.env || [];
    const getEnv = (k) => (env.find(e => e.name === k)?.value || '').trim();
    const sourceEnv      = getEnv('SOURCE').toLowerCase();
    const tableFilterRaw = getEnv('TABLE_FILTER');

    // 2. Determine which source prefixes apply to this job
    const allPrefixes = { postgres: 'postgres.', oracle: 'oracle.', mongodb: 'mongodb.' };
    const sources = sourceEnv ? [sourceEnv] : Object.keys(allPrefixes);

    // 3. Pull all Kafka topics via the shared kafkaExec helper
    const stdout = await kafkaExec(
      `/kafka/bin/kafka-topics.sh --bootstrap-server ${KAFKA_BOOTSTRAP} --list --command-config ${SASL_CFG_FILE}`
    );
    const allTopics = stdout.split('\n')
      .map(t => t.trim())
      .filter(t => t && !t.startsWith('__') && !t.startsWith('SLF4J'));

    // 4. Filter to topics relevant to this job's source(s)
    const jobTopics = allTopics.filter(t =>
      sources.some(src => t.startsWith(allPrefixes[src] || (src + '.')))
    ).sort();

    // 5. Parse current TABLE_FILTER into a list of active topic names
    const activeFilter = tableFilterRaw
      ? tableFilterRaw.split(',').map(t => t.trim()).filter(Boolean)
      : [];

    res.json({ topics: jobTopics, tableFilter: activeFilter, sources });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Get streaming pod logs — find pod via the deployment's own pod-template labels
app.get('/api/streaming/:name/logs', async (req, res) => {
  try {
    const { name } = req.params;
    // Fetch the deployment to get its matchLabels selector
    const dr = await k8sFetch(`/apis/apps/v1/namespaces/${K8S_NAMESPACE}/deployments/${name}`);
    const dep = await dr.json();
    const matchLabels = dep?.spec?.selector?.matchLabels || {};
    // Build labelSelector string from all matchLabels
    const selector = Object.entries(matchLabels)
      .map(([k, v]) => `${encodeURIComponent(k)}=${encodeURIComponent(v)}`)
      .join(',');
    const r = await k8sFetch(`/api/v1/namespaces/${K8S_NAMESPACE}/pods?labelSelector=${selector}`);
    const body = await r.json();
    const pod = body.items && body.items[0];
    if (!pod) return res.json({ logs: 'No pod found for deployment ' + name });
    const podName = pod.metadata.name;
    const lr = await k8sFetch(`/api/v1/namespaces/${K8S_NAMESPACE}/pods/${podName}/log?tailLines=150&timestamps=true`);
    const logs = await lr.text();
    res.json({ logs, podName });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ═══════════════════════════════════════════════════════════════════════════
//  SPARK CLUSTER  — master topology + per-node K8s resource pressure
// ═══════════════════════════════════════════════════════════════════════════
const SPARK_MASTER_UI = process.env.SPARK_MASTER_UI || 'http://spark-master-svc.prod.svc.cluster.local:8080';

// GET /api/spark/cluster
// Returns:
//   master   — Spark master summary (status, cores, memory)
//   workers  — array of Spark worker objects from /json/
//   apps     — active + completed app counts
//   nodes    — K8s node metrics (cpu/memory usage + capacity) keyed by hostname
app.get('/api/spark/cluster', async (req, res) => {
  // Run Spark master JSON fetch and K8s node metrics in parallel — failures are
  // non-fatal so the UI can show partial data gracefully.
  const [sparkResult, nodeMetricsResult, nodeInfoResult] = await Promise.all([
    fetch(`${SPARK_MASTER_UI}/json/`, { timeout: 6000 })
      .then(r => r.json())
      .catch(e => ({ _error: e.message })),
    k8sFetch('/apis/metrics.k8s.io/v1beta1/nodes')
      .then(r => r.json())
      .catch(e => ({ _error: e.message })),
    k8sFetch('/api/v1/nodes')
      .then(r => r.json())
      .catch(e => ({ _error: e.message })),
  ]);

  // ── Parse Spark master response ──────────────────────────────────────────
  const sparkError = sparkResult._error || null;
  const masterStatus = sparkError ? 'UNREACHABLE' : (sparkResult.status || 'UNKNOWN');
  const workers      = sparkResult.workers     || [];
  const activeApps   = sparkResult.activeapps  || [];
  const completedApps= sparkResult.completedapps || [];

  // ── Parse K8s node capacity ──────────────────────────────────────────────
  // Build a map: nodeName → { cpuCapacity (millicores), memCapacity (bytes), roles }
  const nodeCapacity = {};
  for (const n of (nodeInfoResult.items || [])) {
    const name = n.metadata.name;
    const cap  = n.status?.capacity || {};
    const alloc= n.status?.allocatable || {};
    const roles= Object.keys(n.metadata.labels || {})
      .filter(l => l.startsWith('node-role.kubernetes.io/'))
      .map(l => l.replace('node-role.kubernetes.io/', ''));
    nodeCapacity[name] = {
      cpuCap:    parseCpu(cap.cpu || '0'),
      memCap:    parseMem(cap.memory || '0'),
      cpuAlloc:  parseCpu(alloc.cpu || '0'),
      memAlloc:  parseMem(alloc.memory || '0'),
      roles:     roles.length ? roles : ['worker'],
    };
  }

  // ── Parse K8s node metrics ────────────────────────────────────────────────
  // Build a map: nodeName → { cpuUsage (millicores), memUsage (bytes) }
  const nodeUsage = {};
  for (const n of (nodeMetricsResult.items || [])) {
    nodeUsage[n.metadata.name] = {
      cpuUsage: parseCpu(n.usage?.cpu || '0'),
      memUsage: parseMem(n.usage?.memory || '0'),
    };
  }

  // ── Merge into nodes array ────────────────────────────────────────────────
  const allNodeNames = new Set([
    ...Object.keys(nodeCapacity),
    ...Object.keys(nodeUsage),
  ]);
  const nodes = [...allNodeNames].map(name => {
    const cap = nodeCapacity[name] || {};
    const use = nodeUsage[name]    || {};
    // Match this K8s node to a Spark worker by hostname substring
    const sparkWorker = workers.find(w => {
      const whost = (w.host || w.hostname || '').toLowerCase();
      return whost === name.toLowerCase() || name.toLowerCase().startsWith(whost) || whost.startsWith(name.toLowerCase().split('.')[0]);
    });
    return {
      name,
      roles:       cap.roles || ['worker'],
      cpuCap:      cap.cpuCap   || 0,
      cpuAlloc:    cap.cpuAlloc || 0,
      cpuUsage:    use.cpuUsage || 0,
      memCap:      cap.memCap   || 0,
      memAlloc:    cap.memAlloc || 0,
      memUsage:    use.memUsage || 0,
      // Spark worker fields (may be null if this node has no Spark worker)
      sparkState:  sparkWorker?.state    || null,
      sparkCores:  sparkWorker?.cores    || 0,
      sparkCoresFree: sparkWorker?.coresfree || 0,
      sparkMemMB:  sparkWorker?.memory   || 0,
      sparkMemFreeMB: sparkWorker?.memoryfree || 0,
    };
  });

  res.json({
    sparkError,
    master: {
      status:       masterStatus,
      url:          sparkResult.url || SPARK_MASTER_UI,
      coresTotal:   sparkResult.cores        || 0,
      coresUsed:    sparkResult.coresused    || 0,
      memTotalMB:   sparkResult.memory       || 0,
      memUsedMB:    sparkResult.memoryused   || 0,
      workers:      workers.length,
      workersAlive: workers.filter(w => w.state === 'ALIVE').length,
      activeApps:   activeApps.length,
      completedApps:completedApps.length,
    },
    workers,
    activeApps,
    nodes,
  });
});

// Parse Kubernetes CPU quantity → millicores (integer)
function parseCpu(q) {
  if (!q) return 0;
  q = String(q);
  if (q.endsWith('m')) return parseInt(q, 10);
  if (q.endsWith('n')) return Math.round(parseInt(q, 10) / 1e6);
  return Math.round(parseFloat(q) * 1000);
}

// Parse Kubernetes memory quantity → bytes (integer)
function parseMem(q) {
  if (!q) return 0;
  q = String(q);
  const units = { Ki: 1024, Mi: 1024**2, Gi: 1024**3, Ti: 1024**4, K: 1000, M: 1000**2, G: 1000**3 };
  for (const [suffix, mult] of Object.entries(units)) {
    if (q.endsWith(suffix)) return Math.round(parseFloat(q) * mult);
  }
  return parseInt(q, 10) || 0;
}


// ═══════════════════════════════════════════════════════════════════════════
//  KAFKA TOPICS  (via Debezium worker kafka-topics.sh)
// ═══════════════════════════════════════════════════════════════════════════
const { exec } = require('child_process');
const util = require('util');
const execAsync = util.promisify(exec);

// /kafka/config is part of the image overlay — survives container restarts.
// /tmp is a fresh writable layer on every restart and gets wiped.
const SASL_CFG_FILE = '/kafka/config/kafka-sasl.properties';

// Resolve the debezium-connect-postgres pod name (cached 30s)
let _dbzPodName = null;
let _dbzPodExpiry = 0;
async function getDbzPod() {
  if (_dbzPodName && Date.now() < _dbzPodExpiry) return _dbzPodName;
  const { stdout } = await execAsync(
    `kubectl get pod -n ${K8S_NAMESPACE} -l app=debezium-connect-postgres -o jsonpath='{.items[0].metadata.name}'`,
    { shell: '/bin/bash' }
  );
  const newPod = stdout.trim();
  // If the pod changed (restart), the SASL file in the old pod is gone — force re-write.
  if (newPod !== _dbzPodName) _saslWrittenForPod = null;  // pod changed — force re-write
  _dbzPodName = newPod;
  _dbzPodExpiry = Date.now() + 30000;
  return _dbzPodName;
}

// Kafka exec helper — uses spawnSync to pipe the SASL config via stdin into
// `tee` inside the Debezium pod (requires pods/exec RBAC), then runs the
// kafka CLI command. All arg passing uses array form to avoid shell quoting.
const { execFile, spawnSync } = require('child_process');
const execFileAsync = util.promisify(execFile);

const SASL_PROPS =
  'security.protocol=SASL_PLAINTEXT\n' +
  'sasl.mechanism=SCRAM-SHA-512\n' +
  'sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required ' +
  'username="debezium-user" password="i3uqKrPOaoqWo6JfOZrmSMhtdp7LiN3H";\n';

// Write the SASL properties file into every Debezium pod on first use.
// /kafka/config survives container restarts (image overlay layer).
// We track the last pod written to — if the pod changes (rollout, scale)
// we re-write. No existence check needed because the path is stable.
let _saslWrittenForPod = null;

function ensureSaslFileSync(pod) {
  if (_saslWrittenForPod === pod) return;  // already written to this pod
  const r = spawnSync('kubectl', [
    'exec', '-i', '-n', K8S_NAMESPACE, pod, '-c', 'debezium-connect', '--', 'tee', SASL_CFG_FILE
  ], { input: SASL_PROPS, encoding: 'utf8', timeout: 10000 });
  if (r.status !== 0) throw new Error(`SASL file write failed: ${r.stderr}`);
  _saslWrittenForPod = pod;
}

// 256 MB — large enough for full topic scans with Debezium JSON payloads
const KAFKA_EXEC_MAX_BUFFER = 256 * 1024 * 1024;

// kafkaExec: run a one-shot kafka-* command inside the Debezium pod.
// When called during a compare job pass signal= so the child is killed on abort.
// The child is always registered in _compareChildProcesses so /api/compare/abort
// can SIGKILL it even before the AbortController event propagates.
async function kafkaExec(argsStr, timeoutMs = 20000, signal) {
  const pod = await getDbzPod();
  ensureSaslFileSync(pod);
  if (signal?.aborted) throw Object.assign(new Error('Compare job aborted by user'), { name: 'AbortError' });
  const { spawn } = require('child_process');
  const args = ['exec', '-n', K8S_NAMESPACE, pod, '-c', 'debezium-connect', '--', ...argsStr.trim().split(/\s+/)];
  return new Promise((resolve, reject) => {
    const child = spawn('kubectl', args, { stdio: ['ignore', 'pipe', 'pipe'] });
    _compareChildProcesses.add(child);
    child.on('exit', () => _compareChildProcesses.delete(child));

    let stdoutBuf = '';
    let stderrBuf = '';
    child.stdout.on('data', c => { stdoutBuf += c.toString(); });
    child.stderr.on('data', c => { stderrBuf += c.toString(); });

    const onAbort = () => {
      try { child.kill('SIGKILL'); } catch {}
      reject(Object.assign(new Error('Compare job aborted by user'), { name: 'AbortError' }));
    };
    signal?.addEventListener('abort', onAbort, { once: true });

    const timer = setTimeout(() => {
      try { child.kill('SIGKILL'); } catch {}
      signal?.removeEventListener('abort', onAbort);
      reject(new Error(`kafkaExec timed out after ${timeoutMs}ms`));
    }, timeoutMs);

    child.on('close', code => {
      clearTimeout(timer);
      signal?.removeEventListener('abort', onAbort);
      if (signal?.aborted) return; // already rejected by onAbort
      if (code === 0 || stdoutBuf) resolve(stdoutBuf);
      else reject(new Error(`kafkaExec exited ${code}: ${stderrBuf.slice(0, 300)}`));
    });
    child.on('error', err => {
      clearTimeout(timer);
      signal?.removeEventListener('abort', onAbort);
      reject(err);
    });
  });
}

// List topics
app.get('/api/kafka/topics', async (req, res) => {
  try {
    const stdout = await kafkaExec(
      `/kafka/bin/kafka-topics.sh --bootstrap-server ${KAFKA_BOOTSTRAP} --list --command-config ${SASL_CFG_FILE}`
    );
    const topics = stdout.split('\n').filter(t => t.trim() && !t.startsWith('__') && !t.startsWith('SLF4J')).sort();
    res.json({ topics });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Get topic offsets
app.get('/api/kafka/topics/:topic/offsets', async (req, res) => {
  try {
    const { topic } = req.params;
    const stdout = await kafkaExec(
      `/kafka/bin/kafka-get-offsets.sh --bootstrap-server ${KAFKA_BOOTSTRAP} --topic ${topic} --time latest --command-config ${SASL_CFG_FILE}`
    );
    const partitions = stdout.trim().split('\n')
      .filter(l => l.includes(':'))
      .map(l => {
        const parts = l.split(':');
        return { topic: parts[0], partition: Number(parts[1]), offset: Number(parts[2]) };
      }).filter(p => p.topic);
    res.json({ partitions });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Read N messages from a topic (KSQL-style peek)
app.post('/api/kafka/query', async (req, res) => {
  try {
    const { topic, partition = 0, offset = 'latest', limit = 20 } = req.body;

    let offsetArg = offset;
    if (offset === 'latest') {
      const offOut = await kafkaExec(
        `/kafka/bin/kafka-get-offsets.sh --bootstrap-server ${KAFKA_BOOTSTRAP} --topic ${topic} --time latest --command-config ${SASL_CFG_FILE}`
      );
      const matched = offOut.match(new RegExp(`:${partition}:(\\d+)`));
      const latestOff = matched ? Number(matched[1]) : 0;
      offsetArg = Math.max(0, latestOff - Number(limit));
    }

    const stdout = await kafkaExec(
      `/kafka/bin/kafka-console-consumer.sh --bootstrap-server ${KAFKA_BOOTSTRAP}` +
      ` --topic ${topic} --partition ${partition} --offset ${offsetArg}` +
      ` --max-messages ${limit} --timeout-ms 12000` +
      ` --consumer.config ${SASL_CFG_FILE}`,
      25000
    );

    const messages = stdout.split('\n').filter(l => l.trim() && !l.startsWith('SLF4J') && !l.startsWith('[')).map(l => {
      try { return { raw: l, parsed: JSON.parse(l) }; }
      catch { return { raw: l, parsed: null }; }
    });
    res.json({ messages });
  } catch (e) { res.status(500).json({ error: e.message, messages: [] }); }
});

// ── POST /api/kafka/tail — fast tail: last N messages across ALL partitions ──
// Supports optional `where` clause for server-side filtering.
// When `where` is provided, reads a larger scan batch and filters matching rows.
//
// where syntax (applied against expanded Debezium after/before fields + _op):
//   field = value          →  exact match  (e.g.  id = 123  or  email = test@example.com)
//   field != value         →  not equal
//   field LIKE pattern     →  substring match (case-insensitive, % is wildcard)
//   field > value          →  numeric greater-than
//   field < value          →  numeric less-than
//   Multiple conditions joined by AND are supported.
app.post('/api/kafka/tail', async (req, res) => {
  try {
    const { topic, limit = 20, where = '' } = req.body;
    if (!topic) return res.status(400).json({ error: 'topic is required' });
    const n = Math.min(Number(limit) || 20, 500);

    // When filtering, scan a larger window so we have enough candidates
    const hasFilter = where && where.trim();
    const scanMultiplier = hasFilter ? 50 : 1;   // scan 50× more when filtering

    // ── Parse WHERE clause into a predicate function ──────────────────────
    // Supports: field = val, field != val, field LIKE pat, field > val, field < val
    // Multiple conditions joined by AND.
    function buildPredicate(whereStr) {
      if (!whereStr || !whereStr.trim()) return () => true;
      const COND_RE = /(\w+)\s*(=|!=|LIKE|>|<)\s*'?([^'AND\s][^']*?)'?\s*(?=AND\s|$)/gi;
      const conditions = [];
      let m;
      while ((m = COND_RE.exec(whereStr)) !== null) {
        conditions.push({ field: m[1], op: m[2].toUpperCase(), value: m[3].trim() });
      }
      if (!conditions.length) return () => true;
      return (row) => conditions.every(({ field, op, value }) => {
        const rv = row[field] !== undefined ? String(row[field]) : (row[field.toUpperCase()] !== undefined ? String(row[field.toUpperCase()]) : null);
        if (rv === null) return false;
        switch (op) {
          case '=':    return rv === value || rv.toLowerCase() === value.toLowerCase();
          case '!=':   return rv !== value && rv.toLowerCase() !== value.toLowerCase();
          case 'LIKE': {
            const pat = value.replace(/%/g, '.*').replace(/_/g, '.');
            return new RegExp(pat, 'i').test(rv);
          }
          case '>':    return parseFloat(rv) > parseFloat(value);
          case '<':    return parseFloat(rv) < parseFloat(value);
          default:     return true;
        }
      });
    }
    const predicate = buildPredicate(where);

    // Step 1: get latest offsets for all partitions
    const offOut = await kafkaExec(
      `/kafka/bin/kafka-get-offsets.sh --bootstrap-server ${KAFKA_BOOTSTRAP}` +
      ` --topic ${topic} --time latest --command-config ${SASL_CFG_FILE}`,
      15000
    );
    const partitions = offOut.trim().split('\n')
      .filter(l => l.trim() && !l.startsWith('SLF4J'))
      .map(l => { const p = l.split(':'); return { partition: Number(p[1]), latest: Number(p[2]) }; })
      .filter(p => !isNaN(p.partition) && p.latest > 0);

    if (!partitions.length) return res.json({ rows: [], columns: [], scanned: 0, matched: 0 });

    // Step 2: per-partition read count
    const perPart = Math.max(1, Math.ceil((n * scanMultiplier) / partitions.length));

    // Step 3: read each partition in parallel
    const reads = partitions.map(({ partition, latest }) => {
      const startOffset = Math.max(0, latest - perPart);
      const maxMsgs     = Math.min(perPart, latest - startOffset);
      return kafkaExec(
        `/kafka/bin/kafka-console-consumer.sh --bootstrap-server ${KAFKA_BOOTSTRAP}` +
        ` --topic ${topic} --partition ${partition} --offset ${startOffset}` +
        ` --max-messages ${maxMsgs} --timeout-ms 8000` +
        ` --consumer.config ${SASL_CFG_FILE}`,
        15000
      ).catch(() => '');
    });

    const results = await Promise.all(reads);

    // Step 4: parse, expand Debezium payload, apply WHERE filter
    const allRows = [];
    for (const stdout of results) {
      for (const line of stdout.split('\n')) {
        const l = line.trim();
        if (!l || l.startsWith('SLF4J') || l.startsWith('[')) continue;
        try {
          const msg   = JSON.parse(l);
          const pay   = msg?.payload ?? msg;
          const after = pay?.after ?? pay?.before ?? pay ?? {};
          const row   = { _raw: l, _op: pay?.op || 'r', ...after };
          if (predicate(row)) allRows.push(row);
        } catch {
          const row = { _raw: l };
          if (predicate(row)) allRows.push(row);
        }
      }
    }

    const scanned = allRows.length;

    // Derive columns from matching rows
    const colSet = new Set();
    allRows.forEach(r => Object.keys(r).forEach(k => { if (k !== '_raw' && k !== '_op') colSet.add(k); }));
    const columns = ['_op', ...colSet, '_raw'];

    // Return last `n` matching rows
    const rows = allRows.slice(-n).map(r => columns.map(c => r[c] ?? null));

    res.json({ columns, rows, scanned, matched: allRows.length });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ═══════════════════════════════════════════════════════════════════════════
//  CONNECTOR PRE-FLIGHT CHECK
//  GET /api/connectors/preflight/:source
//  Verifies:
//    1. OpenBao has a non-empty password at secret/platform/<source>
//    2. spark-external-catalogs ConfigMap has a matching entry
//  Returns { ok: true, catalog, baoPath } on success,
//  or { ok: false, error, action } with HTTP 422 on failure.
// ═══════════════════════════════════════════════════════════════════════════

const PREFLIGHT_REQUIREMENTS = {
  postgres: { type: 'database', value: 'cache_testing', baoPath: BAO_PATH.postgres },
  mongodb:  { type: 'database', value: 'cache_testing', baoPath: BAO_PATH.mongodb  },
  oracle:   { type: 'schema',   value: 'CACHE_TESTING', baoPath: BAO_PATH.oracle   },
};

app.get('/api/connectors/preflight/:source', async (req, res) => {
  const { source } = req.params;
  const req_ = PREFLIGHT_REQUIREMENTS[source];
  if (!req_) return res.status(400).json({ ok: false, error: `Unknown source: ${source}` });

  try {
    // ── 1. Check OpenBao has credentials for this source ──────────────────
    let creds;
    try {
      creds = await baoRead(req_.baoPath);
    } catch (e) {
      return res.status(422).json({
        ok: false,
        error: `Cannot read credentials from OpenBao at "${req_.baoPath}": ${e.message}`,
        action: `Ensure OpenBao is unsealed and the secret "${req_.baoPath}" exists with a non-empty "password" field.`,
      });
    }
    const password = creds?.password ?? creds?.cdc_password ?? '';
    if (!password) {
      return res.status(422).json({
        ok: false,
        error: `OpenBao secret "${req_.baoPath}" exists but has no "password" field.`,
        action: `Write the correct password into OpenBao at path "${req_.baoPath}" before creating this connector.`,
      });
    }

    // ── 2. Check spark-external-catalogs ConfigMap ────────────────────────
    const cmR = await k8sFetch(`/api/v1/namespaces/${K8S_NAMESPACE}/configmaps/spark-external-catalogs`);
    if (!cmR.ok) {
      return res.status(422).json({
        ok: false,
        error: `Spark external catalog ConfigMap "spark-external-catalogs" not found (HTTP ${cmR.status}).`,
        action: `Create the Spark external catalog for the ${source} source (${req_.type} "${req_.value}") first, then retry.`,
      });
    }
    const cm = await cmR.json();
    const catalogData = cm.data || {};
    const required = req_.value.toLowerCase();
    const found = Object.entries(catalogData).find(([k, v]) => {
      const haystack = (k + ' ' + v).toLowerCase();
      return haystack.includes(required) && haystack.includes(source);
    });
    if (!found) {
      return res.status(422).json({
        ok: false,
        error: `No Spark external catalog found for source "${source}" with ${req_.type} "${req_.value}" in ConfigMap "spark-external-catalogs".`,
        action: `Go to Spark and create an external catalog for the ${source} source pointing to ${req_.type} "${req_.value}". Ensure its credentials are stored in OpenBao at "${req_.baoPath}" before creating this connector.`,
        catalogData,
      });
    }

    res.json({ ok: true, catalog: found[0], baoPath: req_.baoPath });

  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// ═══════════════════════════════════════════════════════════════════════════
//  NEW CONNECTOR TEMPLATE
//  GET /api/connectors/template/:source
//  Reads credentials directly from OpenBao — K8s secrets never used.
// ═══════════════════════════════════════════════════════════════════════════
app.get('/api/connectors/template/:source', async (req, res) => {
  const { source } = req.params;
  try {
    // Read all credentials from OpenBao
    const [pg, ora, mg, kafka] = await Promise.all([
      baoRead(BAO_PATH.postgres),
      baoRead(BAO_PATH.oracle),
      baoRead(BAO_PATH.mongodb),
      baoRead(BAO_PATH.kafka),
    ]);

    const pgPassword    = pg.password;
    const oraPassword   = ora.cdc_password ?? ora.password;
    const mgoPassword   = mg.password;
    const kafkaSaslJaas = `org.apache.kafka.common.security.scram.ScramLoginModule required username="${kafka.debezium_user}" password="${kafka.debezium_password}";`;

    // Common schema-history Kafka config for Oracle (needs SASL)
    const oraSchemaHistory = {
      'schema.history.internal.kafka.bootstrap.servers': KAFKA_BOOTSTRAP,
      'schema.history.internal.kafka.topic': 'schema-history.oracle',
      'schema.history.internal.producer.security.protocol': 'SASL_PLAINTEXT',
      'schema.history.internal.producer.sasl.mechanism': 'SCRAM-SHA-512',
      'schema.history.internal.producer.sasl.jaas.config': kafkaSaslJaas,
      'schema.history.internal.consumer.security.protocol': 'SASL_PLAINTEXT',
      'schema.history.internal.consumer.sasl.mechanism': 'SCRAM-SHA-512',
      'schema.history.internal.consumer.sasl.jaas.config': kafkaSaslJaas,
    };

    const templates = {
      postgres: {
        'connector.class':                       'io.debezium.connector.postgresql.PostgresConnector',
        'tasks.max':                             '1',
        // topic.prefix includes the database name so that the RegexRouter SMT below
        // can strip the schema segment and produce postgres.cache_testing.<table>
        // (matching the Kafka topic names and Spark subscribePattern).
        'topic.prefix':                          'postgres.cache_testing',
        'database.hostname':                     'postgresql.prod.svc.cluster.local',
        'database.port':                         '5432',
        'database.user':                         'debezium',
        'database.password':                     pgPassword,
        'database.dbname':                       'cache_testing',
        'slot.name':                             'debezium_cache_testing',
        'publication.name':                      'debezium_pub_cache_testing',
        'plugin.name':                           'pgoutput',
        'schema.include.list':                   'public',
        'snapshot.mode':                         'no_data',
        'decimal.handling.mode':                 'double',
        'time.precision.mode':                   'connect',
        'tombstones.on.delete':                  'false',
        'key.converter':                         'org.apache.kafka.connect.json.JsonConverter',
        'key.converter.schemas.enable':          'false',
        'value.converter':                       'org.apache.kafka.connect.json.JsonConverter',
        'value.converter.schemas.enable':        'false',
        // Strip the .public. schema segment from topic names:
        //   postgres.cache_testing.public.customers → postgres.cache_testing.customers
        'transforms':                            'dropSchema',
        'transforms.dropSchema.type':            'org.apache.kafka.connect.transforms.RegexRouter',
        'transforms.dropSchema.regex':           'postgres\\.cache_testing\\.public\\.(.*)',
        'transforms.dropSchema.replacement':     'postgres.cache_testing.$1',
      },
      oracle: {
        'connector.class':             'io.debezium.connector.oracle.OracleConnector',
        'tasks.max':                   '1',
        'topic.prefix':                'oracle',
        'database.hostname':           'oracle-xe.prod.svc.cluster.local',
        'database.port':               '1521',
        'database.user':               'c##dbzcdc',
        'database.password':           oraPassword,
        'database.dbname':             'XE',
        'database.pdb.name':           'XEPDB1',
        'schema.include.list':         'CACHE_TESTING',
        'snapshot.mode':               'schema_only',
        'log.mining.strategy':         'online_catalog',
        'decimal.handling.mode':       'double',
        'time.precision.mode':         'connect',
        'tombstones.on.delete':        'false',
        'key.converter':               'org.apache.kafka.connect.json.JsonConverter',
        'key.converter.schemas.enable':'false',
        'value.converter':             'org.apache.kafka.connect.json.JsonConverter',
        'value.converter.schemas.enable':'false',
        ...oraSchemaHistory,
      },
      mongodb: {
        'connector.class':             'io.debezium.connector.mongodb.MongoDbConnector',
        'tasks.max':                   '1',
        'topic.prefix':                'mongodb',
        'mongodb.connection.string':   `mongodb://debezium:${mgoPassword}@mongodb.prod.svc.cluster.local:27017/?authSource=admin&replicaSet=rs0`,
        'database.include.list':       'cache_testing',
        'snapshot.mode':               'no_data',
        'decimal.handling.mode':       'double',
        'tombstones.on.delete':        'false',
        'key.converter':               'org.apache.kafka.connect.json.JsonConverter',
        'key.converter.schemas.enable':'false',
        'value.converter':             'org.apache.kafka.connect.json.JsonConverter',
        'value.converter.schemas.enable':'false',
      },
    };

    const tmpl = templates[source];
    if (!tmpl) return res.status(400).json({ error: `Unknown source: ${source}` });
    res.json({ template: tmpl });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ═══════════════════════════════════════════════════════════════════════════
//  STOPPED-CONNECTOR CONFIG STORE
//  Persists the connector config to disk when the user clicks Stop so that
//  Start CDC → Resume can recreate it from any browser session / tab.
//  Stored as a JSON file alongside the running process so it survives pod
//  restarts when an emptyDir or hostPath volume is mounted at /data.
// ═══════════════════════════════════════════════════════════════════════════
const STOPPED_CFG_FILE = process.env.STOPPED_CFG_FILE || '/tmp/stopped-connectors.json';

function loadStoppedCfgs() {
  try { return JSON.parse(fs.readFileSync(STOPPED_CFG_FILE, 'utf8')); } catch { return {}; }
}
function saveStoppedCfgs(map) {
  try { fs.writeFileSync(STOPPED_CFG_FILE, JSON.stringify(map, null, 2)); } catch (e) {
    console.error('Could not persist stopped-connector configs:', e.message);
  }
}

// GET  /api/connectors/:source/:name/saved-config  → { config } | 404
app.get('/api/connectors/:source/:name/saved-config', (req, res) => {
  const { source, name } = req.params;
  const map = loadStoppedCfgs();
  const cfg = map[`${source}::${name}`];
  if (!cfg) return res.status(404).json({ error: 'No saved config found' });
  res.json({ config: cfg });
});

// PUT  /api/connectors/:source/:name/saved-config  body: { config }
app.put('/api/connectors/:source/:name/saved-config', (req, res) => {
  const { source, name } = req.params;
  const { config } = req.body;
  if (!config || typeof config !== 'object') return res.status(400).json({ error: 'config object required' });
  const map = loadStoppedCfgs();
  map[`${source}::${name}`] = config;
  saveStoppedCfgs(map);
  res.json({ ok: true });
});

// DELETE /api/connectors/:source/:name/saved-config
app.delete('/api/connectors/:source/:name/saved-config', (req, res) => {
  const { source, name } = req.params;
  const map = loadStoppedCfgs();
  delete map[`${source}::${name}`];
  saveStoppedCfgs(map);
  res.json({ ok: true });
});

// ═══════════════════════════════════════════════════════════════════════════
//  ADD TASK — POST /api/connectors/:source/:name/add-task
//  Adds a new task to a running connector by:
//    1. Fetching the live config to read tasks.max and current table/collection list
//    2. Validating the new tables (supplemental logging for Oracle, publication for PG)
//    3. Merging new tables into the existing include list (no duplicates)
//    4. Incrementing tasks.max by 1
//    5. PUT /config to apply — Debezium redistributes tasks automatically
//
//  Oracle & PostgreSQL: always rejected (only 1 task supported by those connectors).
//  MongoDB: fully supported — each task gets a slice of the collection list.
// ═══════════════════════════════════════════════════════════════════════════
app.post('/api/connectors/:source/:name/add-task', async (req, res) => {
  const { source, name } = req.params;
  const { tables } = req.body;   // ["db.collection", ...]  (new tables to add)

  // Oracle and PostgreSQL only ever support 1 task — reject early with a clear message
  if (source === 'oracle' || source === 'postgres') {
    return res.status(400).json({
      error: `${source === 'oracle' ? 'Oracle' : 'PostgreSQL'} CDC connectors only support a single task thread. ` +
             `tasks.max > 1 is silently capped to 1 by Debezium for log-based sources. ` +
             `To capture more tables, use "Edit Config" → table.include.list instead.`,
      singleTaskOnly: true,
    });
  }

  if (!Array.isArray(tables) || tables.length === 0) {
    return res.status(400).json({ error: 'tables array required' });
  }

  try {
    // ── 1. Fetch live config ────────────────────────────────────────────────
    const cfgR = await dbzRequest(source, 'GET', `/connectors/${name}/config`);
    if (cfgR.status >= 300) {
      return res.status(cfgR.status).json({ error: 'Could not read connector config', detail: cfgR.body });
    }
    const cfg = { ...cfgR.body };

    // ── 2. Merge new tables into existing include list (deduplicated) ───────
    const includeKey = source === 'mongodb' ? 'collection.include.list' : 'table.include.list';
    const existing   = (cfg[includeKey] || '').split(',').map(t => t.trim()).filter(Boolean);
    const toAdd      = tables.filter(t => !existing.includes(t));

    if (toAdd.length === 0) {
      return res.status(409).json({
        error: 'All selected tables are already in the connector\'s include list.',
        existing,
      });
    }

    const merged = [...existing, ...toAdd];

    // ── 3. Increment tasks.max ──────────────────────────────────────────────
    const currentMax = parseInt(cfg['tasks.max'] || '1', 10);
    const newMax     = currentMax + 1;

    cfg[includeKey]  = merged.join(',');
    cfg['tasks.max'] = String(newMax);

    // Remove the opposite key so Debezium doesn't get confused
    if (source === 'mongodb') {
      delete cfg['table.include.list'];
    } else {
      delete cfg['collection.include.list'];
    }

    // ── 4. Inject credentials and PUT /config ──────────────────────────────
    const patchedCfg = await injectCredentials(source, cfg);
    const putR = await dbzRequest(source, 'PUT', `/connectors/${name}/config`, patchedCfg);
    if (putR.status >= 300) {
      return res.status(putR.status).json({ error: 'Debezium rejected config update', detail: putR.body });
    }

    res.json({
      ok: true,
      tasksMax: newMax,
      addedTables: toAdd,
      allTables: merged,
    });

  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ═══════════════════════════════════════════════════════════════════════════
//  COMPARE JOB
//  Compares source table rows (by PK) against the latest state in a Kafka
//  topic (Debezium CDC events).
//
//  GET  /api/compare/pkeys/:source        body: { table, schema, db }
//       → { pkColumns: string[] }
//
//  POST /api/compare/run
//       body: { source, table, schema, db, topic, pkColumns }
//       → { missingInKafka: pk[][], missingInSource: pk[][], matched: number }
//
//  POST /api/compare/backfill
//       body: { source, connector, table, schema, db }
//       → triggers a snapshot for just this table via Debezium ad-hoc signal
//
//  POST /api/compare/delete-from-topic
//       body: { topic, pkRows: pk[][], pkColumns: string[] }
//       → publishes tombstone messages for each PK row into the topic
// ═══════════════════════════════════════════════════════════════════════════

// ── Helper: fetch PK columns for a source table ───────────────────────────
app.get('/api/compare/pkeys/:source', async (req, res) => {
  const { source } = req.params;
  const { table, schema, db } = req.query;
  if (!table) return res.status(400).json({ error: 'table is required' });

  try {
    if (source === 'postgres') {
      const pod = await getSourcePod('postgresql', 'postgres');
      if (!pod) return res.status(503).json({ error: 'No PostgreSQL pod found' });
      const database = db || 'postgres';
      const schemaName = schema || 'public';
      const { stdout } = await execFileAsync('kubectl', [
        'exec', '-n', K8S_NAMESPACE, pod, '--',
        'psql', '-U', 'postgres', '-d', database, '-tAc',
        `SELECT kcu.column_name
         FROM information_schema.table_constraints tc
         JOIN information_schema.key_column_usage kcu
           ON tc.constraint_name = kcu.constraint_name
          AND tc.table_schema = kcu.table_schema
          AND tc.table_name = kcu.table_name
         WHERE tc.constraint_type = 'PRIMARY KEY'
           AND tc.table_schema = '${schemaName}'
           AND tc.table_name = '${table}'
         ORDER BY kcu.ordinal_position`,
      ], { timeout: 10000 });
      const pkColumns = stdout.trim().split('\n').map(l => l.trim()).filter(Boolean);
      return res.json({ pkColumns });

    } else if (source === 'oracle') {
      const connCfg = await oracleConnCfg({});
      const owner = (schema || 'CACHE_TESTING').toUpperCase();
      const tbl   = table.toUpperCase();
      const rows  = await oracleQuery(connCfg,
        `SELECT cols.column_name
         FROM all_constraints cons
         JOIN all_cons_columns cols
           ON cons.constraint_name = cols.constraint_name
          AND cons.owner = cols.owner
         WHERE cons.constraint_type = 'P'
           AND cons.owner = '${owner}'
           AND cons.table_name = '${tbl}'
         ORDER BY cols.position`);
      const pkColumns = rows.map(r => r[0]);
      return res.json({ pkColumns });

    } else if (source === 'mongodb') {
      // MongoDB always uses _id as the primary key
      return res.json({ pkColumns: ['_id'] });

    } else {
      return res.status(400).json({ error: `Unknown source: ${source}` });
    }
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── GET /api/compare/columns/:source — all columns for a table ─────────────
// Returns { columns: [{name, dataType, isPk}] } so the UI can build a column
// picker.  PKs are flagged so the frontend can lock them as always-selected.
app.get('/api/compare/columns/:source', async (req, res) => {
  const { source } = req.params;
  const { table, schema, db } = req.query;
  if (!table) return res.status(400).json({ error: 'table is required' });

  try {
    if (source === 'postgres') {
      const pod = await getSourcePod('postgresql', 'postgres');
      if (!pod) return res.status(503).json({ error: 'No PostgreSQL pod found' });
      const database   = db     || 'postgres';
      const schemaName = schema || 'public';
      // Step 1: fetch column metadata
      const { stdout } = await execFileAsync('kubectl', [
        'exec', '-n', K8S_NAMESPACE, pod, '--',
        'psql', '-U', 'postgres', '-d', database, '--csv', '-c',
        `SELECT c.column_name, c.data_type,` +
        ` CASE WHEN pk.column_name IS NOT NULL THEN 'true' ELSE 'false' END AS is_pk` +
        ` FROM information_schema.columns c` +
        ` LEFT JOIN (` +
        `   SELECT kcu.column_name FROM information_schema.table_constraints tc` +
        `   JOIN information_schema.key_column_usage kcu` +
        `     ON tc.constraint_name=kcu.constraint_name` +
        `    AND tc.table_schema=kcu.table_schema AND tc.table_name=kcu.table_name` +
        `   WHERE tc.constraint_type='PRIMARY KEY'` +
        `     AND tc.table_schema='${schemaName}' AND tc.table_name='${table}'` +
        ` ) pk ON c.column_name=pk.column_name` +
        ` WHERE c.table_schema='${schemaName}' AND c.table_name='${table}'` +
        ` ORDER BY c.ordinal_position`,
      ], { timeout: 10000 });
      const lines = stdout.trim().split('\n').filter(Boolean).slice(1);
      const baseColumns = lines.map(line => {
        const parts = line.split(',').map(v => v.replace(/^"|"$/g, '').trim());
        return { name: parts[0], dataType: parts[1], isPk: parts[2] === 'true', sampleValue: null };
      });
      // Step 2: fetch one sample value per timestamp-like column (separate query, non-fatal)
      const columns = await Promise.all(baseColumns.map(async col => {
        if (!/timestamp|date|time/i.test(col.dataType)) return col;
        try {
          const sql = `SELECT to_char(${col.name}::timestamptz AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS') FROM ${schemaName}.${table} WHERE ${col.name} IS NOT NULL LIMIT 1`;
          const { stdout: sv } = await execFileAsync('kubectl', [
            'exec', '-n', K8S_NAMESPACE, pod, '--',
            'psql', '-U', 'postgres', '-d', database, '-tAc', sql,
          ], { timeout: 8000 });
          const sample = sv.trim();
          if (sample && !sample.startsWith('ERROR')) col.sampleValue = sample;
        } catch { /* non-fatal */ }
        return col;
      }));
      return res.json({ columns });

    } else if (source === 'oracle') {
      const connCfg = await oracleConnCfg({});
      const owner   = (schema || 'CACHE_TESTING').toUpperCase();
      const tbl     = table.toUpperCase();
      // Get column list first, then fetch one sample per timestamp column
      const rows    = await oracleQuery(connCfg,
        `SELECT c.column_name, c.data_type,` +
        ` CASE WHEN pk.column_name IS NOT NULL THEN 'true' ELSE 'false' END AS is_pk` +
        ` FROM all_tab_columns c` +
        ` LEFT JOIN (` +
        `   SELECT cols.column_name FROM all_constraints cons` +
        `   JOIN all_cons_columns cols` +
        `     ON cons.constraint_name=cols.constraint_name AND cons.owner=cols.owner` +
        `   WHERE cons.constraint_type='P'` +
        `     AND cons.owner='${owner}' AND cons.table_name='${tbl}'` +
        ` ) pk ON c.column_name=pk.column_name` +
        ` WHERE c.owner='${owner}' AND c.table_name='${tbl}'` +
        ` ORDER BY c.column_id`);
      // Fetch one sample value per timestamp/date column (separate queries, non-fatal)
      const columns = await Promise.all(rows.map(async r => {
        const col = { name: r[0], dataType: r[1], isPk: r[2] === 'true', sampleValue: null };
        if (/DATE|TIMESTAMP/i.test(r[1])) {
          try {
            const sv = await oracleQuery(connCfg,
              `SELECT TO_CHAR(${r[0]},'YYYY-MM-DD"T"HH24:MI:SS') FROM ${owner}.${tbl} WHERE ${r[0]} IS NOT NULL AND ROWNUM=1`);
            if (sv[0]?.[0]) col.sampleValue = sv[0][0];
          } catch { /* non-fatal */ }
        }
        return col;
      }));
      return res.json({ columns });

    } else if (source === 'mongodb') {
      // MongoDB is schema-less — sample one document to discover field names + types
      const database = db || 'cache_testing';
      try {
        const docs = await withMongo({}, async (client) => {
          return client.db(database).collection(table)
            .aggregate([{ $sample: { size: 1 } }]).toArray();
        });
        if (!docs.length) {
          // Collection is empty — return just _id
          return res.json({ columns: [{ name: '_id', dataType: 'ObjectId', isPk: true, sampleValue: null }] });
        }
        const doc = docs[0];
        const columns = Object.entries(doc).map(([fieldName, val]) => {
          const isPk = fieldName === '_id';
          // Infer a friendly dataType from the JS value
          let dataType = 'string';
          if (val === null || val === undefined) dataType = 'null';
          else if (val instanceof Date)          dataType = 'date';
          else if (typeof val === 'boolean')     dataType = 'boolean';
          else if (typeof val === 'number')      dataType = Number.isInteger(val) ? 'int' : 'double';
          else if (typeof val === 'object' && val._bsontype === 'ObjectId') dataType = 'ObjectId';
          else if (typeof val === 'object' && val._bsontype === 'Decimal128') dataType = 'decimal';
          else if (typeof val === 'object' && !Array.isArray(val)) dataType = 'object';
          else if (Array.isArray(val))           dataType = 'array';
          // For timestamp/date fields include a sample value in YYYY-MM-DDTHH:MM format
          let sampleValue = null;
          if (val instanceof Date) {
            sampleValue = val.toISOString().slice(0, 16); // "YYYY-MM-DDTHH:MM"
          } else if (typeof val === 'string' && /^\d{4}-\d{2}-\d{2}[T ]/.test(val)) {
            sampleValue = val.slice(0, 16).replace(' ', 'T');
            dataType = 'date';
          }
          return { name: fieldName, dataType, isPk, sampleValue };
        });
        // Ensure _id is always first
        columns.sort((a, b) => (a.isPk ? -1 : b.isPk ? 1 : 0));
        return res.json({ columns });
      } catch (mgErr) {
        // Fallback to just _id if sampling fails
        return res.json({ columns: [{ name: '_id', dataType: 'ObjectId', isPk: true, sampleValue: null }] });
      }

    } else {
      return res.status(400).json({ error: `Unknown source: ${source}` });
    }
  } catch (e) { res.status(500).json({ error: e.message }); }
});



// ── Active compare-job: track the spawned child process so it can be killed ─
let _compareAbortController = null;
let _compareChildProcesses  = new Set();  // ALL live kubectl spawns — Kafka + DB
let _compareChildProcess    = null;       // legacy single-ref

// POST /api/compare/abort — force-kill EVERYTHING running under the current compare job
app.post('/api/compare/abort', (req, res) => {
  let killed = false;
  for (const cp of _compareChildProcesses) {
    try { cp.kill('SIGKILL'); } catch {}
    killed = true;
  }
  _compareChildProcesses.clear();
  _compareChildProcess = null;
  if (_compareAbortController) {
    _compareAbortController.abort();
    _compareAbortController = null;
    killed = true;
  }
  res.json({ ok: killed, message: killed ? 'Compare job force-killed' : 'No compare job running' });
});

// ── trackedExecFile ───────────────────────────────────────────────────────────
// Like execFileAsync but the child process is registered in _compareChildProcesses
// so /api/compare/abort can SIGKILL it immediately, regardless of whether the
// Node.js AbortController has propagated yet.
// The signal argument is still forwarded for cooperative cancellation.
function trackedExecFile(file, args, opts = {}) {
  const { spawn } = require('child_process');
  const { signal, timeout, maxBuffer = 10 * 1024 * 1024 } = opts;

  return new Promise((resolve, reject) => {
    // Check abort before even spawning
    if (signal?.aborted) return reject(Object.assign(new Error('Compare job aborted by user'), { name: 'AbortError' }));

    const child = spawn(file, args, { stdio: ['ignore', 'pipe', 'pipe'] });
    _compareChildProcesses.add(child);
    _compareChildProcess = child;
    child.on('exit', () => {
      _compareChildProcesses.delete(child);
      if (_compareChildProcess === child) _compareChildProcess = null;
    });

    let stdoutBuf = Buffer.alloc(0);
    let stderrBuf = Buffer.alloc(0);
    let timedOut  = false;

    child.stdout.on('data', chunk => {
      stdoutBuf = Buffer.concat([stdoutBuf, chunk]);
      if (stdoutBuf.length > maxBuffer) {
        child.kill('SIGKILL');
        reject(new Error(`stdout exceeded maxBuffer (${maxBuffer} bytes)`));
      }
    });
    child.stderr.on('data', chunk => { stderrBuf = Buffer.concat([stderrBuf, chunk]); });

    // Honour Node AbortController signal
    const onAbort = () => {
      try { child.kill('SIGKILL'); } catch {}
      reject(Object.assign(new Error('Compare job aborted by user'), { name: 'AbortError' }));
    };
    signal?.addEventListener('abort', onAbort, { once: true });

    // Optional timeout
    let timer;
    if (timeout) {
      timer = setTimeout(() => {
        timedOut = true;
        try { child.kill('SIGKILL'); } catch {}
        reject(new Error(`kubectl timed out after ${timeout}ms`));
      }, timeout);
    }

    child.on('close', (code) => {
      signal?.removeEventListener('abort', onAbort);
      if (timer) clearTimeout(timer);
      if (timedOut) return; // already rejected
      if (signal?.aborted) return; // already rejected
      if (code === 0) {
        resolve({ stdout: stdoutBuf.toString(), stderr: stderrBuf.toString() });
      } else {
        reject(new Error(`kubectl exited ${code}: ${stderrBuf.toString().slice(0, 300)}`));
      }
    });

    child.on('error', err => {
      signal?.removeEventListener('abort', onAbort);
      if (timer) clearTimeout(timer);
      reject(err);
    });
  });
}

// ── Stdin-piping spawn helper — used for kafka-console-producer ─────────────
// execFileAsync does NOT support the `input` option (it is silently ignored);
// we must use spawn() and write to child.stdin ourselves.
// Returns a Promise<{ stdout, stderr }> — rejects on non-zero exit or timeout.
function spawnWithStdin(file, args, inputData, timeoutMs = 120000) {
  const { spawn } = require('child_process');
  return new Promise((resolve, reject) => {
    const child = spawn(file, args, { stdio: ['pipe', 'pipe', 'pipe'] });

    let stdoutBuf = '';
    let stderrBuf = '';
    let settled   = false;
    let timer;

    const fail = (msg) => {
      if (settled) return;
      settled = true;
      if (timer) clearTimeout(timer);
      try { child.kill('SIGKILL'); } catch {}
      reject(new Error(msg));
    };

    timer = setTimeout(() => fail(`spawnWithStdin timed out after ${timeoutMs}ms`), timeoutMs);

    child.stdout.on('data', c => { stdoutBuf += c; });
    child.stderr.on('data', c => { stderrBuf += c; });

    child.on('close', code => {
      if (settled) return;
      settled = true;
      if (timer) clearTimeout(timer);
      // Strip kubectl informational lines that are not real errors:
      //   "Defaulted container X out of: X, Y"
      // Also strip kafka-console-producer SLF4J/INFO lines which go to stderr
      // but do not indicate failure (the producer exits non-zero on broker warnings).
      const realStderr = stderrBuf.split('\n')
        .filter(l => {
          const t = l.trim();
          if (!t) return false;
          if (/^Defaulted container /i.test(t)) return false;
          if (/^SLF4J/i.test(t)) return false;
          if (/^\[.*\] INFO /i.test(t)) return false;
          if (/^\[.*\] WARN /i.test(t)) return false;
          return true;
        })
        .join('\n');
      if (code === 0 || !realStderr) {
        resolve({ stdout: stdoutBuf, stderr: realStderr });
      } else {
        reject(new Error(`Command failed: ${file} ${args.slice(0,6).join(' ')}… exit ${code}: ${(realStderr || stderrBuf).slice(0, 300)}`));
      }
    });

    child.on('error', err => fail(err.message));

    // Write all input then close stdin so the producer knows it's done
    child.stdin.write(inputData, 'utf8', (writeErr) => {
      if (writeErr) return fail(`stdin write error: ${writeErr.message}`);
      child.stdin.end();
    });
  });
}

// ── Streaming kafkaExec for compare — line-by-line, killable immediately ────
// Returns an async iterable of stdout lines.
// Registers the process in _compareChildProcess so abort can SIGKILL it.
function kafkaExecStream(argsStr, pod) {
  const { spawn } = require('child_process');
  const args = ['exec', '-n', K8S_NAMESPACE, pod, '-c', 'debezium-connect', '--', ...argsStr.trim().split(/\s+/)];
  const child = spawn('kubectl', args, { stdio: ['ignore', 'pipe', 'pipe'] });
  _compareChildProcess = child;
  _compareChildProcesses.add(child);
  child.on('exit', () => {
    if (_compareChildProcess === child) _compareChildProcess = null;
    _compareChildProcesses.delete(child);
  });

  // Capture stderr so callers can surface errors when stdout is empty
  let stderrBuf = '';
  child.stderr.on('data', chunk => { stderrBuf += chunk.toString(); });

  // Return async iterable of lines, with a .getStderr() accessor
  const iterable = {
    getStderr() { return stderrBuf; },
    [Symbol.asyncIterator]() {
      let buf = '';
      let done = false;
      const lines = [];
      let resolve = null;

      child.stdout.on('data', chunk => {
        buf += chunk.toString();
        const parts = buf.split('\n');
        buf = parts.pop(); // last incomplete line stays in buf
        for (const l of parts) lines.push(l);
        if (resolve) { const r = resolve; resolve = null; r(); }
      });
      child.stdout.on('end', () => {
        if (buf) { lines.push(buf); buf = ''; }
        done = true;
        if (resolve) { const r = resolve; resolve = null; r(); }
      });

      return {
        async next() {
          while (!lines.length && !done) {
            await new Promise(r => { resolve = r; });
          }
          if (lines.length) return { value: lines.shift(), done: false };
          return { value: undefined, done: true };
        },
      };
    },
  };
  return iterable;
}

// ── Helper: read ALL messages from a Kafka topic and build a PK → latest-op map
// Returns Map<pkKey(string), { op, pkValues: any[] }>
//
// Strategy: stream stdout line-by-line, parse key for PKs, snip value for op.
// Process is registered in _compareChildProcess and can be SIGKILL-ed instantly.
// scanPartition: drain one Kafka partition into a local Map.
// Runs as a concurrent Promise so multiple partitions can be scanned in parallel.
// startOffset / stopOffset let a timestamp window clip what is consumed:
//   startOffset — first offset to read (0 = beginning)
//   stopOffset  — read up to but not including this offset (undefined = endOffset = latest)
// normalisePkVal: convert a Kafka PK value to a string exactly matching what
// psql --csv emits, so Map key comparison works across both sides.
// Debezium emits integers as JSON numbers; psql CSV emits them as plain digits.
// Both become String(v) which is identical → "182819".
function normalisePkVal(v) {
  if (v === null || v === undefined) return null;
  // Unwrap Debezium value wrapper { value: actual }
  if (typeof v === 'object' && !Array.isArray(v) && 'value' in v) v = v.value;
  if (v === null || v === undefined) return null;
  // Unwrap MongoDB Extended JSON ObjectId: {"$oid":"507f..."} → "507f..."
  // The value may arrive as an already-parsed object or as a JSON string.
  if (typeof v === 'string') {
    const trimmed = v.trim();
    if (trimmed.startsWith('{')) {
      try {
        const parsed = JSON.parse(trimmed);
        if (parsed && typeof parsed.$oid === 'string') return parsed.$oid;
      } catch { /* not JSON, fall through */ }
    }
  } else if (typeof v === 'object' && !Array.isArray(v) && typeof v.$oid === 'string') {
    return v.$oid;
  }
  return String(v);
}

// Normalise a raw Debezium MongoDB key payload so the PK can be looked up
// against sourcePKs (which always stores _id as a plain hex/string value).
// Debezium MongoDB key: { "id": "{\"$oid\":\"507f...\"}" }
//   — field name is "id", not "_id"
//   — value is a JSON-stringified Extended JSON document
// We remap "id" → "_id" and unwrap the $oid so the value matches String(doc._id).
function normaliseMongoPkPayload(payload) {
  if (!payload || typeof payload !== 'object') return payload;
  // If we see "id" but not "_id", treat it as the MongoDB _id key field
  if ('id' in payload && !('_id' in payload)) {
    const remapped = { ...payload, _id: payload.id };
    delete remapped.id;
    return remapped;
  }
  return payload;
}

async function scanPartition({ topic, partition, endOffset, startOffset, stopOffset, pkColumns, extraCols = [], pod, signal }) {
  const localMap = new Map();
  if (signal?.aborted) throw new Error('Compare job aborted by user');

  const fromOffset = (startOffset != null && startOffset > 0) ? startOffset : 0;
  const msgCount   = (stopOffset  != null) ? Math.max(0, stopOffset - fromOffset)
                                           : endOffset - fromOffset;
  if (msgCount <= 0) return localMap;
  let _linesRead = 0;
  let _firstLine = '';

  // --timeout-ms: idle timeout between consecutive messages.
  // 30 s was far too short for large topics — bump to 5 min so a slow broker
  // or a kubectl exec pipe stall doesn't cut the scan short.
  const lineStream = kafkaExecStream(
    `/kafka/bin/kafka-console-consumer.sh --bootstrap-server ${KAFKA_BOOTSTRAP}` +
    ` --topic ${topic} --partition ${partition} --offset ${fromOffset}` +
    ` --max-messages ${msgCount} --timeout-ms 300000` +
    ` --consumer.config ${SASL_CFG_FILE}` +
    ` --property print.key=true --property key.separator=\\t`,
    pod
  );

  for await (const line of lineStream) {
    if (signal?.aborted) throw new Error('Compare job aborted by user');
    if (!line.trim() || line.startsWith('SLF4J') || line.startsWith('[')) continue;
    _linesRead++;
    if (_linesRead === 1) _firstLine = line.slice(0, 120);

    // The consumer is invoked via kafkaExecStream which passes --property key.separator=\t
    // as the literal two-char string "\t" (backslash + t), not a real tab.  Kafka outputs
    // the key/value pair separated by whichever string was given — so we must search for
    // the literal "\t" string first, then fall back to a real tab character.
    const litTabIdx  = line.indexOf('\\t');   // literal backslash-t (2 chars)
    const realTabIdx = line.indexOf('\t');    // real tab (1 char, ASCII 9)
    // Prefer the literal \t match when both are present — it is the actual separator.
    // If neither is found the line has no separator; treat the whole line as the value.
    const sepLen   = litTabIdx >= 0 ? 2 : 1;
    const tabIdx   = litTabIdx >= 0 ? litTabIdx : realTabIdx;
    const rawKey = tabIdx >= 0 ? line.slice(0, tabIdx).trim() : '';
    const rawVal = tabIdx >= 0 ? line.slice(tabIdx + sepLen).trim() : line.trim();

    // ── op from value ─────────────────────────────────────────────────
    let op = 'd'; // tombstone default (empty value)
    if (rawVal) {
      const snip = rawVal.slice(0, 300);
      const opMatch = snip.match(/"op"\s*:\s*"([cdur])"/);
      op = opMatch ? opMatch[1] : 'r';
    }

    // ── PK values from Debezium key JSON ──────────────────────────────
    // Relational key format: {"id":182819}  (flat — no schema/payload wrapper)
    // OR with schema:        {"schema":{...},"payload":{"id":182819}}
    // MongoDB key format:    {"id":"{\"$oid\":\"507f...\"}"}
    //   — field is "id" (not "_id"), value is JSON-stringified Extended JSON.
    //   normaliseMongoPkPayload() remaps "id" → "_id" so pkColumns lookup works.
    let pkValues = null;
    let afterRow  = null;   // parsed after/before payload — needed for extraCols
    if (rawKey) {
      try {
        const keyObj = JSON.parse(rawKey);
        const rawPayload = keyObj.payload ?? keyObj;
        const payload = normaliseMongoPkPayload(rawPayload);
        const vals = pkColumns.map(col => normalisePkVal(payload[col]));
        if (vals.every(v => v !== null && v !== undefined)) pkValues = vals;
      } catch { /* fall through to value fallback */ }
    }

    // Parse the value payload to get the after row (needed for extraCols values
    // and as a PK fallback when the key is absent or unparseable).
    if (rawVal) {
      try {
        const valObj = JSON.parse(rawVal);
        const payload = valObj.payload ?? valObj;
        let row = payload.after ?? payload.before;
        if (typeof row === 'string') { try { row = JSON.parse(row); } catch { row = null; } }
        if (row) afterRow = normaliseMongoPkPayload(row);
      } catch { /* skip */ }
    }

    // Fallback: extract PK from the after/before payload.
    if (!pkValues && afterRow) {
      const vals = pkColumns.map(col => normalisePkVal(afterRow[col]));
      if (vals.every(v => v !== null && v !== undefined)) pkValues = vals;
    }

    if (!pkValues) continue;

    // ── Build composite key: [pk…, colVal…] ──────────────────────────
    // extraCols values come from the Kafka message's after payload.
    // If an extra column is absent in the message, use null so the key
    // still differs from a source row that has a non-null value there.
    const colVals = extraCols.map(col => {
      if (!afterRow) return null;
      const v = afterRow[col] ?? afterRow[col?.toUpperCase()];
      return (v === null || v === undefined) ? null : String(v);
    });
    const mapKey = JSON.stringify([...pkValues, ...colVals]);
    localMap.set(mapKey, { op, pkValues });
  }
  // Attach diagnostics so callers can surface errors when the map is empty
  localMap._stderr    = lineStream.getStderr();
  localMap._linesRead = _linesRead;
  localMap._firstLine = _firstLine;
  return localMap;
}

// ── scanTopicForPKsTargeted ────────────────────────────────────────────────────
// PK-targeted scan — the Kafka equivalent of "WHERE kafka.pk IN (source_pks)".
//
// Opens ONE streaming kafka-console-consumer per partition (offset 0 → end) and
// filters messages in-process against the wantedKeys set.  Kills the consumer
// process the moment every wanted PK has been found — no wasted reads.
//
// This avoids the fatal flaw of the chunk-based approach: each chunk spawned a
// new kubectl exec + Kafka JVM (~3s startup), so a 149M-message topic at 50k
// chunks took 2983 × 3s = 2.5 hours even before reading a single byte.
// One streaming exec amortises that cost across all messages in the partition.
//
// Returns { pkMap, kafkaPlan }
// pkMap: Map<key, {op, pkValues}> — latest entry for each wanted PK found.
// partitionBounds is an optional { startOffsets: Map<partition,offset>, stopOffsets: Map<partition,offset> }
// that constrains the scan to a broker-time window resolved from the DB timestamp filter.
// When supplied the tail/head strategy is replaced by a simple bounded scan within that window.
async function scanTopicForPKsTargeted(topic, pkColumns, wantedKeys, extraCols = [], signal, kafkaConcurrency, _chunkSizeUnused, onProgress, partitionBounds) {
  const { spawn } = require('child_process');
  const prog = onProgress || (() => {});
  const pkMap = new Map();

  // ── Get latest end offsets ────────────────────────────────────────────────
  prog('📨', 'Kafka — targeted PK lookup: getting partition offsets…', topic);
  const latestCmd = `/kafka/bin/kafka-get-offsets.sh --bootstrap-server ${KAFKA_BOOTSTRAP} --topic ${topic} --time latest --command-config ${SASL_CFG_FILE}`;
  const offOut = await kafkaExec(latestCmd, 20000, signal);
  const partitions = offOut.trim().split('\n').filter(l => l.includes(':')).map(l => {
    const parts = l.split(':');
    return { partition: parseInt(parts[1], 10), endOffset: parseInt(parts[2], 10) };
  }).filter(p => !isNaN(p.partition) && p.endOffset > 0);

  if (!partitions.length) {
    prog('⚠️', 'Kafka — no partitions found for topic', topic);
    return { pkMap, kafkaPlan: { topic, mode: 'targeted-pk', partitions: [], scannedTotal: 0 } };
  }

  const totalMessages = partitions.reduce((s, p) => s + p.endOffset, 0);
  const windowMode = !!(partitionBounds?.startOffsets || partitionBounds?.stopOffsets);
  prog('📨', `Kafka — targeted PK scan: ${wantedKeys.size} PK(s) to find across ${partitions.length} partition(s), ${totalMessages.toLocaleString()} total messages${windowMode ? ' [broker-time window active]' : ''}`);

  const pod = await getDbzPod();
  ensureSaslFileSync(pod);

  // ── Shared state across all consumer streams ──────────────────────────────
  // wantedKeys is mutated in-place (passed by reference from caller).
  // seenPkKeys deduplicates: whichever stream finds a PK first wins.
  const seenPkKeys = new Set();
  let scannedTotal = 0;

  // ── Inner helper: parse one line from kafka-console-consumer stdout ───────
  // Returns { pkKey, mapKey, op, pkValues } or null if line can't be parsed.
  const parseLine = (line) => {
    const tabIdx = line.indexOf('\t');
    const rawKey = tabIdx >= 0 ? line.slice(0, tabIdx).trim() : '';
    const rawVal = tabIdx >= 0 ? line.slice(tabIdx + 1).trim() : line.trim();

    let op = 'd';
    if (rawVal) {
      const m = rawVal.slice(0, 300).match(/"op"\s*:\s*"([cdur])"/);
      op = m ? m[1] : 'r';
    }

    let pkValues = null;
    if (rawKey) {
      try {
        const keyObj = JSON.parse(rawKey);
        const payload = normaliseMongoPkPayload(keyObj.payload ?? keyObj);
        const vals = pkColumns.map(col => normalisePkVal(payload[col]));
        if (vals.every(v => v !== null && v !== undefined)) pkValues = vals;
      } catch {}
    }
    if (!pkValues && rawVal) {
      try {
        const valObj = JSON.parse(rawVal);
        const payload = valObj.payload ?? valObj;
        let row = payload.after ?? payload.before;
        if (typeof row === 'string') { try { row = JSON.parse(row); } catch { row = null; } }
        if (row) {
          const norm = normaliseMongoPkPayload(row);
          const vals = pkColumns.map(col => normalisePkVal(norm[col]));
          if (vals.every(v => v !== null && v !== undefined)) pkValues = vals;
        }
      } catch {}
    }
    if (!pkValues) return null;

    const pkKey = JSON.stringify(pkValues);

    let afterRow = null;
    if (extraCols.length > 0 && rawVal) {
      try {
        const valObj = JSON.parse(rawVal);
        const payload = valObj.payload ?? valObj;
        let row = payload.after ?? payload.before;
        if (typeof row === 'string') { try { row = JSON.parse(row); } catch { row = null; } }
        if (row) afterRow = normaliseMongoPkPayload(row);
      } catch {}
    }
    const colVals = extraCols.map(col => {
      if (!afterRow) return null;
      const v = afterRow[col] ?? afterRow[col?.toUpperCase()];
      return (v === null || v === undefined) ? null : String(v);
    });
    const mapKey = JSON.stringify([...pkValues, ...colVals]);

    return { pkKey, mapKey, op, pkValues };
  };

  // ── Inner helper: spawn one kafka-console-consumer and filter by wantedKeys ─
  // fromOffset: starting offset (0 = beginning of topic, or a tail anchor).
  // msgCount:   how many messages to read (endOffset - fromOffset).
  // label:      short name for progress logs ('tail' or 'head').
  // onAllFound: callback invoked when wantedKeys reaches 0.
  // Returns a Promise that resolves when the consumer exits or is killed.
  const spawnConsumer = (partition, endOffset, fromOffset, msgCount, label, onAllFound) =>
    new Promise((resolve, reject) => {
      if (msgCount <= 0 || wantedKeys.size === 0) { resolve(); return; }
      // Check abort before spawning
      if (signal?.aborted) { resolve(); return; }

      const args = [
        'exec', '-n', K8S_NAMESPACE, pod, '-c', 'debezium-connect', '--',
        '/kafka/bin/kafka-console-consumer.sh',
        '--bootstrap-server', KAFKA_BOOTSTRAP,
        '--topic', topic,
        '--partition', String(partition),
        '--offset', String(fromOffset),
        '--max-messages', String(msgCount),
        '--timeout-ms', '300000',
        '--consumer.config', SASL_CFG_FILE,
        '--property', 'print.key=true',
        '--property', 'key.separator=\t',  // real tab character
      ];
      const child = spawn('kubectl', args, { stdio: ['ignore', 'pipe', 'pipe'] });
      _compareChildProcesses.add(child);
      child.on('exit', () => _compareChildProcesses.delete(child));

      const kill = () => { try { child.kill('SIGKILL'); } catch {} };

      // Register abort signal — kills child and resolves immediately so
      // the outer await Promise.all() unblocks and the abort propagates.
      const onAbort = () => { kill(); resolve(); };
      signal?.addEventListener('abort', onAbort, { once: true });

      let buf = '';
      let linesRead = 0;

      child.stderr.on('data', () => {});

      child.stdout.on('data', chunk => {
        if (signal?.aborted) { kill(); return; }
        buf += chunk.toString();
        const lines = buf.split('\n');
        buf = lines.pop();

        for (const line of lines) {
          if (!line.trim() || line.startsWith('SLF4J') || line.startsWith('[')) continue;
          linesRead++;
          scannedTotal++;

          if (linesRead % 100_000 === 0) {
            const pct = Math.round(linesRead / msgCount * 100);
            prog('📨', `Kafka — p${partition} ${label}: ${linesRead.toLocaleString()}/${msgCount.toLocaleString()} (${pct}%) — ${wantedKeys.size} PK(s) remaining`);
          }

          if (signal?.aborted || wantedKeys.size === 0) { kill(); break; }

          const parsed = parseLine(line);
          if (!parsed) continue;
          const { pkKey, mapKey, op, pkValues } = parsed;

          if (!wantedKeys.has(pkKey)) continue;
          if (seenPkKeys.has(pkKey)) continue;

          pkMap.set(mapKey, { op, pkValues });
          seenPkKeys.add(pkKey);
          wantedKeys.delete(pkKey);

          if (wantedKeys.size === 0) {
            prog('✅', `Kafka — all PKs found! (${label} stream, p${partition}, ${linesRead.toLocaleString()} msgs scanned)`);
            kill();
            onAllFound();
            break;
          }
        }
      });

      child.stdout.on('end', () => { signal?.removeEventListener('abort', onAbort); resolve(); });
      child.on('close',     () => { signal?.removeEventListener('abort', onAbort); resolve(); });
      child.on('error', err => { signal?.removeEventListener('abort', onAbort); reject(err); });
    });

  // ── Per-partition scan strategy ───────────────────────────────────────────
  //
  // Two modes:
  //
  // A) Broker-time window mode (partitionBounds supplied):
  //    DB timestamp filter (e.g. updated_at = 2026-08-04) was used to resolve
  //    per-partition Kafka broker-time offsets.  We scan only within that window —
  //    no tail/head split needed because we know exactly where the messages are.
  //
  // B) Default tail+head mode (no partitionBounds):
  //    Run TWO consumers in parallel per partition:
  //      • TAIL consumer — reads the last TAIL_WINDOW messages (recently-fixed rows).
  //      • HEAD consumer — reads from offset 0 forward (initial-snapshot rows).
  //    Both filter against the same wantedKeys set; first one to find a PK wins.
  //    TAIL_WINDOW is 2M messages — large enough for recent CDC/fix traffic.
  //
  const TAIL_WINDOW = 2_000_000;
  const concurrency = Math.min(Math.max(1, kafkaConcurrency || partitions.length), partitions.length);

  for (let i = 0; i < partitions.length; i += concurrency) {
    if (signal?.aborted) throw new Error('Compare job aborted by user');
    if (wantedKeys.size === 0) break;

    const batch = partitions.slice(i, i + concurrency);

    await Promise.all(batch.map(async p => {
      if (wantedKeys.size === 0) return;

      if (windowMode) {
        // ── Mode A: bounded broker-time window ──────────────────────────────
        const winFrom  = partitionBounds.startOffsets?.get(p.partition) ?? 0;
        const winStop  = partitionBounds.stopOffsets?.get(p.partition)  ?? p.endOffset;
        const winCount = Math.max(0, winStop - winFrom);
        if (winCount === 0) {
          prog('📨', `Kafka — p${p.partition}: no messages in broker-time window, skipping`);
          return;
        }
        prog('📨', `Kafka — p${p.partition}: broker-time window scan offset ${winFrom.toLocaleString()}–${winStop.toLocaleString()} (${winCount.toLocaleString()} msgs) for ${wantedKeys.size} PK(s)`);
        await spawnConsumer(p.partition, p.endOffset, winFrom, winCount, 'window', () => {});
      } else {
        // ── Mode B: tail + head parallel scan ───────────────────────────────
        const tailFrom  = Math.max(0, p.endOffset - TAIL_WINDOW);
        const tailCount = p.endOffset - tailFrom;
        const headCount = tailFrom; // offset 0 → tailFrom (the non-overlapping head)

        prog('📨', `Kafka — p${p.partition}: launching tail scan (last ${tailCount.toLocaleString()} msgs) + head scan (first ${headCount.toLocaleString()} msgs) in parallel for ${wantedKeys.size} PK(s)`);

        let allFoundCalled = false;
        const onAllFound = () => { allFoundCalled = true; };

        const promises = [
          spawnConsumer(p.partition, p.endOffset, tailFrom, tailCount, 'tail', onAllFound),
        ];
        if (headCount > 0) {
          promises.push(spawnConsumer(p.partition, p.endOffset, 0, headCount, 'head', onAllFound));
        }
        await Promise.all(promises);
      }
    }));
  }

  if (wantedKeys.size > 0) {
    prog('ℹ️', `Kafka — targeted scan complete: ${seenPkKeys.size} PK(s) found, ${wantedKeys.size} genuinely not in topic (scanned ${scannedTotal.toLocaleString()} messages)`);
  } else {
    prog('✅', `Kafka — all ${seenPkKeys.size} PK(s) confirmed present in Kafka (scanned ${scannedTotal.toLocaleString()} messages)`);
  }

  const kafkaPlan = { topic, mode: windowMode ? 'targeted-pk-windowed' : 'targeted-pk', partitions: partitions.length, concurrency, tailWindow: windowMode ? null : TAIL_WINDOW, windowMode, scannedTotal };
  return { pkMap, kafkaPlan };
}

// resolveTimeOffsets: given an epoch-ms timestamp, return a Map<partition, offset>
// using kafka-get-offsets.sh --time <epochMs>.
// Returns null entries for partitions that had no messages at/after that time.
async function resolveTimeOffsets(topic, epochMs, signal) {
  const out = await kafkaExec(
    `/kafka/bin/kafka-get-offsets.sh --bootstrap-server ${KAFKA_BOOTSTRAP}` +
    ` --topic ${topic} --time ${epochMs} --command-config ${SASL_CFG_FILE}`,
    30000,
    signal
  );
  const map = new Map();
  for (const line of out.trim().split('\n').filter(l => l.includes(':'))) {
    const parts = line.split(':');
    const part  = parseInt(parts[1], 10);
    const off   = parseInt(parts[2], 10);
    if (!isNaN(part)) map.set(part, isNaN(off) ? null : off);
  }
  return map;
}

// scanTopicForPKs: scan all partitions of a Kafka topic for PK presence.
// kafkaConcurrency controls how many partition consumers run in parallel (default: all).
// tsFromMs / tsToMs are optional epoch-ms bounds that clip the scan window per partition.
// scanTopicForPKs returns { pkMap, kafkaPlan } so the caller can include the
// Kafka execution plan in the API response for UI display.
async function scanTopicForPKs(topic, pkColumns, signal, kafkaConcurrency, tsFromMs, tsToMs, onProgress, maxMessagesPerPartition, extraCols = []) {
  const prog = onProgress || (() => {});

  // ── Step 1: get latest (end) offsets — always needed as upper bound ──────
  prog('📨', 'Kafka — getting latest offsets per partition…', topic);
  const latestCmd = `/kafka/bin/kafka-get-offsets.sh --bootstrap-server ${KAFKA_BOOTSTRAP} --topic ${topic} --time latest --command-config ${SASL_CFG_FILE}`;
  const offOut = await kafkaExec(latestCmd, 20000, signal);
  const partitionLines = offOut.trim().split('\n').filter(l => l.includes(':'));
  if (!partitionLines.length) return { pkMap: new Map(), kafkaPlan: { topic, partitions: [], commands: [] } };

  let partitions = partitionLines.map(l => {
    const parts = l.split(':');
    return { partition: parseInt(parts[1], 10), endOffset: parseInt(parts[2], 10) };
  }).filter(p => !isNaN(p.partition) && p.endOffset > 0);

  // If maxMessagesPerPartition is set (DB-first full-scan mode), clamp the
  // startOffset so we only consume the tail of each partition.
  if (maxMessagesPerPartition) {
    partitions = partitions.map(p => ({
      ...p,
      startOffset: Math.max(0, p.endOffset - maxMessagesPerPartition),
    }));
    const totalWindow = partitions.reduce((s,p) => s + (p.endOffset - p.startOffset), 0);
    prog('📨', `Kafka — found ${partitions.length} partition(s), scanning last ${maxMessagesPerPartition.toLocaleString()} msg/partition (${totalWindow.toLocaleString()} total)`);
  } else {
    prog('📨', `Kafka — found ${partitions.length} partition(s), total ${partitions.reduce((s,p)=>s+p.endOffset,0).toLocaleString()} messages`);
  }

  // ── Step 2: resolve timestamp → per-partition offsets ────────────────────
  let startOffsets = null;
  let stopOffsets  = null;
  let fromCmd = null, toCmd = null;

  if (tsFromMs) {
    prog('📨', `Kafka — resolving FROM timestamp offset (${new Date(tsFromMs).toISOString()})…`);
    fromCmd = `/kafka/bin/kafka-get-offsets.sh --bootstrap-server ${KAFKA_BOOTSTRAP} --topic ${topic} --time ${tsFromMs} --command-config ${SASL_CFG_FILE}`;
    startOffsets = await resolveTimeOffsets(topic, tsFromMs, signal);
  }
  if (tsToMs) {
    prog('📨', `Kafka — resolving TO timestamp offset (${new Date(tsToMs).toISOString()})…`);
    toCmd = `/kafka/bin/kafka-get-offsets.sh --bootstrap-server ${KAFKA_BOOTSTRAP} --topic ${topic} --time ${tsToMs} --command-config ${SASL_CFG_FILE}`;
    stopOffsets = await resolveTimeOffsets(topic, tsToMs, signal);
  }

  // ── Step 3: build per-partition window table for the plan ─────────────────
  const partitionWindows = partitions.map(p => ({
    partition:   p.partition,
    startOffset: startOffsets?.get(p.partition) ?? 0,
    stopOffset:  stopOffsets?.get(p.partition)  ?? p.endOffset,
    endOffset:   p.endOffset,
    msgCount:    (stopOffsets?.get(p.partition)  ?? p.endOffset) - (startOffsets?.get(p.partition) ?? 0),
  }));

  // Filter out empty windows
  partitions = partitions.filter(p => {
    const start = startOffsets?.get(p.partition) ?? 0;
    const stop  = stopOffsets?.get(p.partition)  ?? p.endOffset;
    return stop > start;
  });

  if (!partitions.length) {
    prog('⚠️', 'Kafka — no messages in the specified time window');
    const kafkaPlan = {
      topic, bootstrap: KAFKA_BOOTSTRAP,
      tsFromMs, tsToMs, partitionWindows,
      consumerCommand: null,
      offsetCmds: { latest: latestCmd, from: fromCmd, to: toCmd },
      totalMessages: 0,
      note: 'No messages found in the specified time window',
    };
    return { pkMap: new Map(), kafkaPlan };
  }

  const pod = await getDbzPod();
  ensureSaslFileSync(pod);

  const concurrency = Math.min(
    Math.max(1, kafkaConcurrency || partitions.length),
    partitions.length
  );

  // ── Step 4: consume partitions ────────────────────────────────────────────
  const pkMap = new Map();
  const activeWindows = partitionWindows.filter(w => w.msgCount > 0);
  const totalMessages = activeWindows.reduce((s, w) => s + w.msgCount, 0);
  prog('📨', `Kafka — scanning ${partitions.length} partition(s), ${totalMessages.toLocaleString()} message(s), concurrency ${concurrency}`);

  // Build a representative consumer command (for the first active partition)
  const firstW = activeWindows[0];
  const consumerCommand = firstW
    ? `/kafka/bin/kafka-console-consumer.sh --bootstrap-server ${KAFKA_BOOTSTRAP}` +
      ` --topic ${topic} --partition ${firstW.partition}` +
      ` --offset ${firstW.startOffset} --max-messages ${firstW.msgCount}` +
      ` --timeout-ms 300000 --consumer.config ${SASL_CFG_FILE}` +
      ` --property print.key=true --property key.separator=\\t`
    : null;

  for (let i = 0; i < partitions.length; i += concurrency) {
    if (signal?.aborted) throw new Error('Compare job aborted by user');
    const batch = partitions.slice(i, i + concurrency);
    const batchNums = batch.map(p => p.partition).join(',');
    prog('📨', `Kafka — consuming partition(s) [${batchNums}] (batch ${Math.floor(i/concurrency)+1}/${Math.ceil(partitions.length/concurrency)})…`);
    const results = await Promise.all(
      batch.map(p => scanPartition({
        topic,
        partition:   p.partition,
        endOffset:   p.endOffset,
        // Use clamped startOffset (DB-first tail mode) if set on partition,
        // otherwise fall back to timestamp-resolved startOffsets or 0.
        startOffset: p.startOffset ?? startOffsets?.get(p.partition) ?? 0,
        stopOffset:  stopOffsets?.get(p.partition)  ?? undefined,
        pkColumns,
        extraCols,
        pod,
        signal,
      }))
    );
    for (const localMap of results) {
      for (const [key, val] of localMap) pkMap.set(key, val);
    }
  }

  const kafkaPlan = {
    topic,
    bootstrap:       KAFKA_BOOTSTRAP,
    tsFromMs,
    tsToMs,
    totalPartitions: partitions.length,
    totalMessages,
    concurrency,
    partitionWindows: activeWindows,
    consumerCommand,
    offsetCmds: { latest: latestCmd, from: fromCmd, to: toCmd },
  };

  return { pkMap, kafkaPlan };
}

// ── POST /api/compare/preview ────────────────────────────────────────────────
// Builds and returns the exact SQL and Kafka commands that /compare/run will
// execute — without running anything. Called by the UI before/during the run
// so the user can see the real queries immediately.
app.post('/api/compare/preview', (req, res) => {
  const { source, table, schema, db, topic, pkColumns,
          dbParallelDegree, kafkaConcurrency,
          tsColumn, tsFrom, tsTo, tsFilterMode } = req.body;

  if (!table || !topic || !pkColumns?.length) {
    return res.status(400).json({ error: 'table, topic and pkColumns are required' });
  }

  const parallelDegree = Math.min(16, Math.max(1, parseInt(dbParallelDegree, 10) || 4));
  const pgFmt = s => s ? s.replace('T', ' ') : s;

  // ── DB SQL ─────────────────────────────────────────────────────────────────
  let dbSql = null;

  if (source === 'postgres') {
    const schemaName = schema || 'public';
    const cols       = pkColumns.join(', ');
    let where = '';
    if (tsColumn && (tsFrom || tsTo)) {
      const clauses = [];
      if (tsFrom) clauses.push(`${tsColumn} >= TIMESTAMP '${pgFmt(tsFrom)}'`);
      if (tsTo)   clauses.push(`${tsColumn} <= TIMESTAMP '${pgFmt(tsTo)}'`);
      where = '\nWHERE ' + clauses.join('\n  AND ');
    }
    const parallelHint = !tsColumn
      ? `SET max_parallel_workers_per_gather = ${parallelDegree};\nSET enable_seqscan = on;\n`
      : '';
    dbSql = `${parallelHint}SELECT ${cols}\nFROM ${schemaName}.${table}${where};`;

  } else if (source === 'oracle') {
    const owner = (schema || 'CACHE_TESTING').toUpperCase();
    const tbl   = table.toUpperCase();
    const cols  = pkColumns.map(c => c.toUpperCase()).join(', ');
    const hint  = `/*+ PARALLEL(t, ${parallelDegree}) NO_INDEX(t) */`;
    let where = '';
    if (tsColumn && (tsFrom || tsTo)) {
      const clauses = [];
      if (tsFrom) clauses.push(`${tsColumn.toUpperCase()} >= TIMESTAMP '${pgFmt(tsFrom)}'`);
      if (tsTo)   clauses.push(`${tsColumn.toUpperCase()} <= TIMESTAMP '${pgFmt(tsTo)}'`);
      where = '\nWHERE ' + clauses.join('\n  AND ');
    }
    dbSql = `SELECT ${hint} ${cols}\nFROM ${owner}.${tbl} t${where};`;

  } else if (source === 'mongodb') {
    const database = db || 'cache_testing';
    let filter = '{}';
    if (tsColumn && (tsFrom || tsTo)) {
      const parts = [];
      if (tsFrom) parts.push(`$gte: ISODate("${tsFrom}")`);
      if (tsTo)   parts.push(`$lte: ISODate("${tsTo}")`);
      filter = `{ ${tsColumn}: { ${parts.join(', ')} } }`;
    }
    dbSql = `db.${table}.find(\n  ${filter},\n  { _id: 1 }\n)`;
  }

  // ── Kafka commands ─────────────────────────────────────────────────────────
  const tsFromMs = tsFrom ? new Date(tsFrom).getTime() : null;
  const tsToMs   = tsTo   ? new Date(tsTo).getTime()   : null;

  const kafkaCmds = [];

  // Step 1: get latest offsets (always)
  kafkaCmds.push({
    label: '① Get latest offsets per partition',
    cmd: `/kafka/bin/kafka-get-offsets.sh \\\n  --bootstrap-server ${KAFKA_BOOTSTRAP} \\\n  --topic ${topic} \\\n  --time latest \\\n  --command-config ${SASL_CFG_FILE}`,
  });

  // Step 2: resolve FROM timestamp to offsets (if set)
  if (tsFromMs) {
    kafkaCmds.push({
      label: `② Resolve FROM timestamp → start offsets  (${tsFrom})`,
      cmd: `/kafka/bin/kafka-get-offsets.sh \\\n  --bootstrap-server ${KAFKA_BOOTSTRAP} \\\n  --topic ${topic} \\\n  --time ${tsFromMs} \\\n  --command-config ${SASL_CFG_FILE}`,
    });
  }

  // Step 3: resolve TO timestamp to offsets (if set)
  if (tsToMs) {
    kafkaCmds.push({
      label: `③ Resolve TO timestamp → stop offsets  (${tsTo})`,
      cmd: `/kafka/bin/kafka-get-offsets.sh \\\n  --bootstrap-server ${KAFKA_BOOTSTRAP} \\\n  --topic ${topic} \\\n  --time ${tsToMs} \\\n  --command-config ${SASL_CFG_FILE}`,
    });
  }

  // Step 4: consume per-partition (example for partition 0)
  const concurrency = Math.max(1, parseInt(kafkaConcurrency, 10) || 0);
  const fromOff = tsFromMs ? '<startOffset from step ②>' : '0';
  const msgCount = tsToMs  ? '<stopOffset from step ③> - <startOffset>'
                           : '<endOffset from step ①>';
  kafkaCmds.push({
    label: `④ Consume messages per partition  (${concurrency === 0 || !kafkaConcurrency ? 'all partitions in parallel' : `batches of ${concurrency}`})`,
    cmd: `/kafka/bin/kafka-console-consumer.sh \\\n  --bootstrap-server ${KAFKA_BOOTSTRAP} \\\n  --topic ${topic} \\\n  --partition <N> \\\n  --offset ${fromOff} \\\n  --max-messages ${msgCount} \\\n  --timeout-ms 30000 \\\n  --consumer.config ${SASL_CFG_FILE} \\\n  --property print.key=true \\\n  --property key.separator=\\t`,
  });

  res.json({ dbSql, kafkaCmds, source, tsFilterMode: tsFilterMode || 'column' });
});

// ── POST /api/compare/run ─────────────────────────────────────────────────
app.post('/api/compare/run', async (req, res) => {
  const { source, table, schema, db, topic, pkColumns,
          dbParallelDegree, kafkaConcurrency,
          tsColumn, tsFrom, tsTo, tsFilterMode, rowLimit,
          compareColumns = [],
          scanFromOffsets } = req.body;   // scanFromOffsets: {partition:offset,...} anchor from post-fix re-run
  if (!table || !topic || !pkColumns?.length) {
    return res.status(400).json({ error: 'table, topic and pkColumns are required' });
  }

  // ── SSE setup — stream progress events to the browser ────────────────────
  res.setHeader('Content-Type',  'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection',    'keep-alive');
  res.flushHeaders();

  // emit(type, payload) — sends one SSE event; payload is JSON-serialisable
  const emit = (type, payload = {}) => {
    try { res.write(`data: ${JSON.stringify({ type, ...payload })}\n\n`); } catch { /* client gone */ }
  };
  // step helper — logs a timestamped step line to the UI
  const step = (icon, msg, detail = '') => emit('step', { icon, msg, detail, ts: Date.now() });

  // Register a new abort controller for this run; abort any previous one
  if (_compareAbortController) _compareAbortController.abort();
  const ac = new AbortController();
  _compareAbortController = ac;
  const { signal } = ac;

  const t0 = Date.now();
  try {
    // Parse timestamp bounds → epoch-ms.
    const tsFromMs = tsFrom ? new Date(tsFrom).getTime() : undefined;
    const tsToMs   = tsTo   ? new Date(tsTo).getTime()   : undefined;

    // Guard: zero-width or inverted window makes no sense
    if (tsFromMs && tsToMs && tsFromMs >= tsToMs) {
      emit('error', { error: 'Timestamp filter: "From" must be earlier than "To". Both bounds are set to the same time — widen the window.' });
      return res.end();
    }

    step('🔍', `Starting compare: ${source} · ${schema||''}${schema?'.':''}${table}  ↔  ${topic}`);

    // ── Decide execution strategy ─────────────────────────────────────────────
    //
    // A) kafka-only mode (tsFilterMode='kafka-only', no tsColumn):
    //    Kafka: broker-time window scan (scanTopicForPKs with tsFrom/tsTo offsets)
    //    DB: full scan
    //    Diff: Kafka PKs in window vs all DB PKs
    //
    // B) All other cases (tsColumn mode, no filter, db-only, post-fix re-run):
    //    DB: full scan or WHERE filtered
    //    Kafka: targeted PK scan (scanTopicForPKsTargeted) — looks up the exact
    //    source PKs in Kafka. Always correct because it finds the latest message
    //    for each PK regardless of where in the topic it lives.
    //    For post-fix re-runs, scanFromOffsets anchors the scan to the tail so
    //    it finds the newly written messages immediately.
    //
    const dbOnlyMode = tsFilterMode === 'db-only';
    // hasTimestampFilter = true only for kafka-only mode (broker-time window scan).
    // tsColumn mode uses targeted-PK scan (Case B) — the DB column value and the
    // Kafka broker timestamp are the same for real CDC but we locate messages by
    // PK key, not by broker time, so we always find the latest state correctly.
    const hasTimestampFilter = !dbOnlyMode && !tsColumn && (tsFilterMode === 'kafka-only') && !!(tsFromMs || tsToMs);
    const KAFKA_ONLY_IN_LIMIT = 50000;
    const kafkaConcurrencyVal = Math.max(1, parseInt(kafkaConcurrency, 10) || 0);
    const parallelDegree = Math.min(16, Math.max(1, parseInt(dbParallelDegree, 10) || 4));
    const kafkaOnlyMode = (tsFilterMode === 'kafka-only') && !tsColumn && (tsFromMs || tsToMs);

    let sourcePKs    = new Map();
    let kafkaAlive   = new Map();
    let kafkaDeleted = new Set();
    let dbQueryPlan  = null;
    let kafkaPlan    = null;
    let useInListLookup = false;

    // ── Build timestamp WHERE clause for column-based mode ───────────────────
    let pgTsWhere  = '';
    let oraTsWhere = '';
    let mgTsFilter = {};

    if (tsColumn && (tsFrom || tsTo)) {
      // tsFrom/tsTo: "YYYY-MM-DDTHH:MM:SS" (no Z).
      // For PostgreSQL: replace T with a space → standard ANSI timestamp literal.
      // CAST(... AS TIMESTAMP) then AT TIME ZONE 'UTC' is not needed here — we
      // treat the value as local/session time, same as the browser picker intended.
      const pgFmt  = s => s.replace('T', ' ');
      const pgClauses = [];
      if (tsFrom) pgClauses.push(`${tsColumn} >= TIMESTAMP '${pgFmt(tsFrom)}'`);
      if (tsTo)   pgClauses.push(`${tsColumn} <= TIMESTAMP '${pgFmt(tsTo)}'`);
      pgTsWhere = 'WHERE ' + pgClauses.join(' AND ');

      // Oracle: same ANSI TIMESTAMP literal format
      const oraFmt = s => s.replace('T', ' ');
      const oraClauses = [];
      if (tsFrom) oraClauses.push(`${tsColumn.toUpperCase()} >= TIMESTAMP '${oraFmt(tsFrom)}'`);
      if (tsTo)   oraClauses.push(`${tsColumn.toUpperCase()} <= TIMESTAMP '${oraFmt(tsTo)}'`);
      oraTsWhere = 'WHERE ' + oraClauses.join(' AND ');

      if (tsFrom) mgTsFilter.$gte = new Date(tsFrom);
      if (tsTo)   mgTsFilter.$lte = new Date(tsTo);
    }

    // ── Helper: build PK IN-list WHERE for postgres/oracle ───────────────────
    // For single-column PK:  WHERE id IN (1,2,3)
    // For composite PK:      WHERE (a,b) IN ((1,'x'),(2,'y'))
    function buildPkInWhere(pkValsArray, dialect) {
      if (!pkValsArray.length) return 'WHERE 1=0'; // nothing to look up
      const quote = v => {
        if (v === null || v === undefined) return 'NULL';
        const s = String(v);
        // numeric literals need no quoting; everything else gets single-quoted
        return /^-?\d+(\.\d+)?$/.test(s) ? s : `'${s.replace(/'/g, "''")}'`;
      };
      if (pkColumns.length === 1) {
        const vals = pkValsArray.map(row => quote(row[0])).join(',');
        return `WHERE ${dialect === 'oracle' ? pkColumns[0].toUpperCase() : pkColumns[0]} IN (${vals})`;
      }
      // composite
      const colList = dialect === 'oracle'
        ? pkColumns.map(c => c.toUpperCase()).join(',')
        : pkColumns.join(',');
      const rowList = pkValsArray.map(row => `(${row.map(quote).join(',')})`).join(',');
      return `WHERE (${colList}) IN (${rowList})`;
    }

    // ── Helper: parse psql CSV line into normalised PK array ────────────────
    const parsePgCsvRow = line => {
      // Handle quoted fields: "val1","val2" or val1,val2
      const vals = [];
      let cur = '', inQ = false;
      for (let i = 0; i < line.length; i++) {
        const c = line[i];
        if (c === '"') {
          if (inQ && line[i+1] === '"') { cur += '"'; i++; } // escaped quote
          else inQ = !inQ;
        } else if (c === ',' && !inQ) { vals.push(cur); cur = ''; }
        else cur += c;
      }
      vals.push(cur);
      return vals.map(v => v === '' ? null : v);
    };

    // ── Row limit: cap source scan to first N rows by PK order ───────────────
    const rowLimitNum = parseInt(rowLimit, 10) || 0;
    const pgLimitClause  = rowLimitNum > 0 ? ` ORDER BY ${pkColumns.join(', ')} LIMIT ${rowLimitNum}` : '';
    const oraLimitClause = rowLimitNum > 0 ? ` ORDER BY ${pkColumns.map(c=>c.toUpperCase()).join(', ')} FETCH FIRST ${rowLimitNum} ROWS ONLY` : '';
    if (rowLimitNum > 0) step('🔢', `Row limit enabled — comparing first ${rowLimitNum.toLocaleString()} rows by PK order`);

    // allSourceCols = PK columns + any extra columns selected for value-comparison.
    // The order matters: PKs come first so pkValues can always be sliced from the front.
    const extraCols   = compareColumns.filter(c => !pkColumns.includes(c));
    const allSrcCols  = [...pkColumns, ...extraCols];
    const nPk         = pkColumns.length;
    const hasColCmp   = extraCols.length > 0;
    if (hasColCmp) step('🔬', `Column-value compare enabled`,
      `${extraCols.length} extra column(s): ${extraCols.join(', ')}`);

    // ── Step A: fetch PK (+ compareColumns) values from source DB ────────────
    if (source === 'postgres') {
      const pod = await getSourcePod('postgresql', 'postgres');
      if (!pod) { emit('error', { error: 'No PostgreSQL pod found' }); return res.end(); }
      const database   = db || 'postgres';
      const schemaName = schema || 'public';
      const cols       = allSrcCols.join(', ');
      const selectSql  = `SELECT ${cols} FROM ${schemaName}.${table} ${pgTsWhere}${pgLimitClause}`;
      const parallelSet = `SET max_parallel_workers_per_gather = ${parallelDegree}; SET enable_seqscan = on; `;

      if (signal.aborted) throw new Error('Compare job aborted by user');

      // EXPLAIN (non-fatal)
      step('🗄', `DB (PostgreSQL) — running EXPLAIN…`, selectSql);
      try {
        const { stdout: explainOut } = await trackedExecFile('kubectl', [
          'exec', '-n', K8S_NAMESPACE, pod, '--',
          'psql', '-U', 'postgres', '-d', database, '-c', `EXPLAIN ${selectSql}`,
        ], { timeout: 15000, maxBuffer: 1 * 1024 * 1024, signal });
        dbQueryPlan = {
          dialect: 'postgres',
          mode: pgTsWhere ? 'column-where' : 'full-scan',
          sql: selectSql, plan: explainOut.trim(),
        };
        const planMode = pgTsWhere ? '🔍 Column filter' : '📋 Full scan';
        step('📋', `DB plan: ${planMode}`, explainOut.trim().split('\n')[2]?.trim() || '');
      } catch { /* non-fatal */ }

      if (signal.aborted) throw new Error('Compare job aborted by user');

      step('🗄', `DB (PostgreSQL) — fetching ${hasColCmp ? `PK + ${extraCols.length} column(s)` : 'PK set'}${pgTsWhere ? ' with timestamp filter' : ' (full scan)'}…`);
      const wrappedSql = `${parallelSet}${selectSql}`;
      const { stdout } = await trackedExecFile('kubectl', [
        'exec', '-n', K8S_NAMESPACE, pod, '--',
        'psql', '-U', 'postgres', '-d', database, '--csv', '-c', wrappedSql,
      ], { timeout: 120000, maxBuffer: 128 * 1024 * 1024, signal });
      // psql outputs one "SET" line per SET statement before the CSV data.
      // Strip all leading "SET" lines, then skip the CSV column-header row.
      const lines = stdout.trim().split('\n').filter(Boolean);
      let dataStart = 0;
      while (dataStart < lines.length && lines[dataStart].trim() === 'SET') dataStart++;
      for (const line of lines.slice(dataStart + 1)) {  // +1 to skip CSV header
        const allVals = parsePgCsvRow(line);
        const pkVals  = allVals.slice(0, nPk);
        // composite key = [pk…, colVal…] so value-differences are caught
        const key = JSON.stringify(allVals);
        sourcePKs.set(key, pkVals);
      }
      // Emit PK range (min/max) for the fetched batch so the user can see exactly which rows were compared
      if (sourcePKs.size > 0 && nPk === 1) {
        const allPkVals = [...sourcePKs.values()].map(v => v[0]).filter(v => v != null);
        const numericPks = allPkVals.every(v => !isNaN(Number(v)));
        let minPk, maxPk;
        if (numericPks) {
          const nums = allPkVals.map(Number);
          minPk = Math.min(...nums);
          maxPk = Math.max(...nums);
        } else {
          const sorted = [...allPkVals].sort();
          minPk = sorted[0];
          maxPk = sorted[sorted.length - 1];
        }
        step('🔑', `PK range of fetched batch — ${pkColumns[0]}: ${minPk} … ${maxPk}`,
          `${sourcePKs.size} row(s) · min=${minPk} · max=${maxPk}`);
      } else if (sourcePKs.size > 0 && nPk > 1) {
        const first = [...sourcePKs.values()][0].join(', ');
        const last  = [...sourcePKs.values()][sourcePKs.size - 1].join(', ');
        step('🔑', `PK range of fetched batch — first: (${first}) · last: (${last})`,
          `${sourcePKs.size} row(s) · composite PK: ${pkColumns.join(', ')}`);
      }
      step('✅', `DB query complete — ${sourcePKs.size.toLocaleString()} row(s) fetched from ${schemaName}.${table}`);

    } else if (source === 'oracle') {
      if (signal.aborted) throw new Error('Compare job aborted by user');
      const connCfg  = await oracleConnCfg({});
      const owner    = (schema || 'CACHE_TESTING').toUpperCase();
      const tbl      = table.toUpperCase();
      const oraCols  = allSrcCols.map(c => c.toUpperCase()).join(', ');
      const hint     = `/*+ PARALLEL(t, ${parallelDegree}) NO_INDEX(t) */`;
      const selectSql = `SELECT ${hint} ${oraCols} FROM ${owner}.${tbl} t ${oraTsWhere}${oraLimitClause}`;

      step('🗄', `DB (Oracle) — running EXPLAIN PLAN…`, selectSql);
      try {
        if (!signal.aborted) await oracleQuery(connCfg, `EXPLAIN PLAN FOR ${selectSql}`);
        if (!signal.aborted) {
          const planRows = await oracleQuery(connCfg,
            `SELECT PLAN_TABLE_OUTPUT FROM TABLE(DBMS_XPLAN.DISPLAY('PLAN_TABLE',NULL,'BASIC +ROWS +COST'))`);
          dbQueryPlan = {
            dialect: 'oracle',
            mode: oraTsWhere ? 'column-where' : 'full-scan',
            sql: selectSql, plan: planRows.map(r => r[0]).join('\n'),
          };
          step('📋', `DB plan: ${dbQueryPlan.mode}`, planRows[2]?.[0] || '');
        }
      } catch { /* non-fatal */ }

      if (signal.aborted) throw new Error('Compare job aborted by user');
      step('🗄', `DB (Oracle) — fetching ${hasColCmp ? `PK + ${extraCols.length} column(s)` : 'PK set'}${oraTsWhere ? ' with timestamp filter' : ' (full scan)'}…`);
      const rows = await oracleQuery(connCfg, selectSql);
      if (signal.aborted) throw new Error('Compare job aborted by user');
      for (const row of rows) {
        const allVals = row.map(v => (v === null || v === undefined) ? null : String(v));
        const pkVals  = allVals.slice(0, nPk);
        sourcePKs.set(JSON.stringify(allVals), pkVals);
      }
      if (sourcePKs.size > 0 && nPk === 1) {
        const allPkVals = [...sourcePKs.values()].map(v => v[0]).filter(v => v != null);
        const numericPks = allPkVals.every(v => !isNaN(Number(v)));
        let minPk, maxPk;
        if (numericPks) { const nums = allPkVals.map(Number); minPk = Math.min(...nums); maxPk = Math.max(...nums); }
        else { const sorted = [...allPkVals].sort(); minPk = sorted[0]; maxPk = sorted[sorted.length - 1]; }
        step('🔑', `PK range of fetched batch — ${pkColumns[0]}: ${minPk} … ${maxPk}`,
          `${sourcePKs.size} row(s) · min=${minPk} · max=${maxPk}`);
      }
      step('✅', `DB query complete — ${sourcePKs.size.toLocaleString()} row(s) fetched from ${owner}.${tbl}`);

    } else if (source === 'mongodb') {
      if (signal.aborted) throw new Error('Compare job aborted by user');
      const database = db || 'cache_testing';
      const mgQuery  = Object.keys(mgTsFilter).length && tsColumn ? { [tsColumn]: mgTsFilter } : {};
      // For MongoDB, build projection that includes _id + any extra fields
      const mgProjection = { _id: 1 };
      extraCols.forEach(c => { mgProjection[c] = 1; });
      step('🗄', `DB (MongoDB) — fetching _id${hasColCmp ? ` + ${extraCols.length} field(s)` : ''} from ${database}.${table}…`);
      const docs = await withMongo({}, async (client) => {
        let cur = client.db(database).collection(table)
          .find(mgQuery, { projection: mgProjection });
        if (rowLimitNum > 0) cur = cur.limit(rowLimitNum);
        return cur.toArray();
      });
      if (signal.aborted) throw new Error('Compare job aborted by user');
      for (const doc of docs) {
        const id      = String(doc._id);
        const colVals = extraCols.map(c => (doc[c] == null ? null : String(doc[c])));
        const allVals = [id, ...colVals];
        sourcePKs.set(JSON.stringify(allVals), [id]);
      }
      if (sourcePKs.size > 0) {
        const ids = [...sourcePKs.values()].map(v => v[0]);
        step('🔑', `PK range of fetched batch — _id: ${ids[0]} … ${ids[ids.length - 1]}`,
          `${sourcePKs.size} doc(s)`);
      }
      step('✅', `DB query complete — ${sourcePKs.size.toLocaleString()} doc(s) fetched`);

    } else {
      emit('error', { error: `Unknown source: ${source}` });
      return res.end();
    }

    if (signal.aborted) throw new Error('Compare job aborted by user');

    // ── Step B: look up source PKs in Kafka ──────────────────────────────────
    // Strategy:
    //   1. Ensure ksqlDB materialised table exists for this topic (created once,
    //      kept alive permanently).  Once RUNNING, pull queries work exactly like
    //      SQL:  SELECT ROWKEY,payload FROM tbl WHERE ROWKEY IN ('1','2','3')
    //      — instant, no topic scanning, no message iteration.
    //   2. If the table is not yet RUNNING (first ever run on this topic), fall
    //      back to scanTopicForPKsTargeted so the compare still completes.
    //      The next run will hit the cached RUNNING table instead.
    if (sourcePKs.size > 0) {
      const allPkVals = [...sourcePKs.values()];
      const total     = allPkVals.length;

      step('📨', `Kafka — checking ${total.toLocaleString()} PK(s) in topic "${topic}"…`);

      // Ensure stream + table exist and are RUNNING (cached after first time).
      const { tableName, ready: tableReady } = await ksqlEnsureTable(
        topic, (icon, msg) => step(icon, msg)
      );
      kafkaPlan = { topic, mode: tableReady ? 'ksqldb-pull-query' : 'targeted-pk', tableName };

      if (tableReady) {
        // ── Fast path: ksqlDB pull query (instant, like SQL WHERE IN) ────────
        step('📨', `ksqlDB — pull query: WHERE ROWKEY IN (${total} PKs)…`);
        try {
          const pkMap = await ksqlPullByPKs(tableName, pkColumns, allPkVals, extraCols,
            (icon, msg) => step(icon, msg));
          for (const [mapKey, entry] of pkMap) {
            if (entry.op === 'd') { kafkaDeleted.add(mapKey); kafkaAlive.delete(mapKey); }
            else                   { kafkaAlive.set(mapKey, entry.pkValues); kafkaDeleted.delete(mapKey); }
          }
          step('✅', `ksqlDB check complete — ${kafkaAlive.size.toLocaleString()} live, ${kafkaDeleted.size} deleted`);
        } catch (pullErr) {
          step('⚠️', `ksqlDB pull query failed: ${pullErr.message} — falling back to direct scan`);
          tableReady && _ksqlTableReady.delete(topic); // force re-check next run
          // fall through to direct scan below
          const wantedKeys = new Map(allPkVals.map(vals => [JSON.stringify(vals), vals]));
          const scanResult = await scanTopicForPKsTargeted(topic, pkColumns, wantedKeys, extraCols, signal, kafkaConcurrencyVal, 0, (icon, msg, detail) => step(icon, msg, detail), undefined);
          for (const [mapKey, entry] of scanResult.pkMap) {
            if (entry.op === 'd') { kafkaDeleted.add(mapKey); kafkaAlive.delete(mapKey); }
            else                   { kafkaAlive.set(mapKey, entry.pkValues); kafkaDeleted.delete(mapKey); }
          }
          step('✅', `Kafka scan complete — ${kafkaAlive.size.toLocaleString()} live, ${kafkaDeleted.size} deleted`);
        }
      } else {
        // ── Slow path (first run on topic, or post-fix re-run with offset anchor)
        // scanFromOffsets anchors to the tail so fix messages are found immediately.
        const wantedKeys = new Map(allPkVals.map(vals => [JSON.stringify(vals), vals]));
        const scanResult = await scanTopicForPKsTargeted(
          topic, pkColumns, wantedKeys, extraCols, signal, kafkaConcurrencyVal, 0,
          (icon, msg, detail) => step(icon, msg, detail),
          scanFromOffsets ? { startOffsets: scanFromOffsets } : undefined,
        );
        for (const [mapKey, entry] of scanResult.pkMap) {
          if (entry.op === 'd') { kafkaDeleted.add(mapKey); kafkaAlive.delete(mapKey); }
          else                   { kafkaAlive.set(mapKey, entry.pkValues); kafkaDeleted.delete(mapKey); }
        }
        step('✅', `Kafka scan complete — ${kafkaAlive.size.toLocaleString()} live, ${kafkaDeleted.size} deleted`);
      }
    }

    // ── Step C: diff ─────────────────────────────────────────────────────────
    step('⚖️', `Diffing Source vs Kafka${hasColCmp ? ` (PK + ${extraCols.length} column value(s))` : ' (PK only)'}…`);
    const missingInKafka = [];
    for (const [key, pkVals] of sourcePKs) {
      if (!kafkaAlive.has(key)) missingInKafka.push(pkVals);
    }
    const missingInSource = [];
    for (const [key, pkVals] of kafkaAlive) {
      if (!sourcePKs.has(key)) missingInSource.push(pkVals);
    }
    const matched = sourcePKs.size - missingInKafka.length;

    if (_compareAbortController === ac) _compareAbortController = null;

    const elapsedMs = Date.now() - t0;
    step('🏁', `Done — ${matched.toLocaleString()} matched, ${missingInKafka.length} missing in Kafka, ${missingInSource.length} ghost in Kafka`, `${elapsedMs}ms`);

    emit('done', {
      pkColumns,
      compareColumns:  extraCols,
      sourceCount:     sourcePKs.size,
      kafkaAliveCount: kafkaAlive.size,
      matched:         Math.max(0, matched),
      missingInKafka,
      missingInSource,
      dbQueryPlan,
      kafkaPlan,
      dbMode: pgTsWhere || oraTsWhere ? 'column-where' : 'full-scan',
      elapsedMs,
      rowLimit: rowLimitNum || 0,
    });
    res.end();

  } catch (e) {
    if (_compareAbortController === ac) _compareAbortController = null;
    const aborted = e.message?.includes('aborted') || e.name === 'AbortError';
    emit(aborted ? 'aborted' : 'error', { error: e.message, aborted });
    res.end();
  }
});

// ── POST /api/compare/backfill ────────────────────────────────────────────
// Triggers Debezium ad-hoc snapshot for a specific table via the signal topic.
// This re-sends all current rows for that table into the Kafka topic which
// the Iceberg streaming jobs will then MERGE/upsert automatically.
app.post('/api/compare/backfill', async (req, res) => {
  const { source, connector, table, schema } = req.body;
  if (!connector || !table) return res.status(400).json({ error: 'connector and table are required' });

  try {
    // Read current connector config to get signal topic / data collection
    const cfgR = await dbzRequest(source, 'GET', `/connectors/${connector}/config`);
    if (cfgR.status >= 300) {
      return res.status(cfgR.status).json({ error: 'Could not read connector config', detail: cfgR.body });
    }
    const cfg = cfgR.body;

    // Debezium 2.x ad-hoc snapshot signal:
    //   topic: <topic.prefix>.<signalTopic> or the configured signal.data.collection
    //   key:   any string (signal id)
    //   value: {"type":"execute-snapshot","data":{"data-collections":["schema.table"],"type":"incremental"}}
    const topicPrefix  = cfg['topic.prefix'] || source;
    const signalTopic  = cfg['signal.data.collection'] || `${topicPrefix}.debezium_signal`;

    // For Oracle use fully qualified name in uppercase
    const dataCollection = source === 'oracle'
      ? `${(schema||'CACHE_TESTING').toUpperCase()}.${table.toUpperCase()}`
      : source === 'mongodb'
      ? table   // MongoDB: collection name as-is
      : `${schema||'public'}.${table}`;

    const signalKey   = `snapshot-${Date.now()}`;
    const signalValue = JSON.stringify({
      type: 'execute-snapshot',
      data: { 'data-collections': [dataCollection], type: 'incremental' },
    });

    // Write signal message into the signal topic via kafka-console-producer
    const pod = await getDbzPod();
    ensureSaslFileSync(pod);

    const { stderr: signalErr } = await spawnWithStdin('kubectl', [
      'exec', '-i', '-n', K8S_NAMESPACE, pod, '-c', 'debezium-connect', '--',
      '/kafka/bin/kafka-console-producer.sh',
      '--bootstrap-server', KAFKA_BOOTSTRAP,
      '--topic', signalTopic,
      '--property', 'parse.key=true',
      '--property', 'key.separator=\t',
      '--producer.config', SASL_CFG_FILE,
    ], `${signalKey}\t${signalValue}\n`, 60000).catch(e => ({ stderr: e.message }));

    if (signalErr) {
      return res.status(500).json({ error: `Signal failed: ${signalErr.slice(0, 300)}` });
    }

    res.json({
      ok: true,
      message: `Incremental snapshot signal sent for ${dataCollection} on connector "${connector}". Debezium will re-emit all current rows into the topic.`,
      signalTopic,
      dataCollection,
    });

  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ── POST /api/compare/delete-from-topic ──────────────────────────────────
// Publishes a Debezium-format DELETE + tombstone for each ghost PK row so
// the Iceberg streaming job processes a hard delete on those rows.
app.post('/api/compare/delete-from-topic', async (req, res) => {
  const { topic, pkRows, pkColumns, topicPrefix } = req.body;
  if (!topic || !pkRows?.length || !pkColumns?.length) {
    return res.status(400).json({ error: 'topic, pkRows and pkColumns are required' });
  }

  try {
    const pod = await getDbzPod();
    ensureSaslFileSync(pod);

    // Build key\tvalue lines — tab is used as separator (safe: JSON never contains raw tabs).
    // Key   = schema-less JSON object: { pk_col: val, … }  (matches /fix and real Debezium)
    // Value = schema-less DELETE envelope with op='d' and after=null
    // Tombstone = key\t  (empty value) — signals Kafka log compaction to remove entry
    const tsMs = Date.now();
    const lines = [];
    for (const pkValues of pkRows) {
      const pkPayload = {};
      pkColumns.forEach((col, i) => { pkPayload[col] = pkValues[i]; });

      // Schema-less key — matches the format used by /api/compare/fix Phase 1 + Phase 2
      const keyJson = JSON.stringify(pkPayload);

      // Schema-less DELETE event (op=d, after=null, before has pk values)
      const valJson = JSON.stringify({
        op: 'd', before: pkPayload, after: null,
        source: { version: 'manual', connector: 'compare-job', ts_ms: tsMs }, ts_ms: tsMs,
      });

      lines.push(`${keyJson}\t${valJson}`);
      // Tombstone: same key, empty value (for log compaction)
      lines.push(`${keyJson}\t`);
    }

    const { stderr } = await spawnWithStdin('kubectl', [
      'exec', '-i', '-n', K8S_NAMESPACE, pod, '-c', 'debezium-connect', '--',
      '/kafka/bin/kafka-console-producer.sh',
      '--bootstrap-server', KAFKA_BOOTSTRAP,
      '--topic', topic,
      '--property', 'parse.key=true',
      '--property', 'key.separator=\t',
      '--producer.config', SASL_CFG_FILE,
    ], lines.join('\n') + '\n', 120000).catch(e => ({ stderr: e.message }));

    if (stderr) {
      return res.status(500).json({ error: `Produce failed: ${stderr.slice(0, 300)}` });
    }

    res.json({
      ok: true,
      deleted: pkRows.length,
      message: `Published ${pkRows.length} DELETE + tombstone message(s) to topic "${topic}". The Iceberg streaming job will hard-delete these rows on next micro-batch.`,
    });

  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ── POST /api/compare/verify-in-source ───────────────────────────────────
// Fast PK-only check: given pkRows (from Kafka ghost list), return which PKs
// are actually ABSENT from the source DB.
// Uses a PK IN-list query so the DB hits the PK index — no full scan.
// Body: { source, table, schema, db, pkColumns, pkRows }
// Returns: { absentPKs: [[val,…],…], presentPKs: [[val,…],…] }
app.post('/api/compare/verify-in-source', async (req, res) => {
  const { source, table, schema, db, pkColumns, pkRows } = req.body;
  if (!table || !pkColumns?.length || !pkRows?.length)
    return res.status(400).json({ error: 'table, pkColumns and pkRows are required' });

  // Reuse the same buildPkInWhere helper logic inline
  const quote = v => {
    if (v === null || v === undefined) return 'NULL';
    const s = String(v);
    return /^-?\d+(\.\d+)?$/.test(s) ? s : `'${s.replace(/'/g, "''")}'`;
  };
  const buildInWhere = (dialect) => {
    if (pkColumns.length === 1) {
      const vals = pkRows.map(r => quote(r[0])).join(',');
      return `WHERE ${dialect === 'oracle' ? pkColumns[0].toUpperCase() : pkColumns[0]} IN (${vals})`;
    }
    const colList = dialect === 'oracle' ? pkColumns.map(c => c.toUpperCase()).join(',') : pkColumns.join(',');
    const rowList = pkRows.map(row => `(${row.map(quote).join(',')})`).join(',');
    return `WHERE (${colList}) IN (${rowList})`;
  };

  try {
    let foundKeys = new Set();

    if (source === 'postgres') {
      const pod = await getSourcePod('postgresql', 'postgres');
      if (!pod) return res.status(503).json({ error: 'No PostgreSQL pod found' });
      const database   = db     || 'postgres';
      const schemaName = schema || 'public';
      const cols   = pkColumns.join(', ');
      const sql    = `SELECT ${cols} FROM ${schemaName}.${table} ${buildInWhere('postgres')}`;
      const { stdout } = await trackedExecFile('kubectl', [
        'exec', '-n', K8S_NAMESPACE, pod, '--',
        'psql', '-U', 'postgres', '-d', database, '--csv', '-c', sql,
      ], { timeout: 30000, maxBuffer: 16 * 1024 * 1024 });
      const lines = stdout.trim().split('\n').filter(Boolean).slice(1);
      for (const line of lines) {
        const vals = line.split(',').map(v => v.trim().replace(/^"|"$/g, ''));
        foundKeys.add(JSON.stringify(vals.map(v => v === '' ? null : String(v))));
      }

    } else if (source === 'oracle') {
      const connCfg = await oracleConnCfg({});
      const owner   = (schema || 'CACHE_TESTING').toUpperCase();
      const tbl     = table.toUpperCase();
      const cols    = pkColumns.map(c => c.toUpperCase()).join(', ');
      const sql     = `SELECT ${cols} FROM ${owner}.${tbl} t ${buildInWhere('oracle')}`;
      const rows    = await oracleQuery(connCfg, sql);
      for (const row of rows) {
        foundKeys.add(JSON.stringify(row.map(v => (v === null || v === undefined) ? null : String(v))));
      }

    } else if (source === 'mongodb') {
      const database = db || 'cache_testing';
      const ids = pkRows.map(r => {
        const s = r[0];
        return (typeof s === 'string' && /^[0-9a-fA-F]{24}$/.test(s))
          ? new ObjectId(s)
          : s;
      });
      const docs = await withMongo({}, async (client) => {
        return client.db(database).collection(table)
          .find({ _id: { $in: ids } }, { projection: { _id: 1 } }).toArray();
      });
      for (const doc of docs) foundKeys.add(JSON.stringify([String(doc._id)]));

    } else {
      return res.status(400).json({ error: `Unknown source: ${source}` });
    }

    const absentPKs  = [];
    const presentPKs = [];
    for (const row of pkRows) {
      const key = JSON.stringify(row.map(v => (v === null || v === undefined) ? null : String(v)));
      (foundKeys.has(key) ? presentPKs : absentPKs).push(row);
    }
    res.json({ absentPKs, presentPKs, checkedCount: pkRows.length });

  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── POST /api/compare/targeted-backfill ──────────────────────────────────
// Fetch specific missing rows from source by PK IN-list, then publish them
// as Debezium-format CREATE events (op='r' read/snapshot) directly into the
// Kafka topic.  This replicates only the missing rows — no full snapshot.
// Body: { source, table, schema, db, topic, pkColumns, pkRows }
// Returns: { replicated: N, message }
app.post('/api/compare/targeted-backfill', async (req, res) => {
  const { source, table, schema, db, topic, pkColumns, pkRows } = req.body;
  if (!table || !topic || !pkColumns?.length || !pkRows?.length)
    return res.status(400).json({ error: 'table, topic, pkColumns and pkRows are required' });

  const quote = v => {
    if (v === null || v === undefined) return 'NULL';
    const s = String(v);
    return /^-?\d+(\.\d+)?$/.test(s) ? s : `'${s.replace(/'/g, "''")}'`;
  };
  const buildInWhere = (dialect) => {
    if (pkColumns.length === 1) {
      const vals = pkRows.map(r => quote(r[0])).join(',');
      return `WHERE ${dialect === 'oracle' ? pkColumns[0].toUpperCase() : pkColumns[0]} IN (${vals})`;
    }
    const colList = dialect === 'oracle' ? pkColumns.map(c => c.toUpperCase()).join(',') : pkColumns.join(',');
    const rowList = pkRows.map(row => `(${row.map(quote).join(',')})`).join(',');
    return `WHERE (${colList}) IN (${rowList})`;
  };

  try {
    // ── 1. Fetch full rows from source DB using PK IN-list (index seek) ────
    let columns = [];
    let dataRows = []; // array of { colName: value } objects

    if (source === 'postgres') {
      const pod = await getSourcePod('postgresql', 'postgres');
      if (!pod) return res.status(503).json({ error: 'No PostgreSQL pod found' });
      const database   = db     || 'postgres';
      const schemaName = schema || 'public';
      // Fetch ALL columns via SELECT *
      const sql = `SELECT * FROM ${schemaName}.${table} ${buildInWhere('postgres')}`;
      const { stdout } = await trackedExecFile('kubectl', [
        'exec', '-n', K8S_NAMESPACE, pod, '--',
        'psql', '-U', 'postgres', '-d', database, '--csv', '-c', sql,
      ], { timeout: 30000, maxBuffer: 32 * 1024 * 1024 });
      const lines = stdout.trim().split('\n').filter(Boolean);
      if (lines.length < 2) return res.json({ replicated: 0, message: 'No rows found in source for the given PKs' });
      columns = lines[0].split(',').map(v => v.replace(/^"|"$/g, '').trim());
      for (const line of lines.slice(1)) {
        const vals = line.split(',').map(v => v.trim().replace(/^"|"$/g, ''));
        const obj = {};
        columns.forEach((c, i) => { obj[c] = vals[i] === '' ? null : vals[i]; });
        dataRows.push(obj);
      }

    } else if (source === 'oracle') {
      const connCfg = await oracleConnCfg({});
      const owner   = (schema || 'CACHE_TESTING').toUpperCase();
      const tbl     = table.toUpperCase();
      // Get column list first
      const colRows = await oracleQuery(connCfg,
        `SELECT column_name FROM all_tab_columns WHERE owner='${owner}' AND table_name='${tbl}' ORDER BY column_id`);
      columns = colRows.map(r => r[0]);
      const sql = `SELECT * FROM ${owner}.${tbl} t ${buildInWhere('oracle')}`;
      const rows = await oracleQuery(connCfg, sql);
      for (const row of rows) {
        const obj = {};
        columns.forEach((c, i) => { obj[c] = (row[i] === null || row[i] === undefined) ? null : String(row[i]); });
        dataRows.push(obj);
      }

    } else if (source === 'mongodb') {
      const database = db || 'cache_testing';
      // pkRows[i][0] is always a string (from normalisePkVal). Try to coerce
      // each value back to ObjectId — if it looks like a 24-hex-char OID.
      // For collections that use plain-string _ids the string fallback is kept.
      const ids = pkRows.map(r => {
        const s = r[0];
        return (typeof s === 'string' && /^[0-9a-fA-F]{24}$/.test(s))
          ? new ObjectId(s)
          : s;
      });
      const docs = await withMongo({}, async (client) => {
        return client.db(database).collection(table)
          .find({ _id: { $in: ids } }).toArray();
      });
      // For MongoDB: store raw docs so we can emit a proper Debezium envelope
      for (const doc of docs) {
        dataRows.push(doc);
        if (!columns.length) columns = Object.keys(doc).map(String);
      }

    } else {
      return res.status(400).json({ error: `Unknown source: ${source}` });
    }

    if (!dataRows.length)
      return res.status(404).json({ replicated: 0, error: 'No rows found in source for the given PKs — they may have been deleted since the compare ran, or the _id type could not be matched.' });

    // ── 2. Build Debezium-format CREATE (op='r') messages ──────────────────
    // Use the same schema-less format as /api/compare/fix so the Iceberg
    // streaming consumer sees identical message shapes regardless of which
    // endpoint produced the backfill.
    const pod = await getDbzPod();
    ensureSaslFileSync(pod);

    // Cast string values back to native types (numbers as numbers, '' as null).
    // psql --csv and oracleQuery both return everything as strings; this
    // ensures numeric PK columns match real Debezium messages.
    const castVal = (v) => {
      if (v === null || v === undefined || v === '') return null;
      if (/^-?\d+$/.test(v))         return parseInt(v, 10);
      if (/^-?\d+\.\d+$/.test(v))    return parseFloat(v);
      return v;
    };

    const lines = [];
    const tsMs = Date.now();
    for (const rowObj of dataRows) {
      let keyJson, valJson;

      if (source === 'mongodb') {
        // Build a plain-string _id value that matches what sourcePKs stores
        // (String(doc._id) = hex string for ObjectId, or the raw string value).
        const idRaw = rowObj._id;
        const idPlain = (idRaw && typeof idRaw === 'object' && idRaw._bsontype === 'ObjectId')
          ? idRaw.toHexString()
          : String(idRaw);

        // Key uses "_id" field name to match pkColumns = ['_id'].
        // Value is the $oid JSON string so the scanner's normalisePkVal can
        // unwrap it back to the plain hex — keeping the round-trip consistent.
        const idStr = (idRaw && typeof idRaw === 'object' && idRaw._bsontype === 'ObjectId')
          ? JSON.stringify({ $oid: idPlain })
          : JSON.stringify(idRaw);

        // Schema-less key: {"_id": "{\"$oid\":\"...\"}"}
        keyJson = JSON.stringify({ _id: idStr });

        // MongoDB Debezium value envelope: after is a JSON-stringified document
        const afterDoc = JSON.parse(JSON.stringify(rowObj, (_, v) => {
          // Serialise ObjectId as {"$oid":"..."} so downstream can parse it
          if (v && typeof v === 'object' && v._bsontype === 'ObjectId') return { $oid: v.toHexString() };
          return v;
        }));
        valJson = JSON.stringify({
          op:     'r',
          before: null,
          after:  JSON.stringify(afterDoc),
          source: { version: 'manual', connector: 'compare-targeted-backfill', ts_ms: tsMs },
          ts_ms:  tsMs,
        });

      } else {
        // Relational sources (postgres / oracle): flat object, schema-less envelope.
        // For Oracle, column names in rowObj are uppercase (oracleQuery returns them
        // that way) — normalise the after object so keys always match pkColumns case.
        const afterObj = {};
        for (const [col, val] of Object.entries(rowObj)) {
          afterObj[col] = castVal(val);
        }

        // Build key with only PK columns, values cast to native types.
        // Try exact match first, then uppercase fallback for Oracle.
        const keyObj = {};
        pkColumns.forEach(col => {
          const v = rowObj[col] ?? rowObj[col.toUpperCase()];
          keyObj[col] = castVal(v);
        });
        keyJson = JSON.stringify(keyObj);

        valJson = JSON.stringify({
          before: null,
          after:  afterObj,
          source: { version: 'manual', connector: 'compare-targeted-backfill', ts_ms: tsMs },
          op:     'r',
          ts_ms:  tsMs,
        });
      }

      lines.push(`${keyJson}\t${valJson}`);
    }

    const { stderr } = await spawnWithStdin('kubectl', [
      'exec', '-i', '-n', K8S_NAMESPACE, pod, '-c', 'debezium-connect', '--',
      '/kafka/bin/kafka-console-producer.sh',
      '--bootstrap-server', KAFKA_BOOTSTRAP,
      '--topic', topic,
      '--property', 'parse.key=true',
      '--property', 'key.separator=\t',
      '--producer.config', SASL_CFG_FILE,
    ], lines.join('\n') + '\n', 120000).catch(e => ({ stderr: e.message }));

    if (stderr)
      return res.status(500).json({ error: `Produce failed: ${stderr.slice(0, 300)}` });

    res.json({
      ok: true,
      replicated: dataRows.length,
      message: `Replicated ${dataRows.length} missing row(s) into topic "${topic}" as snapshot events (op=r). The Iceberg streaming job will MERGE them on the next micro-batch.`,
    });

  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── POST /api/compare/fix  (SSE) ─────────────────────────────────────────
// Streams live step-by-step progress while fixing missing/ghost rows.
// Body: { source, table, schema, db, topic, connector, pkColumns,
//         missingInKafka: [[pk,…],…], missingInSource: [[pk,…],…] }
// SSE events: { type:'step', icon, msg, detail }
//             { type:'done', replicatedCount, deletedCount, verifyCount, elapsedMs }
//             { type:'error', error }
app.post('/api/compare/fix', async (req, res) => {
  const { source, table, schema, db, topic, pkColumns, tsColumn,
          compareColumns = [],
          missingInKafka = [], missingInSource = [] } = req.body;

  res.setHeader('Content-Type',  'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection',    'keep-alive');
  res.flushHeaders();

  const emit = (type, payload = {}) => {
    try { res.write(`data: ${JSON.stringify({ type, ...payload })}\n\n`); } catch {}
  };
  const step = (icon, msg, detail = '') =>
    emit('step', { icon, msg, detail: detail ? String(detail).slice(0, 300) : '', ts: Date.now() });

  const t0 = Date.now();
  let replicatedCount = 0;
  let deletedCount    = 0;
  let verifyCount     = 0;  // how many of the backfilled PKs were confirmed in Kafka
  // Declared in outer scope so the done event can include it for post-fix re-run anchoring
  let preProduceOffsets = new Map();

  try {
    // ── PHASE 1: Backfill missing-in-Kafka rows ───────────────────────────
    if (missingInKafka.length) {
      step('🔍', `Phase 1 — Fetch missing rows from source DB`,
        `${missingInKafka.length} PK(s) to look up in ${source}`);

      // ── 1a. Fetch full rows from source ──────────────────────────────────
      let dataRows = [];

      if (source === 'postgres') {
        const pod = await getSourcePod('postgresql', 'postgres');
        if (!pod) { emit('error', { error: 'No PostgreSQL pod found' }); return res.end(); }
        const database   = db     || 'postgres';
        const schemaName = schema || 'public';
        const quote = v => {
          if (v === null || v === undefined) return 'NULL';
          const s = String(v);
          return /^-?\d+(\.\d+)?$/.test(s) ? s : `'${s.replace(/'/g, "''")}'`;
        };
        const whereVals = pkColumns.length === 1
          ? missingInKafka.map(r => quote(r[0])).join(',')
          : missingInKafka.map(row => `(${row.map(quote).join(',')})`).join(',');
        const whereClause = pkColumns.length === 1
          ? `WHERE ${pkColumns[0]} IN (${whereVals})`
          : `WHERE (${pkColumns.join(',')}) IN (${whereVals})`;
        const sql = `SELECT * FROM ${schemaName}.${table} ${whereClause}`;

        step('🗄', `Querying PostgreSQL`, `SELECT * FROM ${schemaName}.${table} WHERE ${pkColumns[0]} IN (${missingInKafka.length} values)`);
        const { stdout } = await trackedExecFile('kubectl', [
          'exec', '-n', K8S_NAMESPACE, pod, '--',
          'psql', '-U', 'postgres', '-d', database, '--csv', '-c', sql,
        ], { timeout: 30000, maxBuffer: 32 * 1024 * 1024 });
        // Use the same RFC-4180 CSV parser used by compare/run (handles quoted commas)
        const parseCsv = line => {
          const vals = []; let cur = '', inQ = false;
          for (let i = 0; i < line.length; i++) {
            const c = line[i];
            if (c === '"') {
              if (inQ && line[i+1] === '"') { cur += '"'; i++; }
              else inQ = !inQ;
            } else if (c === ',' && !inQ) { vals.push(cur); cur = ''; }
            else cur += c;
          }
          vals.push(cur);
          return vals.map(v => v === '' ? null : v);
        };
        const lines = stdout.trim().split('\n').filter(Boolean);
        if (lines.length >= 2) {
          const cols = parseCsv(lines[0]);
          for (const line of lines.slice(1)) {
            const vals = parseCsv(line);
            const obj = {};
            cols.forEach((c, i) => { obj[c] = vals[i]; });
            dataRows.push(obj);
          }
        }

      } else if (source === 'oracle') {
        const connCfg = await oracleConnCfg({});
        const owner = (schema || 'CACHE_TESTING').toUpperCase();
        const tbl   = table.toUpperCase();
        const quote = v => {
          if (v === null || v === undefined) return 'NULL';
          const s = String(v);
          return /^-?\d+(\.\d+)?$/.test(s) ? s : `'${s.replace(/'/g, "''")}'`;
        };
        const whereVals = pkColumns.length === 1
          ? missingInKafka.map(r => quote(r[0])).join(',')
          : missingInKafka.map(row => `(${row.map(quote).join(',')})`).join(',');
        const whereClause = pkColumns.length === 1
          ? `WHERE ${pkColumns[0].toUpperCase()} IN (${whereVals})`
          : `WHERE (${pkColumns.map(c=>c.toUpperCase()).join(',')}) IN (${whereVals})`;
        const colRows = await oracleQuery(connCfg,
          `SELECT column_name FROM all_tab_columns WHERE owner='${owner}' AND table_name='${tbl}' ORDER BY column_id`);
        const cols = colRows.map(r => r[0]);
        step('🗄', `Querying Oracle`, `SELECT * FROM ${owner}.${tbl} ${whereClause.slice(0,80)}`);
        const rows = await oracleQuery(connCfg, `SELECT * FROM ${owner}.${tbl} t ${whereClause}`);
        for (const row of rows) {
          const obj = {};
          cols.forEach((c, i) => { obj[c] = (row[i] === null || row[i] === undefined) ? null : String(row[i]); });
          dataRows.push(obj);
        }

      } else if (source === 'mongodb') {
        const database = db || 'cache_testing';
        const ids = missingInKafka.map(r => {
          const s = r[0];
          return (typeof s === 'string' && /^[0-9a-fA-F]{24}$/.test(s)) ? new ObjectId(s) : s;
        });
        step('🗄', `Querying MongoDB`, `db.${database}.${table}.find({ _id: { $in: [${missingInKafka.length} ids] } })`);
        const docs = await withMongo({}, async (client) => {
          return client.db(database).collection(table).find({ _id: { $in: ids } }).toArray();
        });
        dataRows = docs;

      } else {
        emit('error', { error: `Unknown source: ${source}` }); return res.end();
      }

      step('✅', `Source query complete — ${dataRows.length} row(s) fetched`,
        dataRows.length < missingInKafka.length
          ? `⚠ ${missingInKafka.length - dataRows.length} PK(s) not found in source (deleted since compare ran)`
          : `All ${dataRows.length} rows found`);

      if (!dataRows.length) {
        emit('error', { error: `No rows found in source for the given PKs — rows may have been deleted since the compare ran` });
        return res.end();
      }

      // ── 1b. Build Debezium-format messages ───────────────────────────────
      step('🔨', `Building Debezium envelope messages`, `op=r (read/snapshot) for ${dataRows.length} row(s)`);

      const pod = await getDbzPod();
      ensureSaslFileSync(pod);

      // ── Pre-check: ask ksqlDB for the PKs we are about to write ──────────
      // Uses ksqlDB pull query (WHERE ROWKEY IN) — instant, no topic scanning.
      // Any PK already alive in Kafka is skipped so we never duplicate rows.
      const alreadyInKafka = new Set();
      try {
        const { tableName: fixTableName, ready: fixTableReady } = await ksqlEnsureTable(
          topic, (icon, msg) => step(icon, msg)
        );
        const preCheckPkVals = missingInKafka.map(r => r.map(v => v != null ? String(v) : null));
        step('🔍', `Pre-check — ${preCheckPkVals.length} PK(s) via ksqlDB pull query…`);

        let skipCount = 0;
        if (fixTableReady) {
          const pkMap = await ksqlPullByPKs(fixTableName, pkColumns, preCheckPkVals, [], () => {});
          for (const [, entry] of pkMap) {
            if (entry.op !== 'd') {
              alreadyInKafka.add(JSON.stringify(entry.pkValues.map(v => v != null ? String(v) : null)));
              skipCount++;
            }
          }
        } else {
          // Table not ready — fall back to direct scan for pre-check
          const preCheckWanted = new Map(preCheckPkVals.map(pkVals => [JSON.stringify(pkVals), pkVals]));
          const preCheckResult = await scanTopicForPKsTargeted(topic, pkColumns, preCheckWanted, [], undefined, 0, 0, () => {}, undefined);
          for (const [, entry] of preCheckResult.pkMap) {
            if (entry.op !== 'd') {
              alreadyInKafka.add(JSON.stringify(entry.pkValues.map(v => v != null ? String(v) : null)));
              skipCount++;
            }
          }
        }
        if (skipCount > 0) {
          step('ℹ️', `Pre-check: ${skipCount}/${missingInKafka.length} PK(s) already live in Kafka — will skip`,
            `${missingInKafka.length - skipCount} genuinely missing and will be replicated`);
        } else {
          step('📋', `Pre-check: 0/${missingInKafka.length} PKs found in Kafka — all need replication`);
        }
      } catch (preErr) {
        step('⚠️', `Pre-check failed: ${preErr.message} — proceeding with full replication`);
      }

      const nowMs = Date.now();
      const kafkaLines = [];
      let skippedAlreadyPresent = 0;

      for (const rowObj of dataRows) {
        // Skip rows that the pre-check confirmed are already alive in Kafka.
        // This prevents the fix loop: compare misses a row (tail-scan too short),
        // fix re-writes it, compare misses it again, fix re-writes again, ...
        if (alreadyInKafka.size > 0) {
          const rowPkKey = source === 'mongodb'
            ? (() => {
                const idRaw = rowObj._id;
                const idPlain = (idRaw && typeof idRaw === 'object' && idRaw._bsontype === 'ObjectId')
                  ? idRaw.toHexString() : String(idRaw);
                return JSON.stringify([idPlain]);
              })()
            : JSON.stringify(pkColumns.map(col => {
                const v = rowObj[col] ?? rowObj[col?.toUpperCase()];
                return (v === null || v === undefined) ? null : String(v);
              }));
          if (alreadyInKafka.has(rowPkKey)) {
            skippedAlreadyPresent++;
            continue;
          }
        }

        // Use the row's own timestamp column value (e.g. updated_at) as ts_ms so
        // the message lands within the original compare time window.  Falls back
        // to now if the column is absent or the value can't be parsed.
        let tsMs = nowMs;
        if (tsColumn && rowObj[tsColumn] != null) {
          const parsed = Date.parse(String(rowObj[tsColumn]));
          if (!isNaN(parsed)) tsMs = parsed;
        }
        let keyJson, valJson;
        if (source === 'mongodb') {
          const idRaw   = rowObj._id;
          const idPlain = (idRaw && typeof idRaw === 'object' && idRaw._bsontype === 'ObjectId')
            ? idRaw.toHexString() : String(idRaw);
          const idStr   = (idRaw && typeof idRaw === 'object' && idRaw._bsontype === 'ObjectId')
            ? JSON.stringify({ $oid: idPlain }) : JSON.stringify(idRaw);
          keyJson = JSON.stringify({ _id: idStr });
          const afterDoc = JSON.parse(JSON.stringify(rowObj, (_, v) => {
            if (v && typeof v === 'object' && v._bsontype === 'ObjectId') return { $oid: v.toHexString() };
            return v;
          }));
          valJson = JSON.stringify({
            op: 'r', before: null, after: JSON.stringify(afterDoc),
            source: { version: 'manual', connector: 'compare-fix', ts_ms: tsMs }, ts_ms: tsMs,
          });
        } else {
          // ── Match real Debezium key/value format exactly ───────────────────
          // Real Debezium postgres messages:
          //   key:   {"id": 30862}               — plain JSON, no schema wrapper, numeric
          //   value: {"before":null,"after":{...},"op":"r","ts_ms":...}  — no schema wrapper
          //
          // psql --csv returns all values as strings. Cast numeric-looking values back
          // to numbers so the message matches what Debezium would have written.
          const castVal = (v) => {
            if (v === null || v === undefined) return null;
            if (v === '') return null;
            // integer
            if (/^-?\d+$/.test(v)) return parseInt(v, 10);
            // float
            if (/^-?\d+\.\d+$/.test(v)) return parseFloat(v);
            return v;
          };

          // Build key: plain JSON object with only PK columns, values cast to native types
          const keyObj = {};
          pkColumns.forEach(col => {
            const v = rowObj[col] ?? rowObj[col.toUpperCase()] ?? null;
            keyObj[col] = castVal(v);
          });
          keyJson = JSON.stringify(keyObj);

          // Build after: all columns with values cast to native types
          const afterObj = {};
          for (const [col, val] of Object.entries(rowObj)) {
            afterObj[col] = castVal(val);
          }

          valJson = JSON.stringify({
            before: null,
            after:  afterObj,
            source: { version: 'manual', connector: 'compare-fix', ts_ms: tsMs },
            op:     'r',
            ts_ms:  tsMs,
          });
        }
        kafkaLines.push(`${keyJson}\t${valJson}`);
      }

      if (skippedAlreadyPresent > 0) {
        step('ℹ️', `Skipped ${skippedAlreadyPresent} row(s) already present and alive in Kafka — not re-written`,
          `Only ${kafkaLines.length} row(s) need replication`);
      }
      // If every row was already present, skip produce entirely and mark all as verified
      if (kafkaLines.length === 0 && skippedAlreadyPresent > 0) {
        replicatedCount = 0;
        verifyCount     = skippedAlreadyPresent;
        step('✅', `All ${skippedAlreadyPresent} row(s) confirmed already in Kafka — nothing to replicate`,
          `Re-run compare to confirm (tail window may have been too short to detect them)`);
        // Emit done — no produce, no verify scan needed
        const elapsedMs = Date.now() - t0;
        emit('done', {
          replicatedCount: 0, deletedCount: 0, verifyCount: skippedAlreadyPresent, elapsedMs,
          preProduceOffsets: preProduceOffsets.size > 0 ? Object.fromEntries(preProduceOffsets) : null,
          alreadyPresent: skippedAlreadyPresent,
        });
        return res.end();
      }

      // ── 1c. Produce to Kafka ──────────────────────────────────────────────
      // Tab (\t) is used as key separator — it is never present in JSON payloads.
      // Timeout 120s: kafka-console-producer on a large Kafka cluster can be slow.
      // Any non-empty stderr means failure (timeout, auth error, broker error).
      // ── Capture end-offsets before produce so post-fix re-run can anchor ──
      // This fills preProduceOffsets (partition → offset) which is sent in the
      // 'done' event and used by the frontend to anchor the Kafka scan to the
      // exact position where the fix messages were written.
      try {
        const dbzPodForOffsets = await getDbzPod();
        ensureSaslFileSync(dbzPodForOffsets);
        const offsetOut = await kafkaExec(
          `/kafka/bin/kafka-get-offsets.sh --bootstrap-server ${KAFKA_BOOTSTRAP} --topic ${topic} --time latest --command-config ${SASL_CFG_FILE}`,
          15000
        );
        for (const line of offsetOut.trim().split('\n')) {
          const parts = line.trim().split(':');
          if (parts.length >= 3) {
            const partition = Number(parts[1]);
            const offset    = Number(parts[2]);
            if (!isNaN(partition) && !isNaN(offset)) preProduceOffsets.set(partition, offset);
          }
        }
        if (preProduceOffsets.size > 0) {
          step('📍', `Captured pre-produce offsets for ${preProduceOffsets.size} partition(s) — post-fix re-run will anchor here`);
        }
      } catch (offErr) {
        step('⚠️', `Could not capture pre-produce offsets: ${offErr.message} — post-fix re-run will do a full scan`);
      }

      step('📤', `Writing ${kafkaLines.length} message(s) to Kafka topic`, topic);
      const { stderr: produceErr } = await spawnWithStdin('kubectl', [
        'exec', '-i', '-n', K8S_NAMESPACE, pod, '-c', 'debezium-connect', '--',
        '/kafka/bin/kafka-console-producer.sh',
        '--bootstrap-server', KAFKA_BOOTSTRAP,
        '--topic', topic,
        '--property', 'parse.key=true',
        '--property', 'key.separator=\t',
        '--producer.config', SASL_CFG_FILE,
      ], kafkaLines.join('\n') + '\n', 120000).catch(e => ({ stderr: e.message }));

      if (produceErr) {
        emit('error', { error: `Kafka produce failed: ${produceErr.slice(0, 300)}` });
        return res.end();
      }
      replicatedCount = kafkaLines.length;
      step('✅', `${replicatedCount} message(s) written to Kafka`, `Topic: ${topic}`);

      // ── 1d. Verify — ksqlDB pull query confirms written PKs are visible ───
      // The messages were just produced. ksqlDB materialises them within a few
      // seconds. Poll up to 5 times (2s apart) until all PKs show up.
      step('🔎', `Verifying — ${replicatedCount} PK(s) via ksqlDB pull query…`);
      try {
        const verifyPkVals = dataRows
          .filter(row => {
            const pkValsNorm = source === 'mongodb'
              ? [String((row._id && row._id._bsontype === 'ObjectId') ? row._id.toHexString() : row._id)]
              : pkColumns.map(col => { const v = row[col] ?? row[col?.toUpperCase()]; return v != null ? String(v) : null; });
            return !alreadyInKafka.has(JSON.stringify(pkValsNorm));
          })
          .map(row => source === 'mongodb'
            ? [String((row._id && row._id._bsontype === 'ObjectId') ? row._id.toHexString() : row._id)]
            : pkColumns.map(col => { const v = row[col] ?? row[col?.toUpperCase()]; return v != null ? String(v) : null; })
          );

        const { tableName: verifyTable, ready: verifyReady } = await ksqlEnsureTable(topic, () => {});
        const maxAttempts = 15;   // up to 30s total (15 × 2s) for ksqlDB to materialise
        for (let attempt = 1; attempt <= maxAttempts; attempt++) {
          if (attempt > 1) await new Promise(r => setTimeout(r, 2000));
          try {
            const pkMap = verifyReady
              ? await ksqlPullByPKs(verifyTable, pkColumns, verifyPkVals, [], () => {})
              : new Map();
            verifyCount = [...pkMap.values()].filter(e => e.op !== 'd').length;
            if (verifyCount >= replicatedCount) break;
            if (attempt < maxAttempts)
              step('🔄', `Verify attempt ${attempt}/${maxAttempts} — ${verifyCount}/${replicatedCount} confirmed, waiting 2s…`);
          } catch { /* retry */ }
        }
        if (verifyCount >= replicatedCount) {
          step('✅', `Verified — all ${verifyCount}/${replicatedCount} row(s) confirmed in ksqlDB`);
        } else if (verifyCount > 0) {
          step('⚠️', `Partial verify — ${verifyCount}/${replicatedCount} row(s) confirmed`,
            `${replicatedCount - verifyCount} not yet visible — re-run compare to confirm`);
        } else {
          step('❌', `Verify: 0/${replicatedCount} visible yet — messages written, ksqlDB still catching up. Re-run compare to confirm.`);
        }
      } catch (verifyErr) {
        step('⚠️', `Verify error: ${verifyErr.message}`);
      }
    }

    // ── PHASE 2: Delete ghost rows from Kafka ─────────────────────────────
    if (missingInSource.length) {
      step('🗑', `Phase 2 — Delete ${missingInSource.length} ghost row(s) from Kafka`, topic);
      const pod = await getDbzPod();
      ensureSaslFileSync(pod);
      const tsMs = Date.now();
      const ghostLines = [];
      for (const pkValues of missingInSource) {
        // Build schema-less key (matches Phase 1 backfill format and real Debezium messages).
        // For Oracle, pkColumns are lowercase from the UI but the compare/run scan
        // stores values normalised as strings — keep the key column names lowercase
        // so they are consistent with Phase 1 key format.
        const pkPayload = {};
        pkColumns.forEach((col, i) => { pkPayload[col] = pkValues[i] ?? null; });
        const keyJson = JSON.stringify(pkPayload);
        // DELETE event: op='d' + tombstone — schema-less value envelope
        const valJson = JSON.stringify({
          op: 'd', before: pkPayload, after: null,
          source: { version: 'manual', connector: 'compare-fix', ts_ms: tsMs }, ts_ms: tsMs,
        });
        ghostLines.push(`${keyJson}\t${valJson}`);
        ghostLines.push(`${keyJson}\t`); // tombstone
      }
      const { stderr: delErr } = await spawnWithStdin('kubectl', [
        'exec', '-i', '-n', K8S_NAMESPACE, pod, '-c', 'debezium-connect', '--',
        '/kafka/bin/kafka-console-producer.sh',
        '--bootstrap-server', KAFKA_BOOTSTRAP,
        '--topic', topic,
        '--property', 'parse.key=true',
        '--property', 'key.separator=\t',
        '--producer.config', SASL_CFG_FILE,
      ], ghostLines.join('\n') + '\n', 120000).catch(e => ({ stderr: e.message }));

      if (delErr) {
        step('❌', `Ghost delete failed: ${delErr.slice(0, 200)}`);
      } else {
        deletedCount = missingInSource.length;
        step('✅', `${deletedCount} ghost row(s) deleted from Kafka (DELETE + tombstone)`, topic);
      }
    }

    const elapsedMs = Date.now() - t0;
    step('🏁', `Fix complete — ${replicatedCount} replicated, ${deletedCount} deleted, ${verifyCount} verified in Kafka`, `${elapsedMs}ms`);
    emit('done', {
      replicatedCount, deletedCount, verifyCount, elapsedMs,
      preProduceOffsets: preProduceOffsets.size > 0 ? Object.fromEntries(preProduceOffsets) : null,
    });
    res.end();

  } catch (e) {
    emit('error', { error: e.message });
    res.end();
  }
});

// ═══════════════════════════════════════════════════════════════════════════
//  QUERY SPACE  — execute a SELECT against the source database directly
//  POST /api/query/:source  { sql: string }
//  Returns { columns: string[], rows: any[][], rowCount: number }
//  Hard limit: 200 rows (UI sends LIMIT 10 by default, server caps at 200)
// ═══════════════════════════════════════════════════════════════════════════
app.post('/api/query/:source', async (req, res) => {
  const { source } = req.params;
  const { sql } = req.body || {};
  if (!sql || !sql.trim()) return res.status(400).json({ error: 'No SQL provided' });

  // Safety: allow only SELECT / WITH … SELECT (no DDL / DML)
  const trimmed = sql.trim().replace(/\/\*[\s\S]*?\*\//g, '').replace(/--[^\n]*/g, '').trim();
  if (!/^(SELECT|WITH)\b/i.test(trimmed)) {
    return res.status(400).json({ error: 'Only SELECT queries are allowed' });
  }

  try {
    if (source === 'postgres') {
      const pod = await getSourcePod('postgresql', 'postgres');
      if (!pod) return res.status(503).json({ error: 'No PostgreSQL pod found in cluster' });

      // Allow caller to hint the database via  -- db: mydb  comment in the SQL
      const dbMatch = sql.match(/--\s*db:\s*(\S+)/i);
      const db = dbMatch ? dbMatch[1] : 'postgres';

      // Use CSV output mode: first line = header, subsequent lines = data rows
      const wrappedSql = `SELECT * FROM (${trimmed.replace(/;+$/, '')}) __q LIMIT 200`;
      const { stdout, stderr } = await execFileAsync('kubectl', [
        'exec', '-n', K8S_NAMESPACE, pod, '--',
        'psql', '-U', 'postgres', '-d', db, '--csv', '-c', wrappedSql,
      ], { timeout: 20000 }).catch(e => ({ stdout: '', stderr: e.message }));

      if (!stdout.trim() && stderr) return res.status(500).json({ error: stderr.slice(0, 400) });

      // Parse CSV: first line is header
      function parseCsvLine(line) {
        const result = [];
        let cur = '', inQ = false;
        for (let i = 0; i < line.length; i++) {
          const ch = line[i];
          if (ch === '"' && !inQ)          { inQ = true; }
          else if (ch === '"' && inQ) {
            if (line[i+1] === '"')         { cur += '"'; i++; }
            else                           { inQ = false; }
          } else if (ch === ',' && !inQ)   { result.push(cur); cur = ''; }
          else                             { cur += ch; }
        }
        result.push(cur);
        return result;
      }
      const lines = stdout.trim().split('\n').filter(Boolean);
      const columns = lines.length ? parseCsvLine(lines[0]) : [];
      const rows    = lines.slice(1).map(parseCsvLine);

      return res.json({ columns, rows, rowCount: rows.length });

    } else if (source === 'oracle') {
      const connCfg = await oracleConnCfg({});
      // oracleQuery returns string[][] — wrap in FETCH FIRST for safety
      const wrappedSql = trimmed.replace(/;+$/, '');
      // For Oracle we use a rownum wrapper if no FETCH FIRST / ROWNUM already present
      const finalSql = /FETCH\s+FIRST|ROWNUM\s*[<=]/i.test(wrappedSql)
        ? wrappedSql
        : `SELECT * FROM (${wrappedSql}) WHERE ROWNUM <= 200`;

      // Get column names via oracledb describe
      let columns = [];
      let rows = [];
      const pg = await oracledb.getConnection({
        user:          connCfg.user,
        password:      connCfg.password,
        connectString: `${connCfg.hostname}:${connCfg.port}/${connCfg.dbname}`,
      });
      try {
        const result = await pg.execute(finalSql, [], {
          outFormat: oracledb.OUT_FORMAT_ARRAY,
          fetchArraySize: 200,
          maxRows: 200,
        });
        columns = (result.metaData || []).map(m => m.name);
        rows = (result.rows || []).map(r => r.map(v => v == null ? null : String(v)));
      } finally {
        await pg.close().catch(() => {});
      }
      return res.json({ columns, rows, rowCount: rows.length });

    } else if (source === 'mongodb') {
      // For MongoDB the "SQL" is expected to be:  db.collection.find({...}).limit(n)
      // We evaluate it safely by parsing the collection name and filter from the query.
      // Supported syntax:  SELECT * FROM <db>.<collection> [WHERE <json-filter>] [LIMIT n]
      const mongoMatch = trimmed.match(
        /SELECT\s+[\s\S]+?\s+FROM\s+([`"]?[\w-]+[`"]?)\.([`"]?[\w-]+[`"]?)(?:\s+WHERE\s+([\s\S]+?))?(?:\s+LIMIT\s+(\d+))?(?:\s*;)?$/i
      );
      if (!mongoMatch) {
        return res.status(400).json({
          error: 'MongoDB queries must use the format: SELECT * FROM <database>.<collection> [WHERE {...}] [LIMIT n]',
        });
      }
      const [, dbName, collName, whereClause, limitStr] = mongoMatch;
      const limit = Math.min(parseInt(limitStr || '10', 10), 200);
      let filter = {};
      if (whereClause) {
        try { filter = JSON.parse(whereClause.trim()); }
        catch { return res.status(400).json({ error: `WHERE clause must be valid JSON for MongoDB. Got: ${whereClause.slice(0, 100)}` }); }
      }

      const { docs, columns } = await withMongo({}, async (client) => {
        const collection = client.db(dbName).collection(collName);
        const cursor = collection.find(filter).limit(limit);
        const docs = await cursor.toArray();
        const cols = docs.length ? [...new Set(docs.flatMap(d => Object.keys(d)))] : [];
        return { docs, columns: cols };
      });

      const rows = docs.map(d => columns.map(c => {
        const v = d[c];
        if (v === null || v === undefined) return null;
        if (typeof v === 'object') return JSON.stringify(v);
        return String(v);
      }));

      return res.json({ columns, rows, rowCount: rows.length });

    } else {
      return res.status(400).json({ error: `Unknown source: ${source}` });
    }
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ═══════════════════════════════════════════════════════════════════════════
//  KSQLDB CLIENT
//  Talks to ksqlDB REST API.  Two methods:
//    ksqlStatement(ksql, props)  — DDL / admin (POST /ksql)
//    ksqlQuery(ksql, props)      — pull / push SELECT (POST /query)
// ═══════════════════════════════════════════════════════════════════════════

async function ksqlStatement(ksql, streamsProperties = {}) {
  const r = await fetch(`${KSQLDB_URL}/ksql`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/vnd.ksql.v1+json' },
    body: JSON.stringify({ ksql, streamsProperties }),
  });
  const body = await r.json().catch(() => ({}));
  if (!r.ok) throw new Error(body['@type']
    ? `${body['@type']}: ${body.message || JSON.stringify(body)}`
    : `ksqlDB error ${r.status}: ${JSON.stringify(body).slice(0, 300)}`);
  return body;
}

// ksqlQuery: runs a SELECT via POST /query-stream (ksqlDB v2 streaming API).
//
// /query-stream returns newline-delimited JSON and keeps the HTTP connection
// open (push query). We read lines one-by-one and abort as soon as we have
// collected `maxRows` data rows — this avoids waiting for ksqlDB to close
// the connection (which only happens at LIMIT for EMIT CHANGES queries on
// large topics, and can take 40+ seconds).
//
// Response format (application/vnd.ksqlapi.query.v1+json):
//   Line 1:  {"queryId":"...","columnNames":[...],"columnTypes":[...]}
//   Line 2+: [val1, val2, ...]   ← one array per row
//   Last:    {"queryId":"...","terminalMessage":"..."}
//
async function ksqlQuery(ksql, streamsProperties = {}, timeoutMs = 30000) {
  // Extract LIMIT from the SQL so we know when to stop collecting rows.
  const limitMatch = ksql.match(/\bLIMIT\s+(\d+)/i);
  const maxRows    = limitMatch ? parseInt(limitMatch[1], 10) : 500;

  const ac  = new AbortController();
  // Safety timeout — abort if we haven't collected enough rows in time
  const tid = setTimeout(() => ac.abort(), timeoutMs);

  let columns = [];
  const rows  = [];
  let   buf   = '';   // partial-line buffer across chunks

  try {
    const r = await fetch(`${KSQLDB_URL}/query-stream`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/vnd.ksqlapi.query.v1+json' },
      body: JSON.stringify({ sql: ksql, properties: streamsProperties }),
      signal: ac.signal,
    });

    if (!r.ok) {
      const errText = await r.text().catch(() => '');
      let errMsg = errText.slice(0, 300);
      try { const j = JSON.parse(errText); errMsg = j.message || j['@type'] || errMsg; } catch {}
      throw new Error(`ksqlDB /query-stream ${r.status}: ${errMsg}`);
    }

    // Read the response body as a Node.js readable stream
    for await (const chunk of r.body) {
      buf += Buffer.isBuffer(chunk) ? chunk.toString('utf8') : chunk;

      // Process all complete lines in the buffer
      let nl;
      while ((nl = buf.indexOf('\n')) !== -1) {
        const line = buf.slice(0, nl).trim();
        buf = buf.slice(nl + 1);
        if (!line) continue;
        try {
          const obj = JSON.parse(line);
          if (Array.isArray(obj)) {
            rows.push(obj);
            // Stop as soon as we have maxRows — abort the stream
            if (rows.length >= maxRows) { ac.abort(); break; }
          } else if (obj.columnNames) {
            columns = obj.columnNames;
          } else if (obj.header) {
            // v1 fallback header
            columns = obj.header.schema
              ? obj.header.schema.replace(/`/g,'').split(',').map(s => s.trim().split(' ')[0])
              : [];
          } else if (obj.row) {
            rows.push(obj.row.columns || []);
            if (rows.length >= maxRows) { ac.abort(); break; }
          }
          // terminal / finalMessage — stop cleanly
          if (obj.terminalMessage !== undefined || obj['@type'] === 'statement_error') break;
        } catch { /* skip malformed line */ }
      }
      if (rows.length >= maxRows) break;
    }
  } catch (e) {
    // AbortError is expected when we hit maxRows — treat as clean finish
    if (e.name !== 'AbortError') throw e;
  } finally {
    clearTimeout(tid);
  }

  return { columns, rows };
}

// Ensure a ksqlDB stream exists for a given Kafka topic.
// Idempotent: CREATE STREAM IF NOT EXISTS is safe to call repeatedly.
// The stream wraps the raw Debezium JSON value and exposes ROWKEY (= message key).
async function ksqlEnsureStream(topic) {
  // Derive a legal ksqlDB stream name from the topic:
  // Replace dots / hyphens with underscores; uppercase.
  const streamName = topic.replace(/[^a-zA-Z0-9_]/g, '_').toUpperCase();
  const ksql = `CREATE STREAM IF NOT EXISTS ${streamName}` +
    ` (ROWKEY VARCHAR KEY, payload VARCHAR)` +
    ` WITH (KAFKA_TOPIC='${topic}', VALUE_FORMAT='JSON', WRAP_SINGLE_VALUE=false);`;
  await ksqlStatement(ksql);
  return streamName;
}

// ── ksqlEnsureTable ───────────────────────────────────────────────────────────
// Creates a ksqlDB stream + materialised table for a topic and keeps them alive
// permanently — the table is never dropped between jobs.
//
// WHY PERMANENT: replaying 149M messages into RocksDB on every run is too slow.
// Once the table is RUNNING it stays current via its persistent query, so every
// subsequent compare is instant (pull query = SQL WHERE ROWKEY IN (...)).
//
// First run on a topic:
//   - Creates stream+table, kicks off RocksDB replay in the background.
//   - Returns ready=false immediately so the caller falls back to direct scan.
//   - The replay continues in ksqlDB asynchronously.
//
// Every run after that (table already exists and is RUNNING):
//   - Hits the in-process cache (_ksqlTableReady) — returns ready=true instantly.
//   - No ksqlDB round-trip at all.
//
// _ksqlTableReady: topic → true  (set once RUNNING is confirmed)
const _ksqlTableReady = new Map();

async function ksqlEnsureTable(topic, onStep) {
  const log        = onStep || (() => {});
  const streamName = topic.replace(/[^a-zA-Z0-9_]/g, '_').toUpperCase();
  const tableName  = streamName + '_TBL';

  // ── Cache hit: table was confirmed RUNNING in this server session ─────────
  if (_ksqlTableReady.has(topic)) return { streamName, tableName, ready: true };

  // ── Create stream (idempotent) ────────────────────────────────────────────
  try {
    await ksqlStatement(
      `CREATE STREAM IF NOT EXISTS ${streamName}` +
      ` (ROWKEY VARCHAR KEY, payload VARCHAR)` +
      ` WITH (KAFKA_TOPIC='${topic}', VALUE_FORMAT='JSON', WRAP_SINGLE_VALUE=false);`
    );
  } catch (e) {
    if (!/already exists/i.test(e.message)) throw e;
  }

  // ── Create materialised table (idempotent) ────────────────────────────────
  // ksqlDB 0.29 returns HTTP 200 with @type=warning_entity (not an error) when
  // the table already exists with IF NOT EXISTS — so we check the response body
  // rather than relying on a thrown exception.
  let tableJustCreated = false;
  try {
    const createResp = await ksqlStatement(
      `CREATE TABLE IF NOT EXISTS ${tableName} AS` +
      ` SELECT ROWKEY, LATEST_BY_OFFSET(payload) AS payload` +
      ` FROM ${streamName} GROUP BY ROWKEY EMIT CHANGES;`
    );
    // If ksqlDB returned a warning that the table already exists, treat it as
    // "already existed" — do NOT mark as just created.
    const body = Array.isArray(createResp) ? createResp[0] : createResp;
    const alreadyExisted = body?.['@type'] === 'warning_entity' ||
      /already exists/i.test(body?.message || '');
    if (!alreadyExisted) tableJustCreated = true;
  } catch (e) {
    if (!/already exists/i.test(e.message)) throw e;
    // HTTP-level error with "already exists" message — also not newly created.
  }

  // ── Check RUNNING state ───────────────────────────────────────────────────
  // On first creation: check once immediately (no waiting).
  //   If not ready yet → return ready=false so caller falls back to direct scan.
  //   RocksDB replay continues in ksqlDB; next run will find it RUNNING.
  // If table already existed: poll for up to 10s (should already be RUNNING).
  const maxWaitMs = tableJustCreated ? 0 : 10000;
  const pollMs    = 2000;
  const t0w       = Date.now();
  let   ready     = false;

  do {
    try {
      const body  = await ksqlStatement(`DESCRIBE ${tableName};`);
      const info  = Array.isArray(body) ? body[0] : body;
      const wqs   = info?.sourceDescription?.writeQueries || info?.writeQueries || [];
      if ((wqs[0]?.state || '') === 'RUNNING') { ready = true; break; }
    } catch { /* briefly unavailable — retry */ }
    if (Date.now() - t0w < maxWaitMs) await new Promise(r => setTimeout(r, pollMs));
  } while (Date.now() - t0w < maxWaitMs);

  if (ready) {
    _ksqlTableReady.set(topic, true);
    log('✅', `ksqlDB — "${tableName}" is RUNNING — pull queries ready`);
  } else if (tableJustCreated) {
    log('⏳', `ksqlDB — "${tableName}" created — replaying topic in background. This compare uses direct scan; next run will use instant pull query.`);
  } else {
    log('⚠️', `ksqlDB — "${tableName}" not RUNNING yet — falling back to direct scan`);
  }

  return { streamName, tableName, ready };
}

// ksqlPullByPKs — pull query: SELECT ROWKEY, payload FROM <table> WHERE ROWKEY IN (...)
// Returns { pkMap: Map<mapKey, {op, pkValues}>, missing: pkVals[] }
// pkVals that have no row in the table end up in `missing`.
async function ksqlPullByPKs(tableName, pkColumns, allPkVals, extraCols, onStep) {
  const log    = onStep || (() => {});
  const pkMap  = new Map();
  const BATCH  = 500;
  const total  = allPkVals.length;
  let   done   = 0;

  // ROWKEY in the ksqlDB table is the raw Kafka message key string.
  // Real Debezium postgres messages use plain JSON keys without a schema wrapper:
  //   single PK:    {"id":30862}
  //   composite PK: {"order_id":1,"line_id":2}
  // Values are numeric (not strings) where applicable.
  const castPkVal = (v) => {
    if (v === null || v === undefined || v === '') return null;
    if (/^-?\d+$/.test(String(v))) return parseInt(v, 10);
    if (/^-?\d+\.\d+$/.test(String(v))) return parseFloat(v);
    return v;
  };
  const buildKey = (vals) => {
    const obj = {};
    pkColumns.forEach((c, i) => { obj[c] = castPkVal(vals[i]); });
    return JSON.stringify(obj);
  };

  for (let i = 0; i < allPkVals.length; i += BATCH) {
    const batch  = allPkVals.slice(i, i + BATCH);
    // Keys are JSON objects like {"id":30862}.
    // Wrap in single-quotes; escape any single-quotes inside with '' (SQL standard).
    // Double-quotes inside are safe within ksqlDB single-quoted string literals.
    const inList = batch.map(v => `'${buildKey(v).replace(/'/g, "''")}'`).join(', ');
    const sql    = `SELECT ROWKEY, payload FROM ${tableName} WHERE ROWKEY IN (${inList});`;

    const { columns, rows } = await ksqlQuery(sql, {}, 60000);
    const rkIdx  = columns.findIndex(c => c.toUpperCase() === 'ROWKEY');
    const pyIdx  = columns.findIndex(c => c.toUpperCase() === 'PAYLOAD');

    for (const row of rows) {
      const rawKey = rkIdx >= 0 ? String(row[rkIdx] || '') : String(row[0] || '');
      const rawPay = pyIdx >= 0 ? row[pyIdx] : row[1];

      let op = 'r';
      try {
        const pay = typeof rawPay === 'string' ? JSON.parse(rawPay) : rawPay;
        op = pay?.op || pay?.payload?.op || 'r';
        if (!rawPay || rawPay === 'null') op = 'd';
      } catch {}

      // Extract pkVals from the raw key JSON: {"id":30862} → ["30862"]
      let pkVals = [];
      try {
        const keyObj = JSON.parse(rawKey);
        pkVals = pkColumns.map(c => {
          const v = keyObj[c] ?? keyObj[c.toUpperCase()];
          return v != null ? String(v) : null;
        });
      } catch { pkVals = [rawKey]; }

      const colVals = extraCols.map(col => {
        try {
          const pay  = typeof rawPay === 'string' ? JSON.parse(rawPay) : rawPay;
          const after = pay?.after ?? pay?.payload?.after ?? pay;
          return after ? String(after[col] ?? after[col?.toUpperCase()] ?? null) : null;
        } catch { return null; }
      });
      const mapKey = JSON.stringify([...pkVals, ...colVals]);
      pkMap.set(mapKey, { op, pkValues: pkVals });
    }

    done += batch.length;
    if (done % (BATCH * 5) === 0 || done >= total) {
      log('📨', `ksqlDB — ${done.toLocaleString()}/${total.toLocaleString()} PKs checked · ${[...pkMap.values()].filter(e=>e.op!=='d').length} live`);
    }
  }

  return pkMap;
}

// ── GET /api/ksql/streams — list all ksqlDB streams ──────────────────────────
app.get('/api/ksql/streams', async (_req, res) => {
  try {
    const body = await ksqlStatement('LIST STREAMS;');
    const streams = (Array.isArray(body) ? body[0]?.streams : body?.streams) || [];
    res.json({ streams: streams.filter(s => !s.name.startsWith('KSQL_')).map(s => ({ name: s.name, topic: s.topic, format: s.format })) });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── GET /api/ksql/tables — list all materialised tables with state ────────────
app.get('/api/ksql/tables', async (_req, res) => {
  try {
    const body = await ksqlStatement('LIST TABLES;');
    const tables = (Array.isArray(body) ? body[0]?.tables : body?.tables) || [];
    // Filter out ksqlDB internal tables, check state for each
    const userTables = tables.filter(t => !t.name.startsWith('KSQL_'));
    // Fetch state for each table in parallel (DESCRIBE)
    const withState = await Promise.all(userTables.map(async t => {
      try {
        const desc = await ksqlStatement(`DESCRIBE ${t.name};`);
        const sd  = (Array.isArray(desc) ? desc[0] : desc)?.sourceDescription || {};
        const wqs = sd.writeQueries || [];
        const state = wqs[0]?.state || 'BUILDING';
        // Derive the source stream name: strip _TBL suffix
        const streamName = t.name.replace(/_TBL$/, '');
        // Find topic from streams list (best-effort)
        return { name: t.name, streamName, topic: t.topic, state };
      } catch { return { name: t.name, streamName: t.name.replace(/_TBL$/, ''), topic: t.topic, state: 'UNKNOWN' }; }
    }));
    res.json({ tables: withState });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── GET /api/ksql/status — ping ksqlDB ───────────────────────────────────────
app.get('/api/ksql/status', async (_req, res) => {
  try {
    const r = await fetch(`${KSQLDB_URL}/info`);
    const body = await r.json().catch(() => ({}));
    res.json({ ok: r.ok, version: body?.KsqlServerInfo?.version || 'unknown', url: KSQLDB_URL });
  } catch (e) { res.status(503).json({ ok: false, error: e.message, url: KSQLDB_URL }); }
});

// ── POST /api/ksql/query — run a SELECT via ksqlDB ───────────────────────────
// Body: { ksql: "SELECT * FROM ...", streamsProperties: {}, limit: 50 }
// Returns: { columns, rows, rowCount }
app.post('/api/ksql/query', async (req, res) => {
  let { ksql: rawKsql, streamsProperties = {}, topic } = req.body || {};
  if (!rawKsql || !rawKsql.trim()) return res.status(400).json({ error: 'ksql is required' });

  // Safety: only SELECT statements
  const trimmed = rawKsql.trim();
  if (!/^SELECT\b/i.test(trimmed)) {
    return res.status(400).json({ error: 'Only SELECT statements are allowed' });
  }

  // Default: read from earliest so the user actually sees data
  const props = {
    'ksql.streams.auto.offset.reset': 'earliest',
    ...streamsProperties,
  };

  try {
    // If a topic hint is provided, ensure the stream exists first
    if (topic) {
      await ksqlEnsureStream(topic).catch(() => {/* non-fatal — stream may already exist */});
    }
    const { columns, rows } = await ksqlQuery(trimmed, props, 45000);
    res.json({ columns, rows, rowCount: rows.length });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── POST /api/ksql/table-query — fast pull query against a materialised TABLE ──
// Body: { streamName, limit, where }
//   streamName: e.g. "POSTGRES_PUBLIC_CUSTOMERS"
//   limit:      max rows to return (default 20)
//   where:      optional payload-field filter string, e.g. "id = 123" or "email LIKE %gmail%"
//               Supports: =  !=  LIKE  >  <  joined by AND
//
// Strategy:
//   1. Pull all rows from TABLE (SELECT * FROM <TABLE>) — instant from RocksDB
//   2. Parse payload JSON for each row, expand after/before fields
//   3. Apply WHERE filter in Node.js
//   4. Return first `limit` matching rows
app.post('/api/ksql/table-query', async (req, res) => {
  const { streamName, limit = 20, where = '' } = req.body || {};
  if (!streamName) return res.status(400).json({ error: 'streamName is required' });
  const tableName = streamName.toUpperCase() + '_TBL';
  const n = Math.min(Number(limit) || 20, 500);

  // Build predicate from WHERE clause
  function buildPredicate(whereStr) {
    if (!whereStr || !whereStr.trim()) return () => true;
    const COND_RE = /(\w+)\s*(=|!=|LIKE|>=|<=|>|<)\s*'?([^']*?)'?\s*(?=AND\s|$)/gi;
    const conditions = [];
    let m;
    while ((m = COND_RE.exec(whereStr)) !== null) {
      conditions.push({ field: m[1].toLowerCase(), op: m[2].toUpperCase(), value: m[3].trim() });
    }
    if (!conditions.length) return () => true;
    return (row) => conditions.every(({ field, op, value }) => {
      // Check field case-insensitively
      const key = Object.keys(row).find(k => k.toLowerCase() === field);
      if (key === undefined) return false;
      const rv = row[key] !== null && row[key] !== undefined ? String(row[key]) : null;
      if (rv === null) return false;
      switch (op) {
        case '=':   return rv.toLowerCase() === value.toLowerCase();
        case '!=':  return rv.toLowerCase() !== value.toLowerCase();
        case 'LIKE': {
          const pat = '^' + value.replace(/[.+^${}()|[\]\\]/g, '\\$&').replace(/%/g, '.*').replace(/_/g, '.') + '$';
          return new RegExp(pat, 'i').test(rv);
        }
        case '>':   return parseFloat(rv) > parseFloat(value);
        case '<':   return parseFloat(rv) < parseFloat(value);
        case '>=':  return parseFloat(rv) >= parseFloat(value);
        case '<=':  return parseFloat(rv) <= parseFloat(value);
        default:    return true;
      }
    });
  }
  const predicate = buildPredicate(where);

  try {
    // Pull query — no EMIT CHANGES, returns immediately from RocksDB
    // Fetch more than needed so we have enough after filtering
    const fetchLimit = where ? Math.min(n * 100, 10000) : n;
    const sql = `SELECT ROWKEY, payload FROM ${tableName} LIMIT ${fetchLimit};`;
    const { columns, rows } = await ksqlQuery(sql, {}, 30000);

    const rkIdx = columns.findIndex(c => c.toUpperCase() === 'ROWKEY');
    const pyIdx = columns.findIndex(c => c.toUpperCase() === 'PAYLOAD');

    const matched = [];
    for (const row of rows) {
      const rawKey = rkIdx >= 0 ? row[rkIdx] : row[0];
      const rawPay = pyIdx >= 0 ? row[pyIdx] : row[1];
      let expanded = {};
      let op = 'r';
      try {
        const pay  = typeof rawPay === 'string' ? JSON.parse(rawPay) : (rawPay || {});
        const data = pay?.after ?? pay?.before ?? pay ?? {};
        op = pay?.op || 'r';
        expanded = data;
      } catch {}
      const flat = { _rowkey: rawKey, _op: op, ...expanded, _payload: rawPay };
      if (predicate(flat)) {
        matched.push(flat);
        if (matched.length >= n) break;
      }
    }

    // Derive display columns: _op first, then data fields, _rowkey last
    const colSet = new Set();
    matched.forEach(r => Object.keys(r).forEach(k => {
      if (k !== '_op' && k !== '_rowkey' && k !== '_payload') colSet.add(k);
    }));
    const outCols = ['_op', ...colSet, '_rowkey', '_payload'];
    const outRows = matched.map(r => outCols.map(c => r[c] ?? null));

    res.json({ columns: outCols, rows: outRows, tableName, scanned: rows.length, matched: matched.length });
  } catch (e) {
    // Table may not exist yet — return helpful error
    const msg = e.message || '';
    if (/not exist|does not exist|unknown/i.test(msg)) {
      return res.status(404).json({ error: `Table ${tableName} not ready yet. It may still be building — try again in a few seconds.` });
    }
    res.status(500).json({ error: msg });
  }
});

// ── POST /api/ksql/check-pks — check which source PKs are present in Kafka ───
// Uses ksqlDB to look up each PK in the topic via a pull query on a table,
// or a push query on a stream with EMIT CHANGES LIMIT N.
//
// Body:
//   { topic, pkColumns: ['id'], pkValues: [['1'],['2'],...], extraCols: [] }
//
// Returns:
//   { found: Map-serialised as [[key,{op,pkValues}]], notFound: [key,...], checked: N }
//
// Strategy:
//   1. Ensure a ksqlDB TABLE exists (backed by topic, compacted by key).
//   2. Run a pull query: SELECT ROWKEY, * FROM <table> WHERE ROWKEY IN ('k1','k2',...)
//   3. Anything returned = present (alive); tombstones → deleted.
//
app.post('/api/ksql/check-pks', async (req, res) => {
  const { topic, pkColumns = [], pkValues = [], extraCols = [] } = req.body || {};
  if (!topic)           return res.status(400).json({ error: 'topic is required' });
  if (!pkValues.length) return res.json({ found: [], notFound: [], checked: 0 });

  try {
    // Derive table name (same naming as stream but TABLE suffix)
    const tableName = topic.replace(/[^a-zA-Z0-9_]/g, '_').toUpperCase() + '_TBL';
    const streamName = topic.replace(/[^a-zA-Z0-9_]/g, '_').toUpperCase();

    // Ensure backing stream exists
    await ksqlStatement(
      `CREATE STREAM IF NOT EXISTS ${streamName}` +
      ` (ROWKEY VARCHAR KEY, payload VARCHAR)` +
      ` WITH (KAFKA_TOPIC='${topic}', VALUE_FORMAT='JSON', WRAP_SINGLE_VALUE=false);`
    ).catch(() => {});

    // Ensure materialized table exists (needed for pull queries)
    await ksqlStatement(
      `CREATE TABLE IF NOT EXISTS ${tableName} AS` +
      ` SELECT ROWKEY, LATEST_BY_OFFSET(payload) AS payload` +
      ` FROM ${streamName}` +
      ` GROUP BY ROWKEY` +
      ` EMIT CHANGES;`
    ).catch(() => {});

    // Build the key strings from pkValues
    // Single-pk: key = String(pkValues[i][0])
    // Composite: key = JSON.stringify(pkValues[i]) — Debezium writes composite keys as JSON
    const buildKey = (vals) => pkColumns.length === 1
      ? String(vals[0])
      : JSON.stringify(Object.fromEntries(pkColumns.map((c, i) => [c, vals[i]])));

    const keyList = pkValues.map(buildKey);
    const inList  = keyList.map(k => `'${k.replace(/'/g, "''")}'`).join(', ');

    // Pull query on the table
    const pullSql  = `SELECT ROWKEY, payload FROM ${tableName} WHERE ROWKEY IN (${inList});`;
    const { columns, rows } = await ksqlQuery(pullSql, {}, 30000);

    // Build found map: rowkey → {op, pkValues}
    const found    = [];
    const foundKeys = new Set();
    for (const row of rows) {
      const rowKeyIdx = columns.findIndex(c => c.toUpperCase() === 'ROWKEY');
      const payloadIdx= columns.findIndex(c => c.toUpperCase() === 'PAYLOAD');
      const rawKey  = rowKeyIdx >= 0 ? String(row[rowKeyIdx] || '') : String(row[0] || '');
      const rawPay  = payloadIdx >= 0 ? row[payloadIdx] : row[1];

      let op = 'r';
      try {
        const pay = typeof rawPay === 'string' ? JSON.parse(rawPay) : rawPay;
        const opField = pay?.op || pay?.payload?.op;
        if (opField) op = opField;
        if (!rawPay || rawPay === 'null') op = 'd'; // tombstone
      } catch {}

      // Recover pkValues from the key string
      let pkVals = [];
      try {
        if (pkColumns.length === 1) {
          pkVals = [rawKey];
        } else {
          const keyObj = JSON.parse(rawKey);
          pkVals = pkColumns.map(c => keyObj[c] ?? keyObj[c.toUpperCase()] ?? null);
        }
      } catch { pkVals = [rawKey]; }

      // Build composite key matching caller's format: JSON.stringify([pkVals..., extraColVals...])
      const extraColVals = extraCols.map(col => {
        try {
          const pay = typeof rawPay === 'string' ? JSON.parse(rawPay) : rawPay;
          const after = pay?.after ?? pay?.payload?.after ?? pay;
          return after ? String(after[col] ?? after[col?.toUpperCase()] ?? null) : null;
        } catch { return null; }
      });
      const mapKey = JSON.stringify([...pkVals, ...extraColVals]);
      found.push([mapKey, { op, pkValues: pkVals }]);
      foundKeys.add(rawKey);
    }

    const notFound = keyList.filter(k => !foundKeys.has(k));
    res.json({ found, notFound, checked: pkValues.length });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ─── Catch-all: serve SPA ───────────────────────────────────────────────────
app.get('*', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// ═══════════════════════════════════════════════════════════════════════════
//  AUTO-TABLE POLLER
//  Every 60 s: for every new Kafka topic that has no materialised table yet,
//  create a ksqlDB STREAM + TABLE (backed by RocksDB).
//  Pull queries against the TABLE are instant — no topic scanning.
// ═══════════════════════════════════════════════════════════════════════════
const TOPIC_SKIP_RE = /^(__|_confluent|debezium|schema|oracle$|st\.|[A-Z0-9_]+_TBL$)/;

async function autoEnsureTables() {
  try {
    // 1. List all Kafka topics
    const stdout = await kafkaExec(
      `/kafka/bin/kafka-topics.sh --bootstrap-server ${KAFKA_BOOTSTRAP} --list --command-config ${SASL_CFG_FILE}`,
      25000
    );
    const topics = stdout.split('\n')
      .map(t => t.trim())
      .filter(t => t && !TOPIC_SKIP_RE.test(t) && !t.startsWith('SLF4J'));

    // 2. Get existing tables from ksqlDB
    let existingTables = new Set();
    try {
      const body = await ksqlStatement('LIST TABLES;');
      const tables = (Array.isArray(body) ? body[0]?.tables : body?.tables) || [];
      tables.forEach(t => existingTables.add(t.name));
    } catch { /* ksqlDB unreachable — skip */ return; }

    // 3. For each topic without a table: create stream + materialised table
    for (const topic of topics) {
      const streamName = topic.replace(/[^a-zA-Z0-9_]/g, '_').toUpperCase();
      const tableName  = streamName + '_TBL';
      if (existingTables.has(tableName)) continue;

      try {
        // Step A: ensure stream exists (idempotent)
        await ksqlStatement(
          `CREATE STREAM IF NOT EXISTS ${streamName}` +
          ` (ROWKEY VARCHAR KEY, payload VARCHAR)` +
          ` WITH (KAFKA_TOPIC='${topic}', VALUE_FORMAT='JSON', WRAP_SINGLE_VALUE=false);`
        );
        // Step B: create materialised RocksDB table (idempotent)
        await ksqlStatement(
          `CREATE TABLE IF NOT EXISTS ${tableName} AS` +
          ` SELECT ROWKEY, LATEST_BY_OFFSET(payload) AS payload` +
          ` FROM ${streamName} GROUP BY ROWKEY EMIT CHANGES;`
        );
        console.log(`[auto-table] Created stream+table for new topic: ${topic} → ${tableName}`);
      } catch (e) {
        if (!/already exists/i.test(e.message))
          console.warn(`[auto-table] Failed for topic ${topic}: ${e.message}`);
      }
    }
  } catch (e) {
    console.warn('[auto-table] Poller error:', e.message);
  }
}

const server = http.createServer(app);
const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', () => {
  console.log(`Lakehouse UI listening on http://0.0.0.0:${PORT}`);
  // Run once at startup, then every 60 s
  autoEnsureTables();
  setInterval(autoEnsureTables, 60_000);
});
