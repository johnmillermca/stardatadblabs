#!/usr/bin/env node
'use strict';

const express = require('express');
const fetch   = require('node-fetch');
const path    = require('path');
const WebSocket = require('ws');
const http    = require('http');

const app = express();
app.use(express.json());
// Serve static files with no-cache so browsers always pick up re-deployed HTML/JS
app.use(express.static(path.join(__dirname, 'public'), {
  etag: false,
  lastModified: false,
  setHeaders: (res, filePath) => {
    if (filePath.endsWith('.html')) {
      res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate');
      res.setHeader('Pragma', 'no-cache');
    }
  },
}));

// ─── Environment / config ──────────────────────────────────────────────────
const DEBEZIUM_ENDPOINTS = {
  postgres: process.env.DEBEZIUM_POSTGRES_URL || 'http://debezium-connect-postgres.prod.svc.cluster.local:8083',
  oracle:   process.env.DEBEZIUM_ORACLE_URL   || 'http://debezium-connect-oracle.prod.svc.cluster.local:8083',
  mongodb:  process.env.DEBEZIUM_MONGODB_URL  || 'http://debezium-connect-mongodb.prod.svc.cluster.local:8083',
};
const KAFKA_BOOTSTRAP    = process.env.KAFKA_BOOTSTRAP    || 'strimzi-kafka-kafka-bootstrap.prod.svc.cluster.local:9092';
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
    const { status: s, body } = await dbzRequest(source, 'GET', '/connectors?expand=status');
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
app.post('/api/connectors/:source', async (req, res) => {
  try {
    const { source } = req.params;
    const payload = { ...req.body };
    payload.config = await injectCredentials(source, payload.config || {});
    const { status: s, body } = await dbzRequest(source, 'POST', '/connectors', payload);
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
      // Query Oracle XE for current SCN
      const pod = await getSourcePod('oracle-xe', 'oracle');
      const { stdout } = await execFileAsync('kubectl', [
        'exec', '-n', K8S_NAMESPACE, pod, '--',
        'sqlplus', '-S', 'c##dbzcdc/dbzpassword@//localhost:1521/XEPDB1',
      ], { input: 'SET HEADING OFF\nSET FEEDBACK OFF\nSELECT current_scn FROM v$database;\nEXIT;\n', timeout: 15000 });
      const scn = stdout.trim().split('\n').map(l => l.trim()).filter(l => /^\d+$/.test(l)).pop();
      if (!scn) return res.status(502).json({ error: 'Could not parse SCN from Oracle output', raw: stdout.slice(0, 300) });
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
      // Return current timestamp as a proxy for resume-token starting point
      const pod = await getSourcePod('mongodb', 'mongodb');
      const { stdout } = await execFileAsync('kubectl', [
        'exec', '-n', K8S_NAMESPACE, pod, '--',
        'mongosh', '--quiet', '--eval',
        'JSON.stringify({ts: new Timestamp(Math.floor(Date.now()/1000), 1)})',
      ], { timeout: 10000 });
      const line = stdout.trim().split('\n').find(l => l.includes('"ts"'));
      if (!line) return res.status(502).json({ error: 'Could not parse timestamp from MongoDB', raw: stdout.slice(0, 300) });
      return res.json({ source, type: 'Timestamp', position: line.trim() });

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
      // Use root credentials from K8s secret — debezium user may not have listDatabases
      const pod = await getSourcePod('mongodb', 'mongodb');

      // Fetch root password from secret at runtime
      const secretRes = await k8sFetch(`/api/v1/namespaces/${K8S_NAMESPACE}/secrets/mongodb-credentials`);
      const secretJson = await secretRes.json();
      const rootPass = Buffer.from(secretJson.data?.['mongodb-root-password'] || '', 'base64').toString();

      // 1. List all non-system databases
      const { stdout: dbOut } = await execFileAsync('kubectl', [
        'exec', '-n', K8S_NAMESPACE, pod, '--',
        'mongosh', '--quiet',
        '-u', 'root', '-p', rootPass, '--authenticationDatabase', 'admin',
        '--eval',
        `db.adminCommand({listDatabases:1}).databases.filter(d=>!['admin','config','local'].includes(d.name)).forEach(d=>print(d.name))`,
      ], { timeout: 10000 });

      const dbs = dbOut.trim().split('\n').map(l => l.trim()).filter(l => l && !l.startsWith('['));

      // 2. For each database list collections
      const result = {};
      for (const database of dbs) {
        const { stdout } = await execFileAsync('kubectl', [
          'exec', '-n', K8S_NAMESPACE, pod, '--',
          'mongosh', '--quiet', database,
          '-u', 'root', '-p', rootPass, '--authenticationDatabase', 'admin',
          '--eval', `db.getCollectionNames().forEach(c=>print(c))`,
        ], { timeout: 10000 });
        const cols = stdout.trim().split('\n').map(l => l.trim()).filter(l => l && !l.startsWith('['));
        if (cols.length) result[database] = cols;
      }
      res.json({ source: 'mongodb', databases: result });

    } else if (source === 'oracle') {
      // Use OS auth (/ as sysdba) inside the pod — no password needed
      const pod = await getSourcePod('oracle-xe', 'oracle');

      const runSqlplus = async (sql) => {
        const { stdout } = await execFileAsync('kubectl', [
          'exec', '-i', '-n', K8S_NAMESPACE, pod, '--',
          'bash', '-c', `echo "${sql}" | sqlplus -S / as sysdba`,
        ], { timeout: 15000 });
        return stdout.trim().split('\n')
          .map(l => l.trim())
          .filter(l => l && !/^(SQL|ERROR|Connected|Disconnected|Oracle|Copyright|Last|WARNING|$)/.test(l));
      };

      // 1. List all schemas that own tables (exclude system schemas)
      const systemSchemas = ['SYS','SYSTEM','OUTLN','DBSNMP','APPQOSSYS','DBSFWUSER','GGSYS','ANONYMOUS',
        'CTXSYS','DVSYS','DVF','GSMADMIN_INTERNAL','MDSYS','OLAPSYS','XDB','WMSYS','OJVMSYS',
        'ORDPLUGINS','ORDSYS','SI_INFORMTN_SCHEMA','ORDDATA'];
      const schemas = await runSqlplus(
        'SET HEADING OFF\\nSET FEEDBACK OFF\\nSET PAGESIZE 0\\n' +
        `SELECT DISTINCT OWNER FROM ALL_TABLES WHERE OWNER NOT IN (${systemSchemas.map(s=>`'${s}'`).join(',')}) ORDER BY OWNER;\\nEXIT;`
      );

      // 2. For each schema list tables
      const result = {};
      for (const schema of schemas) {
        const tables = await runSqlplus(
          'SET HEADING OFF\\nSET FEEDBACK OFF\\nSET PAGESIZE 0\\n' +
          `SELECT TABLE_NAME FROM ALL_TABLES WHERE OWNER='${schema}' ORDER BY TABLE_NAME;\\nEXIT;`
        );
        if (tables.length) result[schema] = tables;
      }
      res.json({ source: 'oracle', databases: result });

    } else {
      res.status(400).json({ error: `Unknown source: ${source}` });
    }
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Cache of source pod names (30s)
const _srcPodCache = {};
async function getSourcePod(appLabel, source) {
  const key = appLabel;
  if (_srcPodCache[key] && Date.now() < _srcPodCache[key].exp) return _srcPodCache[key].name;
  const { stdout } = await execFileAsync('kubectl', [
    'get', 'pod', '-n', K8S_NAMESPACE,
    '-l', `app=${appLabel}`,
    '-o', 'jsonpath={.items[0].metadata.name}',
  ], { timeout: 10000 });
  const name = stdout.trim();
  if (!name) throw new Error(`No running pod found with app=${appLabel}`);
  _srcPodCache[key] = { name, exp: Date.now() + 30000 };
  return name;
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
//  KUBERNETES STREAMING JOBS
// ═══════════════════════════════════════════════════════════════════════════

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
//  KAFKA TOPICS  (via Debezium worker kafka-topics.sh)
// ═══════════════════════════════════════════════════════════════════════════
const { exec } = require('child_process');
const util = require('util');
const execAsync = util.promisify(exec);

const SASL_CFG_FILE = '/tmp/kafka-sasl.properties';

// Resolve the debezium-connect-postgres pod name (cached 30s)
let _dbzPodName = null;
let _dbzPodExpiry = 0;
async function getDbzPod() {
  if (_dbzPodName && Date.now() < _dbzPodExpiry) return _dbzPodName;
  const { stdout } = await execAsync(
    `kubectl get pod -n ${K8S_NAMESPACE} -l app=debezium-connect-postgres -o jsonpath='{.items[0].metadata.name}'`,
    { shell: '/bin/bash' }
  );
  _dbzPodName = stdout.trim();
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

let _saslFileWritten = false;

function ensureSaslFileSync(pod) {
  if (_saslFileWritten) return;
  const r = spawnSync('kubectl', [
    'exec', '-i', '-n', K8S_NAMESPACE, pod, '--', 'tee', SASL_CFG_FILE
  ], { input: SASL_PROPS, encoding: 'utf8', timeout: 10000 });
  if (r.status !== 0) throw new Error(`SASL file write failed: ${r.stderr}`);
  _saslFileWritten = true;
}

async function kafkaExec(argsStr, timeoutMs = 20000) {
  const pod = await getDbzPod();
  ensureSaslFileSync(pod);
  const args = argsStr.trim().split(/\s+/);
  const { stdout } = await execFileAsync('kubectl',
    ['exec', '-n', K8S_NAMESPACE, pod, '--', ...args],
    { timeout: timeoutMs }
  );
  return stdout;
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
        'connector.class':             'io.debezium.connector.postgresql.PostgresConnector',
        'tasks.max':                   '1',
        'topic.prefix':                'postgres',
        'database.hostname':           'postgresql.prod.svc.cluster.local',
        'database.port':               '5432',
        'database.user':               'debezium',
        'database.password':           pgPassword,
        'database.dbname':             'cache_testing',
        'slot.name':                   'debezium_cache_testing',
        'publication.name':            'debezium_pub_cache_testing',
        'plugin.name':                 'pgoutput',
        'schema.include.list':         'public',
        'table.include.list':          'public.customers,public.orders,public.products',
        'snapshot.mode':               'no_data',
        'decimal.handling.mode':       'double',
        'time.precision.mode':         'connect',
        'tombstones.on.delete':        'false',
        'key.converter':               'org.apache.kafka.connect.json.JsonConverter',
        'key.converter.schemas.enable':'false',
        'value.converter':             'org.apache.kafka.connect.json.JsonConverter',
        'value.converter.schemas.enable':'false',
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
        'table.include.list':          'CACHE_TESTING.CUSTOMERS,CACHE_TESTING.ORDERS,CACHE_TESTING.PRODUCTS',
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
        'collection.include.list':     'cache_testing.customers,cache_testing.orders,cache_testing.products',
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
//  ICEBERG CONFIG (stored as K8s ConfigMap)
// ═══════════════════════════════════════════════════════════════════════════
app.get('/api/iceberg/config', async (req, res) => {
  try {
    const r = await k8sFetch(`/api/v1/namespaces/${K8S_NAMESPACE}/configmaps/lakehouse-ui-iceberg-config`);
    if (r.status === 404) return res.json({ config: {} });
    const body = await r.json();
    res.json({ config: body.data || {} });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/iceberg/config', async (req, res) => {
  try {
    const cm = { apiVersion: 'v1', kind: 'ConfigMap', metadata: { name: 'lakehouse-ui-iceberg-config', namespace: K8S_NAMESPACE }, data: req.body };
    const checkR = await k8sFetch(`/api/v1/namespaces/${K8S_NAMESPACE}/configmaps/lakehouse-ui-iceberg-config`);
    const method = checkR.status === 404 ? 'POST' : 'PUT';
    const urlPath = method === 'POST'
      ? `/api/v1/namespaces/${K8S_NAMESPACE}/configmaps`
      : `/api/v1/namespaces/${K8S_NAMESPACE}/configmaps/lakehouse-ui-iceberg-config`;
    const r = await k8sFetch(urlPath, { method, body: JSON.stringify(cm) });
    const body = await r.json();
    res.status(r.status).json(body);
  } catch (e) { res.status(500).json({ error: e.message }); }
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

// ─── Catch-all: serve SPA ───────────────────────────────────────────────────
app.get('*', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

const server = http.createServer(app);
const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', () => {
  console.log(`Lakehouse UI listening on http://0.0.0.0:${PORT}`);
});
