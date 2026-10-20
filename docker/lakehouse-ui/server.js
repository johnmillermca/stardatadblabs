#!/usr/bin/env node
'use strict';

const express = require('express');
const fetch   = require('node-fetch');
const path    = require('path');
const WebSocket = require('ws');
const http    = require('http');

const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

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
  const res = await fetch(`${base}${path}`, opts);
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

// Create or update connector
app.put('/api/connectors/:source/:name', async (req, res) => {
  try {
    const { source, name } = req.params;
    const { status: s, body } = await dbzRequest(source, 'PUT', `/connectors/${name}/config`, req.body);
    res.status(s).json(body);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Create new connector (POST)
app.post('/api/connectors/:source', async (req, res) => {
  try {
    const { source } = req.params;
    const { status: s, body } = await dbzRequest(source, 'POST', '/connectors', req.body);
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
      // snapshot.mode=never means: skip full snapshot, start streaming only
      if (source === 'postgres') {
        cfg['snapshot.mode'] = 'never';
        delete cfg['slot.drop.on.stop'];
      } else if (source === 'oracle') {
        cfg['snapshot.mode'] = 'schema_only';
        cfg['log.mining.strategy'] = cfg['log.mining.strategy'] || 'online_catalog';
      } else if (source === 'mongodb') {
        cfg['snapshot.mode'] = 'never';
      }
    } else if (mode === 'live' || mode === 'manual') {
      // Pin to a specific SCN / LSN
      if (source === 'oracle') {
        cfg['snapshot.mode'] = 'schema_only';
        cfg['log.mining.start.scn'] = String(position);
      } else if (source === 'postgres') {
        cfg['snapshot.mode'] = 'never';
        // Debezium Postgres: set the WAL LSN via offset override property
        cfg['snapshot.lsn'] = String(position);
      } else if (source === 'mongodb') {
        cfg['snapshot.mode'] = 'never';
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

// ─── Catch-all: serve SPA ───────────────────────────────────────────────────
app.get('*', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

const server = http.createServer(app);
const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', () => {
  console.log(`Lakehouse UI listening on http://0.0.0.0:${PORT}`);
});
