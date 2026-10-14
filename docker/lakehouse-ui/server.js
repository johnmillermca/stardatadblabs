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
//  KUBERNETES STREAMING JOBS
// ═══════════════════════════════════════════════════════════════════════════

// List kafka-to-iceberg deployments
app.get('/api/streaming/deployments', async (req, res) => {
  try {
    const r = await k8sFetch(`/apis/apps/v1/namespaces/${K8S_NAMESPACE}/deployments?labelSelector=component%3Dkafka-to-iceberg`);
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

// Get streaming pod logs
app.get('/api/streaming/:name/logs', async (req, res) => {
  try {
    const { name } = req.params;
    // find pod for deployment
    const r = await k8sFetch(`/api/v1/namespaces/${K8S_NAMESPACE}/pods?labelSelector=app%3D${name}`);
    const body = await r.json();
    const pod = body.items && body.items[0];
    if (!pod) return res.json({ logs: 'No pod found' });
    const podName = pod.metadata.name;
    const lr = await k8sFetch(`/api/v1/namespaces/${K8S_NAMESPACE}/pods/${podName}/log?tailLines=100&timestamps=true`);
    const logs = await lr.text();
    res.json({ logs });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ═══════════════════════════════════════════════════════════════════════════
//  KAFKA TOPICS  (via Debezium worker kafka-topics.sh)
// ═══════════════════════════════════════════════════════════════════════════
const { exec } = require('child_process');
const util = require('util');
const execAsync = util.promisify(exec);

const KAFKA_CMD_BASE = [
  `--bootstrap-server ${KAFKA_BOOTSTRAP}`,
  `--consumer-property security.protocol=SASL_PLAINTEXT`,
  `--consumer-property sasl.mechanism=SCRAM-SHA-512`,
  `--consumer-property "sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=\\"debezium-user\\" password=\\"i3uqKrPOaoqWo6JfOZrmSMhtdp7LiN3H\\";"`,
].join(' ');

// List topics
app.get('/api/kafka/topics', async (req, res) => {
  try {
    const { stdout } = await execAsync(
      `kubectl exec -n ${K8S_NAMESPACE} $(kubectl get pod -n ${K8S_NAMESPACE} -l app=debezium-connect-postgres -o jsonpath='{.items[0].metadata.name}') -- /kafka/bin/kafka-topics.sh --bootstrap-server ${KAFKA_BOOTSTRAP} --list --command-config <(echo -e 'security.protocol=SASL_PLAINTEXT\\nsasl.mechanism=SCRAM-SHA-512\\nsasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="debezium-user" password="i3uqKrPOaoqWo6JfOZrmSMhtdp7LiN3H";') 2>/dev/null`,
      { shell: '/bin/bash' }
    );
    const topics = stdout.split('\n').filter(t => t.trim() && !t.startsWith('__')).sort();
    res.json({ topics });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Get topic offsets
app.get('/api/kafka/topics/:topic/offsets', async (req, res) => {
  try {
    const { topic } = req.params;
    const { stdout } = await execAsync(
      `kubectl exec -n ${K8S_NAMESPACE} $(kubectl get pod -n ${K8S_NAMESPACE} -l app=debezium-connect-postgres -o jsonpath='{.items[0].metadata.name}') -- /kafka/bin/kafka-get-offsets.sh --bootstrap-server ${KAFKA_BOOTSTRAP} --topic ${topic} --time latest --command-config <(echo -e 'security.protocol=SASL_PLAINTEXT\\nsasl.mechanism=SCRAM-SHA-512\\nsasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="debezium-user" password="i3uqKrPOaoqWo6JfOZrmSMhtdp7LiN3H";') 2>/dev/null`,
      { shell: '/bin/bash' }
    );
    const partitions = stdout.trim().split('\n').map(l => {
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
      // get latest - limit
      const { stdout: offOut } = await execAsync(
        `kubectl exec -n ${K8S_NAMESPACE} $(kubectl get pod -n ${K8S_NAMESPACE} -l app=debezium-connect-postgres -o jsonpath='{.items[0].metadata.name}') -- /kafka/bin/kafka-get-offsets.sh --bootstrap-server ${KAFKA_BOOTSTRAP} --topic ${topic} --time latest --command-config <(echo -e 'security.protocol=SASL_PLAINTEXT\\nsasl.mechanism=SCRAM-SHA-512\\nsasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="debezium-user" password="i3uqKrPOaoqWo6JfOZrmSMhtdp7LiN3H";') 2>/dev/null`,
        { shell: '/bin/bash' }
      );
      const matched = offOut.match(new RegExp(`:${partition}:(\\d+)`));
      const latestOff = matched ? Number(matched[1]) : 0;
      offsetArg = Math.max(0, latestOff - Number(limit));
    }

    const { stdout } = await execAsync(
      `kubectl exec -n ${K8S_NAMESPACE} $(kubectl get pod -n ${K8S_NAMESPACE} -l app=debezium-connect-postgres -o jsonpath='{.items[0].metadata.name}') -- /kafka/bin/kafka-console-consumer.sh --bootstrap-server ${KAFKA_BOOTSTRAP} --topic ${topic} --partition ${partition} --offset ${offsetArg} --max-messages ${limit} --timeout-ms 12000 --consumer-property security.protocol=SASL_PLAINTEXT --consumer-property sasl.mechanism=SCRAM-SHA-512 --consumer-property 'sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="debezium-user" password="i3uqKrPOaoqWo6JfOZrmSMhtdp7LiN3H";' 2>/dev/null`,
      { shell: '/bin/bash', timeout: 20000 }
    );

    const messages = stdout.split('\n').filter(l => l.trim()).map(l => {
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
