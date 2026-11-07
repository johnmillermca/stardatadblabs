// =============================================================================
// Screen 12 — Audit  (/access/audit)
// Canvas: filters (Person/app, Component, Result, Time), Export CSV,
//         table (Time, Who, Component, Action, Source IP, Result),
//         Load more, event detail Drawer with raw JSON + trace link.
// =============================================================================
import React, { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { apiFetch, Drawer, LoadingSkeleton, statusPill, formatRelative, useToast } from './components';
import type { AuditRow } from './types';

const COMPONENTS = ['', 'doris', 'kafka', 'memgraph', 'flink', 'sqlmesh', 'etl', 'mdm', 'monitoring', 'console', 'members', 'applications'];
const RESULTS    = ['', 'ok', 'denied', 'waiting', 'blocked'];

export default function AuditPage() {
  const { show: toast } = useToast();

  const [who,       setWho]       = useState('');
  const [component, setComponent] = useState('');
  const [result,    setResult]    = useState('');
  const [from,      setFrom]      = useState('');
  const [to,        setTo]        = useState('');
  const [cursor,    setCursor]    = useState<string | null>(null);
  const [rows,      setRows]      = useState<AuditRow[]>([]);
  const [selected,  setSelected]  = useState<AuditRow | null>(null);

  const buildParams = (cur?: string | null) => {
    const p = new URLSearchParams();
    if (who)       p.set('who', who);
    if (component) p.set('component', component);
    if (result)    p.set('result', result);
    if (from)      p.set('from', from);
    if (to)        p.set('to', to);
    if (cur)       p.set('cursor', cur);
    return p.toString();
  };

  const { data: page, isLoading, isFetching } = useQuery<{ items: AuditRow[]; next: string | null; total: number }>({
    queryKey: ['audit', who, component, result, from, to, cursor],
    queryFn: () => apiFetch(`/api/access/audit?${buildParams(cursor)}`).then(r => r.json()),
    onSuccess: data => {
      setRows(prev => cursor ? [...prev, ...data.items] : data.items);
    },
  });

  const handleSearch = () => {
    setCursor(null);
    setRows([]);
  };

  const handleExport = async () => {
    const res = await apiFetch(`/api/access/audit/export?${buildParams()}`);
    if (!res.ok) { toast('Export failed. Try with a narrower filter.', 'error'); return; }
    const blob = await res.blob();
    const a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = `audit-export-${new Date().toISOString().slice(0, 10)}.csv`;
    a.click();
    URL.revokeObjectURL(a.href);
    toast('Export downloaded.');
  };

  return (
    <div className="audit-page">
      <div className="page-header">
        <div>
          <h1>Audit</h1>
          <p>Every sign-in, role change and data access decision. Kept 10 days.</p>
        </div>
        <button type="button" className="btn btn--ghost" onClick={handleExport}>
          Export CSV
        </button>
      </div>

      {/* ── Filters ──────────────────────────────────────────────────── */}
      <form
        className="audit-filters"
        onSubmit={e => { e.preventDefault(); handleSearch(); }}
      >
        <input
          type="search"
          className="search-input"
          placeholder="Person or app"
          value={who}
          onChange={e => setWho(e.target.value)}
        />
        <select className="select-input" value={component} onChange={e => setComponent(e.target.value)}>
          <option value="">Component</option>
          {COMPONENTS.filter(Boolean).map(c => <option key={c} value={c}>{c}</option>)}
        </select>
        <select className="select-input" value={result} onChange={e => setResult(e.target.value)}>
          <option value="">Result</option>
          {RESULTS.filter(Boolean).map(r => <option key={r} value={r}>{r}</option>)}
        </select>
        <input
          type="datetime-local"
          className="date-input"
          value={from}
          onChange={e => setFrom(e.target.value)}
          aria-label="From"
        />
        <input
          type="datetime-local"
          className="date-input"
          value={to}
          onChange={e => setTo(e.target.value)}
          aria-label="To"
        />
        <button type="submit" className="btn btn--primary">Search</button>
      </form>

      {/* ── Results table ─────────────────────────────────────────────── */}
      {isLoading && rows.length === 0 ? <LoadingSkeleton /> : (
        <>
          {page && <p className="audit-count">Showing {rows.length} of {page.total} events</p>}
          <table className="data-table audit-table">
            <thead>
              <tr>
                <th>Time</th>
                <th>Who</th>
                <th>Component</th>
                <th>Action</th>
                <th>Source IP</th>
                <th>Result</th>
              </tr>
            </thead>
            <tbody>
              {rows.map(row => (
                <tr
                  key={row.eventId}
                  onClick={() => setSelected(row)}
                  style={{ cursor: 'pointer' }}
                >
                  <td>{formatRelative(row.ts)}</td>
                  <td>{row.actorName}</td>
                  <td>{row.component ?? '—'}</td>
                  <td>{row.action}</td>
                  <td>{row.ip ?? '—'}</td>
                  <td>{statusPill(row.result)}</td>
                </tr>
              ))}
            </tbody>
          </table>
          {page?.next && (
            <button
              type="button"
              className="btn btn--ghost"
              onClick={() => setCursor(page.next)}
              disabled={isFetching}
            >
              {isFetching ? 'Loading…' : 'Load more'}
            </button>
          )}
        </>
      )}

      {/* ── Event detail drawer ───────────────────────────────────────── */}
      <Drawer
        title={selected ? `${selected.action}` : ''}
        open={!!selected}
        onClose={() => setSelected(null)}
      >
        {selected && (
          <div className="audit-detail">
            {statusPill(selected.result)}
            <dl>
              <dt>Time</dt>        <dd>{new Date(selected.ts).toLocaleString()}</dd>
              <dt>Who</dt>         <dd>{selected.actorName} ({selected.actorId})</dd>
              {selected.component && <><dt>Component</dt><dd>{selected.component}</dd></>}
              <dt>Action</dt>      <dd>{selected.action}</dd>
              {selected.ip && <><dt>Source IP</dt><dd>{selected.ip}</dd></>}
              {selected.requestId && <><dt>Request ID</dt><dd><code>{selected.requestId}</code></dd></>}
              {selected.traceId && (
                <>
                  <dt>Trace</dt>
                  <dd>
                    <a
                      href={`https://grafana.stardatadb.com/explore?query=${selected.traceId}`}
                      target="_blank"
                      rel="noreferrer"
                    >
                      {selected.traceId}
                    </a>
                  </dd>
                </>
              )}
              {selected.before !== undefined && <><dt>Before</dt><dd><code>{JSON.stringify(selected.before)}</code></dd></>}
              {selected.after  !== undefined && <><dt>After</dt> <dd><code>{JSON.stringify(selected.after)}</code></dd></>}
              {selected.reason && <><dt>Reason</dt><dd>{selected.reason}</dd></>}
            </dl>

            {selected.targetId && (
              <a href={`/access/members/${selected.targetId}`} className="btn btn--ghost btn--sm">
                Open person
              </a>
            )}

            <details className="audit-detail__json">
              <summary>Raw JSON</summary>
              <pre>{JSON.stringify(selected, null, 2)}</pre>
            </details>

            <button
              type="button"
              className="btn btn--ghost btn--sm"
              onClick={() => navigator.clipboard.writeText(JSON.stringify(selected, null, 2))}
            >
              Copy JSON
            </button>
          </div>
        )}
      </Drawer>
    </div>
  );
}
