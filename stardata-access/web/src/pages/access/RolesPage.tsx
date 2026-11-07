// =============================================================================
// Screen 9 — Roles & token claims  (/access/roles)
// Canvas: read-only table from GET /api/access/roles.
//         Badge "Defined in Git (Terraform)".
//         Static "How a role reaches a service" section.
//         Sample access token for Priya Shah.
// =============================================================================
import React from 'react';
import { useQuery } from '@tanstack/react-query';
import { apiFetch, LoadingSkeleton } from './components';
import type { RoleCatalogueItem } from './types';

const FLOW_STEPS = [
  'Owner or Admin sets levels in Members → console backend creates or updates one ZITADEL authorization.',
  'On the next token, ZITADEL calls the stardata-claims Action (preaccesstoken) which adds the short claims.',
  'Each service reads its own claim: Doris proxy doris_role, Kafka kafka_role, Grafana monitoring_role…',
  'Access tokens live 10 minutes, so changes reach every service within 10 minutes.',
];

const SAMPLE_TOKEN = {
  sub: '284019333120',
  email: 'priya@stardatadb.com',
  doris_role: 'analyst',
  kafka_role: 'viewer',
  etl_role: 'viewer',
  mdm_role: 'steward',
  monitoring_role: 'viewer',
  platform_roles: [],
  exp: 1793988000,
};

export default function RolesPage() {
  const { data: catalogue = [], isLoading } = useQuery<RoleCatalogueItem[]>({
    queryKey: ['roles'],
    queryFn: () => apiFetch('/api/access/roles').then(r => r.json()),
    staleTime: 300_000,
  });

  if (isLoading) return <LoadingSkeleton />;

  return (
    <div className="roles-page">
      <div className="page-header">
        <div>
          <h1>Roles</h1>
          <p>Every role is a project role in ZITADEL project <code>stardata-platform</code>. A person holds at most one level per component.</p>
        </div>
        <span className="badge badge--info">Defined in Git (Terraform)</span>
      </div>

      {/* ── Role catalogue table ──────────────────────────────────────── */}
      <section>
        <h2>Role catalogue</h2>
        <table className="data-table roles-table">
          <thead>
            <tr>
              <th>Component</th>
              <th>Role keys</th>
              <th>Levels (low → high)</th>
              <th>Token claim</th>
              <th>What it allows</th>
              <th>People per level</th>
            </tr>
          </thead>
          <tbody>
            {catalogue.map(item => (
              <tr key={`${item.component}`}>
                <td><strong>{item.component}</strong></td>
                <td><code>{item.prefix}</code></td>
                <td>{item.levels.join(' · ')}</td>
                <td><code>{item.claim}</code></td>
                <td>{item.allows}</td>
                <td>{item.counts.join(' / ')}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </section>

      {/* ── How a role reaches a service ─────────────────────────────── */}
      <section className="card roles-flow">
        <h2>How a role reaches a service</h2>
        <ol className="roles-flow__steps">
          {FLOW_STEPS.map((step, i) => (
            <li key={i}>{step}</li>
          ))}
        </ol>
      </section>

      {/* ── Sample access token ───────────────────────────────────────── */}
      <section className="card roles-sample">
        <h2>Sample access token (Priya Shah)</h2>
        <pre className="code-block">{JSON.stringify(SAMPLE_TOKEN, null, 2)}</pre>
      </section>
    </div>
  );
}
