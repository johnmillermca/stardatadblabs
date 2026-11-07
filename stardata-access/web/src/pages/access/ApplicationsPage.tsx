// =============================================================================
// Screen 7 — Applications  (/access/apps)
// Canvas: table of app identities + inline "New application" panel.
// Status pill: green, amber at 14 days, red at 7.
// Credential types: client_cert, client_secret, app_password.
// Leads into Screen 8 (App created dialog) on create/rotate.
// =============================================================================
import React, { useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import {
  apiFetch, Pill, ConfirmDialog, CopyField, useToast, LoadingSkeleton, formatDate,
} from './components';
import AppCreatedDialog from './AppCreatedDialog';
import type { AppIdentity, AppCreatedResult, RoleCatalogueItem } from './types';

type CredentialType = 'client_cert' | 'client_secret' | 'app_password';

function credentialStatusPill(app: AppIdentity) {
  if (app.status === 'expiring_critical') return <Pill label="Expires soon" variant="red" />;
  if (app.status === 'expiring_soon')    return <Pill label="Expires soon" variant="amber" />;
  if (app.status === 'revoked')          return <Pill label="Revoked" variant="grey" />;
  if (app.expiresAt) return <Pill label={`Valid to ${formatDate(app.expiresAt)}`} variant="green" />;
  if (app.rotatedAt) return <Pill label={`Rotated ${formatDate(app.rotatedAt)}`} variant="green" />;
  return <Pill label="Active" variant="green" />;
}

export default function ApplicationsPage() {
  const qc = useQueryClient();
  const { show: toast } = useToast();

  const { data: apps = [], isLoading } = useQuery<AppIdentity[]>({
    queryKey: ['apps'],
    queryFn: () => apiFetch('/api/access/apps').then(r => r.json()),
  });

  const { data: catalogue = [] } = useQuery<RoleCatalogueItem[]>({
    queryKey: ['roles'],
    queryFn: () => apiFetch('/api/access/roles').then(r => r.json()),
    staleTime: 300_000,
  });

  // ── New application form state ────────────────────────────────────────────
  const [showForm, setShowForm] = useState(false);
  const [formName, setFormName]   = useState('');
  const [formOwner, setFormOwner] = useState('');
  const [formRoles, setFormRoles] = useState<Set<string>>(new Set());
  const [formCred, setFormCred]   = useState<CredentialType>('client_secret');
  const [nameError, setNameError] = useState('');

  // Data-only levels from catalogue (no engineer, no admin, no platform.*)
  const appRoles = catalogue.flatMap(item =>
    item.component !== 'platform'
      ? item.levels
          .filter(l => !['engineer', 'admin'].includes(l))
          .map(l => `${item.component}.${l}`)
      : []
  );

  // ── Create mutation ───────────────────────────────────────────────────────
  const [createdResult, setCreatedResult] = useState<AppCreatedResult | null>(null);

  const createMutation = useMutation({
    mutationFn: () =>
      apiFetch('/api/access/apps', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          name: formName,
          ownerUserId: formOwner,
          dataRoles: [...formRoles],
          credentialType: formCred,
        }),
      }).then(async r => {
        if (!r.ok) {
          const b = await r.json();
          throw new Error(b.message ?? 'create_failed');
        }
        return r.json() as Promise<AppCreatedResult>;
      }),
    onSuccess: result => {
      qc.invalidateQueries({ queryKey: ['apps'] });
      setCreatedResult(result);
      setShowForm(false);
    },
    onError: (err: Error) => setNameError(err.message),
  });

  // ── Rotate mutation ───────────────────────────────────────────────────────
  const [rotatingApp, setRotatingApp] = useState<AppIdentity | null>(null);

  const rotateMutation = useMutation({
    mutationFn: (appId: string) =>
      apiFetch(`/api/access/apps/${appId}/rotate`, { method: 'POST' })
        .then(r => r.json() as Promise<AppCreatedResult>),
    onSuccess: result => {
      qc.invalidateQueries({ queryKey: ['apps'] });
      setCreatedResult(result);
      setRotatingApp(null);
      toast('New credential created. The old one stays valid for 24 hours.');
    },
  });

  // ── Revoke mutation ───────────────────────────────────────────────────────
  const [revokingApp, setRevokingApp] = useState<AppIdentity | null>(null);

  const revokeMutation = useMutation({
    mutationFn: ({ id, reason }: { id: string; reason: string }) =>
      apiFetch(`/api/access/apps/${id}/revoke`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ reason }),
      }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['apps'] });
      toast('Application revoked. All credentials removed immediately.');
      setRevokingApp(null);
    },
  });

  if (isLoading) return <LoadingSkeleton />;

  return (
    <div className="apps-page">
      <div className="page-header">
        <div>
          <h1>Applications</h1>
          <p>Identities for services, jobs and BI tools. A person never shares their own login with an app.</p>
        </div>
      </div>

      {/* ── Applications table ────────────────────────────────────────── */}
      <section>
        <h2>Applications ({apps.length})</h2>
        {apps.length === 0
          ? <p className="empty-state">No applications yet.</p>
          : (
            <table className="data-table">
              <thead>
                <tr>
                  <th>Application</th>
                  <th>Data roles</th>
                  <th>Credential</th>
                  <th>Status</th>
                  <th>Owner</th>
                  <th></th>
                </tr>
              </thead>
              <tbody>
                {apps.map(app => (
                  <tr key={app.userId}>
                    <td>
                      <strong>{app.name}</strong>
                      {app.description && <p className="table-sub">{app.description}</p>}
                    </td>
                    <td>{app.dataRoles.map(r => <span key={r} className="role-tag">{r.split('.')[1]}</span>)}</td>
                    <td>{app.credentialType.replace('_', ' ')}</td>
                    <td>{credentialStatusPill(app)}</td>
                    <td>{app.ownerName}</td>
                    <td className="table-actions">
                      <button
                        type="button"
                        className="btn btn--ghost btn--sm"
                        onClick={() => rotateMutation.mutate(app.userId)}
                        disabled={rotateMutation.isPending}
                      >
                        Rotate
                      </button>
                      <button
                        type="button"
                        className="btn btn--danger-ghost btn--sm"
                        onClick={() => setRevokingApp(app)}
                      >
                        Revoke
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )
        }
      </section>

      {/* ── New application panel ─────────────────────────────────────── */}
      <section className="card">
        <div className="card__header">
          <h2>New application</h2>
          <p>Give it only the roles it needs. The secret is shown once and never again.</p>
        </div>
        {!showForm
          ? <button type="button" className="btn btn--primary" onClick={() => setShowForm(true)}>New application</button>
          : (
            <form className="app-form" onSubmit={e => { e.preventDefault(); createMutation.mutate(); }}>
              <label className="form-field">
                <span>Name</span>
                <input
                  type="text"
                  value={formName}
                  onChange={e => { setFormName(e.target.value.toLowerCase().replace(/[^a-z0-9-]/g, '')); setNameError(''); }}
                  pattern="[a-z0-9-]{3,40}"
                  required
                  placeholder="lowercase, numbers and dashes"
                />
                {nameError && <span className="form-error">{nameError}</span>}
              </label>

              <label className="form-field">
                <span>Owner</span>
                <input
                  type="text"
                  value={formOwner}
                  onChange={e => setFormOwner(e.target.value)}
                  required
                  placeholder="Owner's email or user ID"
                />
                <span className="form-hint">Gets the expiry reminders.</span>
              </label>

              <fieldset className="form-fieldset">
                <legend>Data roles</legend>
                <p className="form-hint">Engineer levels are for people only. Apps cannot get them.</p>
                <div className="checkbox-grid">
                  {appRoles.map(role => (
                    <label key={role} className="checkbox-label">
                      <input
                        type="checkbox"
                        checked={formRoles.has(role)}
                        onChange={e => setFormRoles(prev => {
                          const next = new Set(prev);
                          e.target.checked ? next.add(role) : next.delete(role);
                          return next;
                        })}
                      />
                      {role.replace('.', ' ')}
                    </label>
                  ))}
                </div>
              </fieldset>

              <fieldset className="form-fieldset">
                <legend>How it signs in</legend>
                {([
                  ['client_cert',   'Client certificate', 'Best for Doris from your own code. Valid 90 days.'],
                  ['client_secret', 'Client ID and secret', 'For Kafka and Memgraph. Short-lived tokens.'],
                  ['app_password',  'App password', 'Doris only, for tools that need a username and password. 90 days.'],
                ] as const).map(([val, label, hint]) => (
                  <label key={val} className="radio-label">
                    <input
                      type="radio"
                      name="cred-type"
                      value={val}
                      checked={formCred === val}
                      onChange={() => setFormCred(val)}
                    />
                    <div>
                      <strong>{label}</strong>
                      <p className="form-hint">{hint}</p>
                    </div>
                  </label>
                ))}
              </fieldset>

              <div className="form-actions">
                <button type="button" className="btn btn--ghost" onClick={() => setShowForm(false)}>Cancel</button>
                <button
                  type="submit"
                  className="btn btn--primary"
                  disabled={createMutation.isPending || !formName || !formOwner}
                >
                  {createMutation.isPending ? 'Creating…' : 'Create application'}
                </button>
              </div>
            </form>
          )
        }
      </section>

      {/* ── App created dialog (Screen 8) ──────────────────────────── */}
      {createdResult && (
        <AppCreatedDialog
          result={createdResult}
          onClose={() => setCreatedResult(null)}
        />
      )}

      {/* ── Revoke confirm ──────────────────────────────────────────── */}
      {revokingApp && (
        <ConfirmDialog
          title={`Revoke ${revokingApp.name}`}
          description="This removes all credentials and the authorization at once."
          onConfirm={reason => revokeMutation.mutate({ id: revokingApp.userId, reason })}
          onCancel={() => setRevokingApp(null)}
          isSubmitting={revokeMutation.isPending}
        />
      )}
    </div>
  );
}
