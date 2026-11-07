// =============================================================================
// Screen 11 — Sign-in policy  (/access/policy)
// Canvas: stat tiles (ZITADEL pods, Login v2 pods, DB instances, Config drift,
//         Signing keys), settings table, break-glass note. Owner only. Read-only.
//         "Open change in Git" links to the Terraform folder.
// =============================================================================
import React from 'react';
import { useQuery } from '@tanstack/react-query';
import { apiFetch, LoadingSkeleton, formatDate } from './components';
import type { PolicyStatus } from './types';

const TERRAFORM_REPO_URL = 'https://github.com/johnmillermca/stardatadblabs/tree/main/zitadel/terraform';

export default function PolicyPage() {
  const { data: policy, isLoading } = useQuery<PolicyStatus>({
    queryKey: ['policy'],
    queryFn: () => apiFetch('/api/access/policy').then(r => r.json()),
    staleTime: 60_000,
  });

  if (isLoading) return <LoadingSkeleton />;
  if (!policy)   return <p>Could not load policy settings.</p>;

  const { settings } = policy;

  return (
    <div className="policy-page">
      <div className="page-header">
        <div>
          <h1>Sign-in policy</h1>
          <p>How people sign in, read from ZITADEL. Changes go through Git and Terraform so every environment matches.</p>
          <span className="badge badge--info">Owner only</span>
        </div>
        <a href={TERRAFORM_REPO_URL} className="btn btn--ghost" target="_blank" rel="noreferrer">
          Open change in Git
        </a>
      </div>

      {/* ── Stat tiles ───────────────────────────────────────────────── */}
      <div className="stat-tiles">
        <div className="stat-tile">
          <span className="stat-tile__label">ZITADEL</span>
          <span className="stat-tile__value">v4 · {policy.zitadelPods.ready} / {policy.zitadelPods.total} pods</span>
        </div>
        <div className="stat-tile">
          <span className="stat-tile__label">Login v2</span>
          <span className="stat-tile__value">{policy.loginPods.ready} / {policy.loginPods.total} pods</span>
        </div>
        <div className="stat-tile">
          <span className="stat-tile__label">Database</span>
          <span className="stat-tile__value">PostgreSQL 16 · {policy.dbInstances.ready} / {policy.dbInstances.total} instances</span>
        </div>
        <div className={`stat-tile ${policy.driftStatus !== 'clean' ? 'stat-tile--warn' : ''}`}>
          <span className="stat-tile__label">Config drift</span>
          <span className="stat-tile__value">
            {policy.driftStatus === 'clean' ? 'None' : policy.driftStatus === 'drifted' ? '⚠ Drift detected' : 'Unknown'}
          </span>
          <span className="stat-tile__sub">Terraform plan · {formatDate(policy.driftCheckedAt)}</span>
        </div>
        <div className="stat-tile">
          <span className="stat-tile__label">Signing keys</span>
          <span className="stat-tile__value">rotated {formatDate(policy.signingKeyRotatedAt)}</span>
          <span className="stat-tile__sub">next rotation {formatDate(policy.signingKeyNextRotation)}</span>
        </div>
      </div>

      {/* ── Current settings table ────────────────────────────────────── */}
      <section className="card">
        <h2>Current settings</h2>
        <table className="data-table policy-table">
          <thead>
            <tr><th>Setting</th><th>Value</th><th>Where it lives in ZITADEL</th></tr>
          </thead>
          <tbody>
            {[
              ['Self sign-up',         settings.selfSignUp ? 'On' : 'Off',         'Login policy · allow register'],
              ['Email verification',   settings.emailVerification,                  'default'],
              ['MFA',                  settings.mfa,                                'Login policy · force MFA (per role via Action check)'],
              ['Passkeys',             settings.passkeys,                           'Login policy · passwordless allowed'],
              ['Password',             settings.password,                           'Password complexity policy'],
              ['Lockout',              settings.lockout,                            'Lockout policy'],
              ['Session',              settings.sessionIdle + ' idle, ' + settings.sessionMax + ' max', 'Login policy lifetimes'],
              ['Access token',         settings.accessToken,                        'App setting stardata-console'],
              ['Allowed email domains', settings.allowedEmailDomains.join(', ') + ' (others flagged in Members)', 'Console backend check'],
              ['Branding',             settings.branding,                           'Label policy'],
              ['Email (SMTP)',         settings.smtp,                               'SMTP configuration'],
            ].map(([setting, value, where]) => (
              <tr key={setting as string}>
                <td>{setting}</td>
                <td>{value}</td>
                <td className="text-muted">{where}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </section>

      {/* ── Break-glass note ──────────────────────────────────────────── */}
      <section className="card policy-breakglass">
        <h2>Break-glass</h2>
        <p>
          One emergency Owner account exists only in ZITADEL, with a hardware key. It is used in ZITADEL's own
          console when this console is down. Last used: never. <a href="/access/audit?action=breakglass">Audit</a>
        </p>
      </section>
    </div>
  );
}
