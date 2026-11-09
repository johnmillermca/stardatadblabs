// =============================================================================
// Screen 5 — Member detail  (/access/members/:id)
// Canvas: header with state pill + action buttons, Access levels card,
//         Active sessions card, Sign-in & change history card.
// =============================================================================
import React, { useState } from 'react';
import { useParams, Link, useNavigate } from 'react-router-dom';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import {
  apiFetch, Avatar, LevelGrid, ClaimsPreview, ConfirmDialog, statusPill,
  DataTable, formatRelative, formatDate, useToast, LoadingSkeleton,
} from './components';
import type { MemberDetail, RoleCatalogueItem, Levels, PlatformRole, Session, AuditRow } from './types';

export default function MemberDetailPage() {
  const { id } = useParams<{ id: string }>();
  const qc = useQueryClient();
  const navigate = useNavigate();
  const { show: toast } = useToast();

  // ── Fetch member detail ───────────────────────────────────────────────────
  const { data: member, isLoading } = useQuery<MemberDetail>({
    queryKey: ['member', id],
    queryFn: () => apiFetch(`/api/access/members/${id}`).then(r => r.json()),
  });

  const { data: catalogue = [] } = useQuery<RoleCatalogueItem[]>({
    queryKey: ['roles'],
    queryFn: () => apiFetch('/api/access/roles').then(r => r.json()),
    staleTime: 300_000,
  });

  // ── Level edit state ──────────────────────────────────────────────────────
  const [editLevels, setEditLevels] = useState<Levels | null>(null);
  const [editPlatform, setEditPlatform] = useState<PlatformRole | null>(null);
  const isEditing = editLevels !== null;

  const startEdit = () => {
    setEditLevels(member?.levels ?? {});
    setEditPlatform(member?.platform ?? 'member');
  };
  const cancelEdit = () => { setEditLevels(null); setEditPlatform(null); };

  const saveMutation = useMutation({
    mutationFn: () =>
      apiFetch(`/api/access/members/${id}/levels`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ platform: editPlatform, levels: editLevels }),
      }).then(r => r.json()),
    onSuccess: result => {
      qc.invalidateQueries({ queryKey: ['member', id] });
      cancelEdit();
      if (result.applied) {
        toast('Access levels saved. Changes take effect on the person's next token (within 10 min).');
      } else {
        toast(`Approval request ${result.approvalId} created — a second Owner must approve.`, 'info');
      }
    },
  });

  // ── Reset MFA ─────────────────────────────────────────────────────────────
  const resetMfaMutation = useMutation({
    mutationFn: () =>
      apiFetch(`/api/access/members/${id}/reset-mfa`, { method: 'POST' }),
    onSuccess: () => toast('MFA methods removed. The person must re-enrol at next sign-in.'),
  });

  // ── Password reset ────────────────────────────────────────────────────────
  const passwordResetMutation = useMutation({
    mutationFn: () =>
      apiFetch(`/api/access/members/${id}/password-reset`, { method: 'POST' }),
    onSuccess: () => toast('Password reset email sent.'),
  });

  // ── Revoke ────────────────────────────────────────────────────────────────
  const [showRevoke, setShowRevoke] = useState(false);
  const revokeMutation = useMutation({
    mutationFn: (reason: string) =>
      apiFetch(`/api/access/members/${id}/revoke`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ reason }),
      }),
    onSuccess: () => {
      toast('Access revoked. All sessions ended.', 'info');
      navigate('/access/members');
    },
  });

  // ── End single session ────────────────────────────────────────────────────
  const endSessionMutation = useMutation({
    mutationFn: (sessionId: string) =>
      apiFetch(`/api/access/members/${id}/sessions/${sessionId}`, { method: 'DELETE' }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['member', id] });
      toast('Session ended.');
    },
  });

  // ── End all sessions ──────────────────────────────────────────────────────
  const endAllMutation = useMutation({
    mutationFn: () =>
      apiFetch(`/api/access/members/${id}/revoke`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ reason: 'Admin ended all sessions', sessionsOnly: true }),
      }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['member', id] });
      toast('All sessions ended.');
    },
  });

  if (isLoading) return <LoadingSkeleton />;
  if (!member) return <p>Member not found.</p>;

  const levels = isEditing ? editLevels! : member.levels;
  const platform = isEditing ? editPlatform! : member.platform;

  return (
    <div className="member-detail-page">
      {/* ── Breadcrumb ─────────────────────────────────────────────────── */}
      <nav className="breadcrumb">
        <Link to="/access/members">Members</Link> / {member.name}
      </nav>

      {/* ── Header ────────────────────────────────────────────────────── */}
      <div className="member-detail-header">
        <Avatar name={member.name} />
        <div className="member-detail-header__info">
          <h1>{member.name}</h1>
          <p className="member-detail-header__meta">
            {member.email} · member since {formatDate(member.memberSince)} · ZITADEL user {member.zitadelUserId}
          </p>
          {statusPill(member.state)}
        </div>
        <div className="member-detail-header__actions">
          <button
            type="button"
            className="btn btn--ghost"
            onClick={() => resetMfaMutation.mutate()}
            disabled={resetMfaMutation.isPending}
          >
            Reset MFA
          </button>
          <button
            type="button"
            className="btn btn--ghost"
            onClick={() => passwordResetMutation.mutate()}
            disabled={passwordResetMutation.isPending}
          >
            Send password reset
          </button>
          <button
            type="button"
            className="btn btn--danger-ghost"
            onClick={() => setShowRevoke(true)}
          >
            Revoke now
          </button>
        </div>
      </div>

      {/* ── Access levels card ────────────────────────────────────────── */}
      <section className="card">
        <div className="card__header">
          <h2>Access levels</h2>
          <p>One level per component · stored as one ZITADEL authorization</p>
        </div>
        <LevelGrid
          catalogue={catalogue}
          levels={levels}
          platform={platform}
          onChange={(lvs, plt) => { setEditLevels(lvs); setEditPlatform(plt); }}
          readOnly={!isEditing}
        />
        {isEditing && (
          <>
            <ClaimsPreview levels={levels} platform={platform} />
            <div className="card__actions">
              <button type="button" className="btn btn--ghost" onClick={cancelEdit}>Cancel</button>
              <button
                type="button"
                className="btn btn--primary"
                onClick={() => saveMutation.mutate()}
                disabled={saveMutation.isPending}
              >
                {saveMutation.isPending ? 'Saving…' : 'Save changes'}
              </button>
            </div>
          </>
        )}
        {!isEditing && (
          <>
            <ClaimsPreview levels={member.levels} platform={member.platform} />
            <div className="card__actions">
              <button type="button" className="btn btn--primary" onClick={startEdit}>Edit levels</button>
            </div>
          </>
        )}
      </section>

      {/* ── Active sessions card ──────────────────────────────────────── */}
      <section className="card">
        <div className="card__header">
          <h2>Active sessions</h2>
          <button
            type="button"
            className="btn btn--ghost btn--sm"
            onClick={() => endAllMutation.mutate()}
            disabled={endAllMutation.isPending || !member.sessions.length}
          >
            End all sessions
          </button>
        </div>
        {member.sessions.length === 0
          ? <p className="empty-state">No active sessions.</p>
          : (
            <table className="data-table">
              <thead>
                <tr><th>Where</th><th>IP</th><th>Signed in</th><th>Method</th><th></th></tr>
              </thead>
              <tbody>
                {member.sessions.map((s: Session) => (
                  <tr key={s.id}>
                    <td>{s.userAgent}</td>
                    <td>{s.ip}</td>
                    <td>{formatRelative(s.createdAt)}</td>
                    <td>{s.method}</td>
                    <td>
                      <button
                        type="button"
                        className="btn btn--ghost btn--sm"
                        onClick={() => endSessionMutation.mutate(s.id)}
                      >
                        End
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )
        }
      </section>

      {/* ── Sign-in and change history card ──────────────────────────── */}
      <section className="card">
        <div className="card__header">
          <h2>Sign-in and change history</h2>
          <Link to={`/access/audit?who=${member.id}`} className="btn btn--ghost btn--sm">Open in Audit</Link>
        </div>
        {member.history.length === 0
          ? <p className="empty-state">No recent events.</p>
          : (
            <table className="data-table">
              <thead><tr><th>When</th><th>Event</th><th>Result</th><th>IP</th></tr></thead>
              <tbody>
                {member.history.map((row: AuditRow) => (
                  <tr key={row.eventId}>
                    <td>{formatRelative(row.ts)}</td>
                    <td>{row.action}</td>
                    <td>{statusPill(row.result)}</td>
                    <td>{row.ip ?? '—'}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )
        }
      </section>

      {/* ── Account card ──────────────────────────────────────────────── */}
      <section className="card member-detail-account">
        <h2>Account</h2>
        <dl>
          <dt>Email</dt><dd>{member.account.emailVerified ? 'verified' : 'unverified'}</dd>
          <dt>MFA</dt><dd>{member.account.mfaMethods.join(' · ') || 'none enrolled'}</dd>
          <dt>Password</dt>
          <dd>{member.account.passwordChangedAt ? `changed ${formatDate(member.account.passwordChangedAt)}` : '—'}</dd>
          <dt>Last sign-in</dt><dd>{member.account.lastSignIn ? formatRelative(member.account.lastSignIn) : '—'}</dd>
          <dt>Failed sign-ins</dt><dd>{member.account.failedSignInsLast30Days} in 30 days</dd>
        </dl>
      </section>

      {/* ── Revoke confirm dialog ──────────────────────────────────────── */}
      {showRevoke && (
        <ConfirmDialog
          title={`Revoke access for ${member.name}`}
          description="This removes the ZITADEL authorization, ends every session, revokes refresh tokens, deletes console sessions in Valkey and app passwords at the StarData proxy. Access tokens already issued stop working within 10 minutes."
          requireName={member.name}
          onConfirm={reason => revokeMutation.mutate(reason)}
          onCancel={() => setShowRevoke(false)}
          isSubmitting={revokeMutation.isPending}
        />
      )}
    </div>
  );
}
