// =============================================================================
// Screen 10 — Approvals  (/access/approvals)
// Canvas: stat tiles (Waiting for you, Owners count, Admins count),
//         table with Approve/Reject, detail panel (before/after, reason, checks).
//         Approve button disabled for the requester and for Admins.
//         Countdown on expires_at; expired rows grey.
// =============================================================================
import React, { useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { apiFetch, Drawer, LoadingSkeleton, formatCountdown, formatRelative, useToast } from './components';
import type { Approval, ApprovalsDecision } from './types';
import { useMe } from './components';

export default function ApprovalsPage() {
  const qc = useQueryClient();
  const { show: toast } = useToast();
  const { me } = useMe();

  const isAdmin = me?.platformRoles?.includes('admin') && !me?.platformRoles?.includes('owner');

  const { data: approvals = [], isLoading } = useQuery<Approval[]>({
    queryKey: ['approvals'],
    queryFn: () => apiFetch('/api/access/approvals?state=all').then(r => r.json()),
    refetchInterval: 30_000,
  });

  const pending = approvals.filter(a => a.state === 'pending');
  const waitingForMe = pending.filter(a => a.requestedBy !== me?.sub);

  // Count of owners/admins from the catalogue summary (read from approvals summary endpoint)
  const { data: summary } = useQuery<{ owners: number; admins: number }>({
    queryKey: ['approvals-summary'],
    queryFn: () => apiFetch('/api/access/approvals/summary').then(r => r.json()),
  });

  // ── Detail drawer ─────────────────────────────────────────────────────────
  const [selected, setSelected] = useState<Approval | null>(null);

  // ── Decide mutation ───────────────────────────────────────────────────────
  const decideMutation = useMutation({
    mutationFn: ({ id, decision, comment }: { id: string } & ApprovalsDecision) =>
      apiFetch(`/api/access/approvals/${id}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ decision, comment }),
      }).then(r => r.json()),
    onSuccess: (_, vars) => {
      qc.invalidateQueries({ queryKey: ['approvals'] });
      qc.invalidateQueries({ queryKey: ['access-summary'] });
      toast(vars.decision === 'approve' ? 'Change applied and recorded in the audit log.' : 'Approval request rejected.');
      setSelected(null);
    },
  });

  const [decisionReason, setDecisionReason] = useState('');

  const canApprove = (a: Approval) =>
    !isAdmin && a.requestedBy !== me?.sub && a.state === 'pending';

  if (isLoading) return <LoadingSkeleton />;

  return (
    <div className="approvals-page">
      <div className="page-header">
        <h1>Approvals</h1>
        <p>Changes to Owners and Admins need a second Owner. The person who asked can never approve.</p>
      </div>

      {/* ── Stat tiles ───────────────────────────────────────────────── */}
      <div className="stat-tiles">
        <div className="stat-tile stat-tile--highlight">
          <span className="stat-tile__value">{waitingForMe.length}</span>
          <span className="stat-tile__label">Waiting for you</span>
          {waitingForMe.length > 0 && pending[0] && (
            <span className="stat-tile__sub">expires in {formatCountdown(pending[0].expiresAt)}</span>
          )}
        </div>
        <div className="stat-tile">
          <span className="stat-tile__value">{summary?.owners ?? '…'}</span>
          <span className="stat-tile__label">Owners</span>
          <span className="stat-tile__sub">minimum 2 always kept</span>
        </div>
        <div className="stat-tile">
          <span className="stat-tile__value">{summary?.admins ?? '…'}</span>
          <span className="stat-tile__label">Admins</span>
        </div>
      </div>

      {/* ── Approval requests table ───────────────────────────────────── */}
      {approvals.length === 0
        ? <p className="empty-state">No approval requests.</p>
        : (
          <table className="data-table">
            <thead>
              <tr>
                <th>ID</th>
                <th>Change</th>
                <th>Asked by</th>
                <th>When</th>
                <th>Expires</th>
                <th></th>
              </tr>
            </thead>
            <tbody>
              {approvals.map(a => {
                const isExpired = a.state === 'expired' || (a.state === 'pending' && new Date(a.expiresAt) < new Date());
                return (
                  <tr
                    key={a.id}
                    className={isExpired ? 'row--expired' : ''}
                    onClick={() => setSelected(a)}
                    style={{ cursor: 'pointer' }}
                  >
                    <td><code>{a.id}</code></td>
                    <td>{describeChange(a)}</td>
                    <td>{a.requestedByName}</td>
                    <td>{formatRelative(a.requestedAt)}</td>
                    <td>
                      {a.state === 'pending' && !isExpired
                        ? `in ${formatCountdown(a.expiresAt)}`
                        : a.state === 'approved' ? <span className="text-green">Approved by {a.decidedByName}</span>
                        : a.state === 'rejected' ? <span className="text-muted">Rejected</span>
                        : <span className="text-muted">Expired</span>
                      }
                    </td>
                    <td onClick={e => e.stopPropagation()}>
                      {canApprove(a) && (
                        <>
                          <button
                            type="button"
                            className="btn btn--primary btn--sm"
                            onClick={() => { setSelected(a); setDecisionReason(''); }}
                          >
                            Approve
                          </button>
                          <button
                            type="button"
                            className="btn btn--danger-ghost btn--sm"
                            onClick={() => decideMutation.mutate({ id: a.id, decision: 'reject', comment: 'Rejected.' })}
                          >
                            Reject
                          </button>
                        </>
                      )}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        )
      }

      {/* ── Detail drawer ────────────────────────────────────────────── */}
      <Drawer
        title={selected ? `${selected.id} · ${describeChange(selected)}` : ''}
        open={!!selected}
        onClose={() => setSelected(null)}
      >
        {selected && (
          <div className="approval-detail">
            <dl>
              <dt>Today</dt>
              <dd>{selected.payload.from.join(', ') || '(no roles)'}</dd>
              <dt>After</dt>
              <dd>{selected.payload.to.join(', ')}</dd>
              {selected.reason && <><dt>Reason</dt><dd>{selected.reason}</dd></>}
            </dl>
            {selected.checks && (
              <ul className="approval-checks">
                <li>{selected.checks.mfaEnrolled ? '✓' : '✗'} MFA enrolled</li>
                <li>{selected.checks.signedInWithin30Days ? '✓' : '✗'} Signed in within 30 days</li>
              </ul>
            )}
            {canApprove(selected) && (
              <div className="approval-detail__actions">
                <p>Approving writes the change to ZITADEL and records both names in the audit log.</p>
                <label className="form-field">
                  <span>Comment (optional)</span>
                  <textarea
                    value={decisionReason}
                    onChange={e => setDecisionReason(e.target.value)}
                    rows={2}
                  />
                </label>
                <div className="dialog__actions">
                  <button
                    type="button"
                    className="btn btn--danger-ghost"
                    onClick={() => decideMutation.mutate({ id: selected.id, decision: 'reject', comment: decisionReason })}
                    disabled={decideMutation.isPending}
                  >
                    Reject
                  </button>
                  <button
                    type="button"
                    className="btn btn--primary"
                    onClick={() => decideMutation.mutate({ id: selected.id, decision: 'approve', comment: decisionReason })}
                    disabled={decideMutation.isPending}
                  >
                    {decideMutation.isPending ? 'Applying…' : 'Approve'}
                  </button>
                </div>
              </div>
            )}
          </div>
        )}
      </Drawer>
    </div>
  );
}

function describeChange(a: Approval): string {
  const to = a.payload.to;
  const name = a.targetUserName;
  if (a.action === 'set_platform_role')    return `Make ${name} ${to.join(', ')}`;
  if (a.action === 'remove_platform_role') return `Remove ${a.payload.from.join(', ')} role from ${name}`;
  return a.action;
}
