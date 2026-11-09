// =============================================================================
// Screen 4 — Members  (/access/members)
// Canvas: "Waiting for approval" cards with LevelGrid + Approve/Decline,
//         personal-domain warning, "All members" searchable table.
//         Includes the Invite dialog (Screen 6) inline.
// =============================================================================
import React, { useState } from 'react';
import { Link } from 'react-router-dom';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import {
  apiFetch, Avatar, LoadingSkeleton, ConfirmDialog, LevelGrid, ClaimsPreview,
  statusPill, useToast, formatRelative,
} from './components';
import InviteDialog from './InviteDialog';
import type { Member, ApproveResult, RoleCatalogueItem, Levels, PlatformRole, PagedResponse } from './types';

export default function MembersPage() {
  const qc = useQueryClient();
  const { show: toast } = useToast();

  // ── Fetch waiting members ────────────────────────────────────────────────
  const { data: waiting, isLoading: waitingLoading } = useQuery<PagedResponse<Member>>({
    queryKey: ['members', 'waiting'],
    queryFn: () => apiFetch('/api/access/members?state=waiting').then(r => r.json()),
  });

  // ── Fetch active/all members ─────────────────────────────────────────────
  const [search, setSearch] = useState('');
  const [cursor, setCursor] = useState<string | null>(null);
  const [allMembers, setAllMembers] = useState<Member[]>([]);

  const { data: activePage, isLoading: activeLoading } = useQuery<PagedResponse<Member>>({
    queryKey: ['members', 'active', search, cursor],
    queryFn: () => {
      const params = new URLSearchParams({ state: 'active' });
      if (search) params.set('q', search);
      if (cursor) params.set('cursor', cursor);
      return apiFetch(`/api/access/members?${params}`).then(r => r.json());
    },
    onSuccess: data => {
      setAllMembers(prev => cursor ? [...prev, ...data.items] : data.items);
    },
  });

  // ── Fetch role catalogue for LevelGrid ───────────────────────────────────
  const { data: catalogue = [] } = useQuery<RoleCatalogueItem[]>({
    queryKey: ['roles'],
    queryFn: () => apiFetch('/api/access/roles').then(r => r.json()),
    staleTime: 300_000,
  });

  // ── Per-card pending levels state ────────────────────────────────────────
  const [pendingLevels, setPendingLevels] = useState<Record<string, { levels: Levels; platform: PlatformRole }>>({});
  const getCardState = (m: Member) => pendingLevels[m.id] ?? { levels: m.levels, platform: m.platform };

  // ── Approve mutation ─────────────────────────────────────────────────────
  const approveMutation = useMutation({
    mutationFn: ({ id, levels, platform }: { id: string; levels: Levels; platform: PlatformRole }) =>
      apiFetch(`/api/access/members/${id}/approve`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ platform, levels }),
      }).then(r => r.json() as Promise<ApproveResult>),
    onSuccess: (result, vars) => {
      qc.invalidateQueries({ queryKey: ['members'] });
      qc.invalidateQueries({ queryKey: ['access-summary'] });
      if (result.state === 'active') {
        toast('Roles granted. They will reach the person's next token within 10 minutes.');
      } else {
        toast(`Approval request ${result.approvalId} created — a second Owner must approve.`, 'info');
      }
    },
  });

  // ── Decline mutation ─────────────────────────────────────────────────────
  const [declining, setDeclining] = useState<Member | null>(null);
  const declineMutation = useMutation({
    mutationFn: ({ id, reason }: { id: string; reason: string }) =>
      apiFetch(`/api/access/members/${id}/decline`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ reason }),
      }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['members'] });
      qc.invalidateQueries({ queryKey: ['access-summary'] });
      toast('Access declined. The person has been notified.', 'info');
      setDeclining(null);
    },
  });

  // ── Invite dialog ─────────────────────────────────────────────────────────
  const [showInvite, setShowInvite] = useState(false);

  return (
    <div className="members-page">
      <div className="page-header">
        <div>
          <h1>Members</h1>
          <p>Approve sign-ups and choose one level per component. Owners and Admins only.</p>
        </div>
        <button type="button" className="btn btn--primary" onClick={() => setShowInvite(true)}>
          Invite by email
        </button>
      </div>

      {/* ── Waiting for approval ────────────────────────────────────────── */}
      {waitingLoading ? <LoadingSkeleton /> : (
        <section className="members-waiting">
          <h2 className="section-title">
            Waiting for approval
            {waiting?.items.length ? <span className="section-count"> {waiting.items.length} people</span> : null}
          </h2>
          {!waiting?.items.length
            ? <p className="empty-state">No one is waiting.</p>
            : waiting.items.map(m => {
              const card = getCardState(m);
              const hasLevel = Object.values(card.levels).some(Boolean) || card.platform !== 'member';

              return (
                <div key={m.id} className={`member-card${m.personalDomain ? ' member-card--warning' : ''}`}>
                  <div className="member-card__header">
                    <Avatar name={m.name} />
                    <div>
                      <strong>{m.name}</strong>
                      <p className="member-card__meta">
                        {m.email} · email {m.emailVerified ? 'verified' : 'unverified'} · signed up {formatRelative(m.createdAt)}
                      </p>
                    </div>
                  </div>

                  {m.personalDomain && (
                    <p className="member-card__domain-warning">
                      {m.name.split(' ')[0]} signed up with a personal address. Check who they are before you approve.
                    </p>
                  )}

                  <LevelGrid
                    catalogue={catalogue}
                    levels={card.levels}
                    platform={card.platform}
                    onChange={(levels, platform) =>
                      setPendingLevels(prev => ({ ...prev, [m.id]: { levels, platform } }))
                    }
                  />

                  <p className="member-card__hint">Roles reach the person's tokens within 10 minutes.</p>

                  <div className="member-card__actions">
                    <button
                      type="button"
                      className="btn btn--primary"
                      disabled={!hasLevel || approveMutation.isPending}
                      onClick={() => approveMutation.mutate({ id: m.id, levels: card.levels, platform: card.platform })}
                    >
                      Approve with these roles
                    </button>
                    <button
                      type="button"
                      className="btn btn--danger-ghost"
                      onClick={() => setDeclining(m)}
                    >
                      Decline
                    </button>
                  </div>
                </div>
              );
            })
          }
        </section>
      )}

      {/* ── All members table ─────────────────────────────────────────────── */}
      <section className="members-all">
        <h2 className="section-title">
          All members {activePage ? `(${allMembers.length})` : ''}
        </h2>
        <input
          type="search"
          className="search-input"
          placeholder="Find a member"
          value={search}
          onChange={e => { setSearch(e.target.value); setCursor(null); setAllMembers([]); }}
        />
        {activeLoading && allMembers.length === 0 ? <LoadingSkeleton /> : (
          <table className="data-table">
            <thead>
              <tr>
                <th>Person</th>
                <th>Platform</th>
                <th>StarData</th>
                <th>Kafka</th>
                <th>StarGraph</th>
                <th>ETL Studio</th>
                <th>Last sign-in</th>
                <th></th>
              </tr>
            </thead>
            <tbody>
              {allMembers.map(m => (
                <tr key={m.id}>
                  <td>
                    <div><strong>{m.name}</strong></div>
                    <div className="table-sub">{m.email}</div>
                  </td>
                  <td>{m.platform}</td>
                  <td>{m.levels.doris ?? '—'}</td>
                  <td>{m.levels.kafka ?? '—'}</td>
                  <td>{m.levels.memgraph ?? '—'}</td>  {/* StarGraph */}
                  <td>{m.levels.etl ?? '—'}</td>
                  <td>{m.lastSignIn ? formatRelative(m.lastSignIn) : '—'}</td>
                  <td>
                    <Link to={`/access/members/${m.id}`} className="btn btn--ghost btn--sm">Edit</Link>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
        {activePage?.next && (
          <button type="button" className="btn btn--ghost" onClick={() => setCursor(activePage.next)}>
            Load more
          </button>
        )}
        <p className="members-all__note">
          Revoke now ends the person's sessions and stops new tokens at once. Tokens already issued expire within 10 minutes.
        </p>
      </section>

      {/* ── Decline confirm dialog ───────────────────────────────────────── */}
      {declining && (
        <ConfirmDialog
          title={`Decline ${declining.name}`}
          description="This will deactivate the user and send a notification email."
          onConfirm={reason => declineMutation.mutate({ id: declining.id, reason })}
          onCancel={() => setDeclining(null)}
          isSubmitting={declineMutation.isPending}
        />
      )}

      {/* ── Invite dialog (Screen 6) ─────────────────────────────────────── */}
      {showInvite && <InviteDialog catalogue={catalogue} onClose={() => setShowInvite(false)} />}
    </div>
  );
}
