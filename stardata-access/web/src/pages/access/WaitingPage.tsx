// =============================================================================
// Screen 3 — Waiting for approval  (/waiting)
// Canvas: centred card, three-step timeline, Check again + Sign out buttons.
// Polls GET /api/me every 30 s. When status → active: force token refresh then
// navigate to Home.
// =============================================================================
import React, { useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { apiFetch, formatDate } from './components';
import type { MeResponse } from './types';

export default function WaitingPage() {
  const navigate = useNavigate();
  const qc = useQueryClient();

  const { data: me, refetch, isRefetching } = useQuery<MeResponse>({
    queryKey: ['me'],
    queryFn: () => apiFetch('/api/me').then(r => r.json()),
    refetchInterval: 30_000,   // poll every 30 s
  });

  const refreshMutation = useMutation({
    mutationFn: () => apiFetch('/auth/refresh', { method: 'POST' }).then(r => r.json()),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['me'] });
    },
  });

  // When status becomes active, force token refresh and go home
  useEffect(() => {
    if (me?.status === 'active') {
      refreshMutation.mutate(undefined, {
        onSettled: () => navigate('/', { replace: true }),
      });
    }
  }, [me?.status]);

  const handleSignOut = () => {
    window.location.href = '/auth/logout';
  };

  return (
    <div className="waiting-page">
      <div className="waiting-card">
        {/* Header */}
        <div className="waiting-card__header">
          <div className="avatar waiting-card__avatar">
            {me?.name?.split(' ').map(w => w[0]).slice(0, 2).join('').toUpperCase() ?? '…'}
          </div>
          <div>
            <h1 className="waiting-card__title">Your account is waiting for approval</h1>
            <p className="waiting-card__subtitle">
              An Owner or Admin has been told. When they give you access, your components appear here.
              You don't need to sign up again.
            </p>
          </div>
        </div>

        {/* Three-step timeline */}
        <ol className="waiting-timeline">
          <li className="waiting-timeline__step waiting-timeline__step--done">
            <span className="waiting-timeline__icon">✓</span>
            <div>
              <strong>Account created</strong>
              {me?.email && <p className="waiting-timeline__detail">{me.email}</p>}
            </div>
          </li>
          <li className="waiting-timeline__step waiting-timeline__step--done">
            <span className="waiting-timeline__icon">✓</span>
            <div>
              <strong>Email verified</strong>
              {me?.email && <p className="waiting-timeline__detail">Nov 5, 08:57</p>}
            </div>
          </li>
          <li className="waiting-timeline__step waiting-timeline__step--inprogress">
            <span className="waiting-timeline__icon waiting-timeline__icon--spinner" />
            <div>
              <strong>Admin gives you access</strong>
              <p className="waiting-timeline__detail">
                Usually within one working day. You will get an email.
              </p>
            </div>
          </li>
        </ol>

        {/* Actions */}
        <div className="waiting-card__actions">
          <button
            type="button"
            className="btn btn--primary"
            onClick={() => refetch()}
            disabled={isRefetching}
          >
            {isRefetching ? 'Checking…' : 'Check again'}
          </button>
          <button type="button" className="btn btn--ghost" onClick={handleSignOut}>
            Sign out
          </button>
        </div>

        <p className="waiting-card__note">
          Wrong email address? Sign out and create the account again with your work address.
        </p>
      </div>
    </div>
  );
}
