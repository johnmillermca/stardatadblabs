// =============================================================================
// Screen 6 — Invite a person  (dialog on Members page)
// Canvas: modal with email, first/last name, LevelGrid, optional message.
// ZITADEL creates the account and sends the invite email (valid 3 days).
// No approval step for invited users.
// =============================================================================
import React, { useState } from 'react';
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { apiFetch, LevelGrid, useToast } from './components';
import type { RoleCatalogueItem, Levels, PlatformRole, InvitePayload } from './types';

interface InviteDialogProps {
  catalogue: RoleCatalogueItem[];
  onClose: () => void;
}

export default function InviteDialog({ catalogue, onClose }: InviteDialogProps) {
  const qc = useQueryClient();
  const { show: toast } = useToast();

  const [email, setEmail]         = useState('');
  const [firstName, setFirstName] = useState('');
  const [lastName, setLastName]   = useState('');
  const [levels, setLevels]       = useState<Levels>({});
  const [platform, setPlatform]   = useState<PlatformRole>('member');
  const [message, setMessage]     = useState('');
  const [fieldError, setFieldError] = useState<string | null>(null);

  const canSubmit =
    email.trim().includes('@') &&
    firstName.trim().length > 0 &&
    lastName.trim().length > 0;

  const inviteMutation = useMutation({
    mutationFn: (payload: InvitePayload) =>
      apiFetch('/api/access/members/invite', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      }).then(async r => {
        if (r.status === 409) {
          const body = await r.json();
          throw new Error(body.memberId ?? 'conflict');
        }
        if (!r.ok) throw new Error('invite_failed');
        return r.json();
      }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['members'] });
      toast(`Invite sent to ${email}. They will get an email with a link valid 3 days.`);
      onClose();
    },
    onError: (err: Error) => {
      if (err.message !== 'invite_failed') {
        setFieldError(`This email already has an account. Open member: /access/members/${err.message}`);
      } else {
        setFieldError('Failed to send invite. Please try again.');
      }
    },
  });

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    setFieldError(null);
    inviteMutation.mutate({ email, firstName, lastName, platform, levels, message: message || undefined });
  };

  return (
    <div className="dialog-overlay" role="dialog" aria-modal="true" aria-labelledby="invite-title">
      <div className="dialog dialog--wide">
        <div className="dialog__header">
          <h2 id="invite-title">Invite a person</h2>
          <p>
            ZITADEL creates the account and emails an invite link (valid 3 days). They set a password or
            passkey; no approval step.
          </p>
          <button type="button" className="dialog__dismiss" onClick={onClose} aria-label="Close">✕</button>
        </div>

        <form onSubmit={handleSubmit} className="dialog__body">
          <div className="form-row">
            <label className="form-field">
              <span>First name</span>
              <input
                type="text"
                value={firstName}
                onChange={e => setFirstName(e.target.value)}
                required
                autoComplete="given-name"
              />
            </label>
            <label className="form-field">
              <span>Last name</span>
              <input
                type="text"
                value={lastName}
                onChange={e => setLastName(e.target.value)}
                required
                autoComplete="family-name"
              />
            </label>
          </div>

          <label className="form-field">
            <span>Work email</span>
            <input
              type="email"
              value={email}
              onChange={e => setEmail(e.target.value)}
              required
              autoComplete="email"
              placeholder="name@stardatadb.com"
            />
          </label>

          <fieldset className="form-fieldset">
            <legend>Start with these levels</legend>
            <LevelGrid
              catalogue={catalogue}
              levels={levels}
              platform={platform}
              onChange={(lvs, plt) => { setLevels(lvs); setPlatform(plt); }}
            />
          </fieldset>

          <label className="form-field">
            <span>Optional message to the person</span>
            <textarea
              value={message}
              onChange={e => setMessage(e.target.value)}
              maxLength={500}
              rows={3}
              placeholder="e.g. Welcome to the platform! You've been added to the analytics team."
            />
          </label>

          {fieldError && <p className="form-error">{fieldError}</p>}

          <div className="dialog__actions">
            <button type="button" className="btn btn--ghost" onClick={onClose} disabled={inviteMutation.isPending}>
              Cancel
            </button>
            <button
              type="submit"
              className="btn btn--primary"
              disabled={!canSubmit || inviteMutation.isPending}
            >
              {inviteMutation.isPending ? 'Sending…' : 'Send invite'}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}
