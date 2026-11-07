// =============================================================================
// Screen 8 — Application created  (dialog)
// Canvas: client ID, secret (or cert/key/app-password), Copy buttons,
//         Download as .env, "shown once" warning, "I have stored the secret" checkbox.
// NEVER puts the secret into TanStack Query cache — lives in component state only.
// Closing asks "Have you saved the secret?" when checkbox not checked.
// =============================================================================
import React, { useRef, useState } from 'react';
import { CopyField } from './components';
import type { AppCreatedResult } from './types';

interface AppCreatedDialogProps {
  result: AppCreatedResult;
  onClose: () => void;
}

export default function AppCreatedDialog({ result, onClose }: AppCreatedDialogProps) {
  const [confirmed, setConfirmed] = useState(false);

  const handleClose = () => {
    if (!confirmed) {
      if (!window.confirm('Have you saved the secret? It cannot be shown again.')) {
        return;
      }
    }
    onClose();
  };

  const buildEnvContent = (): string => {
    const lines: string[] = [
      `# StarData application: ${result.name}`,
      `# Created: ${new Date().toISOString()}`,
      `# Roles: ${result.rolesGranted.join(', ')}`,
      `# Owner: ${result.ownerName}`,
      `# Expires: ${result.expiresAt}`,
      '',
      `ZITADEL_TOKEN_URL=${result.tokenUrl}`,
    ];
    if (result.clientId)     lines.push(`ZITADEL_CLIENT_ID=${result.clientId}`);
    if (result.clientSecret) lines.push(`ZITADEL_CLIENT_SECRET=${result.clientSecret}`);
    if (result.appPassword)  lines.push(`APP_PASSWORD=${result.appPassword}`);
    if (result.certificate)  lines.push(`# certificate: (see downloaded .pem)`);
    return lines.join('\n');
  };

  const handleDownloadEnv = () => {
    const blob = new Blob([buildEnvContent()], { type: 'text/plain' });
    const a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = `${result.name}.env`;
    a.click();
    URL.revokeObjectURL(a.href);
  };

  return (
    <div className="dialog-overlay" role="dialog" aria-modal="true" aria-labelledby="app-created-title">
      <div className="dialog dialog--wide">
        <h2 id="app-created-title">{result.name} is ready</h2>
        <p className="dialog__warning">
          Copy the secret now. It is shown once and cannot be shown again; use Rotate if it is lost.
        </p>

        {result.clientId && (
          <CopyField label="Client ID" value={result.clientId} />
        )}
        {result.clientSecret && (
          <CopyField label="Client secret" value={result.clientSecret} masked />
        )}
        {result.appPassword && (
          <CopyField label="App password" value={result.appPassword} masked />
        )}
        {result.certificate && (
          <CopyField label="Certificate" value={result.certificate} masked />
        )}
        {result.privateKey && (
          <CopyField label="Private key" value={result.privateKey} masked />
        )}
        <CopyField label="Token URL" value={result.tokenUrl} />

        <p className="dialog__meta">
          Roles granted: {result.rolesGranted.join(' · ')} · Owner: {result.ownerName} · Secret expires {result.expiresAt}
        </p>

        <label className="checkbox-label dialog__confirm-checkbox">
          <input
            type="checkbox"
            checked={confirmed}
            onChange={e => setConfirmed(e.target.checked)}
          />
          I have stored the secret in our secrets manager
        </label>

        <div className="dialog__actions">
          <button type="button" className="btn btn--ghost" onClick={handleDownloadEnv}>
            Download as .env
          </button>
          <button
            type="button"
            className="btn btn--primary"
            onClick={handleClose}
          >
            Done
          </button>
        </div>
      </div>
    </div>
  );
}
