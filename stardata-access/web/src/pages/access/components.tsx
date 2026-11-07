// =============================================================================
// Shared components — StarData access console
// Build these first; all 10 screens compose from them.
// =============================================================================
import React, { createContext, useContext, useEffect, useRef, useState } from 'react';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { Link, Outlet, useNavigate } from 'react-router-dom';
import type { Component, Levels, MeResponse, PlatformRole, RoleCatalogueItem, SummaryBadges } from './types';

// ── API helpers ───────────────────────────────────────────────────────────────

/** Fetch with automatic 401 redirect and CSRF header injection. */
export async function apiFetch(input: string, init?: RequestInit): Promise<Response> {
  const csrfToken = document.cookie
    .split('; ')
    .find(row => row.startsWith('csrf='))
    ?.split('=')[1];

  const method = init?.method?.toUpperCase() ?? 'GET';
  const headers = new Headers(init?.headers);
  if (['POST', 'PUT', 'DELETE', 'PATCH'].includes(method) && csrfToken) {
    headers.set('X-CSRF-Token', csrfToken);
  }

  const res = await fetch(input, { ...init, headers, credentials: 'include' });
  if (res.status === 401) {
    window.location.href = `/auth/login?return=${encodeURIComponent(window.location.pathname)}`;
    throw new Error('unauthenticated');
  }
  return res;
}

// ── MeContext — current user from GET /api/me ─────────────────────────────────

interface MeCtx { me: MeResponse | null; isLoading: boolean }
const MeContext = createContext<MeCtx>({ me: null, isLoading: true });
export const useMe = () => useContext(MeContext);

// ── AccessLayout ─────────────────────────────────────────────────────────────
// Topbar + sidebar group "ACCESS (ZITADEL)" with live badges.
// Badges: waiting count (Members), pending approvals count (Approvals).
// Refetched every 60 s.

export function AccessLayout() {
  const { data: me, isLoading: meLoading } = useQuery<MeResponse>({
    queryKey: ['me'],
    queryFn: () => apiFetch('/api/me').then(r => r.json()),
    staleTime: 30_000,
  });

  const { data: summary } = useQuery<SummaryBadges>({
    queryKey: ['access-summary'],
    queryFn: () => apiFetch('/api/access/summary').then(r => r.json()),
    refetchInterval: 60_000,
    enabled: !!(me?.platformRoles?.length),
  });

  const isOwner = me?.platformRoles?.includes('owner');
  const isAdmin = me?.platformRoles?.includes('admin');
  const canAccess = isOwner || isAdmin;

  return (
    <MeContext.Provider value={{ me: me ?? null, isLoading: meLoading }}>
      <div className="access-layout">
        <nav className="sidebar-group">
          <span className="sidebar-group-label">ACCESS (ZITADEL)</span>
          {canAccess && (
            <ul>
              <li>
                <Link to="/access/members">
                  Members {summary?.waitingCount ? <Badge count={summary.waitingCount} /> : null}
                </Link>
              </li>
              <li><Link to="/access/apps">Applications</Link></li>
              <li><Link to="/access/roles">Roles</Link></li>
              <li>
                <Link to="/access/approvals">
                  Approvals {summary?.pendingApprovalsCount ? <Badge count={summary.pendingApprovalsCount} /> : null}
                </Link>
              </li>
              {isOwner && <li><Link to="/access/policy">Sign-in policy</Link></li>}
              <li><Link to="/access/audit">Audit</Link></li>
            </ul>
          )}
        </nav>
        <main className="access-main">
          <Outlet />
        </main>
      </div>
    </MeContext.Provider>
  );
}

// ── RequireRole ───────────────────────────────────────────────────────────────
// Route guard — shows 403 page when the person lacks the required platform role.

interface RequireRoleProps {
  roles: PlatformRole[];
  children: React.ReactNode;
}

export function RequireRole({ roles, children }: RequireRoleProps) {
  const { me, isLoading } = useMe();
  if (isLoading) return <LoadingSkeleton />;
  if (!me) return null;
  if (me.status === 'waiting') {
    window.location.replace('/waiting');
    return null;
  }
  const has = roles.some(r => me.platformRoles?.includes(r));
  if (!has) {
    const needed = roles.join(' or ');
    return (
      <div className="access-forbidden">
        <h2>Access restricted</h2>
        <p>This page requires the <strong>{needed}</strong> role.</p>
      </div>
    );
  }
  return <>{children}</>;
}

// ── LevelPicker ──────────────────────────────────────────────────────────────
// Segmented control for one component: None + its ordered levels.
// Levels are loaded from GET /api/access/roles — never hard-coded.

interface LevelPickerProps {
  component: Component | 'platform';
  levels: string[];
  value: string | null;
  onChange: (level: string | null) => void;
  disabled?: boolean;
  disabledLevels?: string[];   // levels to hide for app identities
}

export function LevelPicker({ component, levels, value, onChange, disabled, disabledLevels = [] }: LevelPickerProps) {
  const options = ['none', ...levels.filter(l => !disabledLevels.includes(l))];
  return (
    <div className={`level-picker${disabled ? ' level-picker--disabled' : ''}`} role="group" aria-label={component}>
      <span className="level-picker__label">{componentLabel(component)}</span>
      <div className="level-picker__options">
        {options.map(opt => (
          <button
            key={opt}
            type="button"
            className={`level-picker__option${value === opt || (opt === 'none' && !value) ? ' level-picker__option--active' : ''}`}
            onClick={() => !disabled && onChange(opt === 'none' ? null : opt)}
            disabled={disabled}
            aria-pressed={value === opt || (opt === 'none' && !value)}
          >
            {opt}
          </button>
        ))}
      </div>
    </div>
  );
}

// ── LevelGrid ─────────────────────────────────────────────────────────────────
// All LevelPickers for one person/app, plus Platform picker.

interface LevelGridProps {
  catalogue: RoleCatalogueItem[];
  levels: Levels;
  platform: PlatformRole;
  onChange: (levels: Levels, platform: PlatformRole) => void;
  disableEngineerAdmin?: boolean;   // for app identities
  readOnly?: boolean;
}

export function LevelGrid({ catalogue, levels, platform, onChange, disableEngineerAdmin = false, readOnly = false }: LevelGridProps) {
  const [platformNote, setPlatformNote] = useState(false);

  const handleLevelChange = (comp: Component, level: string | null) => {
    onChange({ ...levels, [comp]: level ?? undefined }, platform);
  };

  const handlePlatformChange = (p: string | null) => {
    const next = (p ?? 'member') as PlatformRole;
    setPlatformNote(next === 'admin' || next === 'owner');
    onChange(levels, next);
  };

  return (
    <div className="level-grid">
      {catalogue
        .filter(item => item.component !== 'platform')
        .map(item => (
          <LevelPicker
            key={item.component}
            component={item.component as Component}
            levels={item.levels}
            value={levels[item.component as Component] ?? null}
            onChange={lv => handleLevelChange(item.component as Component, lv)}
            disabled={readOnly}
            disabledLevels={disableEngineerAdmin ? ['engineer', 'admin'] : []}
          />
        ))}
      <LevelPicker
        component="platform"
        levels={['member', 'admin', 'owner']}
        value={platform}
        onChange={handlePlatformChange}
        disabled={readOnly}
      />
      {platformNote && (
        <p className="level-grid__note">Making someone Admin or Owner needs a second Owner to approve.</p>
      )}
    </div>
  );
}

// ── ClaimsPreview ─────────────────────────────────────────────────────────────
// Pure function: shows the JWT claims the chosen levels will produce.

interface ClaimsPreviewProps {
  levels: Levels;
  platform: PlatformRole;
}

export function ClaimsPreview({ levels, platform }: ClaimsPreviewProps) {
  const claims: string[] = [];
  (Object.entries(levels) as [Component, string | undefined][]).forEach(([comp, lv]) => {
    if (lv) claims.push(`${comp}_role=${lv}`);
  });
  const platformRoles = platform === 'member' ? '[]' : `["platform.${platform}"]`;
  claims.push(`platform_roles=${platformRoles}`);

  return (
    <p className="claims-preview">
      <span className="claims-preview__label">Token claims after saving: </span>
      {claims.map((c, i) => (
        <span key={c}>
          <code className="claims-preview__claim">{c}</code>
          {i < claims.length - 1 && ' · '}
        </span>
      ))}
    </p>
  );
}

// ── ConfirmDialog ─────────────────────────────────────────────────────────────
// Destructive action confirm: reason required; for revoke, must type the name.

interface ConfirmDialogProps {
  title: string;
  description: string;
  requireName?: string;      // if set, user must type this value to confirm
  onConfirm: (reason: string) => void;
  onCancel: () => void;
  isSubmitting?: boolean;
}

export function ConfirmDialog({ title, description, requireName, onConfirm, onCancel, isSubmitting }: ConfirmDialogProps) {
  const [reason, setReason] = useState('');
  const [nameInput, setNameInput] = useState('');
  const canConfirm = reason.trim().length > 0 && (!requireName || nameInput === requireName);

  return (
    <div className="dialog-overlay" role="dialog" aria-modal="true" aria-labelledby="confirm-title">
      <div className="dialog">
        <h2 id="confirm-title">{title}</h2>
        <p>{description}</p>
        {requireName && (
          <label className="dialog__field">
            <span>Type <strong>{requireName}</strong> to confirm</span>
            <input
              type="text"
              value={nameInput}
              onChange={e => setNameInput(e.target.value)}
              autoComplete="off"
              placeholder={requireName}
            />
          </label>
        )}
        <label className="dialog__field">
          <span>Reason <span aria-hidden="true">*</span></span>
          <textarea
            value={reason}
            onChange={e => setReason(e.target.value)}
            rows={3}
            placeholder="Required"
          />
        </label>
        <div className="dialog__actions">
          <button type="button" className="btn btn--ghost" onClick={onCancel} disabled={isSubmitting}>
            Cancel
          </button>
          <button
            type="button"
            className="btn btn--danger"
            onClick={() => onConfirm(reason)}
            disabled={!canConfirm || isSubmitting}
          >
            {isSubmitting ? 'Working…' : 'Confirm'}
          </button>
        </div>
      </div>
    </div>
  );
}

// ── DataTable ─────────────────────────────────────────────────────────────────
// Generic table with server-side cursor paging. Delegate sort/filter to callers.

interface Column<T> {
  key: string;
  header: string;
  render: (row: T) => React.ReactNode;
}

interface DataTableProps<T> {
  columns: Column<T>[];
  rows: T[];
  rowKey: (row: T) => string;
  nextCursor?: string | null;
  onLoadMore?: () => void;
  isLoading?: boolean;
  emptyMessage?: string;
}

export function DataTable<T>({ columns, rows, rowKey, nextCursor, onLoadMore, isLoading, emptyMessage = 'No results.' }: DataTableProps<T>) {
  if (isLoading && rows.length === 0) return <LoadingSkeleton />;
  if (!isLoading && rows.length === 0) return <p className="table-empty">{emptyMessage}</p>;

  return (
    <div className="data-table-wrapper">
      <table className="data-table">
        <thead>
          <tr>{columns.map(col => <th key={col.key}>{col.header}</th>)}</tr>
        </thead>
        <tbody>
          {rows.map(row => (
            <tr key={rowKey(row)}>
              {columns.map(col => <td key={col.key}>{col.render(row)}</td>)}
            </tr>
          ))}
        </tbody>
      </table>
      {nextCursor && onLoadMore && (
        <button type="button" className="btn btn--ghost" onClick={onLoadMore} disabled={isLoading}>
          Load more
        </button>
      )}
    </div>
  );
}

// ── Pill ──────────────────────────────────────────────────────────────────────

type PillVariant = 'green' | 'amber' | 'red' | 'grey' | 'blue';
interface PillProps { label: string; variant: PillVariant }

export function Pill({ label, variant }: PillProps) {
  return <span className={`pill pill--${variant}`}>{label}</span>;
}

export function statusPill(status: string) {
  const map: Record<string, PillVariant> = {
    active: 'green', ok: 'green',
    waiting: 'amber', expiring_soon: 'amber',
    expiring_critical: 'red', denied: 'red', blocked: 'red',
    deactivated: 'grey', revoked: 'grey', expired: 'grey',
  };
  return <Pill label={status} variant={map[status] ?? 'grey'} />;
}

// ── Drawer ────────────────────────────────────────────────────────────────────

interface DrawerProps {
  title: string;
  open: boolean;
  onClose: () => void;
  children: React.ReactNode;
}

export function Drawer({ title, open, onClose, children }: DrawerProps) {
  useEffect(() => {
    const handler = (e: KeyboardEvent) => e.key === 'Escape' && onClose();
    document.addEventListener('keydown', handler);
    return () => document.removeEventListener('keydown', handler);
  }, [onClose]);

  if (!open) return null;

  return (
    <>
      <div className="drawer-backdrop" onClick={onClose} aria-hidden="true" />
      <aside className="drawer" role="dialog" aria-modal="true" aria-labelledby="drawer-title">
        <div className="drawer__header">
          <h3 id="drawer-title">{title}</h3>
          <button type="button" className="drawer__close" onClick={onClose} aria-label="Close">✕</button>
        </div>
        <div className="drawer__body">{children}</div>
      </aside>
    </>
  );
}

// ── CopyField ─────────────────────────────────────────────────────────────────
// Never writes to localStorage or logs the value.

interface CopyFieldProps {
  label: string;
  value: string;
  masked?: boolean;   // show truncated display but copy full value
}

export function CopyField({ label, value, masked }: CopyFieldProps) {
  const [copied, setCopied] = useState(false);

  const handleCopy = () => {
    navigator.clipboard.writeText(value).then(() => {
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    });
  };

  const display = masked ? `${value.slice(0, 4)}...(${value.length} characters)...${value.slice(-4)}` : value;

  return (
    <div className="copy-field">
      <label className="copy-field__label">{label}</label>
      <div className="copy-field__row">
        <code className="copy-field__value">{display}</code>
        <button type="button" className="btn btn--ghost copy-field__btn" onClick={handleCopy}>
          {copied ? 'Copied!' : 'Copy'}
        </button>
      </div>
    </div>
  );
}

// ── Toast ─────────────────────────────────────────────────────────────────────

interface ToastMessage { id: string; message: string; variant: 'success' | 'error' | 'info' }
const ToastContext = createContext<{ show: (msg: string, variant?: ToastMessage['variant']) => void }>({
  show: () => {},
});
export const useToast = () => useContext(ToastContext);

export function ToastProvider({ children }: { children: React.ReactNode }) {
  const [toasts, setToasts] = useState<ToastMessage[]>([]);

  const show = (message: string, variant: ToastMessage['variant'] = 'success') => {
    const id = crypto.randomUUID();
    setToasts(t => [...t, { id, message, variant }]);
    setTimeout(() => setToasts(t => t.filter(x => x.id !== id)), 4000);
  };

  return (
    <ToastContext.Provider value={{ show }}>
      {children}
      <div className="toast-container" aria-live="polite">
        {toasts.map(t => (
          <div key={t.id} className={`toast toast--${t.variant}`}>{t.message}</div>
        ))}
      </div>
    </ToastContext.Provider>
  );
}

// ── Avatar initials ───────────────────────────────────────────────────────────

export function Avatar({ name }: { name: string }) {
  const initials = name.split(' ').map(w => w[0]).slice(0, 2).join('').toUpperCase();
  return <span className="avatar" aria-hidden="true">{initials}</span>;
}

// ── Badge ─────────────────────────────────────────────────────────────────────

export function Badge({ count }: { count: number }) {
  return <span className="badge">{count}</span>;
}

// ── Loading skeleton ──────────────────────────────────────────────────────────

export function LoadingSkeleton() {
  return (
    <div className="loading-skeleton" aria-busy="true" aria-label="Loading…">
      {[1, 2, 3].map(i => <div key={i} className="loading-skeleton__row" />)}
    </div>
  );
}

// ── Helpers ───────────────────────────────────────────────────────────────────

function componentLabel(comp: Component | 'platform'): string {
  const labels: Record<string, string> = {
    doris: 'Doris', kafka: 'Kafka', memgraph: 'Memgraph / StarGraph',
    flink: 'Flink', sqlmesh: 'SQLMesh', etl: 'ETL Studio',
    mdm: 'MDM', monitoring: 'Monitoring', platform: 'Platform',
  };
  return labels[comp] ?? comp;
}

export function formatRelative(iso: string): string {
  const diff = Date.now() - new Date(iso).getTime();
  const min = Math.floor(diff / 60_000);
  if (min < 1) return 'just now';
  if (min < 60) return `${min} min ago`;
  const h = Math.floor(min / 60);
  if (h < 24) return `${h}h ago`;
  const d = Math.floor(h / 24);
  return `${d} day${d !== 1 ? 's' : ''} ago`;
}

export function formatDate(iso: string): string {
  return new Date(iso).toLocaleDateString('en-GB', { day: 'numeric', month: 'short', year: 'numeric' });
}

export function formatCountdown(iso: string): string {
  const ms = new Date(iso).getTime() - Date.now();
  if (ms <= 0) return 'expired';
  const min = Math.ceil(ms / 60_000);
  if (min < 60) return `${min} min`;
  const h = Math.ceil(min / 60);
  return `${h}h`;
}
