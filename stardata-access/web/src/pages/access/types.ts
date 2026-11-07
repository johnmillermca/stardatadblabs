// =============================================================================
// Shared types for all 12 StarData access console screens
// Matches the API contract defined in Step 5 of the technical guide
// =============================================================================

export type Component =
  | 'doris'
  | 'kafka'
  | 'memgraph'
  | 'flink'
  | 'sqlmesh'
  | 'etl'
  | 'mdm'
  | 'monitoring';

export type Levels = Partial<Record<Component, string>>;
// e.g. { doris: 'analyst', kafka: 'viewer' }

export type PlatformRole = 'member' | 'admin' | 'owner';

export type MemberState = 'waiting' | 'active' | 'deactivated';

export interface Member {
  id: string;
  name: string;
  email: string;
  state: MemberState;
  emailVerified: boolean;
  personalDomain: boolean;    // true = email not @stardatadb.com; show warning
  createdAt: string;          // ISO8601
  platform: PlatformRole;
  levels: Levels;
  lastSignIn?: string;        // ISO8601 or undefined
  mfa?: boolean;
}

export interface MemberDetail extends Member {
  zitadelUserId: string;
  memberSince: string;
  sessions: Session[];
  history: AuditRow[];        // last 10 rows from Doris
  account: AccountInfo;
}

export interface Session {
  id: string;
  userAgent: string;
  ip: string;
  createdAt: string;
  method: string;             // e.g. "password + passkey", "device flow"
}

export interface AccountInfo {
  emailVerified: boolean;
  mfaMethods: string[];       // e.g. ["Passkey (MacBook)", "authenticator app"]
  passwordChangedAt?: string;
  lastSignIn?: string;
  failedSignInsLast30Days: number;
}

export interface RoleCatalogueItem {
  component: Component | 'platform';
  prefix: string;             // e.g. "doris.*"
  levels: string[];           // e.g. ["viewer","analyst","engineer","admin"]
  claim: string;              // e.g. "doris_role"
  allows: string;             // human description
  counts: number[];           // people per level, same order as levels
}

export interface AppIdentity {
  userId: string;             // ZITADEL machine user ID
  name: string;
  description?: string;
  dataRoles: string[];        // e.g. ["doris.analyst"]
  credentialType: 'client_secret' | 'jwt_key' | 'client_cert' | 'app_password';
  status: 'ok' | 'expiring_soon' | 'expiring_critical' | 'revoked';
  expiresAt?: string;
  rotatedAt?: string;
  ownerName: string;
  ownerUserId: string;
}

export interface AppCreatedResult {
  userId: string;
  name: string;
  clientId?: string;
  clientSecret?: string;
  certificate?: string;
  privateKey?: string;
  appPassword?: string;
  tokenUrl: string;
  rolesGranted: string[];
  ownerName: string;
  expiresAt: string;
}

export type ApprovalState = 'pending' | 'approved' | 'rejected' | 'expired';
export type ApprovalAction = 'set_platform_role' | 'remove_platform_role';

export interface Approval {
  id: string;                 // e.g. "A-1029"
  action: ApprovalAction;
  targetUserId: string;
  targetUserName: string;
  payload: {
    from: string[];
    to: string[];
  };
  reason?: string;
  requestedBy: string;        // ZITADEL user ID
  requestedByName: string;
  requestedAt: string;
  expiresAt: string;
  state: ApprovalState;
  decidedBy?: string;
  decidedByName?: string;
  decidedAt?: string;
  checks?: {
    mfaEnrolled: boolean;
    signedInWithin30Days: boolean;
  };
}

export interface AuditRow {
  eventId: string;
  ts: string;
  source: 'zitadel' | 'console';
  actorId: string;
  actorName: string;
  targetType?: string;
  targetId?: string;
  targetName?: string;
  component?: string;
  action: string;
  before?: unknown;
  after?: unknown;
  result: 'ok' | 'denied' | 'waiting' | 'blocked';
  reason?: string;
  ip?: string;
  userAgent?: string;
  requestId?: string;
  traceId?: string;
}

export interface PolicySettings {
  selfSignUp: boolean;
  emailVerification: string;
  mfa: string;
  passkeys: string;
  password: string;
  lockout: string;
  sessionIdle: string;
  sessionMax: string;
  accessToken: string;
  allowedEmailDomains: string[];
  branding: string;
  smtp: string;
}

export interface PolicyStatus {
  zitadelPods: { ready: number; total: number };
  loginPods: { ready: number; total: number };
  dbInstances: { ready: number; total: number };
  driftStatus: 'clean' | 'drifted' | 'unknown';
  driftCheckedAt: string;
  signingKeyRotatedAt: string;
  signingKeyNextRotation: string;
  settings: PolicySettings;
}

export interface SummaryBadges {
  waitingCount: number;
  pendingApprovalsCount: number;
}

export interface MeResponse {
  sub: string;
  name: string;
  email: string;
  status: 'active' | 'waiting' | 'deactivated';
  platformRoles: PlatformRole[];
  roles: Levels;
  mfa: boolean;
  sessionExpiresAt: string;
  csrf: string;
}

// ── API response shapes ───────────────────────────────────────────────────────

export interface PagedResponse<T> {
  items: T[];
  next: string | null;   // cursor for next page; null = last page
}

export interface ApproveResult {
  state: 'active' | 'pending_approval';
  approvalId?: string;
}

export interface InvitePayload {
  email: string;
  firstName: string;
  lastName: string;
  platform: PlatformRole;
  levels: Levels;
  message?: string;
}

export interface LevelsPayload {
  platform: PlatformRole;
  levels: Levels;
}

export interface ApprovalsDecision {
  decision: 'approve' | 'reject';
  comment?: string;
}
