# Security Model

How SyncForge isolates tenants, authenticates callers, and authorizes actions.

## Tenant isolation (PostgreSQL Row-Level Security)

Two Postgres roles:

- **`syncforge_app`** — subject to `FORCE ROW LEVEL SECURITY` on every
  tenant-scoped table. Even the table owner cannot bypass isolation. Every
  query must run inside a tenant context (`db.WithTenant` sets the GUC
  `app.tenant_id` for the transaction). Reads with no tenant context return
  zero rows (fail-closed).
- **`syncforge_engine`** — `BYPASSRLS`, used for cross-tenant administration
  (tenant management, key verification, internal workers). Worker tenant-scoped
  operations still go through `WithTenant`, so RLS remains active for them.

Tenant identity is never passed by the client as a parameter — it is derived
from the authenticated principal (API key or session token) and injected into
the transaction context, so a caller cannot read another tenant's rows.

## Authentication

Two credential types, both role-bearing:

1. **API keys** — `api_keys` stores only the SHA-256 hash; the raw key is shown
   once at creation. Verified via the admin pool before a tenant context exists.
2. **User sessions** — `users` stores bcrypt-hashed passwords. Login records a
   server-side `sessions` row (keyed by a `jti`) and returns an HMAC-signed
   token carrying `tenant_id`, `role`, `jti`. Verification checks signature,
   expiry, **and** that a live (unrevoked) session row exists — so logout and
   rotation take effect immediately.

### Multi-factor

- **TOTP** (RFC 6238, dependency-free `internal/totp`) — login requires a
  6-digit code when a user has MFA enabled.
- **Backup codes** — single-use, SHA-256-hashed at rest, consumed atomically on
  login (replay impossible).

### OIDC SSO

`POST /api/v1/auth/oidc/login` verifies an external ID token against the
issuer's JWKS (RS256, issuer/audience/expiry) before resolving or
auto-provisioning the tenant user, then issues a normal SyncForge session.

## Authorization (RBAC)

Roles: `VIEWER` < `DEVELOPER` < `OPERATOR` < `ADMIN`.

`requireRole(min)` — layered on top of `authenticate` (API key or user session)
— rejects any caller whose rank is below the endpoint's requirement (403).

| Role | Capabilities |
|---|---|
| VIEWER | read-only (list/get) across all surfaces |
| DEVELOPER | + create connections, create/run sync jobs, create reconciliations |
| OPERATOR | + DLQ retry/discard, conflict resolve/dismiss, finding apply/dismiss |
| ADMIN | + tenant management, API-key management, user management, sessions |

Additional guards:

- A key cannot mint another above its own rank; a key cannot revoke itself.
- An ADMIN cannot grant a role above their own.
- Password changes/resets revoke all of the target user's sessions.

## Brute-force protection

- **Account lockout**: `login_attempts` records failures; after N failures in
  a window, further attempts are rejected (429) even with a correct password,
  reset on successful login.
- **Per-IP throttle**: a best-effort Redis fixed-window limit on login rate.

## Audit

`audit_log` records every operator/security action (logins, key/user
management, conflict/finding decisions, DLQ actions, MFA enrollment) with the
acting identity. `sync_operations` is a per-write ledger of every destination
mutation.
