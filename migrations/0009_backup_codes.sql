-- Phase 14: recovery backup codes. When a user enables MFA they may also
-- generate a set of single-use backup codes for logging in when the
-- authenticator is unavailable. Only hashes are stored; the raw codes are shown
-- once at generation.
BEGIN;

ALTER TABLE users
    ADD COLUMN backup_codes text[] NOT NULL DEFAULT '{}';

COMMIT;
