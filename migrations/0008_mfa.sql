-- Phase 13: multi-factor authentication. Users may enroll a TOTP secret
-- (RFC 6238); when enabled, login additionally requires a time-based 6-digit
-- code. The secret is stored base32-encoded and nullable until enrolled.
BEGIN;

ALTER TABLE users
    ADD COLUMN totp_secret  text,
    ADD COLUMN totp_enabled boolean NOT NULL DEFAULT false;

COMMIT;
