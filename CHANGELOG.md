# Changelog

## v0.4.0

### Added

- Added `fetch_uid_overlap` to repair delayed/out-of-order IMAP visibility without reprocessing messages already marked `\Deleted`.
- Added throttle-aware backoff via `throttle_backoff_ms`, plus `constrained.example.yaml` for picky IMAP providers.
- Android/status diagnostics now show sender/watcher connect counts and active throttle cool-downs.

### Changed

- Watchers preserve UID cursors across reconnects, and wedged IDLE `DONE` waits now force reconnect instead of stalling receive forever.
- IMAP connect, throttle, and shutdown-EXPUNGE problems are now visible at normal log levels.
- CI now produces sideloadable signed debug Android APKs and stamps build metadata from release tags or workflow overrides.

## v0.3.1

### Added

- Configurable draft address headers:
  - Per-account `message_from` overrides the generated From header.
  - `message_to` can be fixed or contain `{random}` for a fresh random-looking To header per draft.

## v0.3.0

### Added

- Configurable draft subjects:
  - `message_subject` replaces the previously hardcoded `TIT` subject.
  - `message_subject_mode: random` can generate a fresh random subject for every draft.
- Optional subject-level client routing:
  - `subject_client_id: true` prefixes drafts with a compact two-hex-digit client ID token.
  - Multi-client receivers can skip messages for other clients after fetching headers only, instead of downloading and decoding every message body.
- Multi-client encrypted deployments no longer need to share one frame-encryption secret across every client. Introduce server-side per-client encryption keys:
  - `client_encryption_passphrases` maps client IDs to dedicated passphrases. Each client can keep only its own `encryption_passphrase`; the server chooses the matching key by `client_id`. The old single `encryption_passphrase` remains supported as a legacy/default fallback.
- Decrypt key selection optimizations:
  - Subject client-ID hints are used to try the expected per-client key first. Without a hint, recently successful client keys are tried before scanning the remaining configured keys.
- Better decrypt-failure logs:
  - When a subject provided a client-ID hint, decrypt warnings now include `client_id_hint=<id>`.

### Optimizations and stability

- Receivers can now do a lightweight `FETCH ENVELOPE` pass first, use the Subject client-ID token to skip unrelated messages, and only fetch full bodies for likely-owned messages.

### Security notes

- Shared IMAP credentials still allow denial-of-service behavior such as deleting, hiding, or flooding mailbox messages. This is a by-design limitation of the IMAP shared-account tunneling itself.

## v0.2.0

### Added

- OAuth2 / XOAUTH2 IMAP authentication:
  - Added `docs/oauth.md` with provider-helper guidance.
  - Added an Outlook.com / Microsoft 365 token helper at `tools/outlook_oauth2_token.py`.
- Android plugin diagnostics UI:
  - The Shadowsocks Android plugin APK now includes a tiny launcher app - opening **T.I.T.(s.)** while the SS profile is running shows tunnel/account status and recent logs.
- Local diagnostics HTTP API:
  - `status_addr` enables a local status/log endpoint for standalone or plugin use.
- IMAP/provider compatibility controls:
  - `disable_idle` forces NOOP+FETCH polling even when the server advertises IDLE.
  - `startup_cleanup_connection` controls whether stale-message cleanup uses a dedicated connection, the watcher connection, or fallback mode.

### Changed

- Polling fallback now sends NOOP before FETCH to refresh mailbox state.
- Improved startup stale-message cleanup so large old folders are handled before normal receive processing, with a fallback path for providers that reject concurrent IMAP sessions. Startup cleanup defaults to `fallback`, so providers that reject extra IMAP connections can still clean up old messages via the main watcher connection.
- Frame round-robin multipath is documented as experimental and potentially unstable when account latency or server performance differs.


## v0.1.1

Baseline release for this changelog. See `v0.2.0` and `v0.3.0` for user-visible changes made after it.
