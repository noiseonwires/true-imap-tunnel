# Changelog

## v0.4.0

### Added

- IMAP consistency repair for providers that expose APPENDed messages to other sessions out of UID order:
  - New `fetch_uid_overlap` setting rechecks a trailing UID window after each normal fetch.
  - The repair scan skips messages already marked `\Deleted`, keeping the common path lightweight.
- Android status panel now shows per-account `connects=` counters for both the sender and the watcher, making silent IMAP reconnect storms visible at a glance.
- Throttle-aware backoff for constrained / picky IMAP servers:
  - New `throttle_backoff_ms` setting (default 0 / disabled). When the server returns an error containing a known rate-limit / quota / session-limit marker (`OVERQUOTA`, `TOOMANY`, `INUSE`, `LIMIT`, `[TRYAGAIN]`, `BYE`, `rate limit`, `throttle`, `unavailable`, `server busy`, etc.), the sender and watcher jump directly to this floor instead of climbing the standard exponential ramp (which is capped by `reconnect_max_delay_ms` and unsuitable as a true cool-down).
  - APPEND retry loop now short-circuits when the server returns a throttle marker, instead of burning through the remaining attempts.
  - Status JSON exposes `sender_throttled_for_ms` / `watcher_throttled_for_ms` while a cool-down is active; the Android status panel highlights throttled accounts with a `THROTTLED Xms remaining` line.
- New `constrained.example.yaml` profile bundling the known-good settings for picky free-tier IMAP servers (Yandex, Mail.ru, similar): `disable_idle: true`, slower polling, longer reconnect backoff, smaller batches, more aggressive EXPUNGE, and `throttle_backoff_ms: 300000`.

### Changed

- CI now publishes a signed debug Android plugin APK instead of renaming the unsigned release APK, using a stable checked-in debug keystore so downloaded CI artifacts can be sideloaded and updated directly.
- CI now derives the build version from release tags (or a workflow-dispatch override) and passes it to both Go binaries and Android plugin metadata/native builds.
- Watchers now preserve their UID cursor across reconnects when `UIDVALIDITY` is unchanged, so messages that arrive during a reconnect are not skipped.
- IDLE behavior is documented more clearly: even in IDLE mode, receivers periodically run safety fetches so provider IDLE limits or missed notifications do not stall the tunnel indefinitely.
- IDLE wait now has a 10s deadline on `DONE`/tagged-response, so a wedged IMAP IDLE (observed on Yandex under concurrent same-account load) forces a reconnect instead of stalling the receive loop indefinitely.
- Sender's initial `connect()` failure inside `Run()` is now logged at WARN (was silent until DEBUG). Authentication failures, TLS handshake errors, and similar startup problems now appear at the default `info` log level.
- Best-effort `EXPUNGE` flush on watcher shutdown is now logged at WARN instead of DEBUG when it fails.

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
