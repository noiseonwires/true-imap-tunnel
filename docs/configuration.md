# Configuration and diagnostics

This page contains the longer configuration notes, examples, tuning hints, and
troubleshooting steps. The fully commented example file is
[`config.example.yaml`](../config.example.yaml).

## IMAP folders and client IDs

Create two folders for tunnel traffic, for example:

- `TunnelC2S` - client writes here, server reads here.
- `TunnelS2C` - server writes here, client reads here.

Better use your own folder names if you do not want them to look too obvious.

The program tries to create missing folders automatically, but it is safer to
create them manually in your mail UI/admin panel first. Use empty, dedicated
folders. Do not point the tunnel at your Inbox unless you enjoy chaos.

For one client and one server, `client_id` can stay unset/zero. If several
clients share the same server and the same IMAP folder pair, give every client a
unique `client_id` from `1` to `255`. The top byte of every `streamID` carries
that user/client ID; the server preserves it in replies, and each client only
fetches/deletes messages addressed to its own ID.

In Shadowsocks/SIP003 mode, the Shadowsocks profile port can also act as the
client ID override because Shadowsocks Android exposes it as `SS_REMOTE_PORT`.
The mapping is `port % 256`, with `0` mapped to `255`.

## Standalone usage

Example: expose a remote SOCKS5 proxy running on the server host.

1. On the server host, run Dante/Xray/any TCP service locally, e.g.
   `127.0.0.1:1080`.
2. Run T.I.T.(S.) server mode on that host with `target: "127.0.0.1:1080"`.
3. Run T.I.T.(S.) client mode on your device/laptop with
   `listen: "127.0.0.1:1080"`.
4. Configure your app/browser to use local `127.0.0.1:1080`.

The same pattern works for VLESS/Xray-core (`target` points to the Xray inbound),
SSH (`target: "127.0.0.1:22"`), or basically any TCP endpoint that will sit
still long enough to be tunneled.

## Client YAML example

```yaml
mode: client
listen: "127.0.0.1:1080"
client_id: 1

accounts:
  - name: "primary"
    host: "imap.example.com:993"
    username: "tunnel@example.com"
    password: "app-password"
    tls: "implicit"
    folder_send: "TunnelC2S"
    folder_recv: "TunnelS2C"

encryption_passphrase: "same secret on both sides"
async_data_send: true
batch_delay_ms: 2
ping_interval_ms: 0
```

Run:

```powershell
.\bin\true-imap-tunnel.exe -config .\client.yaml
```

## Server YAML example

```yaml
mode: server
target: "127.0.0.1:1080"

accounts:
  - name: "primary"
    host: "imap.example.com:993"
    username: "tunnel@example.com"
    password: "app-password"
    tls: "implicit"
    folder_send: "TunnelS2C"
    folder_recv: "TunnelC2S"

encryption_passphrase: "same secret on both sides"
async_data_send: true
batch_delay_ms: 2
```

Run:

```powershell
.\bin\true-imap-tunnel.exe -config .\server.yaml
```

## Common options

These options can be used on both sides unless noted otherwise.

```yaml
# Optional fallback. If omitted and local DNS fails at startup, 1.1.1.1:53 is used.
dns_servers:
  - "1.1.1.1"

# Encrypt frames stored in IMAP. Empty/unset disables this extra layer.
encryption_passphrase: "same secret on both sides"

# Receiver behavior.
reorder: true
poll_interval_ms: 3000 # this is for servers that don't support IDLE
active_poll_interval_ms: 100
active_poll_duration_ms: 5000

lazy_expunge_threshold: 16
lazy_expunge_max_age_ms: 30000

# Sender batching.
batch_delay_ms: 2        # 0 disables; 1-5 ms is usually good
batch_max_frames: 64
batch_max_kb: 256
batch_queue_size: 256
async_data_send: true

# Optional startup RTT probe. 0 = probe once; >0 repeats every N ms; <0 disables.
ping_interval_ms: 0

# Experimental. May reduce startup latency, but can also cause funny issues.
# zero_rtt_open: true
```

Authentication can use either `password` or OAuth2/XOAUTH2 fields such as
`oauth2_token_command`. Gmail and Microsoft 365 usually require XOAUTH2 because
plain IMAP passwords are disabled there.

## Things worth tuning

- `batch_delay_ms: 2` is the default and a good latency/efficiency compromise.
  Use `0` for minimum per-stream latency or `5-10` only when throughput matters
  more than first-byte latency.
  > **Worth testing on your own setup.** The "best" value is highly dependent
  > on the tunneled protocol (interactive SSH vs bulk HTTPS vs Shadowsocks)
  > and the IMAP server's APPEND/FETCH characteristics. Try `0` and then
  > sweep `5`, `10`, `20` — observed throughput *and* stability (reconnect
  > frequency, stalled streams, server-side throttling) can differ
  > dramatically. Pick the value that stays stable for your workload.
- `async_data_send: true` helps interactive proxy workloads because TCP DATA
  frames do not wait synchronously for every IMAP APPEND.
- `inbound_queue_wait_ms: 30000` is the default per-stream TCP-write
  backpressure window. If the target socket temporarily stops accepting upload
  data, a per-stream overflow drainer waits for queue space instead of blocking
  the shared IMAP watcher or immediately resetting the stream. Lower it only if
  you prefer fast failure over surviving upload stalls.
- If you tunnel Shadowsocks through T.I.T.(S.), you can usually leave
  `encryption_passphrase` empty. Shadowsocks traffic is already encrypted, and
  double-encrypting every tunnel frame only adds CPU work and bytes of overhead.
- `zero_rtt_open: true` saves one tunnel round trip for new streams, but it is
  opt-in. Early DATA may need to be buffered or discarded if the server-side
  target dial is late or fails, so use it only after testing the actual protocol
  you plan to tunnel.
- `poll_interval_ms` matters only when IDLE is missing or during safety fetches.
  Polling is inherently slower and may feel unreliable compared with IDLE.
- `lazy_expunge_threshold: 1` forces immediate cleanup if your IMAP server has
  strict folder quotas, at the cost of extra IMAP round trips.

## Customizing how messages look in the mailbox

Two config knobs let you change how each tunnel draft appears to anyone (or
anything) browsing the IMAP folder in a normal mail client:

- `message_format` (`attachment` — default — or `text`) selects how the frame
  payload is carried inside the message body. `attachment` produces a binary
  MIME part (paperclip icon, "Open attachment" UI); `text` puts the same
  base64 directly into a `text/plain` body, so the draft renders as an
  ordinary email with a base64 blob in it and no attachment indicator.
- `attachment_filename` (default `tunnel.bin`) is the filename hint embedded
  in `Content-Disposition` when `message_format: attachment`. Change it to
  something innocuous (`notes.txt`, `image.dat`, etc.) if `tunnel.bin` is
  too on the nose for your threat model.

The two endpoints do NOT have to agree on `message_format` — the decoder
accepts either. `attachment` mode is slightly more robust against IMAP servers
that splice unrelated text (e.g. "external sender" banners) into the body,
because such text becomes a separate MIME part instead of mixing with the
base64. See [`../config.example.yaml`](../config.example.yaml) for the full
commentary.

## Multipath

Multipath is enabled by listing multiple IMAP accounts. Each account provides an
independent transport path with its own send and receive folders.

Default mode:

```yaml
multipath_mode: stream_affinity
```

`stream_affinity` pins each TCP stream to one account for its lifetime.
Different streams are distributed across accounts. This is the safest mode and
usually the best choice.

Experimental mode:

```yaml
multipath_mode: frame_round_robin
```

`frame_round_robin` can spread DATA frames from one stream across multiple
connected/proven accounts. This may improve bandwidth for one heavy stream, but
it is experimental and may be unstable: more reordering, jitter, and edge cases
are expected. Use it only on both sides and only after validating your workload.

When using multipath:

- Keep `reorder: true`.
- Use matching account lists/folders on both sides.
- Start with `stream_affinity`.
- Watch logs for per-account RTT and reconnect churn.

## Diagnostics

If the tunnel does not work, start with the boring stuff. It is almost always
the boring stuff.

1. Run both sides with debug logs:

   ```powershell
   .\bin\true-imap-tunnel.exe -config .\client.yaml -v
   .\bin\true-imap-tunnel.exe -config .\server.yaml -v
   ```

   Use `-v -v` only for short sessions; trace logs are very noisy.

2. Check IMAP login and folders. The client `folder_send` must equal the server
   `folder_recv`, and the server `folder_send` must equal the client
   `folder_recv`.
3. Confirm the server can dial `target` locally. Test Dante/Xray/SSH directly on
   the server host first.
4. Check whether the IMAP server supports IDLE. If it does not, the tunnel uses
   polling, which is slower and less reliable.
5. If you use `encryption_passphrase`, verify it is identical on both sides.
   Mismatches show decrypt/authentication errors and frames are dropped.
6. If hostname resolution fails, set `dns_servers: ["1.1.1.1"]` or
   `dns_server=1.1.1.1` in SIP003 mode.
7. If multiple clients share folders, verify every client has a unique
   `client_id`.
8. Use `ping_interval_ms: 5000` temporarily to keep RTT probes flowing. The
   server logs client build/version from Ping payloads, useful for detecting old
   Android plugin builds.
9. For Shadowsocks Android/plugin problems, collect `adb logcat` output. The
   exact commands are in [`../android-plugin/README.md`](../android-plugin/README.md).
