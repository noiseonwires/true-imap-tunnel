#!/usr/bin/env bash
# Smoke test: python web server -> true-imap-tunnel (server + client) -> parallel curl downloads.
#
# Generates client/server configs from env vars unless CONFIG_DIR (with client.yaml + server.yaml) is set:
#   TITS_IMAP_HOST  TITS_IMAP_PORT (993)  TITS_IMAP_USER  TITS_IMAP_PASS
#   TITS_IMAP_TLS (implicit)  TITS_ENC_PASS (smoke-test)
# Optional: TITS_BIN <prebuilt tunnel binary>  WEB_PORT  LISTEN_PORT  CONFIG_DIR
# When CONFIG_DIR is used, the server config's target must be 127.0.0.1:$WEB_PORT (this script's web server).

set -euo pipefail

WEB_PORT="${WEB_PORT:-18080}"
LISTEN_PORT="${LISTEN_PORT:-12222}"
CONFIG_DIR="${CONFIG_DIR:-}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
WORK="$(mktemp -d)"
PIDS=()

cleanup() {
  for pid in "${PIDS[@]:-}"; do kill "$pid" 2>/dev/null || true; done
  rm -rf "$WORK"
}
trap cleanup EXIT

fail() { echo "FAIL: $*" >&2; exit 1; }

sha() {
  if command -v sha256sum >/dev/null 2>&1; then sha256sum "$1" | awk '{print $1}';
  else shasum -a 256 "$1" | awk '{print $1}'; fi
}

PY=python3
command -v "$PY" >/dev/null 2>&1 || PY=python
command -v "$PY" >/dev/null 2>&1 || fail "python not found"

FILES="f1.bin:16384 f2.bin:65536 f3.bin:131072 f4.bin:262144 f5.bin:33554432"

mkdir -p "$WORK/www"
for spec in $FILES; do
  name="${spec%%:*}"; size="${spec##*:}"
  head -c "$size" /dev/urandom > "$WORK/www/$name"
  sha "$WORK/www/$name" > "$WORK/expect_$name"
done

BIN="${TITS_BIN:-}"
if [ -z "$BIN" ]; then
  BIN="$WORK/tit"
  ( cd "$REPO_ROOT" && go build -o "$BIN" ./cmd/true-imap-tunnel ) || fail "go build failed"
fi

if [ -n "$CONFIG_DIR" ]; then
  SERVER_CFG="$CONFIG_DIR/server.yaml"; CLIENT_CFG="$CONFIG_DIR/client.yaml"
else
  : "${TITS_IMAP_HOST:?set TITS_IMAP_HOST or CONFIG_DIR}"
  : "${TITS_IMAP_USER:?set TITS_IMAP_USER}"
  : "${TITS_IMAP_PASS:?set TITS_IMAP_PASS}"
  PORT="${TITS_IMAP_PORT:-993}"; TLS="${TITS_IMAP_TLS:-implicit}"; ENC="${TITS_ENC_PASS:-smoke-test}"
  SERVER_CFG="$WORK/server.yaml"; CLIENT_CFG="$WORK/client.yaml"
  cat > "$SERVER_CFG" <<EOF
mode: server
target: "127.0.0.1:$WEB_PORT"
log_level: info
accounts:
  - name: "primary"
    host: "$TITS_IMAP_HOST:$PORT"
    username: "$TITS_IMAP_USER"
    password: "$TITS_IMAP_PASS"
    tls: "$TLS"
    folder_send: "TunnelS2C"
    folder_recv: "TunnelC2S"
reorder: true
encryption_passphrase: "$ENC"
open_timeout_sec: 45
dial_timeout_sec: 10
poll_interval_ms: 2000
active_poll_interval_ms: 150
zero_rtt_open: true
async_data_send: true
EOF
  cat > "$CLIENT_CFG" <<EOF
mode: client
listen: "127.0.0.1:$LISTEN_PORT"
log_level: info
accounts:
  - name: "primary"
    host: "$TITS_IMAP_HOST:$PORT"
    username: "$TITS_IMAP_USER"
    password: "$TITS_IMAP_PASS"
    tls: "$TLS"
    folder_send: "TunnelC2S"
    folder_recv: "TunnelS2C"
reorder: true
encryption_passphrase: "$ENC"
open_timeout_sec: 45
dial_timeout_sec: 10
poll_interval_ms: 2000
active_poll_interval_ms: 150
zero_rtt_open: true
async_data_send: true
EOF
fi

"$PY" -m http.server "$WEB_PORT" --bind 127.0.0.1 --directory "$WORK/www" >/dev/null 2>&1 &
PIDS+=($!)
"$BIN" -config "$SERVER_CFG" > "$WORK/server.log" 2>&1 &
PIDS+=($!)
"$BIN" -config "$CLIENT_CFG" > "$WORK/client.log" 2>&1 &
PIDS+=($!)

BASE="http://127.0.0.1:$LISTEN_PORT"
ready=0
for _ in $(seq 1 60); do
  sleep 2
  code="$(curl -s -o "$WORK/probe.out" -w '%{http_code}' --max-time 30 "$BASE/f1.bin" || true)"
  if [ "$code" = "200" ]; then ready=1; break; fi
done
if [ "$ready" != "1" ]; then fail "tunnel not ready / probe download failed (see $WORK/client.log)"; fi

DLPIDS=()
DLNAMES=()
for spec in $FILES; do
  name="${spec%%:*}"
  ( curl -s -o "$WORK/dl_$name" -w '%{http_code}' --max-time 600 "$BASE/$name" > "$WORK/code_$name" ) &
  DLPIDS+=($!)
  DLNAMES+=("$name")
done

rc=0
for i in "${!DLPIDS[@]}"; do
  wait "${DLPIDS[$i]}" || true
  name="${DLNAMES[$i]}"
  code="$(cat "$WORK/code_$name" 2>/dev/null || echo 000)"
  got="$(sha "$WORK/dl_$name" 2>/dev/null || echo none)"
  want="$(cat "$WORK/expect_$name")"
  if [ "$code" = "200" ] && [ "$got" = "$want" ]; then
    echo "  $name OK"
  else
    echo "  $name FAIL (http=$code)"; rc=1
  fi
done

if [ "$rc" != "0" ]; then fail "one or more downloads did not match"; fi
echo "PASS: $(echo $FILES | wc -w | tr -d ' ') files downloaded in parallel and verified"
