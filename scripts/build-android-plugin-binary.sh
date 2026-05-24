#!/usr/bin/env bash
set -euo pipefail

OUT="$1"
VERSION_NAME="${2:-dev}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
HASH="$(git -C "$ROOT" rev-parse --short HEAD 2>/dev/null || echo unknown)"
DATE="$(date +%Y%m%d-%H%M%S)"

mkdir -p "$(dirname "$OUT")"
cd "$ROOT"
GOOS=android GOARCH=arm64 CGO_ENABLED=0 \
  go build -trimpath -ldflags "-s -w -X main.buildVersion=$VERSION_NAME -X main.buildDate=$DATE -X main.buildHash=$HASH" \
  -o "$OUT" ./cmd/true-imap-tunnel
