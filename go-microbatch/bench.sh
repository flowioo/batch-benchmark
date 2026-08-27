#!/usr/bin/env bash
# go-microbatch bench entry
# Usage: bench.sh <duration_sec> <batch_max> <window_ms> <concurrency> <payload_bytes> <output_json>
set -euo pipefail

DURATION="${1:-30}"
BATCH="${2:-500}"
WINDOW="${3:-50}"
CONCURRENCY="${4:-8}"
PAYLOAD="${5:-64}"
OUT="${6:-result.json}"

cd "$(dirname "$0")"
go build -o bench . 2>/dev/null
exec ./bench "$DURATION" "$BATCH" "$WINDOW" "$CONCURRENCY" "$PAYLOAD" "$OUT"
