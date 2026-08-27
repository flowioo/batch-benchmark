#!/usr/bin/env bash
# rust-microbatch bench entry
# Usage: bench.sh <duration_sec> <batch_max> <window_ms> <concurrency> <payload_bytes> <output_json>
set -euo pipefail

DURATION="${1:-30}"
BATCH="${2:-500}"
WINDOW="${3:-50}"
CONCURRENCY="${4:-8}"
PAYLOAD="${5:-64}"
OUT="${6:-result.json}"

cd "$(dirname "$0")"
cargo build --release 2>/dev/null
exec ./target/release/rust-microbatch "$DURATION" "$BATCH" "$WINDOW" "$CONCURRENCY" "$PAYLOAD" "$OUT"
