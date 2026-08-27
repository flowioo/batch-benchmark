#!/usr/bin/env bash
# java-microbatch bench entry
# Usage: bench.sh <duration_sec> <batch_max> <window_ms> <concurrency> <payload_bytes> <output_json>
set -euo pipefail

DURATION="${1:-30}"
BATCH="${2:-500}"
WINDOW="${3:-50}"
CONCURRENCY="${4:-8}"
PAYLOAD="${5:-64}"
OUT="${6:-result.json}"

cd "$(dirname "$0")"
mkdir -p target/classes
CP="lib/jctools-core-4.0.5.jar"
javac -cp "$CP" -d target/classes src/main/java/bench/Bench.java
exec java -cp "target/classes:$CP" bench.Bench "$DURATION" "$BATCH" "$WINDOW" "$CONCURRENCY" "$PAYLOAD" "$OUT"
