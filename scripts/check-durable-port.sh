#!/usr/bin/env bash
# Offline Rust checks and interoperability with a locally pinned reference.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
reference=${1:-../pi-mono}
native_output=$(mktemp "${TMPDIR:-/tmp}/pi-durable-check.XXXXXX")
trap 'rm -f "$native_output"' EXIT
python3 scripts/durable-inventory.py "$reference" --check
cargo fmt -p pi-chord -p pi-durable -p pi-telemetry --check
cargo test --locked -p pi-chord -p pi-durable -p pi-telemetry
cargo run --locked -q -p pi-durable --example conversation -- "Hello"
cargo test --locked -p pi-ai --test telemetry
cargo check --locked -p pi-agent -p pi-coding-agent
cargo test --locked -p pi-coding-agent --test extensions_platform js_models_requests_
cargo clippy --locked -p pi-chord -p pi-durable -p pi-telemetry --all-targets -- -D warnings
node --experimental-strip-types scripts/golden/gen-durable-output.mjs "$reference" --check
cargo run --locked -q -p pi-chord --example reference_deltas > "$native_output"
node --experimental-strip-types scripts/golden/gen-durable-delta.mjs "$reference" --check --verify-native "$native_output"
node --conditions=source --experimental-strip-types scripts/golden/gen-durable-storage.mjs "$reference" --check --native
node --conditions=source --experimental-strip-types scripts/golden/gen-durable-session.mjs "$reference" --check
node --conditions=source --experimental-strip-types scripts/golden/gen-durable-submissions.mjs "$reference" --check
node --conditions=source --experimental-strip-types scripts/golden/gen-durable-prompt.mjs "$reference" --check
node --conditions=source --experimental-strip-types scripts/golden/gen-durable-compaction.mjs "$reference" --check
node --conditions=source --experimental-strip-types scripts/golden/gen-durable-tool-results.mjs "$reference" --check
node --conditions=source --experimental-strip-types scripts/golden/gen-durable-storage.mjs "$reference" --check --native --jsonl --interchange
node --conditions=source --experimental-strip-types scripts/golden/gen-durable-storage.mjs "$reference" --check --native --jsonl --reopen
node --conditions=source --experimental-strip-types scripts/golden/gen-durable-storage.mjs "$reference" --check --native --sqlite --interchange
node --conditions=source --experimental-strip-types scripts/golden/gen-durable-storage.mjs "$reference" --check --native --sqlite --reopen
node scripts/run-stock-durable-sqlite.mjs "$reference" --report=/tmp/pi-durable-stock-sqlite-report.json
node scripts/run-stock-durable-jsonl.mjs "$reference" --report=/tmp/pi-durable-stock-jsonl-report.json
node scripts/run-stock-durable-tasks.mjs "$reference" --report=/tmp/pi-durable-stock-tasks-report.json
node scripts/run-stock-durable-tasks.mjs "$reference" --suite=tools --report=/tmp/pi-durable-stock-tools-report.json
node scripts/run-stock-durable-tasks.mjs "$reference" --suite=host-inputs --report=/tmp/pi-durable-stock-host-inputs-report.json
node scripts/run-stock-durable-tasks.mjs "$reference" --suite=structured-output --report=/tmp/pi-durable-stock-structured-output-report.json
node scripts/run-stock-durable-tasks.mjs "$reference" --suite=structured --report=/tmp/pi-durable-stock-structured-report.json
node scripts/run-stock-durable-tasks.mjs "$reference" --suite=read-only --report=/tmp/pi-durable-stock-read-only-report.json
node scripts/run-stock-durable-tasks.mjs "$reference" --suite=conversations --report=/tmp/pi-durable-stock-conversations-report.json
node scripts/run-stock-durable-tasks.mjs "$reference" --suite=tool-recovery --report=/tmp/pi-durable-stock-tool-recovery-report.json
node scripts/run-stock-durable-tasks.mjs "$reference" --suite=nested --report=/tmp/pi-durable-stock-nested-report.json
node scripts/run-stock-durable-tasks.mjs "$reference" --suite=nested-restart --report=/tmp/pi-durable-stock-nested-restart-report.json
node scripts/run-stock-durable-tasks.mjs "$reference" --suite=view --report=/tmp/pi-durable-stock-view-report.json
node scripts/run-stock-durable-tasks.mjs "$reference" --suite=events --report=/tmp/pi-durable-stock-events-report.json
node scripts/run-stock-durable-tasks.mjs "$reference" --suite=generation --report=/tmp/pi-durable-stock-generation-report.json
node scripts/run-stock-durable-tasks.mjs "$reference" --suite=generation-recovery --report=/tmp/pi-durable-stock-generation-recovery-report.json
node scripts/run-stock-durable-tasks.mjs "$reference" --suite=graph --report=/tmp/pi-durable-stock-graph-report.json
node scripts/run-stock-durable-tasks.mjs "$reference" --suite=inspect --report=/tmp/pi-durable-stock-inspect-report.json
node scripts/run-stock-durable-tasks.mjs "$reference" --suite=submissions --report=/tmp/pi-durable-stock-submissions-report.json
node scripts/run-stock-durable-context.mjs "$reference" --report=/tmp/pi-durable-stock-context-report.json
node scripts/run-stock-durable-prompt.mjs "$reference" --report=/tmp/pi-durable-stock-prompt-report.json
node scripts/run-stock-durable-compaction.mjs "$reference" --report=/tmp/pi-durable-stock-compaction-report.json
node scripts/run-stock-durable-registry.mjs "$reference" --report=/tmp/pi-durable-stock-registry-report.json
