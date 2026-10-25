#!/usr/bin/env bash
# Offline Rust checks and interoperability with a locally pinned reference.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
reference=${1:-../pi-mono}
native_output=$(mktemp "${TMPDIR:-/tmp}/pi-durable-check.XXXXXX")
trap 'rm -f "$native_output"' EXIT
python3 scripts/durable-inventory.py "$reference" --check
cargo fmt -p pi-chord -p pi-durable -p pi-telemetry -p pi-durable-codec-stock --check
rustfmt --edition 2024 --check crates/pi-json/src/utf8.rs
rustfmt --edition 2024 --check crates/pi-json/src/strict.rs crates/pi-codemode/src/json_run.rs crates/pi-codemode/tests/json_run.rs
rustfmt --edition 2024 --check crates/pi-json/src/diff.rs crates/pi-coding-agent/src/core/tools/jsdiff.rs
cargo test --locked -p pi-json -p pi-chord -p pi-durable -p pi-telemetry -p pi-codemode -p pi-jsrt
cargo run --locked -q -p pi-durable --example conversation -- "Hello"
cargo test --locked -p pi-ai --test telemetry
cargo check --locked -p pi-agent -p pi-coding-agent
cargo test --locked -p pi-coding-agent --test extensions_platform js_models_requests_
cargo clippy --locked -p pi-json -p pi-chord -p pi-durable -p pi-telemetry -p pi-durable-codec-stock -p pi-codemode -p pi-jsrt --all-targets -- -D warnings
cargo clippy --locked -p pi-durable-codec-stock --target wasm32-unknown-unknown -- -D warnings
node --experimental-strip-types scripts/golden/gen-durable-output.mjs "$reference" --check
cargo run --locked -q -p pi-chord --example reference_deltas > "$native_output"
node --experimental-strip-types scripts/golden/gen-durable-delta.mjs "$reference" --check --verify-native "$native_output"
node --conditions=source --experimental-strip-types scripts/golden/gen-durable-storage.mjs "$reference" --check --native
node --conditions=source --experimental-strip-types scripts/golden/gen-durable-session.mjs "$reference" --check
node --conditions=source --experimental-strip-types scripts/golden/gen-durable-submissions.mjs "$reference" --check
node --conditions=source --experimental-strip-types scripts/golden/gen-durable-prompt.mjs "$reference" --check
node --conditions=source --experimental-strip-types scripts/golden/gen-durable-compaction.mjs "$reference" --check
node --conditions=source --experimental-strip-types scripts/golden/gen-durable-tool-results.mjs "$reference" --check
node --conditions=source --experimental-strip-types scripts/golden/gen-durable-edit.mjs "$reference" --check
node --experimental-strip-types scripts/golden/gen-durable-codemode-output.mjs "$reference" --check
node --conditions=source --experimental-strip-types scripts/golden/gen-durable-storage.mjs "$reference" --check --native --jsonl --interchange
node --conditions=source --experimental-strip-types scripts/golden/gen-durable-storage.mjs "$reference" --check --native --jsonl --reopen
node --conditions=source --experimental-strip-types scripts/golden/gen-durable-storage.mjs "$reference" --check --native --sqlite --interchange
node --conditions=source --experimental-strip-types scripts/golden/gen-durable-storage.mjs "$reference" --check --native --sqlite --reopen
node scripts/run-stock-durable-sqlite.mjs "$reference" --report=/tmp/pi-durable-stock-sqlite-report.json
node scripts/run-stock-durable-jsonl.mjs "$reference" --report=/tmp/pi-durable-stock-jsonl-report.json
node scripts/run-stock-durable-codecs.mjs "$reference" --report=/tmp/pi-durable-stock-codecs-report.json
node scripts/run-stock-durable-env.mjs "$reference" --suite=all --report=/tmp/pi-durable-stock-env-report.json
node scripts/run-stock-durable-env.mjs "$reference" --suite=spill --report=/tmp/pi-durable-stock-env-spill-report.json
node scripts/run-stock-durable-env.mjs "$reference" --suite=conformance --report=/tmp/pi-durable-stock-env-conformance-report.json
node scripts/run-stock-durable-builtins.mjs "$reference" --report=/tmp/pi-durable-stock-builtins-report.json
node scripts/run-stock-durable-builtins.mjs "$reference" --suite=read-differential --report=/tmp/pi-durable-stock-read-differential-report.json
node scripts/run-stock-durable-builtins.mjs "$reference" --suite=images --report=/tmp/pi-durable-stock-native-images-report.json
node scripts/run-stock-durable-tasks.mjs "$reference" --report=/tmp/pi-durable-stock-tasks-report.json
node scripts/run-stock-durable-session.mjs "$reference" --suite=definitions --report=/tmp/pi-durable-stock-session-definitions-report.json
node scripts/run-stock-durable-session.mjs "$reference" --suite=tables --report=/tmp/pi-durable-stock-session-tables-report.json
node scripts/run-stock-durable-session.mjs "$reference" --suite=forks --report=/tmp/pi-durable-stock-session-forks-report.json
node scripts/run-stock-durable-tasks.mjs "$reference" --suite=codemode-models-native --report=/tmp/pi-durable-stock-codemode-models-report.json
node scripts/run-stock-durable-tasks.mjs "$reference" --suite=codemode --report=/tmp/pi-durable-stock-codemode-report.json
node scripts/run-stock-durable-tasks.mjs "$reference" --suite=codemode-describe --report=/tmp/pi-durable-stock-codemode-describe-report.json
node scripts/run-stock-durable-tasks.mjs "$reference" --suite=codemode-checkpoints --report=/tmp/pi-durable-stock-codemode-checkpoints-report.json
node scripts/run-stock-durable-tasks.mjs "$reference" --suite=search --report=/tmp/pi-durable-stock-search-report.json
node scripts/run-stock-durable-tasks.mjs "$reference" --suite=tools --report=/tmp/pi-durable-stock-tools-report.json
node scripts/run-stock-durable-tasks.mjs "$reference" --suite=host-inputs --report=/tmp/pi-durable-stock-host-inputs-report.json
node scripts/run-stock-durable-tasks.mjs "$reference" --suite=structured-output --report=/tmp/pi-durable-stock-structured-output-report.json
node scripts/run-stock-durable-tasks.mjs "$reference" --suite=structured --report=/tmp/pi-durable-stock-structured-report.json
node scripts/run-stock-durable-tasks.mjs "$reference" --suite=read-only --report=/tmp/pi-durable-stock-read-only-report.json
node scripts/run-stock-durable-tasks.mjs "$reference" --suite=conversations --report=/tmp/pi-durable-stock-conversations-report.json
node scripts/run-stock-durable-tasks.mjs "$reference" --suite=reports --report=/tmp/pi-durable-stock-reports-report.json
node scripts/run-stock-durable-tasks.mjs "$reference" --suite=lifecycle --report=/tmp/pi-durable-stock-lifecycle-report.json
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
printf 'Durable port checks passed.\n'
