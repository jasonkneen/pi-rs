#!/usr/bin/env bash
# Offline Rust checks and interoperability with a locally pinned reference.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
reference=${1:-../pi-mono}
native_output=$(mktemp "${TMPDIR:-/tmp}/pi-durable-check.XXXXXX")
trap 'rm -f "$native_output"' EXIT
python3 scripts/durable-inventory.py "$reference" --check
cargo fmt -p pi-chord -p pi-durable --check
cargo test --locked -p pi-chord -p pi-durable
cargo clippy --locked -p pi-chord -p pi-durable --all-targets -- -D warnings
node --experimental-strip-types scripts/golden/gen-durable-output.mjs "$reference" --check
cargo run --locked -q -p pi-chord --example reference_deltas > "$native_output"
node --experimental-strip-types scripts/golden/gen-durable-delta.mjs "$reference" --check --verify-native "$native_output"
node --conditions=source --experimental-strip-types scripts/golden/gen-durable-storage.mjs "$reference" --check --native
