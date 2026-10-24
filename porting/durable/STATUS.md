# Durable port status

Goal: the complete native Rust port described in PLAN.md. Status: active, incomplete.

Worktree: `/Users/jkneen/Documents/GitHub/pi-rs-durable`, branch `feat/pi-durable`.
The original `pi-rs` worktree remains on clean `dev` at `2a274f9`.
Reference: `/Users/jkneen/Documents/GitHub/pi-mono`, commit `5e63241d0294ed3fcfc4388c737a8ea27e09c74e`.

## Implemented so far

- A native `pi-chord` crate with strict JSON ownership, UTF-16 strings, persisted operation tuples, path validation, path interning and base resets, immutable replay, compact revision diffs, prepare/adopt lifecycle, invocation contexts, cancellation, explicit telemetry parents, bounded JSON/native watches, bounded output streams, incremental decoding, and progress commit pacing.
- TypeScript-generated data fixtures. The reference produces 142 operation batches, 13 revision diffs, 42 rejected requests, 176 output bounds cases and 744 chunked output streams. Rust consumes these fixtures. The TypeScript reference also replays Rust-generated operations.
- A pinned inventory of all 84 durable source modules, their declaration/reexport names, package entry points and dependencies, and source/test hashes. Durable contains 27,009 source lines and 1,039 statically discovered literal tests. Dynamic and parameterized test counts require executing the reference suites.
- A parity ledger containing every durable module/test file and the required Chord subset. Entries remain pending or partial until their full contracts have been mapped and verified.

## Remaining work

1. Finish Chord parity: SQLite facade/adapters/migrations; complete test-case mapping; tracker/replay allocation and retention parity; embedded-runtime draft bindings.
2. Add `pi-durable`. No durable crate, sessions, transactions, storage backend, scheduler or agent runtime has been implemented yet.
3. Port the complete memory storage/session/transaction and conformance suites, then JSONL recovery, scheduler, agent tasks, SQLite, environment/tools/images/search/codemode, browser/worker adapters, JS bindings and testing exports.
4. Verify crash recovery, durable publication ordering, ID allocation across restart, replay policies, cross-language storage interchange, both document retention modes, and integration with existing pi-ai/pi-codemode versions.

## Design details that must survive continuation

- Standard `serde_json::Value` cannot represent lone UTF-16 surrogates. Actual reference operation streams produce them through truncation. Chord's `json::Value` and `JsonString` preserve these units, including object keys and operation payloads, and serialize them as JSON escapes. Do not replace them with lossy UTF-8 conversion.
- JSON numeric equality uses JavaScript numbers, and stringify follows JavaScript index-key ordering and number formatting.
- `json::Value` deserializes through raw JSON. Direct JSON serialization/deserialization preserves its strings. Avoid routing it through `serde_json::Value` or serde's buffered tagged-enum representation. Storage record decoders need raw-value dispatch, with regressions for nested lone-surrogate payloads.
- The current native tracker clones a draft tree and replay clones owned values. Reference overlay/copy-on-write allocation and retention behavior still needs work. Do not mark these modules fully verified based on semantic fixtures alone.
- Native revision operation shape is not canonical in Chord. Existing golden cases match exactly, and independently generated operations replay in the reference; broader shape/performance-sensitive cases remain to be mapped.
- Context waiting starts an independent Tokio task. Cancelling the waiter leaves that operation running, matching promise observation semantics. Watches strip the frame's cancellation, retain at most 100 pending frames, and stop without waiting for a callback already running.
- No Node or TypeScript runtime is part of the new crate. Node is used only by fixture generators and the reference verifier.

## Verification

Run `scripts/check-durable-port.sh /Users/jkneen/Documents/GitHub/pi-mono`.
For this worktree, reuse compiled dependencies with
`CARGO_TARGET_DIR=/Users/jkneen/Documents/GitHub/pi-rs/target`.

The full check script passed on 2026-10-24: 47 Rust tests, warning-free Clippy, formatting, pinned inventory and both fixture generators, and reference replay of all 13 native-generated revision cases.

Focused checks cover the implemented Chord subset. Whole-workspace tests and durable conformance have not run. The full objective remains open.
