# Durable port status

Goal: the complete native Rust port described in PLAN.md. Status: active, incomplete.

Worktree: `/Users/jkneen/Documents/GitHub/pi-rs-durable`, branch `feat/pi-durable`.
The original `pi-rs` worktree remains on clean `dev` at `2a274f9`.
Reference: `/Users/jkneen/Documents/GitHub/pi-mono`, commit `5e63241d0294ed3fcfc4388c737a8ea27e09c74e`.

## Implemented so far

- A native `pi-chord` crate with strict JSON ownership, UTF-16 strings, persisted operation tuples, path validation, path interning and base resets, immutable replay, compact revision diffs, prepare/adopt lifecycle, invocation contexts, cancellation, explicit telemetry parents, bounded JSON/native watches, bounded output streams, incremental decoding, and progress commit pacing.
- Chord SQLite executor/database contracts, atomic migrations, native SQLite connection worker, and the structural Durable Object host adapter. Transaction submission/retirement is serialized, unrelated operations wait outside transactions, stale handles fail, and rollback failures retain both errors. The host adapter is tested using real SQLite behind a local stand-in; no deployed worker has been tested.
- `pi-durable` persisted records and tagged decoders, branded IDs, entry tokens, typed singleton/family document definitions, migration/checkpoint helpers, memory storage, ordered/fork-aware scans, task/submission indexes, document lifetimes/copies/history, and prepared commits.
- All 26 reference storage conformance cases run unmodified against the native memory backend over the test bridge. A separate run against TypeScript memory storage generates 430 independently expected requests/results, replayed by 26 native tests. The runner-independent native `create_storage_conformance()` can run the same contracts against future backends.
- TypeScript-generated data fixtures. The reference produces 142 operation batches, 13 revision diffs, 42 rejected requests, 176 output bounds cases and 744 chunked output streams. Rust consumes these fixtures. The TypeScript reference also replays Rust-generated operations.
- A pinned inventory of all 84 durable source modules, their declaration/reexport names, package entry points and dependencies, and source/test hashes. Durable contains 27,009 source lines and 1,039 statically discovered literal tests. Dynamic and parameterized test counts require executing the reference suites.
- A parity ledger containing every durable module/test file and the required Chord subset. Entries remain pending or partial until their full contracts have been mapped and verified.

## Remaining work

1. Finish Chord parity: complete test-case mapping, tracker/replay allocation and retention parity, embedded-runtime draft bindings, and portable/native dependency boundaries.
2. Next implement Session and Tx, including the transaction line, read-after-write rules, document drafts/caching/watches, task creation/transitions, forks, cancellation, failures, and commit publication. Typed document definitions and backend storage exist; the Session/Tx services and task definitions do not.
3. Then implement JSONL persistence/recovery, scheduler and agent tasks, durable SQLite storage and schema, environments/tools/images/search/codemode, IndexedDB and worker integrations, JS bindings and remaining testing exports. Chord's SQL facade is implemented; the durable SQL backend is not.
4. Verify crash recovery, durable publication ordering, ID allocation across restart, replay policies, cross-language storage interchange, both document retention modes, and integration with existing pi-ai/pi-codemode versions.

## Design details that must survive continuation

- Standard `serde_json::Value` cannot represent lone UTF-16 surrogates. Actual reference operation streams produce them through truncation. Chord's `json::Value` and `JsonString` preserve these units, including object keys and operation payloads, and serialize them as JSON escapes. Do not replace them with lossy UTF-8 conversion.
- JSON numeric equality uses JavaScript numbers, and stringify follows JavaScript index-key ordering and number formatting.
- `json::Value` deserializes through raw JSON. Direct JSON serialization/deserialization preserves its strings. Avoid routing it through `serde_json::Value` or serde's buffered tagged-enum representation. Storage record decoders need raw-value dispatch, with regressions for nested lone-surrogate payloads.
- Durable record `kind`, family `key`, `requestId`, outcome reasons and other arbitrary string payloads use `JsonString`. The reference conformance suite explicitly distinguishes different lone-surrogate identities. Preserve them in every persistent index. Model messages are retained as lossless JSON wire records until provider conversion; provider/harness integration remains open.
- `TaskState` retains memos only in live variants. `SubmissionState` distinguishes input and passive-write lifecycles. `StorageWrite::Task` boxes the larger task record. Record and definition versions use `u64` to cover JavaScript's safe-integer range.
- `MemoryStorage::prepare_commit` resolves document copies and validates all global IDs/actions before mutation. `PreparedMemoryCommit::apply` is idempotent and rejects stale competing preparations. `commit` serializes adoption while allowing reads inside the callback. Prepared writes are immutable Rust borrows, rather than runtime-frozen JavaScript objects.
- SQLite operations enqueue at method call, independently of their returned waiters. A submitted transaction operation must execute before commit even if its waiter is dropped. The native regression reproduced a dropped write compared with the reference, then passed after lease checking and worker submission were protected by the same mutex.
- The current native tracker clones a draft tree and replay clones owned values. Reference overlay/copy-on-write allocation and retention behavior still needs work. Do not mark these modules fully verified based on semantic fixtures alone.
- Native revision operation shape is not canonical in Chord. Existing golden cases match exactly, and independently generated operations replay in the reference; broader shape/performance-sensitive cases remain to be mapped.
- Context waiting starts an independent Tokio task. Cancelling the waiter leaves that operation running, matching promise observation semantics. Watches strip the frame's cancellation, retain at most 100 pending frames, and stop without waiting for a callback already running.
- No Node or TypeScript runtime is part of the new crate. Node is used only by fixture generators and the reference verifier.

## Verification

Run `scripts/check-durable-port.sh /Users/jkneen/Documents/GitHub/pi-mono`.
For this worktree, reuse compiled dependencies with
`CARGO_TARGET_DIR=/Users/jkneen/Documents/GitHub/pi-rs/target`.

The full check script passed on 2026-10-24 after adding SQLite and durable memory storage: 107 Rust unit/integration tests and one compile-fail documentation test, warning-free Clippy, formatting, pinned inventory, all fixture generators, reference replay of 13 native-generated revision cases, and all 26 unmodified reference storage conformance cases against native memory storage.

Focused checks cover the implemented Chord subset, durable record/definition helpers, and memory backend. Whole-workspace tests, persistent-backend conformance, real browser/worker checks and crash-recovery gates remain open. The full objective remains open.
