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
- The Session/transaction kernel now includes eager queued operations, draft revocation and unfinished-operation draining, table read-after-write rules, document migration/checkpoints/retirement, lazy definition-free fork copies, task candidates/ownership/times, submissions, adoption/publication, first-error failure handling, close hooks, cancellation, caching and incarnation-bound watches. Native typed snapshots share immutable revisions while cached. Host timers provide foreground fallback and optional background eviction.
- A new independent reference runner generates 16 Session traces (94 operations) in zero and infinite retention modes. Native tests compare returned values/errors, admitted storage batches and complete publications, including migrations, checkpoints, families, all fork policies, task ownership and submission settlement. Another 38 native Session/cache/timer tests cover ordering, failures, cancellation, close and observation behavior. The full upstream Session test suites are not yet mapped case by case.
- TypeScript-generated data fixtures. The reference produces 142 operation batches, 13 revision diffs, 42 rejected requests, 176 output bounds cases and 744 chunked output streams. Rust consumes these fixtures. The TypeScript reference also replays Rust-generated operations.
- A pinned inventory of all 84 durable source modules, their declaration/reexport names, package entry points and dependencies, and source/test hashes. Durable contains 27,009 source lines and 1,039 statically discovered literal tests. Dynamic and parameterized test counts require executing the reference suites.
- A parity ledger containing every durable module/test file and the required Chord subset. Entries remain pending or partial until their full contracts have been mapped and verified.

## Remaining work

1. Finish Chord parity: complete test-case mapping, tracker/replay allocation and retention parity, embedded-runtime draft bindings, and portable/native dependency boundaries.
2. Finish Session/Tx parity: map every upstream test, preserve mutation-recording structural no-ops through native/embedded draft bindings, exercise guarded off-line backend operations, and verify the remaining lifecycle/cache edge cases. Kernel task creation is available as `Transaction::create_task_record`; executable task definitions, typed task tokens and task runtime do not exist yet.
3. Next implement JSONL persistence/recovery and task definitions/scheduler, then the agent harness, durable SQLite storage and schema, environments/tools/images/search/codemode, IndexedDB and worker integrations, JS bindings and remaining testing exports. Chord's SQL facade is implemented; the durable SQL backend is not.
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
- `DocDraft::update` records the difference of its final typed value; it cannot observe a push/pop or other sequence that ends at the original value. The reference can persist such nonempty structural no-ops. Add mutation-recording operations and embedded draft bindings before claiming full Session/Chord parity.
- Transaction operations enqueue at method invocation and run independently of dropped waiters. A callback that settles with pending operations rejects after draining them; drafts and Tx handles become unusable immediately at settlement. Document slots retain distinct old/new incarnations when retirement races an acquisition.
- Backend adoption runs synchronously after durability, before the commit future resolves; publications wait for that resolution. Missing adoption, adoption failure, and backend rejection before/after adoption are separately handled. GuardedStorage exempts request/caller-abort errors only on reads, preserves the first failure identity and drains actual backend operations before closing.
- Session close seals admission synchronously and ends observers in close-listener registration order, then runs `before_close`, drains admitted commits/cleanup writes and closes storage. Cancelling a close waiter leaves cleanup running. Internal commit listeners and watches share registration order; each listener group is snapshotted at its own delivery boundary.
- TelemetryContext now has a nonbreaking `record_event` method for contexts holding an active span; the in-memory span forwards it to the existing `TelemetrySpan::add_event`. Durable's context-level entry/submission events use it. External recording adapters must implement this context capability too; full 1.4 dependency alignment remains open.
- Native revision operation shape is not canonical in Chord. Existing golden cases match exactly, and independently generated operations replay in the reference; broader shape/performance-sensitive cases remain to be mapped.
- Context waiting starts an independent Tokio task. Cancelling the waiter leaves that operation running, matching promise observation semantics. Watches strip the frame's cancellation, retain at most 100 pending frames, and stop without waiting for a callback already running.
- No Node or TypeScript runtime is part of the new crate. Node is used only by fixture generators and the reference verifier.

## Verification

Run `scripts/check-durable-port.sh /Users/jkneen/Documents/GitHub/pi-mono`.
For this worktree, reuse compiled dependencies with
`CARGO_TARGET_DIR=/Users/jkneen/Documents/GitHub/pi-rs/target`.

The full check script passed on 2026-10-24 after adding Session/Tx: 197 Chord/durable/telemetry Rust unit/integration tests and one compile-fail documentation test, warning-free Clippy, formatting, pinned inventory, all fixture generators, reference replay of 13 native-generated revision cases, all 26 unmodified reference storage conformance cases against native memory storage, and 16 Session traces over 94 operations. This includes all 36 existing pi-telemetry tests. The check script now includes that crate. Full log: `/tmp/pi-durable-check-2026-10-24-session.log`.

Focused checks cover the implemented Chord subset, durable records/definitions, memory backend and the Session/transaction contracts above. Whole-workspace tests, complete upstream Session suites, persistent-backend conformance, real browser/worker checks and crash-recovery gates remain open. The full objective remains open.
