# Native pi-durable port

Reference: pi-mono `5e63241d0294ed3fcfc4388c737a8ea27e09c74e`, durable and Chord 1.4.0.
Base: pi-rs `2a274f9`. Worktree: `../pi-rs-durable`, branch `feat/pi-durable`.

## Plan review

The proposed native `pi-chord` and `pi-durable` split follows PORTING.md. Durable storage must remain independent of coding-agent sessions. The implementation order is sound, with these additions:

- Chord requirements include output buffering, UTF-8/UTF-16 handling, invocation context, cancellation, bounded watch delivery, and SQLite migration contracts.
- Browser IndexedDB, worker SQLite, worker wake handling, portable environment contracts, and image processing are tracked alongside native adapters. The reference's browser and worker tests are acceptance inputs.
- Pin each source and test file by hash. Inventory literal tests without claiming that static counts include generated or parameterized cases.
- Check pi-ai and pi-codemode 1.4.0 dependencies against the existing 1.3.0 ports. Reuse requires behavior checks, especially deterministic VM replay and model dispatch.
- Track embedded-runtime compatibility from the first API design, while implementing its bindings after the native contracts exist.
- Treat sequence allocation, ID allocation, failed adoption, commit publication, and listener failures as separate testable contracts.

## Implementation and acceptance

1. Reference inventory and public API map. `scripts/durable-inventory.py <pi-mono> --check` verifies all source/test hashes. Record module parity and remaining work here.
2. Chord subset: strict JSON ownership, operations, codec, immutable replay, revision tracker, context/cancellation, watches, output, SQLite. Port relevant Chord tests and generate independent reference fixtures.
3. Durable data types, definitions, transactions, memory storage, session lifecycle, documents, tasks, forks, scans, submissions. Storage conformance must cover the complete reference suite.
4. JSONL storage: exact records, commit markers, sidecars, fsync ordering, malformed and torn tails, crash/restart, bidirectional interoperability.
5. Scheduler: task reservation, phase transitions, joins, sleep/wake, abort handling, restart limits, recovery, registry changes, cancellation ordering, task graph.
6. Agent runtime: generation, tool replay policies, nested calls, inbox, submissions, context, prompt, compaction, usage, views, events, telemetry.
7. SQLite schema/migrations and native adapter; browser/worker adapters; environments, tools, images, search, deterministic codemode, testing helpers.
8. Embedded JavaScript package exports and bindings, integration examples, documentation and full parity audit.

Acceptance requires focused crate tests, reference fixtures, full conformance on each backend, subprocess crash injection, bidirectional reference storage opening, both retention configurations, cancellation/replay/recovery tests, compile/lint checks, and a runnable native conversation example. Do not mark the port complete while an export, module, backend or offline reference test remains unmapped or unverified.

## Current state

- Reference inventory and parity ledger generated. The native Chord subset, durable storage and Session/transaction services are in progress. SQLite now has native schema/migrations, native/host adapters, cross-language file checks and all 105 stock SQLite tests under both retention settings. Native task graph/watch and inspection services now have stock coverage as well; all 36 stock task cases now pass in both retention modes. Native submission/inbox/live/usage/provider services are implemented with 28 independent reference scenarios and three selected stock submission cases. Native prompt rendering/replay and section/tool planning now pass 142 independent source cases and all nine stock prompt cases in both retention modes. Native compaction now executes select/summarize/retry/abort phases through the Rust model SDK, with eleven stock range/serialization cases and independent native execution tests. Native tool call/execute/abort phases now implement durable intent, dual-safe replay, nested call admission/reuse, progress and settlement, with native tests and independent source fixtures. All 39 stock tool cases and nine stock tool-recovery cases now run unmodified in both retention modes, with native tool execution and original generation/provider/environment callbacks. Stock nested-tool/structured-output bindings, executable generation and the remaining submission/inspection/compaction stock cases remain open. See STATUS.md for the exact native scope, evidence and remaining work.
- All eight milestones remain open until their checks prove parity. A foundational crate compiling is not completion of the port.
- No remote changes or pushes are part of this work.
