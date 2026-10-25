//! Native port of pi-durable 1.4.0. Storage is independent of coding-agent sessions.

pub mod documents;
pub mod entries;
pub mod env;
pub mod errors;
pub mod harness;
pub mod images;
pub mod session;
pub mod storage;
pub mod tasks;
pub mod testing;
pub mod timers;
pub mod tools;
pub mod truncate;
pub mod types;
mod wire;
pub use documents::*;
pub use entries::*;
pub use errors::{Error, ErrorKind, Result};
pub use harness::compaction::{
    COMPACTION_TASK, CompactionCheckpoint, CompactionHooks, CompactionInput, CompactionResult,
    compaction_task,
};
pub use harness::context::ContextView;
pub use harness::inbox::{INBOX_DOC, InboxItem, InboxState, inbox_doc};
pub use harness::inspect::{HarnessInspection, Scheduling, TaskInspection, TaskInspectionState};
pub use harness::live::{LIVE_DOC, LiveState, live_doc};
pub use harness::prompt::{PromptInput, PromptSection};
pub use harness::provider::{PROVIDER_DOC, ProviderState, provider_doc};
pub use harness::scheduler::{ReportTask, TaskReport as ReportInfo};
pub use harness::submissions::{Submission, SubmissionAbort, Submissions, SubmissionsOptions};
pub use harness::task_graph::{
    TaskGraph, TaskGraphNode, TaskGraphOutcome, TaskGraphState, TaskGraphWatch,
};
pub use harness::types::{
    InputEntry, ModelRef, QueueMode, QueueModes, SettledSubmissionRecord, SubmissionDraft,
    UserInput, WhenBusy,
};
pub use harness::usage::{USAGE_DOC, UsageState, usage_doc};
pub use harness::{
    Conversation, ConversationCreateOptions, ConversationCreated, ConversationEntryQuery,
    ConversationInit, ConversationOptions, EnvTarget, EnvironmentFactory, Harness, HarnessOptions,
    create_registry,
};
pub use harness::{runtime::TaskRuntime, state::NextTaskState};
pub use pi_chord::{Context, JsonString, JsonValue, WatchEnd};
pub use session::*;
pub use storage::{
    JsonlStorage, JsonlStorageOptions, MemoryStorage, SqliteStorage, SqliteStorageOptions, Storage,
};
pub use tasks::{
    AnyTask, PhaseHandler, RunningTask, Task, TaskDefinition, TaskValue, TypedTaskId, define_task,
};
pub use types::*;
