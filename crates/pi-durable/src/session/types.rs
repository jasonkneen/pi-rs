use crate::*;
use pi_chord::delta::Op;
use std::sync::Arc;

#[derive(Clone, Copy, Debug, Default)]
pub struct TransactionScope {
    pub conversation_id: Option<ConversationId>,
    pub task_id: Option<TaskId>,
}
#[derive(Clone, Copy, Debug)]
pub enum ConversationOwnership {
    Ownerless,
    Task(TaskId),
}
#[derive(Clone, Copy, Debug)]
pub enum TaskOwnership {
    Conversation,
    Task(TaskId),
}
#[derive(Clone, Debug)]
pub enum TaskTrace {
    Root,
    Carrier(JsonObject),
}
#[derive(Clone, Debug)]
pub struct TaskOptions {
    pub ownership: TaskOwnership,
    pub conversation_id: Option<ConversationId>,
    pub trace: Option<TaskTrace>,
    pub links: Option<Vec<JsonObject>>,
    pub background: bool,
    pub abandon_on_restart: bool,
    pub submission_ids: Option<Vec<SubmissionId>>,
}
impl TaskOptions {
    pub fn new(ownership: TaskOwnership) -> Self {
        Self {
            ownership,
            conversation_id: None,
            trace: None,
            links: None,
            background: false,
            abandon_on_restart: false,
            submission_ids: None,
        }
    }
}
#[derive(Clone, Copy, Debug)]
pub enum EntryHead {
    Entry(EntryId),
    ThisEntry,
}
#[derive(Clone, Debug)]
pub struct EntryDraft {
    pub kind: JsonString,
    pub model: Option<Vec<JsonValue>>,
    pub data: Option<JsonValue>,
    pub head: Option<EntryHead>,
    pub edits: Option<Vec<ContextEdit>>,
}
impl EntryDraft {
    pub fn new(kind: impl Into<JsonString>) -> Self {
        Self {
            kind: kind.into(),
            model: None,
            data: None,
            head: None,
            edits: None,
        }
    }
}
#[derive(Clone, Debug)]
pub struct SubmissionCreate {
    pub conversation_id: ConversationId,
    pub trace: Option<JsonObject>,
    pub request_id: Option<JsonString>,
    pub state: SubmissionState,
}
#[derive(Clone, Debug)]
pub enum SubmissionSettlement {
    Done {
        answer: EntryId,
    },
    Unanswered {
        reason: JsonString,
        detail: Option<JsonValue>,
    },
}

/// The value shares the exact immutable revision installed during adoption.
#[derive(Clone, Debug)]
pub enum DocumentCommitChange {
    Document {
        record: DocumentRecord,
        conversation_id: Option<ConversationId>,
        version: Option<u64>,
        value: Arc<JsonValue>,
        ops: Vec<Op>,
    },
    Copy {
        record: DocumentRecord,
        conversation_id: ConversationId,
        source: DocumentCopySource,
    },
}
impl DocumentCommitChange {
    pub fn record(&self) -> &DocumentRecord {
        match self {
            Self::Document { record, .. } | Self::Copy { record, .. } => record,
        }
    }
}
#[derive(Clone, Debug)]
pub enum CommitChange {
    Conversation(ConversationRecord),
    Entry(EntryRecord),
    Task(Box<TaskRecord>),
    Submission(SubmissionRecord),
    Document(DocumentCommitChange),
}
#[derive(Clone, Debug)]
pub struct CommitPublication {
    pub seq: Seq,
    pub changes: Vec<CommitChange>,
}
#[derive(Clone, Debug)]
pub enum SessionEnd {
    Closed,
    Failed(Error),
}

impl serde::Serialize for DocumentCommitChange {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        use crate::wire::{put, put_optional};
        let mut value = crate::wire::object();
        match self {
            Self::Document {
                record,
                conversation_id,
                version,
                value: revision,
                ops,
            } => {
                put(&mut value, "type", "document");
                put(&mut value, "record", record);
                put_optional(&mut value, "conversationId", conversation_id);
                put_optional(&mut value, "version", version);
                put(&mut value, "value", revision);
                put(&mut value, "ops", ops);
            }
            Self::Copy {
                record,
                conversation_id,
                source,
            } => {
                put(&mut value, "type", "document.copy");
                put(&mut value, "record", record);
                put(&mut value, "conversationId", conversation_id);
                put(&mut value, "source", source);
            }
        }
        value.serialize(serializer)
    }
}
impl serde::Serialize for CommitChange {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        let (kind, value) = match self {
            Self::Conversation(v) => ("conversation", crate::wire::encode(v)),
            Self::Entry(v) => ("entry", crate::wire::encode(v)),
            Self::Task(v) => ("task", crate::wire::encode(v)),
            Self::Submission(v) => ("submission", crate::wire::encode(v)),
            Self::Document(v) => return v.serialize(serializer),
        };
        let mut envelope = crate::wire::object();
        crate::wire::put(&mut envelope, "type", kind);
        crate::wire::put(&mut envelope, "value", &value);
        envelope.serialize(serializer)
    }
}
impl serde::Serialize for CommitPublication {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        let mut value = crate::wire::object();
        crate::wire::put(&mut value, "seq", &self.seq);
        crate::wire::put(&mut value, "changes", &self.changes);
        value.serialize(serializer)
    }
}
