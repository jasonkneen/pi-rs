use crate::types::ConversationId;
use std::sync::Arc;

#[derive(Debug)]
pub enum ErrorKind {
    Delta(pi_chord::delta::Error),
    Message(String),
    StorageRequest(String),
    ReadAfterWrite(String),
    SessionFailed {
        cause: Error,
    },
    ReadOnly(String),
    ConversationBusy(ConversationId),
    DocumentCopy {
        id: crate::types::DocumentId,
        cause: Error,
    },
}
#[derive(Clone, Debug)]
pub struct Error(Arc<ErrorKind>);
pub type Result<T> = std::result::Result<T, Error>;
impl Error {
    pub fn message(message: impl Into<String>) -> Self {
        Self(Arc::new(ErrorKind::Message(message.into())))
    }
    pub fn request(message: impl Into<String>) -> Self {
        Self(Arc::new(ErrorKind::StorageRequest(message.into())))
    }
    pub fn read_after_write(method: impl Into<String>) -> Self {
        Self(Arc::new(ErrorKind::ReadAfterWrite(method.into())))
    }
    pub fn failed(cause: Self) -> Self {
        Self(Arc::new(ErrorKind::SessionFailed { cause }))
    }
    pub fn read_only(what: impl Into<String>) -> Self {
        Self(Arc::new(ErrorKind::ReadOnly(what.into())))
    }
    pub fn busy(id: ConversationId) -> Self {
        Self(Arc::new(ErrorKind::ConversationBusy(id)))
    }
    pub fn document_copy(id: crate::types::DocumentId, cause: Self) -> Self {
        Self(Arc::new(ErrorKind::DocumentCopy { id, cause }))
    }
    pub fn kind(&self) -> &ErrorKind {
        &self.0
    }
    pub fn name(&self) -> &'static str {
        match self.kind() {
            ErrorKind::StorageRequest(_) => "StorageRequestError",
            ErrorKind::ReadAfterWrite(_) => "ReadAfterWrite",
            ErrorKind::SessionFailed { .. } => "SessionFailed",
            ErrorKind::ReadOnly(_) => "ReadOnlySession",
            ErrorKind::ConversationBusy(_) => "ConversationBusy",
            ErrorKind::Delta(pi_chord::delta::Error::Type(_)) => "TypeError",
            ErrorKind::Delta(pi_chord::delta::Error::UnsafePath(_)) => "UnsafePathError",
            ErrorKind::Delta(pi_chord::delta::Error::Path(_)) => "PathError",
            _ => "Error",
        }
    }
    pub fn is_request(&self) -> bool {
        matches!(self.kind(), ErrorKind::StorageRequest(_))
    }
    pub fn same_error(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.kind() {
            ErrorKind::Delta(error) => error.fmt(f),
            ErrorKind::Message(m) | ErrorKind::StorageRequest(m) => f.write_str(m),
            ErrorKind::ReadAfterWrite(method) => write!(
                f,
                "Tx.{method}() cannot read tables after the first table write"
            ),
            ErrorKind::SessionFailed { .. } => {
                f.write_str("Session failed after a storage error; close and reopen it")
            }
            ErrorKind::ReadOnly(what) => write!(f, "Read-only Harness: {what} refused"),
            ErrorKind::ConversationBusy(id) => write!(f, "Conversation {id} is busy"),
            ErrorKind::DocumentCopy { id, .. } => write!(f, "Document copy {id} was rejected"),
        }
    }
}
impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self.kind() {
            ErrorKind::Delta(error) => Some(error),
            ErrorKind::SessionFailed { cause } | ErrorKind::DocumentCopy { cause, .. } => {
                Some(cause)
            }
            _ => None,
        }
    }
}
impl From<pi_chord::delta::Error> for Error {
    fn from(error: pi_chord::delta::Error) -> Self {
        Self(Arc::new(ErrorKind::Delta(error)))
    }
}
impl From<serde_json::Error> for Error {
    fn from(error: serde_json::Error) -> Self {
        Self::message(error.to_string())
    }
}
