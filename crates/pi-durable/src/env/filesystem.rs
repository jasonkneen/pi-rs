use crate::Context;
use std::{
    ffi::OsString,
    future::Future,
    path::{Path, PathBuf},
    pin::Pin,
    sync::Arc,
};
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FileErrorCode {
    Aborted,
    NotFound,
    PermissionDenied,
    NotDirectory,
    IsDirectory,
    Invalid,
    NotSupported,
    Unknown,
}
#[derive(Debug)]
struct Failure {
    code: FileErrorCode,
    message: String,
    path: Option<PathBuf>,
    cause: Option<Arc<dyn std::error::Error + Send + Sync>>,
}
#[derive(Clone, Debug)]
pub struct FileError(Arc<Failure>);
impl FileError {
    pub fn new(code: FileErrorCode, message: impl Into<String>, path: Option<PathBuf>) -> Self {
        Self(Arc::new(Failure {
            code,
            message: message.into(),
            path,
            cause: None,
        }))
    }
    pub fn from_io(error: std::io::Error, path: Option<PathBuf>) -> Self {
        use std::io::ErrorKind::*;
        let code = match error.kind() {
            NotFound => FileErrorCode::NotFound,
            PermissionDenied => FileErrorCode::PermissionDenied,
            NotADirectory => FileErrorCode::NotDirectory,
            IsADirectory => FileErrorCode::IsDirectory,
            InvalidInput | InvalidData => FileErrorCode::Invalid,
            Unsupported => FileErrorCode::NotSupported,
            _ => FileErrorCode::Unknown,
        };
        Self(Arc::new(Failure {
            code,
            message: error.to_string(),
            path,
            cause: Some(Arc::new(error)),
        }))
    }
    pub fn code(&self) -> FileErrorCode {
        self.0.code
    }
    pub fn path(&self) -> Option<&Path> {
        self.0.path.as_deref()
    }
    pub fn same_error(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}
impl std::fmt::Display for FileError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0.message)
    }
}
impl std::error::Error for FileError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.0.cause.as_ref().map(|e| e.as_ref() as _)
    }
}
pub type FileResult<T> = std::result::Result<T, FileError>;
pub type FileFuture<T> = Pin<Box<dyn Future<Output = FileResult<T>> + Send + 'static>>;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FileKind {
    File,
    Directory,
    Symlink,
}
#[derive(Clone, Debug)]
pub struct FileInfo {
    pub name: OsString,
    pub path: PathBuf,
    pub kind: FileKind,
    pub size: u64,
    pub mtime_ms: f64,
}

/// Persistence capability shared by local, remote and embedded environments.
/// Every call returns an expected filesystem failure instead of panicking.
pub trait FileSystem: Send + Sync {
    fn id(&self) -> &str;
    fn cwd(&self) -> PathBuf;
    fn absolute_path(&self, path: &Path, context: &Context) -> FileFuture<PathBuf>;
    fn join_path(&self, parts: &[PathBuf], context: &Context) -> FileFuture<PathBuf>;
    fn read_binary_file(&self, path: &Path, context: &Context) -> FileFuture<Vec<u8>>;
    fn write_file(&self, path: &Path, content: &[u8], context: &Context) -> FileFuture<()>;
    fn append_file(&self, path: &Path, content: &[u8], context: &Context) -> FileFuture<()>;
    fn truncate_file(&self, path: &Path, size: u64, context: &Context) -> FileFuture<()>;
    fn flush_file(&self, path: &Path, context: &Context) -> FileFuture<()>;
    fn rename_file(&self, source: &Path, destination: &Path, context: &Context) -> FileFuture<()>;
    fn list_dir(&self, path: &Path, context: &Context) -> FileFuture<Vec<FileInfo>>;
    fn create_dir(&self, path: &Path, recursive: bool, context: &Context) -> FileFuture<()>;
    fn remove(
        &self,
        path: &Path,
        recursive: bool,
        force: bool,
        context: &Context,
    ) -> FileFuture<()>;
}
