use super::filesystem::*;
use crate::Context;
use parking_lot::RwLock;
use std::{
    ffi::OsString,
    io::Write,
    path::{Component, Path, PathBuf},
    time::UNIX_EPOCH,
};

pub struct NativeFileSystem {
    cwd: RwLock<PathBuf>,
}
impl NativeFileSystem {
    pub fn new(cwd: impl Into<PathBuf>) -> FileResult<Self> {
        let cwd = cwd.into();
        let cwd = if cwd.is_absolute() {
            cwd
        } else {
            std::env::current_dir()
                .map_err(|e| FileError::from_io(e, None))?
                .join(cwd)
        };
        Ok(Self {
            cwd: RwLock::new(normalize(&cwd)),
        })
    }
    pub fn set_cwd(&self, cwd: impl AsRef<Path>) {
        let cwd = self.resolve(cwd.as_ref());
        *self.cwd.write() = cwd;
    }
    fn resolve(&self, path: &Path) -> PathBuf {
        normalize(&self.cwd.read().join(path))
    }
    fn io<T: Send + 'static>(
        &self,
        path: PathBuf,
        context: &Context,
        operation: impl FnOnce(&Path) -> std::io::Result<T> + Send + 'static,
    ) -> FileFuture<T> {
        let context = context.clone();
        let (send, receive) = tokio::sync::oneshot::channel();
        tokio::task::spawn_blocking(move || {
            let result = (|| {
                check(&context, &path)?;
                let value =
                    operation(&path).map_err(|e| FileError::from_io(e, Some(path.clone())))?;
                check(&context, &path)?;
                Ok(value)
            })();
            let _ = send.send(result);
        });
        Box::pin(async move {
            receive
                .await
                .map_err(|e| FileError::new(FileErrorCode::Unknown, e.to_string(), None))?
        })
    }
}
fn check(context: &Context, path: &Path) -> FileResult<()> {
    context
        .check()
        .map_err(|_| FileError::new(FileErrorCode::Aborted, "aborted", Some(path.into())))
}
fn normalize(path: &Path) -> PathBuf {
    let mut result = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                if matches!(result.components().next_back(), Some(Component::Normal(_))) {
                    result.pop();
                } else if !result.has_root() {
                    result.push("..");
                }
            }
            component => result.push(component.as_os_str()),
        }
    }
    if result.as_os_str().is_empty() {
        result.push(".");
    }
    result
}
impl FileSystem for NativeFileSystem {
    fn id(&self) -> &str {
        "local"
    }
    fn cwd(&self) -> PathBuf {
        self.cwd.read().clone()
    }
    fn absolute_path(&self, path: &Path, _context: &Context) -> FileFuture<PathBuf> {
        let path = self.resolve(path);
        Box::pin(async { Ok(path) })
    }
    fn join_path(&self, parts: &[PathBuf], _context: &Context) -> FileFuture<PathBuf> {
        let mut joined = OsString::new();
        for part in parts.iter().filter(|p| !p.as_os_str().is_empty()) {
            if !joined.is_empty() {
                joined.push(std::path::MAIN_SEPARATOR_STR);
            }
            joined.push(part.as_os_str());
        }
        let path = normalize(Path::new(&joined));
        Box::pin(async { Ok(path) })
    }
    fn read_binary_file(&self, path: &Path, context: &Context) -> FileFuture<Vec<u8>> {
        self.io(self.resolve(path), context, |path| std::fs::read(path))
    }
    fn write_file(&self, path: &Path, content: &[u8], context: &Context) -> FileFuture<()> {
        let content = content.to_vec();
        self.io(self.resolve(path), context, move |path| {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::write(path, content)
        })
    }
    fn append_file(&self, path: &Path, content: &[u8], context: &Context) -> FileFuture<()> {
        let content = content.to_vec();
        self.io(self.resolve(path), context, move |path| {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            let mut file = std::fs::OpenOptions::new()
                .append(true)
                .create(true)
                .open(path)?;
            file.write_all(&content)
        })
    }
    fn truncate_file(&self, path: &Path, size: u64, context: &Context) -> FileFuture<()> {
        if size > crate::MAX_SAFE_INTEGER as u64 {
            let path = self.resolve(path);
            return Box::pin(async move {
                Err(FileError::new(
                    FileErrorCode::Invalid,
                    "File size must be a non-negative safe integer",
                    Some(path),
                ))
            });
        }
        self.io(self.resolve(path), context, move |path| {
            std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(path)?
                .set_len(size)
        })
    }
    fn flush_file(&self, path: &Path, context: &Context) -> FileFuture<()> {
        self.io(self.resolve(path), context, |path| {
            std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(path)?
                .sync_all()
        })
    }
    fn rename_file(&self, source: &Path, destination: &Path, context: &Context) -> FileFuture<()> {
        let destination = self.resolve(destination);
        self.io(self.resolve(source), context, move |source| {
            std::fs::rename(source, destination)
        })
    }
    fn list_dir(&self, path: &Path, context: &Context) -> FileFuture<Vec<FileInfo>> {
        self.io(self.resolve(path), context, |path| {
            let mut result = Vec::new();
            for entry in std::fs::read_dir(path)? {
                let entry = entry?;
                let path = entry.path();
                let metadata = match std::fs::symlink_metadata(&path) {
                    Ok(m) => m,
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                    Err(e) => return Err(e),
                };
                let kind = if metadata.is_file() {
                    FileKind::File
                } else if metadata.is_dir() {
                    FileKind::Directory
                } else if metadata.is_symlink() {
                    FileKind::Symlink
                } else {
                    continue;
                };
                result.push(FileInfo {
                    name: entry.file_name(),
                    path,
                    kind,
                    size: metadata.len(),
                    mtime_ms: metadata
                        .modified()?
                        .duration_since(UNIX_EPOCH)
                        .map_or(0.0, |d| d.as_secs_f64() * 1000.0),
                });
            }
            Ok(result)
        })
    }
    fn create_dir(&self, path: &Path, recursive: bool, context: &Context) -> FileFuture<()> {
        self.io(self.resolve(path), context, move |path| {
            if recursive {
                std::fs::create_dir_all(path)
            } else {
                std::fs::create_dir(path)
            }
        })
    }
    fn remove(
        &self,
        path: &Path,
        recursive: bool,
        force: bool,
        context: &Context,
    ) -> FileFuture<()> {
        self.io(self.resolve(path), context, move |path| {
            let result = if recursive {
                match std::fs::symlink_metadata(path) {
                    Ok(m) if m.is_dir() => std::fs::remove_dir_all(path),
                    _ => std::fs::remove_file(path),
                }
            } else {
                std::fs::remove_file(path)
            };
            match result {
                Err(e) if force && e.kind() == std::io::ErrorKind::NotFound => Ok(()),
                result => result,
            }
        })
    }
}
