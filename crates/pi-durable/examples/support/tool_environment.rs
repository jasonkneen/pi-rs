//! Forward the test host's actual environment, including binary I/O and shell
//! output. This is a capability adapter, not a native process implementation.
use super::*;
use base64::{Engine, engine::general_purpose::STANDARD};
use pi_durable::env::*;
use std::path::{Path, PathBuf};

type OwnedEnv = (u64, Arc<dyn ExecutionEnv>);
#[derive(Default)]
pub(super) struct EnvironmentBridge {
    envs: Mutex<HashMap<usize, OwnedEnv>>,
    outputs: Mutex<HashMap<u64, ShellOutput>>,
}
impl EnvironmentBridge {
    pub fn handle(&self, env: &Arc<dyn ExecutionEnv>) -> Option<u64> {
        self.envs
            .lock()
            .get(&(Arc::as_ptr(env) as *const () as usize))
            .map(|(id, _)| *id)
    }
}
struct HostEnv {
    hub: std::sync::Weak<Hub>,
    handle: u64,
    id: String,
    cwd: PathBuf,
}
impl HostEnv {
    fn call(
        &self,
        method: &str,
        args: JsonValue,
        context: &Context,
    ) -> StorageFuture<Option<JsonValue>> {
        self.hub.upgrade().map_or_else(
            || {
                Box::pin(async { Err(Error::message("Environment host closed")) })
                    as StorageFuture<_>
            },
            |hub| {
                hub.call_context(
                    "environment.call",
                    object([
                        ("environment", encode(&self.handle)),
                        ("method", encode(&method)),
                        ("args", args),
                    ]),
                    context,
                )
            },
        )
    }
    fn file<T: DeserializeOwned + Send + 'static>(
        &self,
        method: &str,
        args: JsonValue,
        context: &Context,
    ) -> FileFuture<T> {
        let operation = self.call(method, args, context);
        Box::pin(async move {
            let result = operation
                .await
                .map_err(|error| FileError::new(FileErrorCode::Unknown, error.to_string(), None))?
                .unwrap_or(JsonValue::Null);
            if result["ok"].as_bool() == Some(true) {
                decode(&result["value"]).map_err(|error| {
                    FileError::new(FileErrorCode::Invalid, error.to_string(), None)
                })
            } else {
                let error = &result["error"];
                let code = match error["code"].to_utf8().as_deref() {
                    Some("aborted") => FileErrorCode::Aborted,
                    Some("not_found") => FileErrorCode::NotFound,
                    Some("permission_denied") => FileErrorCode::PermissionDenied,
                    Some("not_directory") => FileErrorCode::NotDirectory,
                    Some("is_directory") => FileErrorCode::IsDirectory,
                    Some("invalid") => FileErrorCode::Invalid,
                    Some("not_supported") => FileErrorCode::NotSupported,
                    _ => FileErrorCode::Unknown,
                };
                Err(FileError::new(
                    code,
                    error["message"]
                        .to_utf8()
                        .unwrap_or_else(|| "Environment file operation failed".into()),
                    error["path"].to_utf8().map(PathBuf::from),
                ))
            }
        })
    }
    fn execution<T: DeserializeOwned + Send + 'static>(
        &self,
        method: &str,
        args: JsonValue,
        context: &Context,
    ) -> ExecutionFuture<T> {
        let operation = self.call(method, args, context);
        Box::pin(async move {
            let result = operation
                .await
                .map_err(|error| {
                    ExecutionError::new(ExecutionErrorCode::Unknown, error.to_string())
                })?
                .unwrap_or(JsonValue::Null);
            if result["ok"].as_bool() == Some(true) {
                decode(&result["value"]).map_err(|error| {
                    ExecutionError::new(ExecutionErrorCode::Unknown, error.to_string())
                })
            } else {
                let error = &result["error"];
                let code = match error["code"].to_utf8().as_deref() {
                    Some("aborted") => ExecutionErrorCode::Aborted,
                    Some("timeout") => ExecutionErrorCode::Timeout,
                    Some("shell_unavailable") => ExecutionErrorCode::ShellUnavailable,
                    Some("spawn_error") => ExecutionErrorCode::SpawnError,
                    Some("callback_error") => ExecutionErrorCode::CallbackError,
                    _ => ExecutionErrorCode::Unknown,
                };
                let failure = ExecutionError::new(
                    code,
                    error["message"]
                        .to_utf8()
                        .unwrap_or_else(|| "Environment execution failed".into()),
                );
                Err(if let Some(path) = error["spillPath"].to_utf8() {
                    failure.with_spill_path(path.into())
                } else {
                    failure
                })
            }
        })
    }
}
impl FileSystem for HostEnv {
    fn id(&self) -> &str {
        &self.id
    }
    fn cwd(&self) -> PathBuf {
        self.cwd.clone()
    }
    fn absolute_path(&self, path: &Path, context: &Context) -> FileFuture<PathBuf> {
        self.file("absolutePath", encode(&[path]), context)
    }
    fn join_path(&self, parts: &[PathBuf], context: &Context) -> FileFuture<PathBuf> {
        self.file("joinPath", encode(&(parts,)), context)
    }
    fn read_binary_file(&self, path: &Path, context: &Context) -> FileFuture<Vec<u8>> {
        let operation = self.file::<String>("readBinaryFile", encode(&[path]), context);
        Box::pin(async move {
            STANDARD
                .decode(operation.await?)
                .map_err(|error| FileError::new(FileErrorCode::Invalid, error.to_string(), None))
        })
    }
    fn write_file(&self, path: &Path, content: &[u8], context: &Context) -> FileFuture<()> {
        self.file(
            "writeFile",
            encode(&(path, STANDARD.encode(content))),
            context,
        )
    }
    fn append_file(&self, path: &Path, content: &[u8], context: &Context) -> FileFuture<()> {
        self.file(
            "appendFile",
            encode(&(path, STANDARD.encode(content))),
            context,
        )
    }
    fn truncate_file(&self, path: &Path, size: u64, context: &Context) -> FileFuture<()> {
        self.file("truncateFile", encode(&(path, size)), context)
    }
    fn flush_file(&self, path: &Path, context: &Context) -> FileFuture<()> {
        self.file("flushFile", encode(&[path]), context)
    }
    fn rename_file(&self, source: &Path, destination: &Path, context: &Context) -> FileFuture<()> {
        self.file("renameFile", encode(&(source, destination)), context)
    }
    fn list_dir(&self, path: &Path, context: &Context) -> FileFuture<Vec<FileInfo>> {
        let operation = self.file::<Vec<JsonValue>>("listDir", encode(&[path]), context);
        Box::pin(async move {
            operation
                .await?
                .into_iter()
                .map(|value| {
                    let read = || -> Result<FileInfo> {
                        Ok(FileInfo {
                            name: field::<String>(&value, "name")?.into(),
                            path: field(&value, "path")?,
                            kind: match value["kind"].to_utf8().as_deref() {
                                Some("file") => FileKind::File,
                                Some("directory") => FileKind::Directory,
                                Some("symlink") => FileKind::Symlink,
                                _ => return Err(Error::message("Unknown file kind")),
                            },
                            size: field(&value, "size")?,
                            mtime_ms: field(&value, "mtimeMs")?,
                        })
                    };
                    read().map_err(|error| {
                        FileError::new(FileErrorCode::Invalid, error.to_string(), None)
                    })
                })
                .collect()
        })
    }
    fn create_dir(&self, path: &Path, recursive: bool, context: &Context) -> FileFuture<()> {
        self.file(
            "createDir",
            encode(&(path, object([("recursive", encode(&recursive))]))),
            context,
        )
    }
    fn remove(
        &self,
        path: &Path,
        recursive: bool,
        force: bool,
        context: &Context,
    ) -> FileFuture<()> {
        self.file(
            "remove",
            encode(&(
                path,
                object([("recursive", encode(&recursive)), ("force", encode(&force))]),
            )),
            context,
        )
    }
}
impl Shell for HostEnv {
    fn exec(
        &self,
        command: ShellCommand,
        options: ShellExecOptions,
        context: &Context,
    ) -> ExecutionFuture<ShellExecResult> {
        let mut wire = object([
            ("cwd", encode(&options.cwd)),
            ("env", encode(&options.env)),
            ("inheritEnv", encode(&options.inherit_env)),
            ("timeout", encode(&options.timeout)),
            ("spill", encode(&options.spill)),
            ("window", encode(&options.window)),
        ]);
        wire.as_object_mut()
            .unwrap()
            .retain(|_, value| !value.is_null());
        let callback = options.on_output.and_then(|callback| {
            self.hub.upgrade().map(|hub| {
                let id = hub.next.fetch_add(1, Ordering::Relaxed);
                hub.environment.outputs.lock().insert(id, callback);
                id
            })
        });
        if let Some(callback) = callback {
            wire.as_object_mut()
                .unwrap()
                .insert("onOutput".into(), encode(&callback));
        }
        let operation = self.execution::<JsonValue>("exec", encode(&(command, wire)), context);
        let weak = self.hub.clone();
        Box::pin(async move {
            let result = operation.await;
            if let (Some(hub), Some(callback)) = (weak.upgrade(), callback) {
                hub.environment.outputs.lock().remove(&callback);
            }
            let value = result?;
            Ok(ShellExecResult {
                exit_code: field(&value, "exitCode").map_err(|error| {
                    ExecutionError::new(ExecutionErrorCode::Unknown, error.to_string())
                })?,
                spill_path: field(&value, "spillPath").map_err(|error| {
                    ExecutionError::new(ExecutionErrorCode::Unknown, error.to_string())
                })?,
            })
        })
    }
    fn cleanup(&self, context: &Context) -> ExecutionFuture<()> {
        self.execution("cleanup", encode(&Vec::<()>::new()), context)
    }
}
impl Hub {
    pub(super) fn build_environment(
        self: &Arc<Self>,
        harness: u64,
        target: pi_durable::EnvTarget,
        context: Context,
    ) -> StorageFuture<Option<Arc<dyn ExecutionEnv>>> {
        let hub = self.clone();
        Box::pin(async move {
            let conversation = target.conversation_id;
            let cwd = encode(&target.cwd);
            let Some(description) = hub
                .call_context(
                    "environment.build",
                    object([
                        ("harness", encode(&harness)),
                        ("conversationId", encode(&conversation)),
                        ("cwd", cwd),
                    ]),
                    &context,
                )
                .await?
            else {
                return Ok(None);
            };
            let handle = field(&description, "handle")?;
            let env: Arc<dyn ExecutionEnv> = Arc::new(HostEnv {
                hub: Arc::downgrade(&hub),
                handle,
                id: field(&description, "namespace")?,
                cwd: field(&description, "cwd")?,
            });
            hub.environment.envs.lock().insert(
                Arc::as_ptr(&env) as *const () as usize,
                (handle, env.clone()),
            );
            Ok(Some(env))
        })
    }
    pub(super) fn environment_request(
        self: &Arc<Self>,
        _method: &str,
        request: JsonValue,
        context: Context,
    ) -> Result<StorageFuture<Option<JsonValue>>> {
        let callback = self
            .environment
            .outputs
            .lock()
            .get(&field(&request, "callback")?)
            .cloned()
            .ok_or_else(|| Error::message("Unknown shell output callback"))?;
        let info = &request["info"];
        let result = callback(
            field(&request, "text")?,
            context,
            ShellOutputInfo {
                stream: if info["stream"].string_equals("stderr") {
                    ShellStream::Stderr
                } else {
                    ShellStream::Stdout
                },
                skipped: field(info, "skipped")?,
            },
        );
        Ok(Box::pin(async move {
            result.map_err(|error| Error::host("ExecutionError", error.to_string().into()))?;
            Ok(None)
        }))
    }
}
