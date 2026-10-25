//! Test-only process transport. Filesystem and reader behavior lives in env.
use base64::{Engine as _, engine::general_purpose::STANDARD};
use parking_lot::Mutex;
use pi_chord::{
    AbortController,
    context::{create_context_key, with_abort_signal, with_context_value},
};
use pi_durable::{Context, env::*};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    io::{BufRead, Write},
    path::PathBuf,
    sync::{
        Arc, LazyLock,
        atomic::{AtomicU64, Ordering},
    },
};
static REQUEST: LazyLock<pi_chord::context::ContextKey<u64>> =
    LazyLock::new(|| create_context_key("stock environment request"));
type Reply = tokio::sync::oneshot::Sender<Value>;
#[derive(Clone)]
enum Reader {
    Text(Arc<dyn TextLineReader>),
    Binary(Arc<dyn BinaryReader>),
    Directory(Arc<dyn DirReader>),
}
struct Hub {
    next: AtomicU64,
    output: Mutex<std::io::Stdout>,
    pending: Mutex<HashMap<u64, Reply>>,
    contexts: Mutex<HashMap<u64, AbortController>>,
    filesystems: Mutex<HashMap<u64, Arc<NativeFileSystem>>>,
    readers: Mutex<HashMap<u64, Reader>>,
    calls: AtomicU64,
    opens: AtomicU64,
    text_reads: AtomicU64,
    binary_reads: AtomicU64,
}
fn code(value: FileErrorCode) -> &'static str {
    match value {
        FileErrorCode::Aborted => "aborted",
        FileErrorCode::NotFound => "not_found",
        FileErrorCode::PermissionDenied => "permission_denied",
        FileErrorCode::NotDirectory => "not_directory",
        FileErrorCode::IsDirectory => "is_directory",
        FileErrorCode::Invalid => "invalid",
        FileErrorCode::NotSupported => "not_supported",
        FileErrorCode::Unknown => "unknown",
    }
}
fn file_error(value: &Value) -> FileError {
    let kind = match value["code"].as_str() {
        Some("aborted") => FileErrorCode::Aborted,
        Some("not_found") => FileErrorCode::NotFound,
        Some("permission_denied") => FileErrorCode::PermissionDenied,
        Some("not_directory") => FileErrorCode::NotDirectory,
        Some("is_directory") => FileErrorCode::IsDirectory,
        Some("invalid") => FileErrorCode::Invalid,
        Some("not_supported") => FileErrorCode::NotSupported,
        _ => FileErrorCode::Unknown,
    };
    FileError::new(
        kind,
        value["message"]
            .as_str()
            .unwrap_or("Host file operation failed"),
        value["path"].as_str().map(PathBuf::from),
    )
}
fn info(value: FileInfo) -> Value {
    json!({"name":value.name.to_string_lossy(),"path":value.path.to_string_lossy(),"kind":match value.kind{FileKind::File=>"file",FileKind::Directory=>"directory",FileKind::Symlink=>"symlink"},"size":value.size,"mtimeMs":value.mtime_ms})
}
fn parse_info(value: Value) -> FileResult<FileInfo> {
    Ok(FileInfo {
        name: value["name"].as_str().unwrap().into(),
        path: value["path"].as_str().unwrap().into(),
        kind: match value["kind"].as_str() {
            Some("file") => FileKind::File,
            Some("directory") => FileKind::Directory,
            Some("symlink") => FileKind::Symlink,
            _ => {
                return Err(FileError::new(
                    FileErrorCode::Invalid,
                    "Unsupported file type",
                    value["path"].as_str().map(PathBuf::from),
                ));
            }
        },
        size: value["size"].as_u64().unwrap(),
        mtime_ms: value["mtimeMs"].as_f64().unwrap(),
    })
}
fn number(value: &Value) -> f64 {
    value
        .as_f64()
        .or_else(|| value.as_str().and_then(|value| value.parse().ok()))
        .unwrap_or(f64::NAN)
}
fn failed(message: &str) -> FileError {
    FileError::new(FileErrorCode::Invalid, message, None)
}
impl Hub {
    fn emit(&self, value: Value) {
        let mut output = self.output.lock();
        writeln!(output, "{value}").unwrap();
        output.flush().unwrap();
    }
    fn call(&self, method: &str, args: Value, context: &Context) -> FileFuture<Value> {
        let id = self.next.fetch_add(1, Ordering::Relaxed);
        let (send, receive) = tokio::sync::oneshot::channel();
        self.pending.lock().insert(id, send);
        self.emit(json!({"type":"callback","id":id,"method":method,"args":args,"context":context.value(&REQUEST).map(|id|*id)}));
        Box::pin(async move {
            let value = receive.await.map_err(|error| failed(&error.to_string()))?;
            if value["ok"].as_bool() == Some(true) {
                Ok(value["value"].clone())
            } else {
                Err(file_error(&value["error"]))
            }
        })
    }
    async fn handle(self: Arc<Self>, request: Value, context: Context) -> FileResult<Value> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        let method = request["method"].as_str().unwrap();
        if method == "stats" {
            return Ok(
                json!({"opens":self.opens.load(Ordering::Relaxed),"calls":self.calls.load(Ordering::Relaxed),"textReads":self.text_reads.load(Ordering::Relaxed),"binaryReads":self.binary_reads.load(Ordering::Relaxed)}),
            );
        }
        if method == "fs.new" {
            let opener = if request["hostIo"].as_bool() == Some(true) {
                let hub = Arc::downgrade(&self);
                Some(Arc::new(
                    move |path: PathBuf,
                          options: OpenFileOptions,
                          context: Context|
                          -> FileFuture<Arc<dyn FileHandle>> {
                        let Some(owner) = hub.upgrade() else {
                            return Box::pin(async { Err(failed("Environment host is closed")) });
                        };
                        let operation=owner.call("file.open",json!({"path":path,"write":options.write,"regular":options.regular,"noFollow":options.no_follow}),&context);
                        let hub = hub.clone();
                        Box::pin(async move {
                            let id = operation.await?.as_u64().unwrap();
                            Ok(Arc::new(HostFile { hub, id }) as _)
                        })
                    },
                ) as FileOpener)
            } else {
                None
            };
            let cwd = request["cwd"].as_str().unwrap();
            let filesystem = Arc::new(match opener {
                Some(opener) => NativeFileSystem::with_file_opener(cwd, opener)?,
                None => NativeFileSystem::new(cwd)?,
            });
            let id = self.next.fetch_add(1, Ordering::Relaxed);
            let value = json!({"id":id,"namespace":filesystem.id()});
            self.filesystems.lock().insert(id, filesystem);
            self.opens.fetch_add(1, Ordering::Relaxed);
            return Ok(value);
        }
        if method.starts_with("reader.") {
            let id = request["reader"].as_u64().unwrap();
            let reader = self
                .readers
                .lock()
                .get(&id)
                .cloned()
                .ok_or_else(|| failed("Unknown native reader"))?;
            return match (method, reader) {
                ("reader.line", Reader::Text(reader)) => {
                    self.text_reads.fetch_add(1, Ordering::Relaxed);
                    Ok(reader
                        .read_line(&context)
                        .await?
                        .map(|line| json!({"text":line.text,"terminated":line.terminated}))
                        .unwrap_or(Value::Null))
                }
                ("reader.info", Reader::Binary(reader)) => Ok(info(reader.info(&context).await?)),
                ("reader.read", Reader::Binary(reader)) => {
                    self.binary_reads.fetch_add(1, Ordering::Relaxed);
                    Ok(json!(
                        STANDARD.encode(
                            reader
                                .read(
                                    number(&request["offset"]),
                                    number(&request["length"]),
                                    &context
                                )
                                .await?
                        )
                    ))
                }
                ("reader.scan", Reader::Binary(reader)) => Ok(serde_json::to_value(
                    reader
                        .scan_lines(
                            number(&request["start"]),
                            request.get("end").map(number),
                            &context,
                        )
                        .await?,
                )
                .unwrap()),
                ("reader.next", Reader::Directory(reader)) => {
                    let page = reader.next(number(&request["max"]), &context).await?;
                    Ok(
                        json!({"entries":page.entries.into_iter().map(info).collect::<Vec<_>>(),"done":page.done}),
                    )
                }
                ("reader.close", Reader::Text(reader)) => {
                    reader.close(&context).await?;
                    Ok(Value::Null)
                }
                ("reader.close", Reader::Binary(reader)) => {
                    reader.close(&context).await?;
                    Ok(Value::Null)
                }
                ("reader.close", Reader::Directory(reader)) => {
                    reader.close(&context).await?;
                    Ok(Value::Null)
                }
                _ => Err(failed("Unknown reader operation")),
            };
        }
        let filesystem = self
            .filesystems
            .lock()
            .get(&request["fs"].as_u64().unwrap())
            .cloned()
            .ok_or_else(|| failed("Unknown native filesystem"))?;
        let path = PathBuf::from(request["path"].as_str().unwrap_or(""));
        match method {
            "fs.cwd" => {
                filesystem.set_cwd(&path);
                Ok(Value::Null)
            }
            "fs.absolute" => Ok(json!(filesystem.absolute_path(&path, &context).await?)),
            "fs.join" => {
                let parts = request["parts"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|value| PathBuf::from(value.as_str().unwrap()))
                    .collect::<Vec<_>>();
                Ok(json!(filesystem.join_path(&parts, &context).await?))
            }
            "fs.readText" => Ok(json!(filesystem.read_text_file(&path, &context).await?)),
            "fs.readLines" => Ok(json!(
                filesystem
                    .read_text_lines(&path, request.get("max").map(number), &context)
                    .await?
            )),
            "fs.readBinary" => Ok(json!(
                STANDARD.encode(filesystem.read_binary_file(&path, &context).await?)
            )),
            "fs.write" | "fs.append" => {
                let bytes = STANDARD.decode(request["data"].as_str().unwrap()).unwrap();
                if method == "fs.write" {
                    filesystem.write_file(&path, &bytes, &context).await?;
                } else {
                    filesystem.append_file(&path, &bytes, &context).await?;
                }
                Ok(Value::Null)
            }
            "fs.truncate" => {
                let size = number(&request["size"]);
                if !size.is_finite()
                    || size < 0.0
                    || size.fract() != 0.0
                    || size > pi_durable::MAX_SAFE_INTEGER as f64
                {
                    return Err(FileError::new(
                        FileErrorCode::Invalid,
                        "File size must be a non-negative safe integer",
                        Some(filesystem.absolute_path(&path, &context).await?),
                    ));
                }
                filesystem
                    .truncate_file(&path, size as u64, &context)
                    .await?;
                Ok(Value::Null)
            }
            "fs.flush" => {
                filesystem.flush_file(&path, &context).await?;
                Ok(Value::Null)
            }
            "fs.rename" => {
                filesystem
                    .rename_file(
                        &path,
                        &PathBuf::from(request["destination"].as_str().unwrap()),
                        &context,
                    )
                    .await?;
                Ok(Value::Null)
            }
            "fs.info" => Ok(info(filesystem.file_info(&path, &context).await?)),
            "fs.list" => Ok(json!(
                filesystem
                    .list_dir(&path, &context)
                    .await?
                    .into_iter()
                    .map(info)
                    .collect::<Vec<_>>()
            )),
            "fs.canonical" => Ok(json!(filesystem.canonical_path(&path, &context).await?)),
            "fs.exists" => Ok(json!(filesystem.exists(&path, &context).await?)),
            "fs.mkdir" => {
                filesystem
                    .create_dir(
                        &path,
                        request["recursive"].as_bool().unwrap_or(true),
                        &context,
                    )
                    .await?;
                Ok(Value::Null)
            }
            "fs.remove" => {
                filesystem
                    .remove(
                        &path,
                        request["recursive"].as_bool().unwrap_or(false),
                        request["force"].as_bool().unwrap_or(false),
                        &context,
                    )
                    .await?;
                Ok(Value::Null)
            }
            "fs.tempDir" => Ok(json!(
                filesystem
                    .create_temp_dir(request["prefix"].as_str(), &context)
                    .await?
            )),
            "fs.tempFile" => Ok(json!(
                filesystem
                    .create_temp_file(
                        request["prefix"].as_str(),
                        request["suffix"].as_str(),
                        &context
                    )
                    .await?
            )),
            "fs.cleanup" => Ok(Value::Null),
            "fs.openText" | "fs.openBinary" | "fs.openDir" => {
                let reader = match method {
                    "fs.openText" => {
                        Reader::Text(filesystem.open_text_line_reader(&path, &context).await?)
                    }
                    "fs.openBinary" => Reader::Binary(
                        filesystem
                            .open_binary_reader(
                                &path,
                                request["noFollow"].as_bool().unwrap_or(false),
                                &context,
                            )
                            .await?,
                    ),
                    _ => Reader::Directory(filesystem.open_dir_reader(&path, &context).await?),
                };
                let id = self.next.fetch_add(1, Ordering::Relaxed);
                self.readers.lock().insert(id, reader);
                Ok(json!(id))
            }
            _ => Err(failed("Unknown native filesystem operation")),
        }
    }
}
struct HostFile {
    hub: std::sync::Weak<Hub>,
    id: u64,
}
impl HostFile {
    fn call(&self, method: &str, args: Value, context: &Context) -> FileFuture<Value> {
        match self.hub.upgrade() {
            Some(hub) => hub.call(method, args, context),
            None => Box::pin(async { Err(failed("Environment host is closed")) }),
        }
    }
}
impl FileHandle for HostFile {
    fn info(&self, context: &Context) -> FileFuture<FileInfo> {
        let operation = self.call("file.info", json!({"file":self.id}), context);
        Box::pin(async move { parse_info(operation.await?) })
    }
    fn read(&self, offset: u64, length: usize, context: &Context) -> FileFuture<Vec<u8>> {
        let operation = self.call(
            "file.read",
            json!({"file":self.id,"offset":offset,"length":length}),
            context,
        );
        Box::pin(async move {
            STANDARD
                .decode(operation.await?.as_str().unwrap())
                .map_err(|error| failed(&error.to_string()))
        })
    }
    fn sync(&self, context: &Context) -> FileFuture<()> {
        let operation = self.call("file.sync", json!({"file":self.id}), context);
        Box::pin(async move {
            operation.await?;
            Ok(())
        })
    }
    fn close(&self, context: &Context) -> FileFuture<()> {
        let operation = self.call("file.close", json!({"file":self.id}), context);
        Box::pin(async move {
            operation.await?;
            Ok(())
        })
    }
}
#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let hub = Arc::new(Hub {
        next: AtomicU64::new(1_000_000),
        output: Mutex::new(std::io::stdout()),
        pending: Mutex::new(HashMap::new()),
        contexts: Mutex::new(HashMap::new()),
        filesystems: Mutex::new(HashMap::new()),
        readers: Mutex::new(HashMap::new()),
        calls: AtomicU64::new(0),
        opens: AtomicU64::new(0),
        text_reads: AtomicU64::new(0),
        binary_reads: AtomicU64::new(0),
    });
    let runtime = tokio::runtime::Handle::current();
    tokio::task::spawn_blocking(move||{
        for line in std::io::stdin().lock().lines(){
            let request:Value=serde_json::from_str(&line.unwrap()).unwrap();let id=request["id"].as_u64().unwrap();
            match request["type"].as_str(){
                Some("callback.result")=>{if let Some(send)=hub.pending.lock().remove(&id){let _=send.send(request["value"].clone());}continue;},
                Some("abort")=>{if let Some(controller)=hub.contexts.lock().get(&id){controller.abort();}continue;},
                _=>{},
            }
            let controller=AbortController::new();if request.get("abortInitially").is_some(){controller.abort();}
            let context=with_context_value(&REQUEST,id,&with_abort_signal(controller.signal(),&Context::default()));
            hub.contexts.lock().insert(id,controller);
            let hub=hub.clone();runtime.spawn(async move{
                let result=hub.clone().handle(request,context).await;
                hub.emit(match result {Ok(value)=>json!({"id":id,"value":value}),Err(error)=>json!({"id":id,"error":{"kind":"file","name":"FileError","code":code(error.code()),"message":error.to_string(),"path":error.path()}})});
                hub.contexts.lock().remove(&id);
            });
        }
        for (_,send) in hub.pending.lock().drain(){let _=send.send(json!({"ok":false,"error":{"message":"Host closed"}}));}
    }).await.unwrap();
}
