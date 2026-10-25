//! Callback transport for unmodified upstream task tests. This process owns the
//! Session, scheduler and invocation state. JS supplies the original task code
//! and original test backend; no scheduling decisions are made in the adapter.
use indexmap::IndexMap;
use parking_lot::Mutex;
use pi_chord::context::{ContextKey, create_context_key, with_context_value};
use pi_chord::{
    AbortController,
    context::{AbortError, AbortReason as SignalReason, with_abort_signal},
};
use pi_durable::{
    harness::{runtime::TaskRuntime as NativeRuntime, scheduler::*, state::NextTaskState},
    storage::{Adopt, StorageFuture},
    *,
};
use serde::{Serialize, de::DeserializeOwned};
use std::{
    collections::{HashMap, HashSet},
    io::{BufRead, Write},
    sync::{
        Arc, LazyLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};
#[path = "support/compaction_bridge.rs"]
mod compaction_bridge;
#[path = "support/context_bridge.rs"]
mod context_bridge;
#[path = "support/document_bridge.rs"]
mod document_bridge;
#[path = "support/task_graph_bridge.rs"]
mod graph_bridge;
#[path = "support/memory_bridge.rs"]
mod memory_bridge;
#[path = "support/prompt_bridge.rs"]
mod prompt_bridge;
#[path = "support/registry_bridge.rs"]
mod registry_bridge;
#[path = "support/task_remote_storage.rs"]
mod remote_storage;
#[path = "support/submission_bridge.rs"]
mod submission_bridge;
#[path = "support/tool_bridge.rs"]
mod tool_bridge;
#[path = "support/tool_environment.rs"]
mod tool_environment;
#[path = "support/view_bridge.rs"]
mod view_bridge;
static CALL_CONTEXT: LazyLock<ContextKey<u64>> =
    LazyLock::new(|| create_context_key("stock-test context"));
static HARNESS_CONTEXT: LazyLock<ContextKey<u64>> =
    LazyLock::new(|| create_context_key("stock-test harness"));
fn native_now() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as f64
}
fn encode(value: &impl Serialize) -> JsonValue {
    pi_chord::json::to_value(value).unwrap()
}
fn object<const N: usize>(fields: [(&str, JsonValue); N]) -> JsonValue {
    JsonValue::Object(
        fields
            .into_iter()
            .map(|(key, value)| (key.into(), value))
            .collect(),
    )
}
fn decode<T: DeserializeOwned>(value: &JsonValue) -> Result<T> {
    serde_json::from_str(&value.to_string()).map_err(Error::from)
}
fn field<T: DeserializeOwned>(value: &JsonValue, key: &str) -> Result<T> {
    decode(&value[key])
}
fn error_value(error: &Error) -> JsonValue {
    let mut value = object([
        ("name", encode(&error.name())),
        ("message", encode(&error.message_text())),
        (
            "aborted",
            encode(&matches!(error.kind(), ErrorKind::Aborted(_))),
        ),
    ]);
    if let ErrorKind::SessionFailed { cause } | ErrorKind::DocumentCopy { cause, .. } = error.kind()
    {
        value
            .as_object_mut()
            .unwrap()
            .insert("cause".into(), error_value(cause));
    }
    value
}
type TaskRuntime = NativeRuntime<JsonValue, JsonValue, JsonValue, u64>;
type JsonTask = Task<JsonValue, JsonValue, JsonValue, u64>;
type ErasedRuntime = NativeRuntime<JsonValue, JsonValue, JsonValue, ()>;
enum Pending {
    Async(tokio::sync::oneshot::Sender<Result<Option<JsonValue>>>),
    Sync(std::sync::mpsc::Sender<Result<Option<JsonValue>>>),
}
struct Hub {
    output: Mutex<std::io::Stdout>,
    next: AtomicU64,
    pending: Mutex<HashMap<u64, Pending>>,
    host_pending: Mutex<HashSet<u64>>,
    phase_serials: Mutex<HashMap<u64, u64>>,
    changed: tokio::sync::Notify,
    input_closed: AtomicBool,
    adoptions: Mutex<HashMap<u64, Adopt>>,
    schedulers: Mutex<HashMap<u64, TaskScheduler>>,
    readers: Mutex<HashMap<u64, Arc<dyn pi_durable::harness::reader::RegistryReader>>>,
    tasks: Mutex<HashMap<u64, JsonTask>>,
    runtimes: Mutex<HashMap<u64, TaskRuntime>>,
    native_runtimes: Mutex<HashMap<u64, ErasedRuntime>>,
    tool: tool_bridge::ToolBridge,
    environment: tool_environment::EnvironmentBridge,
    host_timers: Mutex<HashMap<u64, DisarmTimer>>,
    transactions: Mutex<HashMap<u64, Transaction>>,
    contexts: Mutex<HashMap<u64, AbortController>>,
    phases: AtomicU64,
    commits: AtomicU64,
    context_reads: AtomicU64,
    compaction_reads: AtomicU64,
    registry_reads: AtomicU64,
    agent_reads: AtomicU64,
    agent_resolutions: AtomicU64,
    hook_calls: AtomicU64,
    prompt: prompt_bridge::PromptBridge,
    registry: registry_bridge::RegistryBridge,
    graph: graph_bridge::GraphBridge,
    views: view_bridge::ViewBridge,
    memory: memory_bridge::MemoryBridge,
    documents: document_bridge::DocumentBridge,
    submissions: submission_bridge::SubmissionBridge,
    inspections: AtomicU64,
    ranges: Mutex<HashMap<u64, Arc<pi_durable::harness::context::ContextRange>>>,
}
impl Hub {
    fn emit(&self, value: JsonValue) {
        let mut output = self.output.lock();
        writeln!(output, "{value}").unwrap();
        output.flush().unwrap();
        drop(output);
        self.changed.notify_waiters();
    }
    fn call(&self, method: &str, args: JsonValue) -> StorageFuture<Option<JsonValue>> {
        self.call_context(method, args, &Context::default())
    }
    fn call_context(
        &self,
        method: &str,
        args: JsonValue,
        context: &Context,
    ) -> StorageFuture<Option<JsonValue>> {
        let id = self.next.fetch_add(1, Ordering::Relaxed);
        let (send, receive) = tokio::sync::oneshot::channel();
        {
            let mut pending = self.pending.lock();
            if self.input_closed.load(Ordering::Acquire) {
                return Box::pin(async { Err(Error::message("Test transport is closed")) });
            }
            pending.insert(id, Pending::Async(send));
            if method != "phase" {
                self.host_pending.lock().insert(id);
            }
        }
        self.emit(object([
            ("type", encode(&"callback")),
            ("id", encode(&id)),
            ("method", encode(&method)),
            ("args", args),
            (
                "context",
                encode(&context.value(&CALL_CONTEXT).map(|id| *id)),
            ),
            ("abortable", encode(&context.abort_signal().is_some())),
        ]));
        Box::pin(async move {
            receive
                .await
                .map_err(|error| Error::message(error.to_string()))?
        })
    }
    fn sync(&self, method: &str, args: JsonValue) -> Result<JsonValue> {
        let id = self.next.fetch_add(1, Ordering::Relaxed);
        let (send, receive) = std::sync::mpsc::channel();
        {
            let mut pending = self.pending.lock();
            if self.input_closed.load(Ordering::Acquire) {
                return Err(Error::message("Test transport is closed"));
            }
            pending.insert(id, Pending::Sync(send));
        }
        self.emit(object([
            ("type", encode(&"callback")),
            ("id", encode(&id)),
            ("method", encode(&method)),
            ("args", args),
            ("sync", encode(&true)),
        ]));
        receive
            .recv()
            .map_err(|error| Error::message(error.to_string()))??
            .ok_or_else(|| Error::message("Task callback returned undefined"))
    }
    fn response(&self, value: JsonValue) {
        let id: u64 = field(&value, "id").unwrap();
        self.host_pending.lock().remove(&id);
        let result = if value["error"].is_object() {
            Err(Error::host(
                value["error"]["name"]
                    .to_utf8()
                    .unwrap_or_else(|| "Error".into()),
                value["error"]["message"]
                    .as_string()
                    .cloned()
                    .unwrap_or_else(|| "undefined".into()),
            ))
        } else if value["missing"].as_bool() == Some(true) {
            Ok(None)
        } else {
            Ok(Some(value["value"].clone()))
        };
        if let Some(pending) = self.pending.lock().remove(&id) {
            match pending {
                Pending::Async(send) => {
                    let _ = send.send(result);
                }
                Pending::Sync(send) => {
                    let _ = send.send(result);
                }
            }
        }
    }
    fn definition(self: &Arc<Self>, description: &JsonValue) -> Result<JsonTask> {
        let id: u64 = field(description, "id")?;
        if let Some(task) = self.tasks.lock().get(&id).cloned() {
            return Ok(task);
        }
        let phases: Vec<JsonString> = field(description, "phases")?;
        let name: JsonString = field(description, "name")?;
        let version: u64 = field(description, "version")?;
        let mut handlers = IndexMap::new();
        for phase in phases {
            let hub = self.clone();
            let name = phase.clone();
            handlers.insert(
                phase,
                Arc::new(move |task: RunningTask, runtime: TaskRuntime, context| {
                    hub.phase(id, name.clone(), task, runtime, context)
                }) as PhaseHandler<JsonValue, JsonValue, JsonValue, u64>,
            );
        }
        let hub = self.clone();
        let initial = Arc::new(move |input: &JsonValue| hub.sync("initial", encode(&(id, input))));
        let hub = self.clone();
        let abort = Arc::new(move |task, runtime, context| {
            hub.phase(id, "$abort".into(), task, runtime, context)
        }) as PhaseHandler<JsonValue, JsonValue, JsonValue, u64>;
        let migrate = if description["migrate"].as_bool() == Some(true) {
            let hub = self.clone();
            Some(Arc::new(move |input, checkpoint, version| {
                let value = hub.sync("migrate", encode(&(id, input, checkpoint, version)))?;
                Ok((value["input"].clone(), value["checkpoint"].clone()))
            })
                as pi_durable::tasks::TaskMigration<JsonValue, JsonValue>)
        } else {
            None
        };
        let task = define_task(TaskDefinition {
            name,
            version,
            initial,
            phases: handlers,
            abort,
            migrate,
            hooks: None,
        });
        self.tasks.lock().insert(id, task.clone());
        Ok(task)
    }
    fn phase(
        self: &Arc<Self>,
        definition: u64,
        phase: JsonString,
        task: RunningTask,
        runtime: TaskRuntime,
        context: Context,
    ) -> StorageFuture<()> {
        self.phases.fetch_add(1, Ordering::Relaxed);
        let existing = self
            .runtimes
            .lock()
            .iter()
            .find(|(_, held)| held.same_invocation(&runtime))
            .map(|(id, _)| *id);
        let id = existing.unwrap_or_else(|| {
            let id = self.next.fetch_add(1, Ordering::Relaxed);
            self.runtimes.lock().insert(id, runtime.clone());
            id
        });
        if existing.is_none() {
            let hub = self.clone();
            let signal = runtime.signal();
            tokio::spawn(async move {
                signal.cancelled().await;
                let message = signal.reason().unwrap().waiter_error().message;
                hub.emit(encode(
                    &serde_json::json!({"type":"signal","runtime":id,"message":message}),
                ));
            });
        }
        let serial = {
            let mut serials = self.phase_serials.lock();
            let serial = serials.entry(id).or_default();
            *serial += 1;
            *serial
        };
        let call = self.call(
            "phase",
            encode(&(
                definition,
                phase,
                task.record(),
                id,
                serial,
                context.value(&HARNESS_CONTEXT).map(|id| *id),
            )),
        );
        Box::pin(async move {
            call.await?;
            Ok(())
        })
    }
    fn callback(
        self: &Arc<Self>,
        callback: u64,
        tx: Transaction,
        current: Option<TaskRecord>,
    ) -> StorageFuture<Option<JsonValue>> {
        let id = self.next.fetch_add(1, Ordering::Relaxed);
        self.transactions.lock().insert(id, tx);
        let operation = self.call(
            "change",
            object([
                ("callback", encode(&callback)),
                ("tx", encode(&id)),
                ("current", encode(&current)),
            ]),
        );
        let hub = self.clone();
        Box::pin(async move {
            let result = operation.await;
            hub.transactions.lock().remove(&id);
            result
        })
    }
    fn scheduler(&self, request: &JsonValue) -> Result<TaskScheduler> {
        self.schedulers
            .lock()
            .get(&field::<u64>(request, "harness")?)
            .cloned()
            .ok_or_else(|| Error::message("Unknown test harness"))
    }
    fn transaction(&self, request: &JsonValue) -> Result<Transaction> {
        self.transactions
            .lock()
            .get(&field::<u64>(request, "tx")?)
            .cloned()
            .ok_or_else(|| Error::message("Unknown transaction"))
    }
    fn runtime(&self, request: &JsonValue) -> Result<ErasedRuntime> {
        if let Some(runtime) = self
            .native_runtimes
            .lock()
            .get(&field::<u64>(request, "runtime")?)
            .cloned()
        {
            return Ok(runtime);
        }
        self.runtimes
            .lock()
            .get(&field::<u64>(request, "runtime")?)
            .map(NativeRuntime::erased)
            .ok_or_else(|| Error::message("Unknown test runtime"))
    }
    fn context(&self, id: u64) -> Context {
        let controller = AbortController::new();
        let context = with_abort_signal(controller.signal(), &Context::default());
        self.contexts.lock().insert(id, controller);
        with_context_value(&CALL_CONTEXT, id, &context)
    }
    fn begin(
        self: Arc<Self>,
        request: JsonValue,
        context: Context,
    ) -> Result<StorageFuture<Option<JsonValue>>> {
        fn mapped<T: Serialize + Send + 'static>(
            operation: StorageFuture<T>,
        ) -> StorageFuture<Option<JsonValue>> {
            Box::pin(async move { Ok(Some(encode(&operation.await?))) })
        }
        let method: String = field(&request, "method")?;
        if method.starts_with("memory.") {
            return self.memory_request(&method, request);
        }
        if method.starts_with("view.") {
            return self.view_request(&method, request, context);
        }
        if method.starts_with("environment.") {
            return self.environment_request(&method, request, context);
        }
        if method.starts_with("tool.") {
            return self.tool_request(&method, request, context);
        }
        if method.starts_with("submission.") {
            return self.submission_request(&method, request, context);
        }
        if method.starts_with("doc.") {
            return self.document_request(&method, request, context);
        }
        if method.starts_with("graph.") {
            return self.graph_request(&method, request, context);
        }
        if method.starts_with("compaction.") {
            let value = self.compaction_request(&request);
            return Ok(Box::pin(async move { value }));
        }
        if method.starts_with("prompt.") {
            return self.prompt_request(&method, request, context);
        }
        if method.starts_with("context.") {
            return self.context_request(&method, request, context);
        }
        if method.starts_with("registry.") {
            return Ok(Box::pin(async move {
                tokio::task::spawn_blocking(move || self.registry_request(&method, request))
                    .await
                    .map_err(|error| Error::message(error.to_string()))?
            }));
        }
        Ok(match method.as_str() {
            "open" => Box::pin(async move {
                let harness: u64 = field(&request, "id")?;
                let handle: u64 = field(&request, "storage")?;
                let registry = self.registry_reader(&request)?;
                self.readers.lock().insert(harness, registry.clone());
                let report = self.clone();
                let session_report = self.clone();
                let clock = self.clone();
                let settings = self.clone();
                let resolver = self.clone();
                let environment = self.clone();
                let has_environment = request["environment"].as_bool() == Some(true);
                let host_clock = request["clock"].as_bool() == Some(true);
                let host_settings = request["settings"].as_bool() == Some(true);
                let storage: Arc<dyn Storage> = if let Some(id) = request["nativeMemory"].as_u64() {
                    self.memory.direct_opens.fetch_add(1, Ordering::Relaxed);
                    self.memory_storage(id)?
                } else {
                    Arc::new(remote_storage::RemoteStorage {
                        hub: self.clone(),
                        handle,
                    })
                };
                let scheduler=TaskScheduler::open(storage,registry,SchedulerOptions{
                    env:Arc::new(move |conversation,context|if has_environment {environment.build_environment(harness,conversation,context)}else{Box::pin(async {Ok(None)})}),
                    settings:Arc::new(move || if host_settings {registry_bridge::settings(&settings.sync("settings",encode(&harness))?)} else {Ok(pi_durable::harness::settings::resolve_settings(Default::default()))}),
                    settle_outcome:Arc::new(|tx,record,outcome|Box::pin(pi_durable::harness::live::settle_scheduler_outcome(tx,record,outcome))),withdraw_inputs:Arc::new(|tx,conversation|Box::pin(async move {pi_durable::harness::inbox::withdraw_queued_inputs(&tx,conversation,"aborted").await})),agent:Arc::new(move |request| {resolver.agent_resolutions.fetch_add(1,Ordering::Relaxed);pi_durable::harness::agent::resolve_agent_in(request)}),
                    on_report:Arc::new(move|error,info|report.emit(encode(&serde_json::json!({"type":"report","harness":harness,"error":{"name":error.name(),"message":error.to_string()},"info":{"type":info.kind,"taskId":info.task,"reason":info.reason,"callback":info.callback,"extension":info.extension,"conversationId":info.conversation}})))),..Default::default()
                },SessionOptions{report:Arc::new(move |error| session_report.emit(object([("type",encode(&"report")),("harness",encode(&harness)),("error",error_value(&error)),("info",encode(&serde_json::json!({"type":"session_failed"})))]))),conversation_created:Some(Arc::new(|tx,record|Box::pin(async move { pi_durable::harness::create_conversation_documents(&tx,record).await }))),now:Arc::new(move||if host_clock {decode(&clock.sync("now",encode(&harness)).unwrap()).unwrap()} else {native_now()}),document_retention_ms:field(&request,"retention")?,..Default::default()},&with_context_value(&HARNESS_CONTEXT,harness,&context)).await?;
                let resume = scheduler.clone();
                let clock = self.clone();
                let settings = self.clone();
                let submissions = Submissions::new(
                    scheduler.session().clone(),
                    SubmissionsOptions {
                        now: Arc::new(move || {
                            if host_clock {
                                decode(&clock.sync("now", encode(&harness)).unwrap()).unwrap()
                            } else {
                                native_now()
                            }
                        }),
                        queue_modes: Arc::new(move || {
                            if host_settings {
                                registry_bridge::settings(
                                    &settings.sync("settings", encode(&harness))?,
                                )
                                .map(|settings| QueueModes::from(&settings))
                            } else {
                                Ok(QueueModes::from(
                                    &pi_durable::harness::settings::resolve_settings(
                                        Default::default(),
                                    ),
                                ))
                            }
                        }),
                        resume: Arc::new(move || resume.resume()),
                        telemetry: pi_telemetry::noop_telemetry_context(),
                    },
                )?;
                self.submissions
                    .services
                    .lock()
                    .insert(harness, submissions);
                self.schedulers.lock().insert(harness, scheduler);
                Ok(Some(encode(&harness)))
            }),
            "root" => {
                let scheduler = self.scheduler(&request)?;
                mapped(scheduler.session().commit(
                    move |tx| async move {
                        if let Some(root) = tx.conversation(ROOT_CONVERSATION_ID).await? {
                            return Ok(root.id);
                        }
                        let root = tx.create_root_conversation().await?;
                        if request["options"]["agent"].is_object() {
                            pi_durable::harness::agent::configure(
                                &tx,
                                root.id,
                                field(&request["options"], "agent")?,
                            )
                            .await?;
                        }
                        Ok(root.id)
                    },
                    &context,
                ))
            }
            "inspect" => {
                self.inspections.fetch_add(1, Ordering::Relaxed);
                mapped(self.scheduler(&request)?.inspect(&context))
            }
            "conversationAgent" => {
                let scheduler = self.scheduler(&request)?;
                let harness: u64 = field(&request, "harness")?;
                let conversation = field(&request, "conversation")?;
                let reader = self
                    .readers
                    .lock()
                    .get(&harness)
                    .cloned()
                    .ok_or_else(|| Error::message("Unknown reader"))?;
                let hub = self.clone();
                let settings = self.clone();
                let report = self.clone();
                Box::pin(async move {
                    let request = pi_durable::harness::agent::AgentRequest {
                        session: scheduler.session().clone(),
                        conversation,
                        snapshot: reader.snapshot()?,
                        settings: Arc::new(move || {
                            registry_bridge::settings(&settings.sync("settings", encode(&harness))?)
                        }),
                        report: Arc::new(move |error, extension| {
                            report.emit(object([
                                ("type", encode(&"report")),
                                ("harness", encode(&harness)),
                                ("error", error_value(&error)),
                                (
                                    "info",
                                    object([
                                        ("type", encode(&"extension_error")),
                                        ("extension", encode(&extension)),
                                    ]),
                                ),
                            ]))
                        }),
                        context,
                    };
                    let agent = pi_durable::harness::agent::resolve_agent_in(request)
                        .await?
                        .typed::<registry_bridge::ToolHandle, registry_bridge::SectionHandle>()?;
                    Ok(Some(hub.encode_agent(agent)))
                })
            }
            "conversation" => {
                let id = field(&request, "conversation")?;
                mapped(
                    self.scheduler(&request)?
                        .session()
                        .commit(move |tx| async move { tx.conversation(id).await }, &context),
                )
            }
            "conversation.abort" => mapped(self.scheduler(&request)?.abort_conversation(
                field(&request, "conversation")?,
                request["background"].as_bool().unwrap_or(false),
                &context,
            )),
            "tx.task" => {
                let operation = self.transaction(&request)?.task(field(&request, "task")?);
                Box::pin(async move { Ok(operation.await?.map(|task| encode(&task))) })
            }
            "tx.setTask" => {
                self.transaction(&request)?
                    .set_task(field(&request, "record")?)?;
                Box::pin(async { Ok(None) })
            }
            "tx.createConversation" | "tx.forkConversation" => {
                let ownership = match request["options"]["ownership"]["kind"].to_utf8().as_deref() {
                    Some("ownerless") => ConversationOwnership::Ownerless,
                    Some("task") => ConversationOwnership::Task(field(
                        &request["options"]["ownership"],
                        "taskId",
                    )?),
                    _ => return Err(Error::message("Unknown conversation ownership")),
                };
                let tx = self.transaction(&request)?;
                if method == "tx.createConversation" {
                    Box::pin(async move {
                        let record = tx.create_conversation(ownership).await?;
                        if request["options"]["agent"].is_object() {
                            pi_durable::harness::agent::configure(
                                &tx,
                                record.id,
                                field(&request["options"], "agent")?,
                            )
                            .await?;
                        }
                        Ok(Some(encode(&record)))
                    })
                } else {
                    mapped(tx.fork_conversation(
                        field(&request, "conversation")?,
                        field(&request, "at")?,
                        ownership,
                    ))
                }
            }
            "createConversation" => {
                let scheduler = self.scheduler(&request)?;
                let ownership = match request["options"]["ownership"]["kind"].to_utf8().as_deref() {
                    Some("ownerless") => ConversationOwnership::Ownerless,
                    Some("task") => ConversationOwnership::Task(field(
                        &request["options"]["ownership"],
                        "taskId",
                    )?),
                    _ => return Err(Error::message("Unknown conversation ownership")),
                };
                mapped(scheduler.session().commit(
                    move |tx| async move {
                        let record = tx.create_conversation(ownership).await?;
                        if request["options"]["agent"].is_object() {
                            pi_durable::harness::agent::configure(
                                &tx,
                                record.id,
                                field(&request["options"], "agent")?,
                            )
                            .await?;
                        }
                        Ok(record.id)
                    },
                    &context,
                ))
            }
            "waitForIdle" => {
                let scheduler = self.scheduler(&request)?;
                scheduler.resume()?;
                mapped(scheduler.wait_for_idle(field(&request, "conversation")?, &context))
            }
            "appendEntry" => {
                let tx = self.transaction(&request)?;
                let draft = &request["entry"];
                let mut entry = EntryDraft::new(field::<JsonString>(draft, "kind")?);
                entry.model = field(draft, "model")?;
                entry.head = if draft["head"].is_null() {
                    None
                } else if draft["head"].string_equals("self") {
                    Some(EntryHead::ThisEntry)
                } else {
                    Some(EntryHead::Entry(field(draft, "head")?))
                };
                entry.edits = field(draft, "edits")?;
                entry.data = draft
                    .as_object()
                    .and_then(|draft| draft.get(&JsonString::from("data")))
                    .cloned();
                mapped(tx.append_entry(field(&request, "conversation")?, entry))
            }
            "scanTasks" => mapped(self.transaction(&request)?.scan_tasks(
                field(&request, "query")?,
                field(&request, "limit")?,
                field(&request, "cursor")?,
            )),
            "tx.latestHeadMarker" => {
                let operation = self
                    .transaction(&request)?
                    .latest_head_marker(field(&request, "conversation")?);
                Box::pin(async move { Ok(operation.await?.map(|entry| encode(&entry))) })
            }
            "entry" => {
                let operation = self.transaction(&request)?.entry(field(&request, "entry")?);
                let kind: Option<JsonString> = field(&request, "kind")?;
                Box::pin(async move {
                    Ok(operation
                        .await?
                        .filter(|entry| kind.as_ref().is_none_or(|kind| entry.kind == *kind))
                        .map(|entry| encode(&entry)))
                })
            }
            "createSubmission" => {
                let operation = self
                    .transaction(&request)?
                    .create_submission(field(&request, "record")?);
                Box::pin(async move { Ok(Some(encode(&operation.await?))) })
            }
            "settleSubmission" => {
                self.transaction(&request)?.settle_submission(
                    field(&request, "submission")?,
                    field(&request, "settlement")?,
                )?;
                Box::pin(async { Ok(None) })
            }
            "scanEntries" => {
                let operation = self.transaction(&request)?.scan_entries(
                    field(&request, "query")?,
                    field(&request, "limit")?,
                    field(&request, "cursor")?,
                );
                Box::pin(async move { Ok(Some(encode(&operation.await?))) })
            }
            "entries" => {
                let scheduler = self.scheduler(&request)?;
                let mut query = request["query"].clone();
                query
                    .as_object_mut()
                    .unwrap()
                    .insert("conversationId".into(), request["conversation"].clone());
                let query: EntryQuery = decode(&query)?;
                let limit = field(&request, "limit")?;
                let cursor = field(&request, "cursor")?;
                let operation = scheduler.session().commit(
                    move |tx| async move { tx.scan_entries(query, limit, cursor).await },
                    &context,
                );
                Box::pin(async move { Ok(Some(encode(&operation.await?))) })
            }
            "runtimeContext" => {
                self.context_reads.fetch_add(1, Ordering::Relaxed);
                mapped(self.runtime(&request)?.context(
                    field(&request, "conversation")?,
                    field(&request, "at")?,
                    &context,
                ))
            }
            "phase.turn" => {
                let runtime = self.runtime(&request)?;
                let id: u64 = field(&request, "runtime")?;
                let serial: u64 = field(&request, "serial")?;
                Box::pin(async move {
                    loop {
                        let changed = self.changed.notified();
                        tokio::pin!(changed);
                        changed.as_mut().enable();
                        // The source continues its synchronous/microtask work
                        // before a host timer. Yield once the invocation ends,
                        // another phase starts, or a host promise can block it.
                        if runtime.signal().aborted()
                            || self
                                .phase_serials
                                .lock()
                                .get(&id)
                                .is_some_and(|current| *current > serial)
                            || !self.host_pending.lock().is_empty()
                        {
                            break;
                        }
                        changed.await;
                    }
                    Ok(None)
                })
            }
            "runtimeTimer" => {
                let runtime = self.runtime(&request)?;
                let callback: u64 = field(&request, "callback")?;
                let timer = self.next.fetch_add(1, Ordering::Relaxed);
                let hub = self.clone();
                let disarm = runtime.set_timer(
                    move || {
                        let operation = hub.call("timerFire", encode(&callback));
                        tokio::spawn(async move {
                            let _ = operation.await;
                        });
                    },
                    field(&request, "delay")?,
                );
                self.host_timers.lock().insert(timer, disarm);
                Box::pin(async move { Ok(Some(encode(&timer))) })
            }
            "runtimeDisarmTimer" => {
                if let Some(disarm) = self.host_timers.lock().remove(&field(&request, "timer")?) {
                    disarm();
                }
                Box::pin(async { Ok(None) })
            }
            "runtimeEnv" => {
                let operation = self.runtime(&request)?.env(&context);
                let hub = self.clone();
                Box::pin(async move {
                    Ok(operation
                        .await?
                        .and_then(|env| hub.environment.handle(&env))
                        .map(|id| encode(&id)))
                })
            }
            "runtimeSettings" => {
                let runtime = self.runtime(&request)?;
                Box::pin(async move {
                    tokio::task::spawn_blocking(move || {
                        Ok(Some(registry_bridge::settings_value(&runtime.settings()?)))
                    })
                    .await
                    .map_err(|error| Error::message(error.to_string()))?
                })
            }
            "runtimeEntry" => {
                let operation = self
                    .runtime(&request)?
                    .entry(field(&request, "entry")?, &context);
                Box::pin(async move {
                    Ok(operation
                        .await?
                        .filter(|entry| {
                            request["kind"].is_null()
                                || entry.kind == field::<JsonString>(&request, "kind").unwrap()
                        })
                        .map(|entry| encode(&entry)))
                })
            }
            "runtimeNow" | "runtimeReport" => {
                let runtime = self.runtime(&request)?;
                Box::pin(async move {
                    tokio::task::spawn_blocking(move || {
                        if method == "runtimeNow" {
                            Ok(Some(encode(&runtime.now()?)))
                        } else {
                            runtime
                                .report(Error::message(field::<String>(&request, "message")?))?;
                            Ok(None)
                        }
                    })
                    .await
                    .map_err(|error| Error::message(error.to_string()))?
                })
            }
            "sleepUntil" => mapped(self.runtime(&request)?.sleep_until(
                field(&request, "until")?,
                field(&request, "checkpoint")?,
                &context,
            )),
            "runtimeAgent" => {
                self.agent_reads.fetch_add(1, Ordering::Relaxed);
                let operation = self.runtime(&request)?.agent(&context);
                let hub = self.clone();
                Box::pin(async move {
                    let agent = operation
                        .await?
                        .typed::<registry_bridge::ToolHandle, registry_bridge::SectionHandle>()?;
                    Ok(Some(hub.encode_agent(agent)))
                })
            }
            "runtimeHooks" => {
                let callback: u64 = field(&request, "callback")?;
                let name: JsonString = field(&request, "name")?;
                let lookup = self.clone();
                let invoke = self.clone();
                let key = pi_durable::harness::hooks::HookKey::try_new(
                    name.clone(),
                    move |handlers: Arc<u64>| {
                        decode::<Option<u64>>(&lookup.sync(
                            "hookMember",
                            object([
                                ("handlers", encode(handlers.as_ref())),
                                ("name", encode(&name)),
                            ]),
                        )?)
                    },
                );
                mapped(
                    self.runtimes
                        .lock()
                        .get(&field::<u64>(&request, "runtime")?)
                        .cloned()
                        .ok_or_else(|| Error::message("No generic hooks binding"))?
                        .hooks()
                        .each(&key, move |handler| {
                            invoke.hook_calls.fetch_add(1, Ordering::Relaxed);
                            let call = invoke.call(
                                "hookInvoke",
                                object([
                                    ("callback", encode(&callback)),
                                    ("handler", encode(&handler)),
                                ]),
                            );
                            async move {
                                call.await?;
                                Ok(())
                            }
                        }),
                )
            }
            "configure" => {
                let scheduler = self.scheduler(&request)?;
                let conversation = field(&request, "conversation")?;
                let change = field(&request, "change")?;
                mapped(scheduler.session().commit(
                    move |tx| async move {
                        pi_durable::harness::agent::configure(&tx, conversation, change).await
                    },
                    &context,
                ))
            }
            "runtimeRegistry" => {
                let snapshot = self.runtime_registry(&request)?;
                Box::pin(async move { Ok(Some(encode(&snapshot))) })
            }
            "resume" => {
                self.scheduler(&request)?.resume()?;
                Box::pin(async { Ok(None) })
            }
            "close" => mapped(self.scheduler(&request)?.session().close(&context)),
            "closed" => {
                let session = self.scheduler(&request)?.session().clone();
                Box::pin(async move {
                    Ok(Some(match session.closed().await {
                        SessionEnd::Closed => object([("reason", encode(&"closed"))]),
                        SessionEnd::Failed(error) => object([
                            ("reason", encode(&"failed")),
                            ("error", error_value(&error)),
                        ]),
                    }))
                })
            }
            "getTask" => {
                let id = field(&request, "task")?;
                let scheduler = self.scheduler(&request)?;
                let operation = scheduler
                    .session()
                    .commit(move |tx| async move { tx.task(id).await }, &context);
                Box::pin(async move { Ok(operation.await?.map(|record| encode(&record))) })
            }
            "abortTask" => mapped(
                self.scheduler(&request)?
                    .abort(field(&request, "task")?, &context),
            ),
            "waitForTask" => {
                let scheduler = self.scheduler(&request)?;
                scheduler.resume()?;
                mapped(scheduler.wait_for_task(field(&request, "task")?, &context))
            }
            "runtimeWaitForTask" => mapped(self.runtime(&request)?.wait_for_task(
                TypedTaskId::<JsonValue>::from_untyped(field(&request, "task")?),
                &context,
            )),
            "runtimeGetTask" => {
                let runtime = self.runtime(&request)?;
                let operation = runtime.get_task(
                    TypedTaskId::<JsonValue>::from_untyped(field(&request, "task")?),
                    &context,
                );
                Box::pin(async move { Ok(operation.await?.map(|record| encode(&record))) })
            }
            "outcomes" => {
                let ids: Vec<TaskId> = field(&request, "tasks")?;
                mapped(
                    self.runtime(&request)?.outcomes(
                        &ids.into_iter()
                            .map(TypedTaskId::<JsonValue>::from_untyped)
                            .collect::<Vec<_>>(),
                        &context,
                    ),
                )
            }
            "memo" => {
                let runtime = self.runtime(&request)?;
                let name: JsonString = field(&request, "name")?;
                if let Some(candidate) = request
                    .as_object()
                    .unwrap()
                    .get(&JsonString::from("candidate"))
                {
                    mapped(runtime.store_memo(name, candidate.clone(), &context))
                } else {
                    runtime.memo::<JsonValue>(name, &context)
                }
            }
            "commit" => {
                self.commits.fetch_add(1, Ordering::Relaxed);
                let callback = field(&request, "callback")?;
                if request
                    .as_object()
                    .unwrap()
                    .contains_key(&JsonString::from("runtime"))
                {
                    let runtime = self.runtime(&request)?;
                    let hub = self.clone();
                    mapped(runtime.commit(
                        move |tx, current| async move {
                            hub.callback(callback, tx, Some(current.into_record()))
                                .await?
                                .map(|value| to_next(decode(&value)?))
                                .transpose()
                        },
                        &context,
                    ))
                } else {
                    let scheduler = self.scheduler(&request)?;
                    let conversation = field(&request, "conversation")?;
                    let hub = self.clone();
                    scheduler.session().commit_scoped(
                        move |tx| async move { hub.callback(callback, tx, None).await },
                        &context,
                        TransactionScope {
                            conversation_id: conversation,
                            task_id: None,
                        },
                    )
                }
            }
            "createTask" => {
                let tx = self
                    .transactions
                    .lock()
                    .get(&field::<u64>(&request, "tx")?)
                    .cloned()
                    .ok_or_else(|| Error::message("Unknown transaction"))?;
                let task = self.definition(&request["definition"])?;
                let options = &request["options"];
                let ownership = if options["ownership"]["kind"].string_equals("task") {
                    TaskOwnership::Task(field(&options["ownership"], "taskId")?)
                } else {
                    TaskOwnership::Conversation
                };
                let mut create = TaskOptions::new(ownership);
                create.conversation_id = options
                    .as_object()
                    .unwrap()
                    .get(&JsonString::from("conversationId"))
                    .map(decode)
                    .transpose()?;
                create.background = options["background"].as_bool().unwrap_or(false);
                create.abandon_on_restart = options["abandonOnRestart"].as_bool().unwrap_or(false);
                if request["definition"]["nativeTool"].as_bool() == Some(true) {
                    mapped(tx.create_task(
                        &pi_durable::harness::tool::TOOL_TASK,
                        field(&request, "input")?,
                        create,
                    ))
                } else {
                    mapped(tx.create_task(&task, request["input"].clone(), create))
                }
            }
            "stats" => {
                let value = encode(
                    &serde_json::json!({"nativeMemoryOpens":self.memory.direct_opens.load(Ordering::Relaxed),"nativeMemoryCalls":self.memory.calls.load(Ordering::Relaxed),"nativeMemoryCommits":self.memory.commits.load(Ordering::Relaxed),"viewReads":self.views.reads.load(Ordering::Relaxed),"viewPublications":self.views.publications.load(Ordering::Relaxed),"nativeToolHooks":self.tool.hook_calls.load(Ordering::Relaxed),"nativeToolCalls":self.tool.executions.load(Ordering::Relaxed),"compactionReads":self.compaction_reads.load(Ordering::Relaxed),"promptReplays":self.prompt.replays.load(Ordering::Relaxed),"promptPlans":self.prompt.plans.load(Ordering::Relaxed),"promptRenders":self.prompt.renders.load(Ordering::Relaxed),"promptSectionCalls":self.prompt.section_calls.load(Ordering::Relaxed),"phases":self.phases.load(Ordering::Relaxed),"commits":self.commits.load(Ordering::Relaxed),"contextReads":self.context_reads.load(Ordering::Relaxed),"registryReads":self.registry_reads.load(Ordering::Relaxed),"agentReads":self.agent_reads.load(Ordering::Relaxed),"agentResolutions":self.agent_resolutions.load(Ordering::Relaxed),"hookCalls":self.hook_calls.load(Ordering::Relaxed),"graphReads":self.graph.reads.load(Ordering::Relaxed),"graphFrames":self.graph.frames.load(Ordering::Relaxed),"inspections":self.inspections.load(Ordering::Relaxed),"readerSnapshots":self.registry.reader_snapshots.load(Ordering::Relaxed),"readerSubscriptions":self.registry.reader_subscriptions.load(Ordering::Relaxed),"documentReads":self.documents.reads.load(Ordering::Relaxed),"documentWrites":self.documents.writes.load(Ordering::Relaxed),"documentWatches":self.documents.watches.load(Ordering::Relaxed),"documentFrames":self.documents.watch_frames.load(Ordering::Relaxed),"submissionAdmissions":self.submissions.admissions.load(Ordering::Relaxed),"submissionReads":self.submissions.reads.load(Ordering::Relaxed),"submissionWaits":self.submissions.waits.load(Ordering::Relaxed),"submissionAborts":self.submissions.aborts.load(Ordering::Relaxed)}),
                );
                Box::pin(async move { Ok(Some(value)) })
            }
            other => {
                return Err(Error::message(format!(
                    "Unknown test bridge method {other}"
                )));
            }
        })
    }
}
fn to_next(state: TaskState) -> Result<NextTaskState> {
    Ok(match state {
        TaskState::Running { checkpoint, .. } => NextTaskState::Running { checkpoint },
        TaskState::Waiting {
            checkpoint,
            on,
            policy,
            ..
        } => NextTaskState::Waiting {
            checkpoint,
            on,
            policy,
        },
        TaskState::Sleeping {
            checkpoint, until, ..
        } => NextTaskState::Sleeping { checkpoint, until },
        TaskState::Terminal { outcome } => NextTaskState::Terminal { outcome },
        _ => return Err(Error::message("Not a task-owned next state")),
    })
}
#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    if std::env::args().nth(1).as_deref() == Some("--order-tool-results") {
        let mut input = String::new();
        std::io::Read::read_to_string(&mut std::io::stdin(), &mut input).unwrap();
        let messages: Vec<Arc<JsonValue>> = serde_json::from_str(&input).unwrap();
        println!(
            "{}",
            encode(&pi_durable::harness::context::order_tool_results(&messages))
        );
        return;
    }
    let hub = Arc::new(Hub {
        output: Mutex::new(std::io::stdout()),
        next: AtomicU64::new(1_000_000),
        pending: Mutex::new(HashMap::new()),
        host_pending: Mutex::new(HashSet::new()),
        phase_serials: Mutex::new(HashMap::new()),
        changed: tokio::sync::Notify::new(),
        input_closed: AtomicBool::new(false),
        adoptions: Mutex::new(HashMap::new()),
        schedulers: Mutex::new(HashMap::new()),
        readers: Mutex::new(HashMap::new()),
        tasks: Mutex::new(HashMap::new()),
        runtimes: Mutex::new(HashMap::new()),
        native_runtimes: Mutex::new(HashMap::new()),
        tool: tool_bridge::ToolBridge::default(),
        environment: tool_environment::EnvironmentBridge::default(),
        host_timers: Mutex::new(HashMap::new()),
        transactions: Mutex::new(HashMap::new()),
        contexts: Mutex::new(HashMap::new()),
        phases: AtomicU64::new(0),
        commits: AtomicU64::new(0),
        context_reads: AtomicU64::new(0),
        compaction_reads: AtomicU64::new(0),
        registry_reads: AtomicU64::new(0),
        agent_reads: AtomicU64::new(0),
        agent_resolutions: AtomicU64::new(0),
        hook_calls: AtomicU64::new(0),
        ranges: Mutex::new(HashMap::new()),
        prompt: prompt_bridge::PromptBridge::default(),
        registry: registry_bridge::RegistryBridge::default(),
        graph: graph_bridge::GraphBridge::default(),
        views: view_bridge::ViewBridge::default(),
        memory: memory_bridge::MemoryBridge::default(),
        documents: document_bridge::DocumentBridge::default(),
        submissions: submission_bridge::SubmissionBridge::default(),
        inspections: AtomicU64::new(0),
    });
    let runtime = tokio::runtime::Handle::current();
    tokio::task::spawn_blocking(move || {
        let (send, receive) = std::sync::mpsc::channel::<Work>();
        let admission = hub.clone();
        let handle = runtime.clone();
        let lane = std::thread::spawn(move || {
            let _entered = handle.enter();
            for work in receive {
                match work {
                    Work::Request(value, context) => {
                        begin_request(admission.clone(), &handle, value, context)
                    }
                    Work::Reply(value) => admission.response(value),
                    Work::Adopt(id, seq) => {
                        let adopt = admission.adoptions.lock().remove(&id);
                        if let Some(adopt) = adopt {
                            adopt(seq);
                        }
                    }
                    Work::Release(range) => {
                        admission.ranges.lock().remove(&range);
                    }
                }
            }
        });
        for line in std::io::stdin().lock().lines() {
            let value: JsonValue = serde_json::from_str(&line.unwrap()).unwrap();
            match value["type"].to_utf8().as_deref() {
                Some("callback.result") => {
                    let id = field::<u64>(&value, "id").unwrap();
                    let synchronous = matches!(hub.pending.lock().get(&id), Some(Pending::Sync(_)));
                    if synchronous {
                        hub.response(value);
                    } else {
                        send.send(Work::Reply(value)).unwrap();
                    }
                    continue;
                }
                Some("context.release") => {
                    send.send(Work::Release(field(&value, "range").unwrap()))
                        .unwrap();
                    continue;
                }
                Some("storage.adopt") => {
                    send.send(Work::Adopt(
                        field(&value, "id").unwrap(),
                        field(&value, "seq").unwrap(),
                    ))
                    .unwrap();
                    continue;
                }
                Some("abort") => {
                    if let Some(controller) = hub
                        .contexts
                        .lock()
                        .get(&field::<u64>(&value, "id").unwrap())
                    {
                        controller.abort_with_reason(SignalReason::Error(AbortError::new(
                            value["message"].to_utf8().unwrap(),
                        )));
                    }
                    continue;
                }
                _ => {}
            }
            let id = field::<u64>(&value, "id").unwrap();
            let context = hub.context(id);
            if let Some(message) = value["abortInitially"].to_utf8() {
                hub.contexts
                    .lock()
                    .get(&id)
                    .unwrap()
                    .abort_with_reason(SignalReason::Error(AbortError::new(message)));
            }
            if value["method"].to_utf8().as_deref() == Some("close")
                && value["admit"].as_bool() == Some(true)
            {
                let (hub, runtime) = (hub.clone(), runtime.clone());
                runtime
                    .clone()
                    .spawn_blocking(move || begin_request(hub, &runtime, value, context));
                continue;
            }
            // Registry operations are independent of the Session admission
            // line and may be called by a synchronous host callback on it.
            if value["method"].to_utf8().is_some_and(|method| {
                method.starts_with("registry.")
                    || method == "runtimeRegistry"
                    || method == "runtimeTimer"
                    || method == "runtimeDisarmTimer"
                    || method == "runtimeSettings"
                    || method == "runtimeNow"
                    || method == "runtimeReport"
                    || (method.starts_with("graph.")
                        && method != "graph.watch"
                        && method != "graph.turn")
            }) {
                begin_request(hub.clone(), &runtime, value, context);
            } else {
                send.send(Work::Request(value, context)).unwrap();
            }
        }
        // Unblock callbacks before joining admission when the host disappears.
        {
            let mut pending = hub.pending.lock();
            hub.input_closed.store(true, Ordering::Release);
            for (_, waiter) in pending.drain() {
                let error = Err(Error::message("Test transport is closed"));
                match waiter {
                    Pending::Sync(send) => {
                        let _ = send.send(error);
                    }
                    Pending::Async(send) => {
                        let _ = send.send(error);
                    }
                }
            }
        }
        drop(send);
        lane.join().unwrap();
    })
    .await
    .unwrap();
}
enum Work {
    Request(JsonValue, Context),
    Reply(JsonValue),
    Adopt(u64, Seq),
    Release(u64),
}
fn begin_request(
    hub: Arc<Hub>,
    runtime: &tokio::runtime::Handle,
    value: JsonValue,
    context: Context,
) {
    let id = field::<u64>(&value, "id").unwrap();
    let admit = value["admit"].as_bool() == Some(true);
    // Begin Session operations in wire order before observing their futures.
    // Adoption and backend completion use the same lane, while synchronous
    // callback replies stay readable even if adoption calls the host clock.
    let boundary = value["method"].to_utf8().is_some_and(|method| {
        !method.starts_with("graph.")
            && method != "phase.turn"
            && !method.starts_with("registry.")
            && method != "runtimeRegistry"
    });
    let operation = {
        let _entered = runtime.enter();
        hub.clone()
            .begin(value, context)
            .unwrap_or_else(|error| Box::pin(async { Err(error) }))
    };
    if admit {
        hub.emit(object([("type", encode(&"admitted")), ("id", encode(&id))]));
    }
    runtime.spawn(async move {
        let result = operation.await;
        if boundary {
            hub.graph.response_boundary().await;
        }
        hub.contexts.lock().remove(&id);
        hub.emit(match result {
            Ok(Some(value)) => object([("id", encode(&id)), ("value", value)]),
            Ok(None) => object([("id", encode(&id)), ("missing", encode(&true))]),
            Err(error) => object([("id", encode(&id)), ("error", error_value(&error))]),
        });
    });
}
