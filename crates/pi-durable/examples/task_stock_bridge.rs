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
    harness::{catalog::*, runtime::TaskRuntime, scheduler::*, state::NextTaskState},
    storage::{Adopt, StorageFuture},
    *,
};
use serde::{Serialize, de::DeserializeOwned};
use std::{
    collections::HashMap,
    io::{BufRead, Write},
    sync::{
        Arc, LazyLock,
        atomic::{AtomicU64, Ordering},
    },
};
#[path = "support/context_bridge.rs"]
mod context_bridge;
#[path = "support/registry_bridge.rs"]
mod registry_bridge;
#[path = "support/task_remote_storage.rs"]
mod remote_storage;
static CALL_CONTEXT: LazyLock<ContextKey<u64>> =
    LazyLock::new(|| create_context_key("stock-test context"));
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
type JsonTask = Task<JsonValue, JsonValue, JsonValue>;
enum Pending {
    Async(tokio::sync::oneshot::Sender<Result<Option<JsonValue>>>),
    Sync(std::sync::mpsc::Sender<Result<Option<JsonValue>>>),
}
struct Hub {
    output: Mutex<std::io::Stdout>,
    next: AtomicU64,
    pending: Mutex<HashMap<u64, Pending>>,
    adoptions: Mutex<HashMap<u64, Adopt>>,
    schedulers: Mutex<HashMap<u64, TaskScheduler>>,
    catalogs: Mutex<HashMap<u64, TaskCatalog>>,
    tasks: Mutex<HashMap<u64, JsonTask>>,
    runtimes: Mutex<HashMap<u64, TaskRuntime>>,
    transactions: Mutex<HashMap<u64, Transaction>>,
    contexts: Mutex<HashMap<u64, AbortController>>,
    phases: AtomicU64,
    commits: AtomicU64,
    context_reads: AtomicU64,
    registry: registry_bridge::RegistryBridge,
    ranges: Mutex<HashMap<u64, Arc<pi_durable::harness::context::ContextRange>>>,
}
impl Hub {
    fn emit(&self, value: JsonValue) {
        let mut output = self.output.lock();
        writeln!(output, "{value}").unwrap();
        output.flush().unwrap();
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
        self.pending.lock().insert(id, Pending::Async(send));
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
        self.pending.lock().insert(id, Pending::Sync(send));
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
        let result = if value["error"].is_object() {
            Err(Error::message(value["error"]["message"].to_utf8().unwrap()))
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
                Arc::new(move |task: RunningTask, runtime: TaskRuntime, _| {
                    hub.phase(id, name.clone(), task, runtime)
                }) as PhaseHandler<JsonValue, JsonValue, JsonValue>,
            );
        }
        let hub = self.clone();
        let initial = Arc::new(move |input: &JsonValue| hub.sync("initial", encode(&(id, input))));
        let hub = self.clone();
        let abort = Arc::new(move |task, runtime, _| hub.phase(id, "$abort".into(), task, runtime))
            as PhaseHandler<JsonValue, JsonValue, JsonValue>;
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
        let call = self.call("phase", encode(&(definition, phase, task.record(), id)));
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
    fn runtime(&self, request: &JsonValue) -> Result<TaskRuntime> {
        self.runtimes
            .lock()
            .get(&field::<u64>(request, "runtime")?)
            .cloned()
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
                let catalog = TaskCatalog::default();
                let descriptions: Vec<JsonValue> = field(&request, "tasks")?;
                let tasks = descriptions
                    .iter()
                    .map(|description| {
                        Ok(InstalledTask {
                            task: self.definition(description)?.erased(),
                            extension: None,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                catalog.replace(tasks)?;
                let report = self.clone();
                let clock = self.clone();
                let scheduler=TaskScheduler::open(Arc::new(remote_storage::RemoteStorage{hub:self.clone(),handle}),catalog.clone(),SchedulerOptions{
                    on_report:Arc::new(move|error,info|report.emit(encode(&serde_json::json!({"type":"report","harness":harness,"error":{"name":error.name(),"message":error.to_string()},"info":{"type":info.kind,"taskId":info.task,"reason":info.reason}})))),..Default::default()
                },SessionOptions{now:Arc::new(move||decode(&clock.sync("now",encode(&harness)).unwrap()).unwrap()),document_retention_ms:field(&request,"retention")?,..Default::default()},&context).await?;
                self.schedulers.lock().insert(harness, scheduler);
                self.catalogs.lock().insert(harness, catalog);
                Ok(Some(encode(&harness)))
            }),
            "root" => {
                let scheduler = self.scheduler(&request)?;
                mapped(scheduler.session().commit(
                    |tx| async move {
                        Ok(
                            if let Some(root) = tx.conversation(ROOT_CONVERSATION_ID).await? {
                                root.id
                            } else {
                                tx.create_root_conversation().await?.id
                            },
                        )
                    },
                    &context,
                ))
            }
            "resume" => {
                self.scheduler(&request)?.resume()?;
                Box::pin(async { Ok(None) })
            }
            "close" => mapped(self.scheduler(&request)?.session().close(&context)),
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
            "waitForTask" => mapped(
                self.scheduler(&request)?
                    .wait_for_task(field(&request, "task")?, &context),
            ),
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
                mapped(tx.create_task(&task, request["input"].clone(), create))
            }
            "stats" => {
                let value = encode(
                    &serde_json::json!({"phases":self.phases.load(Ordering::Relaxed),"commits":self.commits.load(Ordering::Relaxed),"contextReads":self.context_reads.load(Ordering::Relaxed)}),
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
        adoptions: Mutex::new(HashMap::new()),
        schedulers: Mutex::new(HashMap::new()),
        catalogs: Mutex::new(HashMap::new()),
        tasks: Mutex::new(HashMap::new()),
        runtimes: Mutex::new(HashMap::new()),
        transactions: Mutex::new(HashMap::new()),
        contexts: Mutex::new(HashMap::new()),
        phases: AtomicU64::new(0),
        commits: AtomicU64::new(0),
        context_reads: AtomicU64::new(0),
        ranges: Mutex::new(HashMap::new()),
        registry: registry_bridge::RegistryBridge::default(),
    });
    let runtime = tokio::runtime::Handle::current();
    tokio::task::spawn_blocking(move||{
        for line in std::io::stdin().lock().lines(){let value:JsonValue=serde_json::from_str(&line.unwrap()).unwrap();
            match value["type"].to_utf8().as_deref(){
                Some("callback.result")=>{hub.response(value);continue;}
                Some("context.release")=>{hub.ranges.lock().remove(&field::<u64>(&value,"range").unwrap());continue;}
                Some("storage.adopt")=>{let id=field::<u64>(&value,"id").unwrap();let adopt=hub.adoptions.lock().remove(&id);if let Some(adopt)=adopt{adopt(field(&value,"seq").unwrap());}continue;}
                Some("abort")=>{if let Some(controller)=hub.contexts.lock().get(&field::<u64>(&value,"id").unwrap()){controller.abort_with_reason(SignalReason::Error(AbortError::new(value["message"].to_utf8().unwrap())));}continue;}
                _=>{}
            }
            let call=hub.clone();let id=field::<u64>(&value,"id").unwrap();
            let context=hub.context(id);
            // Begin commands in input order before observing their futures. A close or
            // abort received behind a held reservation must already be on the line.
            let operation={let _entered=runtime.enter();call.clone().begin(value,context).unwrap_or_else(|error|Box::pin(async{Err(error)}))};
            runtime.spawn(async move{let result=operation.await;call.contexts.lock().remove(&id);
                call.emit(match result{Ok(Some(value))=>object([("id",encode(&id)),("value",value)]),Ok(None)=>object([("id",encode(&id)),("missing",encode(&true))]),Err(error)=>encode(&serde_json::json!({"id":id,"error":{"name":error.name(),"message":error.to_string()}}))});
            });
        }
    }).await.unwrap();
}
