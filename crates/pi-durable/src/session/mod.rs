//! One ordered mutation line with durable adoption, revocable drafts and
//! incarnation-bound observations. Storage is the sole persistence authority.
pub(crate) mod executor;
mod forks;
mod guard;
mod transaction;
mod types;
use crate::timers::Timers;
pub use crate::timers::{DisarmTimer, SetTimer};
use crate::{
    storage::{StorageFuture, ready},
    wire, *,
};
use futures::{
    FutureExt,
    future::{BoxFuture, Shared},
};
use guard::{Guard, GuardedStorage};
use indexmap::IndexMap;
use parking_lot::Mutex;
use pi_chord::{
    Watch,
    context::{await_with_context, without_abort_signal},
    delta::{Op, track},
};
use std::{
    collections::HashMap,
    future::Future,
    sync::{Arc, Weak},
    time::{SystemTime, UNIX_EPOCH},
};
pub use transaction::{DocDraft, Transaction};
use transaction::{Loaded, LoadedDocument, TransactionHost};
pub use types::*;

pub(crate) fn panic_error(panic: Box<dyn std::any::Any + Send>) -> Error {
    Error::message(
        panic
            .downcast_ref::<String>()
            .cloned()
            .or_else(|| panic.downcast_ref::<&str>().map(|s| s.to_string()))
            .unwrap_or_else(|| "Session callback panicked".into()),
    )
}
fn cancelled(context: &Context) -> Result<()> {
    context.check().map_err(Error::from)
}
fn await_error(error: pi_chord::context::AwaitError) -> Error {
    match error {
        pi_chord::context::AwaitError::Aborted(e) => e.into(),
        pi_chord::context::AwaitError::Task(e) => Error::message(e.to_string()),
    }
}
fn wall_now() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as f64
}
pub(crate) fn inject_trace(context: &Context) -> Result<Option<JsonObject>> {
    context
        .telemetry()
        .propagation()
        .and_then(|p| p.inject(context.telemetry().as_ref()))
        .map(|c| JsonObject::try_from_value(wire::encode(&c)).map_err(Error::message))
        .transpose()
}
type Report = Arc<dyn Fn(Error) + Send + Sync>;
type Listener = Arc<dyn Fn(&CommitPublication, &Context) -> Result<()> + Send + Sync>;
type CloseListener = Arc<dyn Fn() + Send + Sync>;
type ConversationHook =
    Arc<dyn Fn(Transaction, ConversationRecord) -> StorageFuture<()> + Send + Sync>;
type BeforeClose = Arc<dyn Fn(Session) -> StorageFuture<()> + Send + Sync>;
#[derive(Clone)]
pub struct SessionOptions {
    pub now: Arc<dyn Fn() -> f64 + Send + Sync>,
    pub document_retention_ms: f64,
    pub set_timer: Option<SetTimer>,
    pub report: Report,
    pub read_only: bool,
    pub conversation_created: Option<ConversationHook>,
    pub before_close: Option<BeforeClose>,
}
impl Default for SessionOptions {
    fn default() -> Self {
        Self {
            now: Arc::new(wall_now),
            document_retention_ms: 600_000.0,
            set_timer: None,
            report: Arc::new(|_| {}),
            read_only: false,
            conversation_created: None,
            before_close: None,
        }
    }
}
struct Observer {
    id: DocumentId,
    address: String,
    version: u64,
    watch: Watch,
    pinned: bool,
}
#[derive(Default)]
struct State {
    closing: bool,
    cache: HashMap<String, Loaded>,
    idle: IndexMap<String, f64>,
    attached: HashMap<DocumentId, usize>,
    observers: IndexMap<u64, Observer>,
    listeners: IndexMap<u64, (bool, Listener)>,
    close_listeners: IndexMap<u64, CloseListener>,
    next_listener: u64,
    timer: Option<DisarmTimer>,
    timer_generation: u64,
    sweep_pending: bool,
    background_unavailable: bool,
}
struct Inner {
    guard: Arc<GuardedStorage>,
    executor: executor::Executor,
    storage: Arc<dyn Storage>,
    options: SessionOptions,
    timers: Timers,
    state: Mutex<State>,
    tail: Mutex<Shared<BoxFuture<'static, ()>>>,
    publication: Mutex<Shared<BoxFuture<'static, ()>>>,
    closed: tokio::sync::watch::Sender<Option<SessionEnd>>,
    close_result: tokio::sync::watch::Sender<Option<Result<()>>>,
}
#[derive(Clone)]
pub struct Session {
    inner: Arc<Inner>,
}
pub struct Subscription {
    stop: Option<Box<dyn FnOnce() + Send>>,
}
impl Subscription {
    pub fn stop(mut self) {
        if let Some(stop) = self.stop.take() {
            stop();
        }
    }
}
impl Drop for Subscription {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            stop();
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CacheSizes {
    pub cached: usize,
    pub idle: usize,
    pub attached: usize,
    pub sweep_jobs: usize,
}

impl Session {
    pub fn new(storage: Arc<dyn Storage>, mut options: SessionOptions) -> Self {
        if options.document_retention_ms.is_nan() || options.document_retention_ms < 0.0 {
            options.document_retention_ms = 600_000.0;
        }
        let guard = GuardedStorage::new(storage);
        let (closed, _) = tokio::sync::watch::channel(None);
        let (close_result, _) = tokio::sync::watch::channel(None);
        let session = Self {
            inner: Arc::new(Inner {
                timers: Timers::new(options.set_timer.clone(), options.report.clone()),
                storage: Arc::new(Guard(guard.clone())),
                guard: guard.clone(),
                executor: executor::Executor::default(),
                options,
                state: Mutex::new(State::default()),
                tail: Mutex::new(async {}.boxed().shared()),
                publication: Mutex::new(async {}.boxed().shared()),
                closed,
                close_result,
            }),
        };
        let weak = Arc::downgrade(&session.inner);
        guard.on_failure(Arc::new(move |error| {
            if let Some(inner) = weak.upgrade() {
                let session = Session { inner };
                session.begin_close(Context::default());
                session.report(error);
            }
        }));
        session
    }
    fn healthy(&self) -> Result<()> {
        self.inner.guard.healthy()
    }
    pub fn failure(&self) -> Option<Error> {
        self.inner.guard.failure()
    }
    pub fn failed(&self) -> StorageFuture<()> {
        self.inner.guard.failed()
    }
    pub fn fail(&self, error: Error) {
        self.inner.guard.fail(error);
    }
    pub(crate) fn guarded_storage(&self) -> Arc<dyn Storage> {
        self.inner.storage.clone()
    }
    pub(crate) fn read_on_line<T, F, Fut>(&self, read: F) -> StorageFuture<T>
    where
        T: Send + 'static,
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Result<T>> + Send + 'static,
    {
        self.admit(move |session| async move {
            session.healthy()?;
            read().await
        })
    }
    pub(crate) fn usable(&self) -> Result<()> {
        self.healthy()?;
        if self.inner.state.lock().closing {
            Err(Error::message("Session is closed"))
        } else {
            Ok(())
        }
    }
    pub(crate) fn report(&self, error: Error) {
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            (self.inner.options.report)(error)
        }));
    }
    fn now(&self) -> f64 {
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| (self.inner.options.now)()))
            .unwrap_or_else(|_| wall_now())
    }
    /// Poll a service continuation on this Session's FIFO without holding its
    /// mutation line. Transports use this to preserve service-result ordering.
    pub fn spawn(&self, operation: impl Future<Output = ()> + Send + 'static) {
        self.inner.executor.spawn(operation);
    }
    fn enqueue<T, F, Fut>(&self, callback: F) -> StorageFuture<T>
    where
        T: Send + 'static,
        F: FnOnce(Self) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T>> + Send + 'static,
    {
        let (send, receive) = tokio::sync::oneshot::channel();
        let (done, finished) = tokio::sync::oneshot::channel();
        let previous = {
            let mut tail = self.inner.tail.lock();
            std::mem::replace(
                &mut *tail,
                async move {
                    let _ = finished.await;
                }
                .boxed()
                .shared(),
            )
        };
        let session = self.clone();
        self.spawn(async move {
            previous.await;
            let result = std::panic::AssertUnwindSafe(async { callback(session).await })
                .catch_unwind()
                .await
                .unwrap_or_else(|p| Err(panic_error(p)));
            let _ = send.send(result);
            let _ = done.send(());
        });
        Box::pin(async move {
            receive
                .await
                .map_err(|_| Error::message("Session operation stopped"))?
        })
    }
    fn admit<T, F, Fut>(&self, callback: F) -> StorageFuture<T>
    where
        T: Send + 'static,
        F: FnOnce(Self) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T>> + Send + 'static,
    {
        let state = self.inner.state.lock();
        if let Err(error) = self.healthy() {
            return ready(Err(error));
        }
        if state.closing {
            return ready(Err(Error::message("Session is closed")));
        }
        self.enqueue(callback)
    }
    pub fn commit<T, F, Fut>(&self, callback: F, context: &Context) -> StorageFuture<T>
    where
        T: Send + 'static,
        F: FnOnce(Transaction) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T>> + Send + 'static,
    {
        self.commit_scoped(callback, context, TransactionScope::default())
    }
    pub fn commit_scoped<T, F, Fut>(
        &self,
        callback: F,
        context: &Context,
        scope: TransactionScope,
    ) -> StorageFuture<T>
    where
        T: Send + 'static,
        F: FnOnce(Transaction) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T>> + Send + 'static,
    {
        // Serialize admission with close and queue insertion, including across threads.
        let state = self.inner.state.lock();
        if let Err(error) = self.healthy() {
            return ready(Err(error));
        }
        if state.closing {
            return ready(Err(Error::message("Session is closed")));
        }
        let context = context.clone();
        self.enqueue(move |session| async move {
            session.healthy()?;
            cancelled(&context)?;
            let tx = Transaction::new(Arc::new(session.clone()), context.clone(), scope);
            let result = std::panic::AssertUnwindSafe(async {
                let value = callback(tx.clone()).await?;
                session.healthy()?;
                Ok(value)
            })
            .catch_unwind()
            .await
            .unwrap_or_else(|p| Err(panic_error(p)));
            let value = match result {
                Ok(v) => v,
                Err(e) => {
                    tx.settle_failure().await;
                    session.sweep();
                    return Err(e);
                }
            };
            let writes = match tx.settle_success().await {
                Ok(w) => w,
                Err(e) => {
                    session.sweep();
                    return Err(e);
                }
            };
            if writes.is_empty() {
                tx.discard();
                session.sweep();
                return Ok(value);
            }
            if session.inner.options.read_only {
                tx.discard();
                session.sweep();
                return Err(Error::read_only("a commit with writes"));
            }
            let adoption = Arc::new(Mutex::new(None));
            let capture = adoption.clone();
            let adopt_tx = tx.clone();
            let adopt_writes = writes.clone();
            let result = session
                .inner
                .storage
                .commit(
                    &writes,
                    Box::new(move |seq| {
                        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            adopt_tx.adopt(seq, &adopt_writes)
                        }))
                        .unwrap_or_else(|p| Err(panic_error(p)));
                        let mut capture = capture.lock();
                        if capture.is_some() {
                            *capture = Some(Err(Error::message(
                                "Storage adopted the batch more than once",
                            )));
                        } else {
                            *capture = Some(result);
                        }
                    }),
                    &without_abort_signal(&context),
                )
                .await;
            if let Err(error) = result {
                if adoption.lock().is_none() {
                    tx.discard();
                }
                return Err(error);
            }
            let publication = adoption.lock().take().unwrap_or_else(|| {
                Err(Error::message(
                    "Storage committed without adopting the batch",
                ))
            });
            let publication = match publication {
                Ok(p) => p,
                Err(e) => {
                    session.inner.guard.fail(e.clone());
                    return Err(e);
                }
            };
            session.publish(&publication, &context);
            session.sweep();
            Ok(value)
        })
    }
    pub fn subscribe_commits(
        &self,
        listener: impl Fn(&CommitPublication, &Context) -> Result<()> + Send + Sync + 'static,
    ) -> Result<Subscription> {
        self.subscribe(listener, false)
    }
    pub fn subscribe_internal(
        &self,
        listener: impl Fn(&CommitPublication, &Context) -> Result<()> + Send + Sync + 'static,
    ) -> Result<Subscription> {
        self.subscribe(listener, true)
    }
    fn subscribe(
        &self,
        listener: impl Fn(&CommitPublication, &Context) -> Result<()> + Send + Sync + 'static,
        internal: bool,
    ) -> Result<Subscription> {
        let id = {
            let mut state = self.inner.state.lock();
            self.healthy()?;
            if state.closing {
                return Err(Error::message("Session is closed"));
            }
            state.next_listener += 1;
            let id = state.next_listener;
            state.listeners.insert(id, (internal, Arc::new(listener)));
            id
        };
        let weak = Arc::downgrade(&self.inner);
        Ok(Subscription {
            stop: Some(Box::new(move || {
                if let Some(inner) = weak.upgrade() {
                    inner.state.lock().listeners.shift_remove(&id);
                }
            })),
        })
    }
    pub fn subscribe_close(
        &self,
        listener: impl Fn() + Send + Sync + 'static,
    ) -> Result<Subscription> {
        let listener: CloseListener = Arc::new(listener);
        let mut state = self.inner.state.lock();
        self.healthy()?;
        if state.closing {
            return Err(Error::message("Session is closed"));
        }
        state.next_listener += 1;
        let id = state.next_listener;
        state.close_listeners.insert(id, listener);
        let weak = Arc::downgrade(&self.inner);
        Ok(Subscription {
            stop: Some(Box::new(move || {
                if let Some(inner) = weak.upgrade() {
                    inner.state.lock().close_listeners.shift_remove(&id);
                }
            })),
        })
    }
    pub(crate) fn publication_settled(&self) -> Shared<BoxFuture<'static, ()>> {
        self.inner.publication.lock().clone()
    }
    fn publish(&self, publication: &CommitPublication, context: &Context) {
        // JS promise continuations run after the synchronous publication stack.
        // Native waiters may run on another thread; keep that same boundary.
        struct Finish(Option<tokio::sync::oneshot::Sender<()>>);
        impl Drop for Finish {
            fn drop(&mut self) {
                if let Some(send) = self.0.take() {
                    let _ = send.send(());
                }
            }
        }
        let (send, receive) = tokio::sync::oneshot::channel();
        *self.inner.publication.lock() = async move {
            let _ = receive.await;
        }
        .boxed()
        .shared();
        let _finish = Finish(Some(send));
        use pi_telemetry::{AttributeValue, SpanAttributes};
        for change in &publication.changes {
            let event = match change {
                CommitChange::Entry(entry) => Some((
                    "pi.durable.entry",
                    SpanAttributes::from([
                        (
                            "entry.id".into(),
                            AttributeValue::Number(entry.id.get() as f64),
                        ),
                        (
                            "entry.kind".into(),
                            AttributeValue::String(entry.kind.to_string()),
                        ),
                    ]),
                )),
                CommitChange::Submission(submission)
                    if matches!(
                        submission.state.status(),
                        SubmissionStatus::Done | SubmissionStatus::Unanswered
                    ) =>
                {
                    let mut attributes = SpanAttributes::from([
                        (
                            "submission.id".into(),
                            AttributeValue::Number(submission.id().get() as f64),
                        ),
                        (
                            "status".into(),
                            AttributeValue::String(
                                if submission.state.status() == SubmissionStatus::Done {
                                    "done"
                                } else {
                                    "unanswered"
                                }
                                .into(),
                            ),
                        ),
                    ]);
                    if let SubmissionState::InputDone { answer, .. } = submission.state {
                        attributes.insert(
                            "answer.entry_id".into(),
                            AttributeValue::Number(answer.get() as f64),
                        );
                    }
                    Some(("pi.durable.submission", attributes))
                }
                _ => None,
            };
            if let Some((name, attributes)) = event
                && let Err(p) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    context.telemetry().record_event(name, attributes)
                }))
            {
                self.report(panic_error(p));
            }
        }
        for change in &publication.changes {
            if let CommitChange::Document(change) = change {
                self.touch(&address_id(&change.record().create.address()));
            }
        }
        for internal in [true, false] {
            let listeners = self
                .inner
                .state
                .lock()
                .listeners
                .values()
                .filter(|(i, _)| *i == internal)
                .map(|(_, listener)| listener.clone())
                .collect::<Vec<_>>();
            for listener in listeners {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    listener(publication, context)
                }))
                .unwrap_or_else(|p| Err(panic_error(p)));
                if let Err(error) = result {
                    if internal {
                        self.inner.guard.fail(error)
                    } else {
                        self.report(error)
                    }
                }
            }
        }
    }
    fn deliver_observer(&self, token: u64, publication: &CommitPublication, context: &Context) {
        for change in &publication.changes {
            let CommitChange::Document(DocumentCommitChange::Document {
                record,
                version,
                value,
                ops,
                ..
            }) = change
            else {
                continue;
            };
            let now = self.now();
            let delivery = {
                let mut state = self.inner.state.lock();
                let Some(observer) = state
                    .observers
                    .get_mut(&token)
                    .filter(|o| o.id == record.id())
                else {
                    continue;
                };
                let watch = observer.watch.clone();
                if record.retired_at.is_some() {
                    let pinned = observer.pinned;
                    observer.pinned = false;
                    let address = observer.address.clone();
                    if pinned {
                        Self::unpin_locked(&mut state, record.id(), &address, now);
                    }
                    Some((watch, None, Vec::new()))
                } else if let Some(version) = version {
                    let operations = if observer.version == *version {
                        ops.clone()
                    } else {
                        observer.version = *version;
                        vec![Op::Replace(value.as_ref().clone())]
                    };
                    if operations.is_empty() {
                        None
                    } else {
                        Some((watch, Some(value.clone()), operations))
                    }
                } else {
                    None
                }
            };
            if let Some((watch, value, ops)) = delivery {
                if let Some(value) = value {
                    watch.advance(value, ops, context.clone());
                } else {
                    watch.retire(context.clone());
                }
            }
        }
    }
    pub fn snapshot<T: DocumentState, S: DocScope>(
        &self,
        token: &DocToken<T, S>,
        owner: S::Owner,
        context: &Context,
    ) -> StorageFuture<Option<Arc<T>>> {
        let address = token.address(owner);
        self.decode_current(
            address_id(&address),
            self.snapshot_raw(token.definition.clone(), address, context),
        )
    }
    pub fn snapshot_family<T: DocumentState, I, S: DocScope>(
        &self,
        token: &DocFamilyToken<T, I, S>,
        owner: S::Owner,
        key: impl Into<JsonString>,
        context: &Context,
    ) -> StorageFuture<Option<Arc<T>>> {
        let address = token.address(owner, key);
        self.decode_current(
            address_id(&address),
            self.snapshot_raw(token.definition.clone(), address, context),
        )
    }
    fn decode_current<T: DocumentState>(
        &self,
        address: String,
        future: StorageFuture<Option<Arc<JsonValue>>>,
    ) -> StorageFuture<Option<Arc<T>>> {
        let session = self.clone();
        Box::pin(async move {
            let Some(value) = future.await? else {
                return Ok(None);
            };
            let cached = session.inner.state.lock().cache.get(&address).cloned();
            let current = cached.filter(|l| Arc::ptr_eq(l.lock().tracker.lock().value(), &value));
            let type_id = std::any::TypeId::of::<T>();
            if let Some(current) = &current {
                let existing = current.lock().typed.get(&type_id).cloned();
                if let Some(existing) = existing {
                    return Ok(Some(
                        existing
                            .downcast::<T>()
                            .expect("typed revision key matches value"),
                    ));
                }
            }
            let decoded = Arc::new(wire::decode::<T>(&value).map_err(Error::message)?);
            if let Some(current) = current {
                let mut loaded = current.lock();
                if Arc::ptr_eq(loaded.tracker.lock().value(), &value) {
                    let existing = loaded
                        .typed
                        .entry(type_id)
                        .or_insert_with(|| decoded.clone())
                        .clone();
                    return Ok(Some(
                        existing
                            .downcast::<T>()
                            .expect("typed revision key matches value"),
                    ));
                }
            }
            Ok(Some(decoded))
        })
    }
    pub fn snapshot_raw(
        &self,
        definition: Arc<AnyDocDefinition>,
        address: DocumentAddress,
        context: &Context,
    ) -> StorageFuture<Option<Arc<JsonValue>>> {
        if let Err(error) = self
            .usable()
            .and_then(|()| definition.check_address(&address))
        {
            return ready(Err(error));
        }
        let id = address_id(&address);
        if let Some(loaded) = self.cached(&id) {
            let loaded = loaded.lock();
            if let Err(error) = definition
                .check_record_scope(&loaded.record.create)
                .and_then(|()| {
                    definition.check_record_version(&loaded.record.create, loaded.stored_version)
                })
            {
                return ready(Err(error));
            }
            if loaded.value_version == definition.version() {
                return ready(Ok(Some(loaded.tracker.lock().value().clone())));
            }
        }
        let context = context.clone();
        self.admit(move |session| async move {
            session.healthy()?;
            Ok(session
                .load(definition, address, context)
                .await?
                .map(|loaded| loaded.lock().tracker.lock().value().clone()))
        })
    }
    /// A component already holding the Session line may load a document without
    /// queuing another operation behind itself. Keep incarnation and revision together.
    pub(crate) async fn document_on_line(
        &self,
        definition: Arc<AnyDocDefinition>,
        address: DocumentAddress,
        context: &Context,
    ) -> Result<Option<(DocumentRecord, u64, Arc<JsonValue>)>> {
        definition.check_address(&address)?;
        Ok(self
            .load(definition, address, context.clone())
            .await?
            .map(|loaded| {
                let loaded = loaded.lock();
                let value = loaded.tracker.lock().value().clone();
                (loaded.record.clone(), loaded.value_version, value)
            }))
    }
    pub fn snapshot_as_of<T: DocumentState, S: RewindableScope>(
        &self,
        token: &DocToken<T, S>,
        owner: S::Owner,
        at: EntryId,
        context: &Context,
    ) -> StorageFuture<Option<Arc<T>>> {
        decode_snapshot(self.snapshot_as_of_raw(
            token.definition.clone(),
            token.address(owner),
            at,
            context,
        ))
    }
    pub fn snapshot_family_as_of<T: DocumentState, I, S: RewindableScope>(
        &self,
        token: &DocFamilyToken<T, I, S>,
        owner: S::Owner,
        key: impl Into<JsonString>,
        at: EntryId,
        context: &Context,
    ) -> StorageFuture<Option<Arc<T>>> {
        decode_snapshot(self.snapshot_as_of_raw(
            token.definition.clone(),
            token.address(owner, key),
            at,
            context,
        ))
    }
    pub fn snapshot_as_of_raw(
        &self,
        definition: Arc<AnyDocDefinition>,
        mut address: DocumentAddress,
        at: EntryId,
        context: &Context,
    ) -> StorageFuture<Option<Arc<JsonValue>>> {
        if let Err(error) = self
            .usable()
            .and_then(|()| definition.check_address(&address))
        {
            return ready(Err(error));
        }
        let context = context.clone();
        self.admit(move |session| async move {
            session.healthy()?;
            let DocumentScope::Conversation { conversation_id } = address.scope else {
                return Err(Error::message(
                    "Historical snapshots require conversation scope",
                ));
            };
            let entry = session
                .inner
                .storage
                .entry_in(conversation_id, at, &context)
                .await?
                .ok_or_else(|| {
                    Error::message(format!(
                        "Entry {at} is not visible from conversation {conversation_id}"
                    ))
                })?;
            address.scope = DocumentScope::Conversation {
                conversation_id: entry.entry.conversation_id,
            };
            let point = DocumentPoint::At(entry.commit_seq);
            let Some(record) = session
                .inner
                .storage
                .find_document(&address, point, &context)
                .await?
            else {
                return Ok(None);
            };
            let stored = session
                .inner
                .storage
                .document(record.id(), point, &context)
                .await?
                .ok_or_else(|| {
                    Error::message(format!("Historical document {} is missing", record.id()))
                })?;
            Ok(Some(Arc::new(definition.materialize(stored)?.into_value())))
        })
    }
    pub fn watch_doc<T: DocumentState, S: DocScope>(
        &self,
        token: &DocToken<T, S>,
        owner: S::Owner,
        context: &Context,
    ) -> StorageFuture<Option<DocumentWatch<T>>> {
        typed_watch(self.watch_raw(token.definition.clone(), token.address(owner), context))
    }
    pub fn watch_doc_family<T: DocumentState, I, S: DocScope>(
        &self,
        token: &DocFamilyToken<T, I, S>,
        owner: S::Owner,
        key: impl Into<JsonString>,
        context: &Context,
    ) -> StorageFuture<Option<DocumentWatch<T>>> {
        typed_watch(self.watch_raw(token.definition.clone(), token.address(owner, key), context))
    }
    pub fn watch_raw(
        &self,
        definition: Arc<AnyDocDefinition>,
        address: DocumentAddress,
        context: &Context,
    ) -> StorageFuture<Option<Watch>> {
        if let Err(error) = self
            .usable()
            .and_then(|()| definition.check_address(&address))
            .and_then(|()| cancelled(context))
        {
            return ready(Err(error));
        }
        let context = context.clone();
        let acquisition_context = context.clone();
        let operation = self.admit(move |session| async move {
            session.healthy()?;
            cancelled(&acquisition_context)?;
            let Some(loaded) = session
                .load(definition, address.clone(), acquisition_context.clone())
                .await?
            else {
                cancelled(&acquisition_context)?;
                return Ok(None);
            };
            cancelled(&acquisition_context)?;
            let (id, version, value) = {
                let loaded = loaded.lock();
                (
                    loaded.record.id(),
                    loaded.value_version,
                    loaded.tracker.lock().value().clone(),
                )
            };
            let address = address_id(&address);
            let weak = Arc::downgrade(&session.inner);
            let report = weak.clone();
            let mut state = session.inner.state.lock();
            state.next_listener += 1;
            let token = state.next_listener;
            let watch = Watch::new(
                value.as_ref().clone(),
                move || detach_observer(weak, token),
                move |error| {
                    if let Some(inner) = report.upgrade() {
                        Session { inner }.report(Error::message(error));
                    }
                },
            );
            *state.attached.entry(id).or_default() += 1;
            state.idle.shift_remove(&address);
            state.observers.insert(
                token,
                Observer {
                    id,
                    address,
                    version,
                    watch: watch.clone(),
                    pinned: true,
                },
            );
            let delivery = Arc::downgrade(&session.inner);
            state.listeners.insert(
                token,
                (
                    true,
                    Arc::new(move |publication, context| {
                        if let Some(inner) = delivery.upgrade() {
                            Session { inner }.deliver_observer(token, publication, context);
                        }
                        Ok(())
                    }),
                ),
            );
            let closing = Arc::downgrade(&session.inner);
            let closing_watch = watch.clone();
            state.close_listeners.insert(
                token,
                Arc::new(move || {
                    let failure = closing
                        .upgrade()
                        .and_then(|inner| inner.guard.failure())
                        .map(|e| e.to_string());
                    closing_watch.end(failure);
                }),
            );
            drop(state);
            if let Some(signal) = acquisition_context.abort_signal() {
                watch
                    .observe_cancellation(signal.clone())
                    .map_err(Error::message)?;
            }
            if let Err(error) = cancelled(&acquisition_context) {
                watch.cancel();
                return Err(error);
            }
            Ok(Some(watch))
        });
        // Acquisition cancellation rejects immediately; queued/in-flight work still
        // runs through its cancellation checks and cannot leave an attached watch.
        Box::pin(async move {
            await_with_context(operation, &context)
                .await
                .map_err(await_error)?
        })
    }
    pub fn unload_documents(&self) -> StorageFuture<()> {
        if let Err(error) = self.usable() {
            return ready(Err(error));
        }
        self.admit(|session| async move {
            session.healthy()?;
            let timer = {
                let mut state = session.inner.state.lock();
                state.cache.clear();
                state.idle.clear();
                state.timer.take()
            };
            if let Some(timer) = timer {
                timer();
            }
            Ok(())
        })
    }
    pub fn cache_sizes(&self) -> CacheSizes {
        let s = self.inner.state.lock();
        CacheSizes {
            cached: s.cache.len(),
            idle: s.idle.len(),
            attached: s.attached.len(),
            sweep_jobs: usize::from(s.sweep_pending),
        }
    }
    fn touch(&self, address: &str) {
        let now = self.now();
        let retention = self.inner.options.document_retention_ms;
        {
            let mut s = self.inner.state.lock();
            let Some(id) = s.cache.get(address).map(|l| l.lock().record.id()) else {
                return;
            };
            if s.attached.contains_key(&id) {
                return;
            }
            if s.idle
                .get(address)
                .is_some_and(|last| now - *last < 1000.0_f64.min(retention / 10.0))
            {
                return;
            }
            s.idle.shift_remove(address);
            s.idle.insert(address.into(), now);
        }
        self.arm_sweep();
    }
    fn sweep(&self) {
        let cutoff = self.now() - self.inner.options.document_retention_ms;
        let timer = {
            let mut s = self.inner.state.lock();
            let expired = s
                .idle
                .iter()
                .filter(|(_, at)| **at <= cutoff)
                .map(|(a, _)| a.clone())
                .collect::<Vec<_>>();
            for address in expired {
                s.idle.shift_remove(&address);
                s.cache.remove(&address);
            }
            s.sweep_pending = false;
            s.timer_generation += 1;
            s.timer.take()
        };
        if let Some(timer) = timer {
            timer();
        }
        self.arm_sweep();
    }
    fn arm_sweep(&self) {
        let retention = self.inner.options.document_retention_ms;
        if !retention.is_finite() {
            return;
        }
        let now = self.now();
        let (generation, delay) = {
            let mut s = self.inner.state.lock();
            if s.closing || s.background_unavailable || s.sweep_pending || s.timer.is_some() {
                return;
            }
            let Some((_, at)) = s.idle.first() else {
                return;
            };
            let delay = (*at + retention - now).max(0.0);
            s.timer_generation += 1;
            (s.timer_generation, delay)
        };
        let weak = Arc::downgrade(&self.inner);
        let callback = Box::new(move || {
            if let Some(inner) = weak.upgrade() {
                let session = Session { inner };
                {
                    let mut state = session.inner.state.lock();
                    if state.closing || state.timer_generation != generation {
                        return;
                    }
                    state.timer = None;
                    state.sweep_pending = true;
                }
                drop(session.enqueue(move |session| async move {
                    let current = {
                        let state = session.inner.state.lock();
                        state.sweep_pending && state.timer_generation == generation
                    };
                    if current {
                        session.sweep();
                    }
                    Ok(())
                }));
            }
        });
        // Zero retention uses the mutation line's next turn even on hosts that
        // decline background timers; cold reads must not retain idle trackers.
        if retention == 0.0 {
            callback();
            return;
        }
        let timer = self.inner.timers.background(callback, delay);
        let mut state = self.inner.state.lock();
        if state.timer_generation == generation && !state.sweep_pending {
            if timer.is_none() {
                state.background_unavailable = true;
            }
            state.timer = timer;
        } else {
            drop(state);
            if let Some(timer) = timer {
                timer();
            }
        }
    }
    fn unpin_locked(state: &mut State, id: DocumentId, address: &str, now: f64) {
        let Some(count) = state.attached.get_mut(&id) else {
            return;
        };
        *count -= 1;
        if *count == 0 {
            state.attached.remove(&id);
            if state
                .cache
                .get(address)
                .is_some_and(|l| l.lock().record.id() == id)
            {
                state.idle.shift_remove(address);
                state.idle.insert(address.into(), now);
            }
        }
    }
    pub fn close(&self, context: &Context) -> StorageFuture<()> {
        self.begin_close(without_abort_signal(context));
        let mut result = self.inner.close_result.subscribe();
        let context = context.clone();
        Box::pin(async move {
            let wait = async move {
                loop {
                    if let Some(result) = result.borrow_and_update().clone() {
                        return result;
                    }
                    result
                        .changed()
                        .await
                        .map_err(|_| Error::message("Session close stopped"))?;
                }
            };
            await_with_context(wait, &context)
                .await
                .map_err(await_error)?
        })
    }
    fn begin_close(&self, context: Context) {
        let (listeners, timer) = {
            let mut state = self.inner.state.lock();
            if state.closing {
                return;
            }
            state.closing = true;
            state.timer_generation += 1;
            (
                std::mem::take(&mut state.close_listeners),
                state.timer.take(),
            )
        };
        if let Some(timer) = timer {
            timer();
        }
        for listener in listeners.into_values() {
            if let Err(p) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| listener())) {
                self.report(panic_error(p));
            }
        }
        let session = self.clone();
        tokio::spawn(async move {
            if let Some(hook) = &session.inner.options.before_close {
                let result = std::panic::AssertUnwindSafe(async { hook(session.clone()).await })
                    .catch_unwind()
                    .await
                    .unwrap_or_else(|p| Err(panic_error(p)));
                if let Err(error) = result {
                    session.inner.guard.fail(error);
                }
            }
            session.enqueue_close(context);
        });
    }
    /// Cleanup writes are ordered after admitted work and publish no events.
    /// Harness shutdown uses this for marks that must survive the next restart.
    pub fn write_at_close(
        &self,
        writes: impl FnOnce() -> Result<Vec<StorageWrite>> + Send + 'static,
    ) -> StorageFuture<()> {
        let state = self.inner.state.lock();
        if !state.closing {
            return ready(Err(Error::message("Session is not closing")));
        }
        self.enqueue(move |session| async move {
            if session.inner.guard.failure().is_some() {
                return Ok(());
            }
            let writes = writes()?;
            if !writes.is_empty() {
                session
                    .inner
                    .storage
                    .commit(&writes, Box::new(|_| {}), &Context::default())
                    .await?;
            }
            Ok(())
        })
    }
    fn enqueue_close(&self, context: Context) {
        drop(self.enqueue(move |session| async move {
            let (observers, timer) = {
                let mut state = session.inner.state.lock();
                state.cache.clear();
                state.idle.clear();
                state.attached.clear();
                state.listeners.clear();
                (std::mem::take(&mut state.observers), state.timer.take())
            };
            if let Some(timer) = timer {
                timer();
            }
            let failure = session.inner.guard.failure().map(|e| e.to_string());
            for observer in observers.into_values() {
                observer.watch.end(failure.clone());
            }
            let result = session.inner.guard.close_backend(context).await;
            let end = session
                .inner
                .guard
                .failure()
                .map_or(SessionEnd::Closed, SessionEnd::Failed);
            session.inner.closed.send_replace(Some(end));
            session
                .inner
                .close_result
                .send_replace(Some(result.clone()));
            result
        }));
    }
    pub async fn closed(&self) -> SessionEnd {
        let mut closed = self.inner.closed.subscribe();
        loop {
            if let Some(end) = closed.borrow_and_update().clone() {
                return end;
            }
            if closed.changed().await.is_err() {
                return SessionEnd::Closed;
            }
        }
    }
}
impl TransactionHost for Session {
    fn storage(&self) -> Arc<dyn Storage> {
        self.inner.storage.clone()
    }
    fn now(&self) -> f64 {
        (self.inner.options.now)()
    }
    fn cached(&self, address: &str) -> Option<Loaded> {
        let loaded = self.inner.state.lock().cache.get(address).cloned();
        if loaded.is_some() {
            self.touch(address);
        }
        loaded
    }
    fn load(
        &self,
        definition: Arc<AnyDocDefinition>,
        address: DocumentAddress,
        context: Context,
    ) -> StorageFuture<Option<Loaded>> {
        let session = self.clone();
        Box::pin(async move {
            let id = address_id(&address);
            if let Some(cached) = session.cached(&id) {
                let loaded = cached.lock();
                definition.check_record_scope(&loaded.record.create)?;
                definition.check_record_version(&loaded.record.create, loaded.stored_version)?;
                if loaded.value_version == definition.version() {
                    return Ok(Some(cached.clone()));
                }
            }
            session.inner.state.lock().cache.remove(&id);
            session.inner.state.lock().idle.shift_remove(&id);
            let Some(record) = session
                .inner
                .storage
                .find_document(&address, DocumentPoint::Current, &context)
                .await?
            else {
                return Ok(None);
            };
            let stored = session
                .inner
                .storage
                .document(record.id(), DocumentPoint::Current, &context)
                .await?
                .ok_or_else(|| {
                    Error::message(format!("Current document {} is missing", record.id()))
                })?;
            let version = stored.version;
            let deltas_since_base = stored.deltas_since_base;
            let value = definition.materialize(stored)?;
            let loaded = Arc::new(Mutex::new(LoadedDocument {
                record,
                stored_version: version,
                value_version: definition.version(),
                deltas_since_base,
                tracker: Arc::new(Mutex::new(track(value.into_value()))),
                typed: HashMap::new(),
            }));
            session.install(id, loaded.clone());
            Ok(Some(loaded))
        })
    }
    fn install(&self, address: String, document: Loaded) {
        self.inner
            .state
            .lock()
            .cache
            .insert(address.clone(), document);
        self.touch(&address);
    }
    fn evict(&self, address: &str, id: DocumentId) {
        let mut state = self.inner.state.lock();
        if state
            .cache
            .get(address)
            .is_some_and(|l| l.lock().record.id() == id)
        {
            state.cache.remove(address);
            state.idle.shift_remove(address);
        }
    }
    fn conversation_created(
        &self,
        tx: Transaction,
        record: ConversationRecord,
    ) -> StorageFuture<()> {
        self.inner
            .options
            .conversation_created
            .as_ref()
            .map_or_else(|| ready(Ok(())), |hook| hook(tx, record))
    }
}
fn detach_observer(weak: Weak<Inner>, token: u64) {
    if let Some(inner) = weak.upgrade() {
        let session = Session { inner };
        let now = session.now();
        {
            let mut state = session.inner.state.lock();
            state.listeners.shift_remove(&token);
            state.close_listeners.shift_remove(&token);
            if let Some(observer) = state.observers.shift_remove(&token)
                && observer.pinned
            {
                Session::unpin_locked(&mut state, observer.id, &observer.address, now);
            }
        }
        session.arm_sweep();
    }
}
fn decode_snapshot<T: serde::de::DeserializeOwned + Send + Sync + 'static>(
    future: StorageFuture<Option<Arc<JsonValue>>>,
) -> StorageFuture<Option<Arc<T>>> {
    Box::pin(async move {
        future
            .await?
            .map(|value| wire::decode(&value).map(Arc::new).map_err(Error::message))
            .transpose()
    })
}
pub struct DocumentWatch<T> {
    watch: Watch,
    marker: std::marker::PhantomData<fn() -> T>,
}
impl<T> Clone for DocumentWatch<T> {
    fn clone(&self) -> Self {
        Self {
            watch: self.watch.clone(),
            marker: std::marker::PhantomData,
        }
    }
}
pub struct DocumentFrame<T> {
    pub value: Option<Arc<T>>,
    pub ops: Arc<[Op]>,
    pub context: Context,
}
impl<T: DocumentState> DocumentWatch<T> {
    pub fn value(&self) -> Result<Option<Arc<T>>> {
        let v = self.watch.value();
        if v.is_null() {
            Ok(None)
        } else {
            wire::decode(&v)
                .map(|v| Some(Arc::new(v)))
                .map_err(Error::message)
        }
    }
    pub fn raw(&self) -> &Watch {
        &self.watch
    }
    pub fn start<F, Fut>(&self, listener: F) -> Result<()>
    where
        F: Fn(DocumentFrame<T>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        let listener = Arc::new(listener);
        self.watch
            .start(move |frame| {
                let listener = listener.clone();
                async move {
                    let value = if frame.value.is_null() {
                        None
                    } else {
                        Some(Arc::new(wire::decode(&frame.value)?))
                    };
                    listener(DocumentFrame {
                        value,
                        ops: frame.ops,
                        context: frame.context,
                    })
                    .await
                    .map_err(|e| e.to_string())
                }
            })
            .map_err(Error::message)
    }
    pub fn stop(&self) -> WatchEnd {
        self.watch.stop()
    }
    pub async fn closed(&self) -> WatchEnd {
        self.watch.closed().await
    }
}
fn typed_watch<T: Send + 'static>(
    future: StorageFuture<Option<Watch>>,
) -> StorageFuture<Option<DocumentWatch<T>>> {
    Box::pin(async move {
        Ok(future.await?.map(|watch| DocumentWatch {
            watch,
            marker: std::marker::PhantomData,
        }))
    })
}
