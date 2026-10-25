//! Native memory storage for stock task tests. Decorated host stores keep their
//! original callbacks; plain memory stores share this backend with the Session.
use super::*;
use pi_durable::storage::StoredEntry;

#[derive(Default)]
pub(super) struct MemoryBridge {
    stores: Mutex<HashMap<u64, Arc<ObservedMemory>>>,
    pub direct_opens: AtomicU64,
    pub calls: Arc<AtomicU64>,
    pub commits: Arc<AtomicU64>,
}
struct ObservedMemory {
    backend: MemoryStorage,
    calls: Arc<AtomicU64>,
    commits: Arc<AtomicU64>,
}
macro_rules! forward {
    ($name:ident($($arg:ident:$ty:ty),*) -> $out:ty) => {
        fn $name(&self,$($arg:$ty,)*context:&Context)->StorageFuture<$out> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            self.backend.$name($($arg,)* context)
        }
    };
}
impl Storage for ObservedMemory {
    fn commit(
        &self,
        writes: &[StorageWrite],
        adopt: Adopt,
        context: &Context,
    ) -> StorageFuture<Seq> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        self.commits.fetch_add(1, Ordering::Relaxed);
        self.backend.commit(writes, adopt, context)
    }
    fn mint_id(&self) -> StorageFuture<i64> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        self.backend.mint_id()
    }
    forward!(conversation(id:ConversationId) -> Option<ConversationRecord>);
    forward!(scan_conversations(query:&ConversationQuery,limit:usize,cursor:Option<&Cursor>) -> Page<ConversationRecord>);
    forward!(entry(id:EntryId) -> Option<StoredEntry>);
    forward!(entry_in(conversation:ConversationId,id:EntryId) -> Option<StoredEntry>);
    forward!(find_latest_head_marker(conversation:ConversationId,at:Option<EntryId>) -> Option<EntryRecord>);
    forward!(scan_entries(query:&EntryQuery,limit:usize,cursor:Option<&Cursor>) -> Page<EntryRecord>);
    forward!(task(id:TaskId) -> Option<TaskRecord>);
    forward!(scan_tasks(query:&TaskQuery,limit:usize,cursor:Option<&Cursor>) -> Page<TaskRecord>);
    forward!(submission(id:SubmissionId) -> Option<SubmissionRecord>);
    forward!(scan_submissions(query:&SubmissionQuery,limit:usize,cursor:Option<&Cursor>) -> Page<SubmissionRecord>);
    forward!(submission_by_request(conversation:ConversationId,request:&JsonString) -> Option<SubmissionRecord>);
    forward!(find_document(address:&DocumentAddress,at:DocumentPoint) -> Option<DocumentRecord>);
    forward!(document(id:DocumentId,at:DocumentPoint) -> Option<StoredDocument>);
    forward!(scan_documents(query:&DocumentQuery,limit:usize,cursor:Option<&Cursor>) -> Page<DocumentRecord>);
    forward!(close() -> ());
}
impl Hub {
    pub(super) fn memory_storage(&self, id: u64) -> Result<Arc<dyn Storage>> {
        self.memory
            .stores
            .lock()
            .get(&id)
            .cloned()
            .map(|store| store as Arc<dyn Storage>)
            .ok_or_else(|| Error::message("Unknown native memory store"))
    }
    pub(super) fn memory_request(
        self: &Arc<Self>,
        method: &str,
        request: JsonValue,
    ) -> Result<StorageFuture<Option<JsonValue>>> {
        if method == "memory.new" {
            let id = self.next.fetch_add(1, Ordering::Relaxed);
            self.memory.stores.lock().insert(
                id,
                Arc::new(ObservedMemory {
                    backend: MemoryStorage::new(),
                    calls: self.memory.calls.clone(),
                    commits: self.memory.commits.clone(),
                }),
            );
            return Ok(Box::pin(async move { Ok(Some(encode(&id))) }));
        }
        let storage = self.memory_storage(field(&request, "memory")?)?;
        let callback: Option<u64> = field(&request, "adopt")?;
        let weak = Arc::downgrade(self);
        let adoption_error = Arc::new(Mutex::new(None));
        let capture = adoption_error.clone();
        let operation = pi_durable::testing::storage_request(
            storage.as_ref(),
            &field::<String>(&request, "operation")?,
            &field::<Vec<JsonValue>>(&request, "args")?,
            Box::new(move |seq| {
                if let (Some(hub), Some(callback)) = (weak.upgrade(), callback)
                    && let Err(error) = hub.sync(
                        "memory.adopt",
                        object([("callback", encode(&callback)), ("seq", encode(&seq))]),
                    )
                {
                    *capture.lock() = Some(error);
                }
            }),
        );
        Ok(Box::pin(async move {
            let result = operation.await;
            if let Some(error) = adoption_error.lock().take() {
                return Err(error);
            }
            result
        }))
    }
}
