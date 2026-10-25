//! Original event adapters attach to native conversation view mounts.
use super::*;
use pi_chord::delta::Op;
use pi_durable::harness::view::*;
use std::sync::Weak;

#[derive(Default)]
pub(super) struct ViewBridge {
    services: Mutex<HashMap<u64, ConversationViews>>,
    releases: Mutex<HashMap<u64, DetachView>>,
    subscriptions: Mutex<HashMap<u64, Subscription>>,
    pub reads: AtomicU64,
    pub publications: AtomicU64,
}
struct HostViewObserver {
    hub: Weak<Hub>,
    id: u64,
}
impl ViewObserver for HostViewObserver {
    fn advance(&self, value: Arc<ConversationView>, ops: &[Op], _: &Context) -> Result<()> {
        if let Some(hub) = self.hub.upgrade() {
            hub.sync(
                "view.advance",
                object([
                    ("observer", encode(&self.id)),
                    ("value", hub.revision(value)),
                    ("ops", encode(&ops)),
                ]),
            )?;
        }
        Ok(())
    }
    fn publication(
        &self,
        before: Arc<ConversationView>,
        after: Arc<ConversationView>,
        ops: &[Op],
        publication: &CommitPublication,
        _: &Context,
    ) -> Result<()> {
        if let Some(hub) = self.hub.upgrade() {
            hub.views.publications.fetch_add(1, Ordering::Relaxed);
            hub.sync(
                "view.publication",
                object([
                    ("observer", encode(&self.id)),
                    ("before", hub.revision(before)),
                    ("after", hub.revision(after)),
                    ("ops", encode(&ops)),
                    ("publication", encode(publication)),
                ]),
            )?;
        }
        Ok(())
    }
    fn end(&self, failure: Option<Error>) -> Result<()> {
        if let Some(hub) = self.hub.upgrade() {
            hub.sync(
                "view.end",
                object([
                    ("observer", encode(&self.id)),
                    (
                        "failure",
                        failure.as_ref().map(error_value).unwrap_or(JsonValue::Null),
                    ),
                ]),
            )?;
        }
        Ok(())
    }
}
impl Hub {
    pub(super) fn view_request(
        self: &Arc<Self>,
        method: &str,
        request: JsonValue,
        context: Context,
    ) -> Result<StorageFuture<Option<JsonValue>>> {
        if method == "view.detach" {
            let release = self
                .views
                .releases
                .lock()
                .remove(&field(&request, "observer")?);
            if let Some(release) = release {
                release();
            }
            return Ok(Box::pin(async { Ok(None) }));
        }
        if method == "view.unsubscribeCommits" {
            self.views
                .subscriptions
                .lock()
                .remove(&field(&request, "subscription")?);
            return Ok(Box::pin(async { Ok(None) }));
        }
        if method == "view.subscribeCommits" {
            let callback: u64 = field(&request, "callback")?;
            let weak = Arc::downgrade(self);
            let subscription =
                self.scheduler(&request)?
                    .session()
                    .subscribe_commits(move |publication, _| {
                        if let Some(hub) = weak.upgrade() {
                            hub.sync(
                                "commit.publication",
                                object([
                                    ("callback", encode(&callback)),
                                    ("publication", encode(publication)),
                                ]),
                            )?;
                        }
                        Ok(())
                    })?;
            let id = self.next.fetch_add(1, Ordering::Relaxed);
            self.views.subscriptions.lock().insert(id, subscription);
            return Ok(Box::pin(async move { Ok(Some(encode(&id))) }));
        }
        let harness: u64 = field(&request, "harness")?;
        let views = {
            let mut services = self.views.services.lock();
            if let Some(views) = services.get(&harness) {
                views.clone()
            } else {
                let views = ConversationViews::new(self.scheduler(&request)?.session().clone())?;
                services.insert(harness, views.clone());
                views
            }
        };
        self.views.reads.fetch_add(1, Ordering::Relaxed);
        let conversation = field(&request, "conversation")?;
        if method == "view.watch" {
            let acquire = views.watch(conversation, &context);
            let hub = self.clone();
            return Ok(Box::pin(async move {
                Ok(Some(hub.register_watch(
                    graph_bridge::ValueWatch::Conversation(acquire.await?),
                )))
            }));
        }
        if method != "view.attach" {
            return Err(Error::message(format!("Unknown view method {method}")));
        }
        let callback: u64 = field(&request, "callback")?;
        let hub = self.clone();
        let assigned = Arc::new(AtomicU64::new(0));
        let capture = assigned.clone();
        let callback_context = context.clone();
        let acquire = views.attach(
            conversation,
            move |value, release, _| async move {
                let id = hub.next.fetch_add(1, Ordering::Relaxed);
                capture.store(id, Ordering::Release);
                hub.views.releases.lock().insert(id, release);
                let operation = hub.call_context(
                    "view.create",
                    object([
                        ("harness", encode(&harness)),
                        ("callback", encode(&callback)),
                        ("observer", encode(&id)),
                        ("value", hub.revision(value)),
                    ]),
                    &callback_context,
                );
                if let Err(error) = operation.await {
                    hub.views.releases.lock().remove(&id);
                    return Err(error);
                }
                Ok(Arc::new(HostViewObserver {
                    hub: Arc::downgrade(&hub),
                    id,
                }))
            },
            &context,
        );
        let hub = self.clone();
        Ok(Box::pin(async move {
            match acquire.await {
                Ok(_) => Ok(None),
                Err(error) => {
                    let id = assigned.load(Ordering::Acquire);
                    hub.views.releases.lock().remove(&id);
                    if id != 0 {
                        let _ = hub.sync("view.discard", encode(&id));
                    }
                    Err(error)
                }
            }
        }))
    }
}
