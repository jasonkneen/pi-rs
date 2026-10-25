//! Native conversation view and event acquisition for unmodified stock tests.
use super::*;

#[derive(Default)]
pub(super) struct ViewBridge {
    subscriptions: Mutex<HashMap<u64, Subscription>>,
    pub reads: AtomicU64,
    pub event_reads: AtomicU64,
    pub event_frames: AtomicU64,
}
impl Hub {
    pub(super) fn view_request(
        self: &Arc<Self>,
        method: &str,
        request: JsonValue,
        context: Context,
    ) -> Result<StorageFuture<Option<JsonValue>>> {
        if method == "view.unsubscribeCommits" {
            self.views
                .subscriptions
                .lock()
                .remove(&field(&request, "subscription")?);
            return Ok(Box::pin(async { Ok(None) }));
        }
        if method == "view.subscribeClose" {
            let callback: u64 = field(&request, "callback")?;
            let weak = Arc::downgrade(self);
            let subscription = self.session(&request)?.subscribe_close(move || {
                if let Some(hub) = weak.upgrade() {
                    let _ = hub.sync("session.closeListener", encode(&callback));
                }
            })?;
            let id = self.next.fetch_add(1, Ordering::Relaxed);
            self.views.subscriptions.lock().insert(id, subscription);
            return Ok(Box::pin(async move { Ok(Some(encode(&id))) }));
        }
        if method == "view.subscribeCommits" {
            let callback: u64 = field(&request, "callback")?;
            let weak = Arc::downgrade(self);
            let subscription =
                self.session(&request)?
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
        let views = self.facade(&request)?.views().clone();
        self.views.reads.fetch_add(1, Ordering::Relaxed);
        let conversation = field(&request, "conversation")?;
        if method == "view.events" {
            self.views.event_reads.fetch_add(1, Ordering::Relaxed);
            let acquire = pi_durable::harness::events::watch_events(&views, conversation, &context);
            let hub = self.clone();
            return Ok(Box::pin(async move {
                let stream = acquire.await?;
                let snapshot = encode(stream.snapshot.as_ref());
                let mut descriptor = hub.register_watch(graph_bridge::ValueWatch::Events(stream));
                descriptor
                    .as_object_mut()
                    .unwrap()
                    .insert("eventSnapshot".into(), snapshot);
                Ok(Some(descriptor))
            }));
        }
        if method == "view.watch" {
            let acquire = views.watch(conversation, &context);
            let hub = self.clone();
            return Ok(Box::pin(async move {
                Ok(Some(hub.register_watch(
                    graph_bridge::ValueWatch::Conversation(acquire.await?),
                )))
            }));
        }
        Err(Error::message(format!("Unknown view method {method}")))
    }
}
