//! Native graph/watch transport; JS supplies only the original listeners.
use super::*;
use pi_chord::WatchEnd;
use pi_durable::harness::task_graph::{TaskGraph, TaskGraphWatch};
#[derive(Default)]
pub(super) struct GraphBridge {
    watches: Mutex<HashMap<u64, ObservedWatch>>,
    revisions: Mutex<Vec<(u64, Arc<TaskGraph>)>>,
    pub reads: AtomicU64,
    pub frames: AtomicU64,
}
#[derive(Clone, Copy, Default)]
struct Activity {
    serial: u64,
    active: bool,
}
#[derive(Clone)]
struct ObservedWatch {
    watch: TaskGraphWatch,
    activity: tokio::sync::watch::Sender<Activity>,
}
impl GraphBridge {
    pub(super) async fn response_boundary(&self) {
        let watches = self.watches.lock().values().cloned().collect::<Vec<_>>();
        for observed in watches {
            let mut activity = observed.activity.subscribe();
            loop {
                if activity.borrow().active {
                    break;
                }
                tokio::select! {
                    _ = observed.watch.delivery_idle() => break,
                    changed = activity.changed() => if changed.is_err() { break; },
                }
            }
        }
    }
}
fn end(reason: WatchEnd) -> JsonValue {
    match reason {
        WatchEnd::Stopped => encode(&serde_json::json!({"reason":"stopped"})),
        WatchEnd::Cancelled => encode(&serde_json::json!({"reason":"cancelled"})),
        WatchEnd::Retired => encode(&serde_json::json!({"reason":"retired"})),
        WatchEnd::Ended => encode(&serde_json::json!({"reason":"ended"})),
        WatchEnd::Failed { error } => {
            encode(&serde_json::json!({"reason":"failed","error":{"message":error}}))
        }
        WatchEnd::ListenerError { error } => {
            encode(&serde_json::json!({"reason":"listener_error","error":{"message":error}}))
        }
    }
}
impl Hub {
    fn graph_value(&self, value: Arc<TaskGraph>) -> JsonValue {
        let mut revisions = self.graph.revisions.lock();
        let revision =
            if let Some((id, _)) = revisions.iter().find(|(_, held)| Arc::ptr_eq(held, &value)) {
                *id
            } else {
                let id = self.next.fetch_add(1, Ordering::Relaxed);
                revisions.push((id, value.clone()));
                id
            };
        object([
            ("revision", encode(&revision)),
            ("value", encode(value.as_ref())),
        ])
    }
    pub(super) fn graph_request(
        self: &Arc<Self>,
        method: &str,
        request: JsonValue,
        context: Context,
    ) -> Result<StorageFuture<Option<JsonValue>>> {
        if method == "graph.watch" {
            self.graph.reads.fetch_add(1, Ordering::Relaxed);
            let watch = self.scheduler(&request)?.watch_task_graph(&context);
            let hub = self.clone();
            return Ok(Box::pin(async move {
                let watch = watch.await?;
                let id = hub.next.fetch_add(1, Ordering::Relaxed);
                let value = hub.graph_value(watch.value());
                let (activity, _) = tokio::sync::watch::channel(Activity::default());
                hub.graph
                    .watches
                    .lock()
                    .insert(id, ObservedWatch { watch, activity });
                Ok(Some(object([("watch", encode(&id)), ("snapshot", value)])))
            }));
        }
        let id: u64 = field(&request, "watch")?;
        let observed = self
            .graph
            .watches
            .lock()
            .get(&id)
            .cloned()
            .ok_or_else(|| Error::message("Unknown graph watch"))?;
        let watch = observed.watch;
        if method == "graph.turn" {
            let serial: u64 = field(&request, "serial")?;
            return Ok(Box::pin(async move {
                let mut activity = observed.activity.subscribe();
                loop {
                    if activity.borrow().serial > serial {
                        break;
                    }
                    tokio::select! {
                        _ = watch.delivery_idle() => break,
                        changed = activity.changed() => if changed.is_err() { break; },
                    }
                }
                Ok(None)
            }));
        }
        let result = match method {
            "graph.value" => Some(self.graph_value(watch.value())),
            "graph.start" => {
                let callback: u64 = field(&request, "callback")?;
                let hub = self.clone();
                let activity = observed.activity;
                watch
                    .start(move |frame| {
                        hub.graph.frames.fetch_add(1, Ordering::Relaxed);
                        let serial = activity.borrow().serial + 1;
                        let args = object([
                            ("watch", encode(&id)),
                            ("serial", encode(&serial)),
                            ("callback", encode(&callback)),
                            ("snapshot", hub.graph_value(frame.value)),
                            ("ops", encode(&frame.ops)),
                        ]);
                        let call = hub.call_context("graphFrame", args, &frame.context);
                        activity.send_replace(Activity {
                            serial,
                            active: true,
                        });
                        let activity = activity.clone();
                        async move {
                            let result = call.await.map(|_| ()).map_err(|error| error.to_string());
                            activity.send_modify(|activity| activity.active = false);
                            result
                        }
                    })
                    .map_err(Error::message)?;
                None
            }
            "graph.stop" => Some(end(watch.stop())),
            "graph.cancel" => Some(end(watch.cancel())),
            "graph.closed" => {
                return Ok(Box::pin(async move { Ok(Some(end(watch.closed().await))) }));
            }
            other => return Err(Error::message(format!("Unknown graph method {other}"))),
        };
        Ok(Box::pin(async move { Ok(result) }))
    }
}
