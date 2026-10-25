//! Serialized watch delivery, bounded to 100 pending frames.

use crate::{
    JsonValue,
    context::{AbortSignal, Context, without_abort_signal},
    delta::Op,
};
use futures::FutureExt;
use parking_lot::Mutex;
use std::{
    collections::VecDeque,
    future::Future,
    pin::Pin,
    sync::{Arc, Weak},
};

const MAX_PENDING_WATCH_FRAMES: usize = 100;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WatchEnd {
    Stopped,
    Cancelled,
    Retired,
    Ended,
    ListenerError { error: String },
    Failed { error: String },
}

#[derive(Clone)]
pub struct WatchFrame<T> {
    pub value: Arc<T>,
    pub ops: Arc<[Op]>,
    pub context: Context,
    retires: bool,
}
type Callback<T> = Arc<
    dyn Fn(WatchFrame<T>) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send>> + Send + Sync,
>;
type Replace<T> = Arc<dyn Fn() -> Arc<T> + Send + Sync>;
type Overflow<T> = Arc<dyn Fn(&Arc<T>) -> Vec<Op> + Send + Sync>;

struct State<T> {
    value: Arc<T>,
    listener: Option<Callback<T>>,
    detach: Option<Box<dyn FnOnce() + Send>>,
    report: Option<Arc<dyn Fn(String) + Send + Sync>>,
    replace: Option<Replace<T>>,
    overflow: Overflow<T>,
    pending: VecDeque<WatchFrame<T>>,
    started: bool,
    scheduled: bool,
    running: bool,
    retired: bool,
    cancellation_installed: bool,
    end: Option<WatchEnd>,
}
struct Inner<T> {
    state: Mutex<State<T>>,
    closed: tokio::sync::watch::Sender<Option<WatchEnd>>,
    delivery: tokio::sync::watch::Sender<u64>,
}

pub struct Watch<T = JsonValue> {
    inner: Arc<Inner<T>>,
}
impl<T> Clone for Watch<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<T: Send + Sync + 'static> Watch<T> {
    /// A typed immutable revision with JSON operations for exact-frame replay.
    pub fn with_frames(
        value: Arc<T>,
        detach: impl FnOnce() + Send + 'static,
        report: impl Fn(String) + Send + Sync + 'static,
        overflow: impl Fn(&Arc<T>) -> Vec<Op> + Send + Sync + 'static,
    ) -> Self {
        Self::build(value, detach, report, None, Arc::new(overflow))
    }
    /// A typed stream whose overflow value is derived from its current source,
    /// rather than from the last delivered batch.
    pub fn with_source(
        value: Arc<T>,
        detach: impl FnOnce() + Send + 'static,
        report: impl Fn(String) + Send + Sync + 'static,
        replace: impl Fn() -> Arc<T> + Send + Sync + 'static,
    ) -> Self {
        Self::build(
            value,
            detach,
            report,
            Some(Arc::new(replace)),
            Arc::new(|_| Vec::new()),
        )
    }
    fn build(
        value: Arc<T>,
        detach: impl FnOnce() + Send + 'static,
        report: impl Fn(String) + Send + Sync + 'static,
        replace: Option<Replace<T>>,
        overflow: Overflow<T>,
    ) -> Self {
        let (closed, _) = tokio::sync::watch::channel(None);
        let (delivery, _) = tokio::sync::watch::channel(0);
        Self {
            inner: Arc::new(Inner {
                closed,
                delivery,
                state: Mutex::new(State {
                    value,
                    listener: None,
                    detach: Some(Box::new(detach)),
                    report: Some(Arc::new(report)),
                    replace,
                    overflow,
                    pending: VecDeque::new(),
                    started: false,
                    scheduled: false,
                    running: false,
                    retired: false,
                    cancellation_installed: false,
                    end: None,
                }),
            }),
        }
    }
    pub fn value(&self) -> Arc<T> {
        self.inner.state.lock().value.clone()
    }
    pub fn end_reason(&self) -> Option<WatchEnd> {
        self.inner.state.lock().end.clone()
    }
    /// Wait for the delivery loop to become idle. A pending listener keeps it
    /// active; stopped and unstarted watches are idle. Foreign-runtime adapters
    /// use this boundary when handing their completed callbacks back to Rust.
    pub async fn delivery_idle(&self) {
        let mut delivery = self.inner.delivery.subscribe();
        loop {
            {
                let state = self.inner.state.lock();
                if state.end.is_some() || !state.started || (!state.scheduled && !state.running) {
                    return;
                }
            }
            if delivery.changed().await.is_err() {
                return;
            }
        }
    }

    pub fn start<F, Fut>(&self, listener: F) -> Result<(), String>
    where
        F: Fn(WatchFrame<T>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), String>> + Send + 'static,
    {
        {
            let mut state = self.inner.state.lock();
            if state.started {
                return Err("Watch is already started".into());
            }
            if state.end.is_some() {
                return Err("Watch is stopped".into());
            }
            state.started = true;
            state.listener = Some(Arc::new(move |frame| Box::pin(listener(frame))));
        }
        self.schedule();
        Ok(())
    }

    pub fn advance(&self, value: Arc<T>, ops: Vec<Op>, context: Context) {
        self.enqueue(value, ops, context, false);
    }

    fn enqueue(&self, value: Arc<T>, ops: Vec<Op>, context: Context, retires: bool) {
        // A replacement callback belongs to the source and can acquire its locks.
        // Never call it while holding the watch mutex.
        let replace = {
            let state = self.inner.state.lock();
            if state.end.is_some() || state.retired {
                return;
            }
            if !retires && state.pending.len() >= MAX_PENDING_WATCH_FRAMES {
                state.replace.clone()
            } else {
                None
            }
        };
        let replacement = replace.map(|replace| replace());
        {
            let mut state = self.inner.state.lock();
            if state.end.is_some() || state.retired {
                return;
            }
            let overflowed = state.pending.len() >= MAX_PENDING_WATCH_FRAMES;
            if overflowed {
                state.pending.clear();
            }
            let (value, ops) = if overflowed && !retires {
                let value = replacement.unwrap_or(value);
                let ops = (state.overflow)(&value);
                (value, ops)
            } else {
                (value, ops)
            };
            state.retired = retires;
            state.pending.push_back(WatchFrame {
                value,
                ops: ops.into(),
                context,
                retires,
            });
        }
        self.schedule();
    }

    fn schedule(&self) {
        {
            let mut state = self.inner.state.lock();
            if !state.started
                || state.pending.is_empty()
                || state.end.is_some()
                || state.scheduled
                || state.running
            {
                return;
            }
            state.scheduled = true;
        }
        let weak = Arc::downgrade(&self.inner);
        tokio::spawn(async move {
            // Acquisition/start/advance never invoke a callback on their stack.
            tokio::task::yield_now().await;
            Self::drain(weak).await;
        });
    }

    async fn drain(weak: Weak<Inner<T>>) {
        let Some(inner) = weak.upgrade() else {
            return;
        };
        {
            let mut state = inner.state.lock();
            state.scheduled = false;
            if state.end.is_some() || !state.started || state.running {
                return;
            }
            state.running = true;
        }
        loop {
            let next = {
                let mut state = inner.state.lock();
                if state.end.is_some() {
                    state.running = false;
                    return;
                }
                match (state.pending.pop_front(), state.listener.clone()) {
                    (Some(mut frame), Some(listener)) => {
                        state.value = frame.value.clone();
                        frame.context = without_abort_signal(&frame.context);
                        Some((frame, listener))
                    }
                    _ => {
                        state.running = false;
                        None
                    }
                }
            };
            let Some((frame, listener)) = next else {
                inner
                    .delivery
                    .send_modify(|epoch| *epoch = epoch.wrapping_add(1));
                return;
            };
            let retires = frame.retires;
            let result = std::panic::AssertUnwindSafe(async move { listener(frame).await })
                .catch_unwind()
                .await;
            let error = match result {
                Ok(Ok(())) => None,
                Ok(Err(error)) => Some(error),
                Err(panic) => Some(
                    panic
                        .downcast_ref::<String>()
                        .cloned()
                        .or_else(|| panic.downcast_ref::<&str>().map(|s| (*s).into()))
                        .unwrap_or_else(|| "Watch listener panicked".into()),
                ),
            };
            let watch = Self {
                inner: inner.clone(),
            };
            if let Some(error) = error {
                let report = {
                    let state = inner.state.lock();
                    if state.end.is_none() {
                        state.report.clone()
                    } else {
                        None
                    }
                };
                if let Some(report) = report {
                    report(error.clone());
                }
                watch.terminate(WatchEnd::ListenerError { error });
                return;
            }
            if retires {
                watch.terminate(WatchEnd::Retired);
                return;
            }
        }
    }

    fn terminate(&self, end: WatchEnd) -> WatchEnd {
        let (end, detach, listener, report, replace, pending) = {
            let mut state = self.inner.state.lock();
            if let Some(end) = &state.end {
                return end.clone();
            }
            state.end = Some(end.clone());
            state.cancellation_installed = false;
            (
                end,
                state.detach.take(),
                state.listener.take(),
                state.report.take(),
                state.replace.take(),
                std::mem::take(&mut state.pending),
            )
        };
        // Source callbacks and captured values may reenter the watch on drop.
        drop((listener, report, replace, pending));
        if let Some(detach) = detach {
            detach();
        }
        self.inner.closed.send_replace(Some(end.clone()));
        self.inner
            .delivery
            .send_modify(|epoch| *epoch = epoch.wrapping_add(1));
        end
    }

    pub fn stop(&self) -> WatchEnd {
        self.terminate(WatchEnd::Stopped)
    }
    pub fn cancel(&self) -> WatchEnd {
        self.terminate(WatchEnd::Cancelled)
    }
    pub fn end(&self, failure: Option<String>) -> WatchEnd {
        self.terminate(failure.map_or(WatchEnd::Ended, |error| WatchEnd::Failed { error }))
    }
    pub async fn closed(&self) -> WatchEnd {
        let mut receiver = self.inner.closed.subscribe();
        loop {
            if let Some(end) = receiver.borrow_and_update().clone() {
                return end;
            }
            if receiver.changed().await.is_err() {
                return WatchEnd::Ended;
            }
        }
    }
    pub fn observe_cancellation(&self, signal: AbortSignal) -> Result<(), String> {
        {
            let mut state = self.inner.state.lock();
            if state.cancellation_installed {
                return Err("Watch cancellation is already installed".into());
            }
            if state.end.is_some() {
                return Ok(());
            }
            state.cancellation_installed = true;
        }
        // Store no strong reference from the cancellation observer back into the source.
        let weak = Arc::downgrade(&self.inner);
        let mut closed = self.inner.closed.subscribe();
        if signal.aborted() {
            self.cancel();
            return Ok(());
        }
        if closed.borrow().is_some() {
            return Ok(());
        }
        tokio::spawn(async move {
            tokio::select! {
                _ = signal.cancelled() => { if let Some(inner) = weak.upgrade() { Self {inner}.cancel(); } }
                _ = closed.changed() => {}
            }
        });
        Ok(())
    }
}

impl Watch<JsonValue> {
    pub fn new(
        value: JsonValue,
        detach: impl FnOnce() + Send + 'static,
        report: impl Fn(String) + Send + Sync + 'static,
    ) -> Self {
        Self::with_replacement(value, detach, report, None)
    }
    pub fn with_replacement(
        value: JsonValue,
        detach: impl FnOnce() + Send + 'static,
        report: impl Fn(String) + Send + Sync + 'static,
        replace: Option<Replace<JsonValue>>,
    ) -> Self {
        Self::build(
            Arc::new(value),
            detach,
            report,
            replace,
            Arc::new(|value| vec![Op::Replace(value.as_ref().clone())]),
        )
    }
    pub fn retire(&self, context: Context) {
        self.enqueue(
            Arc::new(JsonValue::Null),
            vec![Op::Replace(JsonValue::Null)],
            context,
            true,
        );
    }
}

pub struct NativeWatch<T> {
    watch: Watch<T>,
}
impl<T> Clone for NativeWatch<T> {
    fn clone(&self) -> Self {
        Self {
            watch: self.watch.clone(),
        }
    }
}
impl<T: Send + Sync + 'static> NativeWatch<T> {
    pub fn new(
        value: Arc<T>,
        detach: impl FnOnce() + Send + 'static,
        report: impl Fn(String) + Send + Sync + 'static,
    ) -> Self {
        Self {
            watch: Watch::build(value, detach, report, None, Arc::new(|_| Vec::new())),
        }
    }
    pub fn value(&self) -> Arc<T> {
        self.watch.value()
    }
    pub fn start<F, Fut>(&self, listener: F) -> Result<(), String>
    where
        F: Fn(Arc<T>, Context) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), String>> + Send + 'static,
    {
        self.watch
            .start(move |frame| listener(frame.value, frame.context))
    }
    pub fn advance(&self, value: Arc<T>, context: Context) {
        self.watch.advance(value, Vec::new(), context);
    }
    pub fn stop(&self) -> WatchEnd {
        self.watch.stop()
    }
    pub fn cancel(&self) -> WatchEnd {
        self.watch.cancel()
    }
    pub fn end(&self, failure: Option<String>) -> WatchEnd {
        self.watch.end(failure)
    }
    pub async fn closed(&self) -> WatchEnd {
        self.watch.closed().await
    }
    pub fn observe_cancellation(&self, signal: AbortSignal) -> Result<(), String> {
        self.watch.observe_cancellation(signal)
    }
}
