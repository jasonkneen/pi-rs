//! One cooperative FIFO poll queue per Session. Ready continuations share the
//! Session's order; pending I/O leaves the queue free for other operations.
use futures::{
    FutureExt,
    task::{ArcWake, waker_ref},
};
use parking_lot::Mutex;
use std::{
    cell::RefCell,
    collections::{HashMap, VecDeque},
    future::Future,
    pin::Pin,
    sync::{
        Arc, OnceLock, Weak,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
};

type JobFuture = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;
thread_local! {
    static CURRENT: RefCell<Option<Executor>> = const { RefCell::new(None) };
}
#[derive(Clone)]
pub(crate) struct Executor(Arc<Inner>);
struct Inner {
    state: Mutex<State>,
    runtime: OnceLock<tokio::runtime::Handle>,
    changed: tokio::sync::Notify,
}
#[derive(Default)]
struct State {
    ready: VecDeque<Arc<Job>>,
    running: bool,
    stopped: bool,
}
struct Job {
    executor: Weak<Inner>,
    future: Mutex<Option<JobFuture>>,
    queued: AtomicBool,
}
impl Default for Executor {
    fn default() -> Self {
        Self(Arc::new(Inner {
            state: Mutex::new(State::default()),
            runtime: OnceLock::new(),
            changed: tokio::sync::Notify::new(),
        }))
    }
}
impl Executor {
    pub fn spawn(&self, operation: impl Future<Output = ()> + Send + 'static) {
        self.0.runtime.get_or_init(tokio::runtime::Handle::current);
        let job = Arc::new(Job {
            executor: Arc::downgrade(&self.0),
            future: Mutex::new(Some(Box::pin(async move {
                // Domain jobs publish their own panics. An unexpected panic
                // must still drop that job without stranding the whole queue.
                let _ = std::panic::AssertUnwindSafe(operation).catch_unwind().await;
            }))),
            queued: AtomicBool::new(false),
        });
        job.schedule();
    }
    fn enqueue(&self, job: Arc<Job>) {
        let start = {
            let mut state = self.0.state.lock();
            if state.stopped {
                return;
            }
            state.ready.push_back(job);
            if state.running {
                false
            } else {
                state.running = true;
                true
            }
        };
        self.0.changed.notify_one();
        if start {
            let executor = self.clone();
            // Capture the guard before spawning so cancellation before the
            // first poll releases queued futures as well.
            let guard = RunGuard {
                executor: self.clone(),
                completed: false,
                active: HashMap::new(),
            };
            self.0
                .runtime
                .get()
                .expect("executor has a runtime")
                .spawn(async move { executor.run(guard).await });
        }
    }
    async fn run(&self, mut guard: RunGuard) {
        // Pending jobs stay alive even if they have not retained a Waker.
        // Owning them in the Tokio task also releases them on shutdown.
        let mut polls = 0;
        loop {
            let changed = self.0.changed.notified();
            let job = {
                let mut state = self.0.state.lock();
                match state.ready.pop_front() {
                    Some(job) => Some(job),
                    None if guard.active.is_empty() => {
                        state.running = false;
                        guard.completed = true;
                        return;
                    }
                    None => None,
                }
            };
            let Some(job) = job else {
                changed.await;
                continue;
            };
            job.queued.store(false, Ordering::Release);
            let future = job.future.lock().take();
            if let Some(mut future) = future {
                let previous = CURRENT.with(|current| current.replace(Some(self.clone())));
                let restore = Restore(previous);
                let waker = waker_ref(&job);
                let mut context = Context::from_waker(&waker);
                let id = Arc::as_ptr(&job) as usize;
                if future.as_mut().poll(&mut context) == Poll::Pending {
                    *job.future.lock() = Some(future);
                    guard.active.insert(id, job.clone());
                } else {
                    guard.active.remove(&id);
                }
                drop(restore);
            }
            polls += 1;
            if polls == 64 {
                polls = 0;
                tokio::task::yield_now().await;
            }
        }
    }
}
struct Restore(Option<Executor>);
impl Drop for Restore {
    fn drop(&mut self) {
        CURRENT.with(|current| current.replace(self.0.take()));
    }
}
struct RunGuard {
    executor: Executor,
    completed: bool,
    // Drop seals the queue before these futures run their destructors.
    active: HashMap<usize, Arc<Job>>,
}
impl Drop for RunGuard {
    fn drop(&mut self) {
        if !self.completed {
            let ready = {
                let mut state = self.executor.0.state.lock();
                state.running = false;
                state.stopped = true;
                std::mem::take(&mut state.ready)
            };
            // Detach every future before dropping any of them. A shared future
            // can retain this job's waker, and wake/drop another job while its
            // own notification lock is held. Leaving the future inside that
            // waker's job creates a cycle or reenters the shared lock at shutdown.
            let futures = self
                .active
                .values()
                .chain(ready.iter())
                .filter_map(|job| job.future.lock().take())
                .collect::<Vec<_>>();
            drop(futures);
            drop(ready);
        }
    }
}
impl Job {
    fn schedule(self: &Arc<Self>) {
        if self.queued.swap(true, Ordering::AcqRel) {
            return;
        }
        if let Some(inner) = self.executor.upgrade() {
            Executor(inner).enqueue(self.clone());
        }
    }
}
impl ArcWake for Job {
    fn wake_by_ref(job: &Arc<Self>) {
        job.schedule();
    }
}
pub(crate) fn spawn(operation: impl Future<Output = ()> + Send + 'static) {
    match CURRENT.with(|current| current.borrow().clone()) {
        Some(executor) => executor.spawn(operation),
        None => {
            tokio::spawn(operation);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::{mpsc, oneshot};

    #[test]
    fn runtime_shutdown_detaches_pending_futures_from_their_own_shared_wakers() {
        struct Dropped(Arc<AtomicBool>);
        impl Drop for Dropped {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let executor = Executor::default();
        let dropped = Arc::new(AtomicBool::new(false));
        let retained = dropped.clone();
        let shared = std::future::pending::<()>().boxed().shared();
        let pending = shared.clone();
        runtime.block_on(async {
            executor.spawn(async move {
                let _retained = Dropped(retained);
                pending.await;
            });
            tokio::task::yield_now().await;
        });
        assert!(!dropped.load(Ordering::SeqCst));
        drop(runtime);
        assert!(dropped.load(Ordering::SeqCst));
        drop(shared);
    }

    #[tokio::test]
    async fn jobs_start_and_resume_in_admission_and_wake_order() {
        let executor = Executor::default();
        let events = Arc::new(Mutex::new(Vec::new()));
        let (ready, mut started) = mpsc::unbounded_channel();
        let (done, mut finished) = mpsc::unbounded_channel();
        let mut gates = Vec::new();
        for id in 0..3 {
            let (send, receive) = oneshot::channel();
            gates.push(send);
            let (events, ready, done) = (events.clone(), ready.clone(), done.clone());
            executor.spawn(async move {
                events.lock().push((id, "start"));
                ready.send(()).unwrap();
                receive.await.unwrap();
                events.lock().push((id, "resume"));
                done.send(()).unwrap();
            });
        }
        for _ in 0..3 {
            started.recv().await.unwrap();
        }
        assert_eq!(*events.lock(), [(0, "start"), (1, "start"), (2, "start")]);
        // The current-thread runtime cannot poll jobs between these sends.
        let [first, second, third]: [_; 3] = gates.try_into().unwrap();
        third.send(()).unwrap();
        first.send(()).unwrap();
        second.send(()).unwrap();
        for _ in 0..3 {
            finished.recv().await.unwrap();
        }
        assert_eq!(
            &events.lock()[3..],
            [(2, "resume"), (0, "resume"), (1, "resume")]
        );
    }

    #[tokio::test]
    async fn nested_eager_jobs_inherit_the_queue_and_pending_io_does_not_block_it() {
        let executor = Executor::default();
        let (blocked, receive) = oneshot::channel();
        let (ready, entered) = oneshot::channel();
        let (done, finished) = oneshot::channel();
        executor.spawn(async move {
            spawn(async move {
                ready.send(()).unwrap();
                receive.await.unwrap();
            });
            spawn(async move {
                done.send(()).unwrap();
            });
        });
        entered.await.unwrap();
        finished.await.unwrap();
        blocked.send(()).unwrap();
    }

    #[test]
    fn pending_jobs_live_until_runtime_shutdown_even_without_a_retained_waker() {
        struct Probe(Arc<AtomicBool>);
        impl Drop for Probe {
            fn drop(&mut self) {
                self.0.store(true, Ordering::Release);
            }
        }
        let dropped = Arc::new(AtomicBool::new(false));
        let probe = Probe(dropped.clone());
        let executor = Executor::default();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        runtime.block_on(async {
            let (ready, entered) = oneshot::channel();
            executor.spawn(async move {
                let _probe = probe;
                ready.send(()).unwrap();
                std::future::pending::<()>().await;
            });
            entered.await.unwrap();
        });
        assert!(!dropped.load(Ordering::Acquire));
        drop(runtime);
        assert!(dropped.load(Ordering::Acquire));
    }

    #[test]
    fn shutdown_before_the_first_poll_releases_queued_jobs() {
        let executor = Executor::default();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let owned = Arc::new(());
        let retained = owned.clone();
        runtime.block_on(async {
            executor.spawn(async move {
                let _retained = retained;
                std::future::pending::<()>().await;
            });
        });
        assert_eq!(Arc::strong_count(&owned), 2);
        drop(runtime);
        assert_eq!(Arc::strong_count(&owned), 1);
        assert!(executor.0.state.lock().ready.is_empty());
    }
}
