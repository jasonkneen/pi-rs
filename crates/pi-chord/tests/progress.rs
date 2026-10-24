use parking_lot::Mutex;
use pi_chord::output::{PROGRESS_BYTES_PER_SECOND, Progress, ProgressClock};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

struct Timer {
    at: f64,
    cancelled: Arc<AtomicBool>,
    callback: Option<Box<dyn FnOnce() + Send>>,
}
#[derive(Default)]
struct ManualClock {
    now: Mutex<f64>,
    timers: Mutex<Vec<Timer>>,
}
impl ProgressClock for ManualClock {
    fn now(&self) -> f64 {
        *self.now.lock()
    }
    fn set_timer(
        &self,
        callback: Box<dyn FnOnce() + Send>,
        delay_ms: f64,
    ) -> Box<dyn FnOnce() + Send> {
        let cancelled = Arc::new(AtomicBool::new(false));
        self.timers.lock().push(Timer {
            at: self.now() + delay_ms,
            cancelled: cancelled.clone(),
            callback: Some(callback),
        });
        Box::new(move || cancelled.store(true, Ordering::SeqCst))
    }
}
impl ManualClock {
    fn advance(&self, now: f64) {
        *self.now.lock() = now;
        loop {
            let timer = {
                let mut timers = self.timers.lock();
                timers
                    .iter()
                    .position(|t| t.at <= now && !t.cancelled.load(Ordering::SeqCst))
                    .map(|i| timers.remove(i))
            };
            let Some(mut timer) = timer else {
                break;
            };
            timer.callback.take().unwrap()();
        }
    }
    fn fire_early(&self) {
        let mut timer = {
            let mut timers = self.timers.lock();
            let index = timers
                .iter()
                .position(|t| !t.cancelled.load(Ordering::SeqCst))
                .unwrap();
            timers.remove(index)
        };
        timer.callback.take().unwrap()();
    }
    fn pending(&self) -> usize {
        self.timers
            .lock()
            .iter()
            .filter(|t| !t.cancelled.load(Ordering::SeqCst))
            .count()
    }
}

#[tokio::test]
async fn first_change_is_immediate_and_later_changes_coalesce_by_written_size() {
    let clock = Arc::new(ManualClock::default());
    let (send, mut receive) = tokio::sync::mpsc::unbounded_channel();
    let progress = Progress::with_clock(
        move || {
            send.send(()).unwrap();
            async { Ok(PROGRESS_BYTES_PER_SECOND) }
        },
        |_| panic!("unexpected error"),
        100.0,
        clock.clone(),
    );
    let first = progress.mark_and_wait();
    receive.recv().await.unwrap();
    first.await.unwrap().unwrap();
    let second = progress.mark_and_wait();
    progress.mark();
    assert_eq!(clock.pending(), 1);
    clock.advance(999.0);
    tokio::task::yield_now().await;
    assert!(receive.try_recv().is_err());
    clock.advance(1000.0);
    receive.recv().await.unwrap();
    second.await.unwrap().unwrap();
    assert!(progress.stop().await.is_empty());
}

#[tokio::test]
async fn early_timer_rechecks_clock_and_stop_disarms_it() {
    let clock = Arc::new(ManualClock::default());
    let progress = Progress::with_clock(|| async { Ok(1) }, |_| {}, 100.0, clock.clone());
    progress.mark_and_wait().await.unwrap().unwrap();
    let pending = progress.mark_and_wait();
    clock.fire_early();
    assert_eq!(clock.pending(), 1);
    let final_waiters = progress.stop().await;
    assert_eq!(clock.pending(), 0);
    assert_eq!(final_waiters.len(), 1);
    final_waiters
        .into_iter()
        .next()
        .unwrap()
        .send(Ok(()))
        .unwrap();
    pending.await.unwrap().unwrap();
    clock.advance(10000.0);
}

#[tokio::test]
async fn at_most_one_write_runs_and_stop_returns_only_the_pending_change() {
    let clock = Arc::new(ManualClock::default());
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let (send, mut receive) = tokio::sync::mpsc::unbounded_channel();
    let progress = Progress::with_clock(
        {
            let gate = gate.clone();
            move || {
                send.send(()).unwrap();
                let gate = gate.clone();
                async move {
                    gate.acquire().await.unwrap().forget();
                    Ok(1)
                }
            }
        },
        |_| {},
        100.0,
        clock,
    );
    let first = progress.mark_and_wait();
    receive.recv().await.unwrap();
    let second = progress.mark_and_wait();
    let stopped = tokio::spawn({
        let progress = progress.clone();
        async move { progress.stop().await }
    });
    tokio::task::yield_now().await;
    assert!(!stopped.is_finished());
    assert!(receive.try_recv().is_err());
    gate.add_permits(1);
    let final_waiters = stopped.await.unwrap();
    first.await.unwrap().unwrap();
    assert_eq!(final_waiters.len(), 1);
    final_waiters
        .into_iter()
        .next()
        .unwrap()
        .send(Ok(()))
        .unwrap();
    second.await.unwrap().unwrap();
    assert!(receive.try_recv().is_err());
}

#[tokio::test]
async fn write_failure_rejects_its_waiters_reports_and_preserves_minimum_interval() {
    let clock = Arc::new(ManualClock::default());
    let (reported, mut reports) = tokio::sync::mpsc::unbounded_channel();
    let progress = Progress::with_clock(
        || async { Err("failure".into()) },
        move |error| {
            reported.send(error).unwrap();
        },
        100.0,
        clock.clone(),
    );
    assert_eq!(
        progress.mark_and_wait().await.unwrap().unwrap_err(),
        "failure"
    );
    assert_eq!(reports.recv().await.unwrap(), "failure");
    let second = progress.mark_and_wait();
    assert_eq!(clock.pending(), 1);
    clock.advance(100.0);
    assert_eq!(second.await.unwrap().unwrap_err(), "failure");
    assert_eq!(reports.recv().await.unwrap(), "failure");
    assert!(progress.stop().await.is_empty());
}
