use pi_chord::context::*;
use pi_chord::delta::Op;
use pi_chord::{JsonValue, NativeWatch, Watch, WatchEnd};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

fn number(i: usize) -> JsonValue {
    JsonValue::from(i)
}
fn advance(watch: &Watch, i: usize) {
    watch.advance(
        Arc::new(number(i)),
        vec![Op::Replace(number(i))],
        BACKGROUND_CONTEXT.clone(),
    );
}

#[tokio::test]
async fn acquisition_value_and_exact_frames_are_held_until_async_start() {
    let watch = Watch::new(number(0), || {}, |_| {});
    advance(&watch, 1);
    advance(&watch, 2);
    assert_eq!(*watch.value(), number(0));
    let (send, mut receive) = tokio::sync::mpsc::unbounded_channel();
    watch
        .start(move |frame| {
            send.send(frame).unwrap();
            async { Ok(()) }
        })
        .unwrap();
    assert!(receive.try_recv().is_err());
    for i in 1..=2 {
        let frame = receive.recv().await.unwrap();
        assert_eq!(*frame.value, number(i));
        assert_eq!(&*frame.ops, &[Op::Replace(number(i))]);
    }
    assert_eq!(*watch.value(), number(2));
    watch.stop();
}

#[tokio::test]
async fn delivery_is_serialized_and_frame_cancellation_is_masked() {
    let key = create_context_key::<String>("frame");
    let (context, controller) = with_cancel(&with_context_value(
        &key,
        "value".into(),
        &BACKGROUND_CONTEXT,
    ));
    controller.abort();
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let (send, mut receive) = tokio::sync::mpsc::unbounded_channel();
    let watch = Watch::new(number(0), || {}, |_| {});
    watch
        .start({
            let gate = gate.clone();
            move |frame| {
                send.send(frame.clone()).unwrap();
                let gate = gate.clone();
                async move {
                    gate.acquire().await.unwrap().forget();
                    Ok(())
                }
            }
        })
        .unwrap();
    watch.advance(Arc::new(number(1)), vec![], context);
    let first = receive.recv().await.unwrap();
    assert!(first.context.abort_signal().is_none());
    assert_eq!(&*first.context.value(&key).unwrap(), "value");
    advance(&watch, 2);
    tokio::task::yield_now().await;
    assert!(receive.try_recv().is_err());
    gate.add_permits(1);
    assert_eq!(*receive.recv().await.unwrap().value, number(2));
    watch.stop();
    gate.add_permits(1);
}

#[tokio::test]
async fn overflow_collapses_101_pending_frames_but_100_remain_exact() {
    for count in [100, 101] {
        let watch = Watch::new(number(0), || {}, |_| {});
        for i in 1..=count {
            advance(&watch, i);
        }
        let (send, mut receive) = tokio::sync::mpsc::unbounded_channel();
        watch
            .start(move |frame| {
                send.send(frame).unwrap();
                async { Ok(()) }
            })
            .unwrap();
        if count == 100 {
            for i in 1..=count {
                assert_eq!(*receive.recv().await.unwrap().value, number(i));
            }
        } else {
            let frame = receive.recv().await.unwrap();
            assert_eq!(*frame.value, number(101));
            assert_eq!(&*frame.ops, &[Op::Replace(number(101))]);
        }
        watch.stop();
    }
}

#[tokio::test]
async fn overflow_uses_source_replacement_without_folding_in_flight_frame() {
    let watch = Watch::with_replacement(
        number(0),
        || {},
        |_| {},
        Some(Arc::new(|| Arc::new(number(999)))),
    );
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let (send, mut receive) = tokio::sync::mpsc::unbounded_channel();
    watch
        .start({
            let gate = gate.clone();
            move |frame| {
                send.send(frame).unwrap();
                let gate = gate.clone();
                async move {
                    gate.acquire().await.unwrap().forget();
                    Ok(())
                }
            }
        })
        .unwrap();
    advance(&watch, 1);
    assert_eq!(*receive.recv().await.unwrap().value, number(1));
    for i in 2..=102 {
        advance(&watch, i);
    }
    gate.add_permits(1);
    let frame = receive.recv().await.unwrap();
    assert_eq!(*frame.value, number(999));
    assert_eq!(&*frame.ops, &[Op::Replace(number(999))]);
    watch.stop();
    gate.add_permits(1);
}

#[tokio::test]
async fn retirement_delivers_null_last_and_ends_only_after_its_callback() {
    let watch = Watch::new(number(0), || {}, |_| {});
    for i in 1..=100 {
        advance(&watch, i);
    }
    watch.retire(BACKGROUND_CONTEXT.clone());
    advance(&watch, 101);
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let (send, mut receive) = tokio::sync::mpsc::unbounded_channel();
    watch
        .start({
            let gate = gate.clone();
            move |frame| {
                send.send(frame).unwrap();
                let gate = gate.clone();
                async move {
                    gate.acquire().await.unwrap().forget();
                    Ok(())
                }
            }
        })
        .unwrap();
    let frame = receive.recv().await.unwrap();
    assert_eq!(*frame.value, JsonValue::Null);
    assert_eq!(&*frame.ops, &[Op::Replace(JsonValue::Null)]);
    assert!(watch.end_reason().is_none());
    gate.add_permits(1);
    assert_eq!(watch.closed().await, WatchEnd::Retired);
}

#[tokio::test]
async fn stopping_is_idempotent_does_not_await_callback_and_releases_source() {
    let source = Arc::new(());
    let weak_source = Arc::downgrade(&source);
    let detached = Arc::new(AtomicUsize::new(0));
    let watch = Watch::new(
        number(0),
        {
            let detached = detached.clone();
            move || {
                drop(source);
                detached.fetch_add(1, Ordering::SeqCst);
            }
        },
        |_| {},
    );
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let (entered, mut receive) = tokio::sync::mpsc::unbounded_channel();
    watch
        .start({
            let gate = gate.clone();
            move |_| {
                entered.send(()).unwrap();
                let gate = gate.clone();
                async move {
                    gate.acquire().await.unwrap().forget();
                    Ok(())
                }
            }
        })
        .unwrap();
    advance(&watch, 1);
    receive.recv().await.unwrap();
    advance(&watch, 2);
    assert_eq!(watch.stop(), WatchEnd::Stopped);
    assert_eq!(watch.stop(), WatchEnd::Stopped);
    assert_eq!(watch.closed().await, WatchEnd::Stopped);
    assert_eq!(detached.load(Ordering::SeqCst), 1);
    assert!(weak_source.upgrade().is_none());
    assert_eq!(
        watch.start(|_| async { Ok(()) }).unwrap_err(),
        "Watch is already started"
    );
    gate.add_permits(1);
    let never_started = Watch::new(number(0), || {}, |_| {});
    never_started.stop();
    assert_eq!(
        never_started.start(|_| async { Ok(()) }).unwrap_err(),
        "Watch is stopped"
    );
}

#[tokio::test]
async fn listener_failure_reports_once_and_drops_pending_frames() {
    let reports = Arc::new(AtomicUsize::new(0));
    let watch = Watch::new(number(0), || {}, {
        let reports = reports.clone();
        move |error| {
            assert_eq!(error, "failure");
            reports.fetch_add(1, Ordering::SeqCst);
        }
    });
    let calls = Arc::new(AtomicUsize::new(0));
    watch
        .start({
            let calls = calls.clone();
            move |_| {
                calls.fetch_add(1, Ordering::SeqCst);
                async { Err("failure".into()) }
            }
        })
        .unwrap();
    advance(&watch, 1);
    advance(&watch, 2);
    assert_eq!(
        watch.closed().await,
        WatchEnd::ListenerError {
            error: "failure".into()
        }
    );
    assert_eq!(reports.load(Ordering::SeqCst), 1);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn cancellation_and_source_end_discard_pending_retirement() {
    for already in [false, true] {
        let watch = Watch::new(number(0), || {}, |_| {});
        let controller = AbortController::new();
        if already {
            controller.abort();
        }
        watch.observe_cancellation(controller.signal()).unwrap();
        controller.abort();
        assert_eq!(watch.closed().await, WatchEnd::Cancelled);
    }
    let watch = Watch::new(number(0), || {}, |_| {});
    watch.retire(BACKGROUND_CONTEXT.clone());
    assert_eq!(watch.end(None), WatchEnd::Ended);
    assert_eq!(*watch.value(), number(0));
    let watch = Watch::new(number(0), || {}, |_| {});
    assert_eq!(
        watch.end(Some("storage".into())),
        WatchEnd::Failed {
            error: "storage".into()
        }
    );
}

#[tokio::test]
async fn native_watch_keeps_identity_and_collapses_backlog() {
    let initial = Arc::new(String::from("initial"));
    let watch = NativeWatch::new(initial.clone(), || {}, |_| {});
    assert!(Arc::ptr_eq(&watch.value(), &initial));
    let newest = Arc::new(String::from("newest"));
    for _ in 0..101 {
        watch.advance(newest.clone(), BACKGROUND_CONTEXT.clone());
    }
    let (send, mut receive) = tokio::sync::mpsc::unbounded_channel();
    watch
        .start(move |value, _| {
            send.send(value).unwrap();
            async { Ok(()) }
        })
        .unwrap();
    assert!(Arc::ptr_eq(&receive.recv().await.unwrap(), &newest));
    assert_eq!(watch.stop(), WatchEnd::Stopped);
}

#[tokio::test]
async fn cancellation_can_only_be_installed_once_on_a_live_watch() {
    let watch = Watch::new(number(0), || {}, |_| {});
    let controller = AbortController::new();
    watch.observe_cancellation(controller.signal()).unwrap();
    assert_eq!(
        watch.observe_cancellation(controller.signal()).unwrap_err(),
        "Watch cancellation is already installed"
    );
    watch.stop();
    assert!(watch.observe_cancellation(controller.signal()).is_ok());
}

#[tokio::test]
async fn delivery_idle_waits_for_the_listener_and_wakes_every_waiter_on_stop() {
    let watch = Watch::new(number(0), || {}, |_| {});
    advance(&watch, 1);
    // An unstarted watch has no active delivery loop.
    watch.delivery_idle().await;
    let (send, mut receive) = tokio::sync::mpsc::unbounded_channel();
    let gate = Arc::new(tokio::sync::Notify::new());
    let release = gate.clone();
    watch
        .start(move |_| {
            send.send(()).unwrap();
            let gate = gate.clone();
            async move {
                gate.notified().await;
                Ok(())
            }
        })
        .unwrap();
    receive.recv().await.unwrap();
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(5), watch.delivery_idle())
            .await
            .is_err()
    );
    let first = watch.clone();
    let second = watch.clone();
    let first = tokio::spawn(async move { first.delivery_idle().await });
    let second = tokio::spawn(async move { second.delivery_idle().await });
    watch.stop();
    release.notify_one();
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        first.await.unwrap();
        second.await.unwrap();
    })
    .await
    .unwrap();
}
