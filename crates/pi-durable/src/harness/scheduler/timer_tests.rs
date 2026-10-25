use super::*;

async fn scheduler(timer: crate::timers::SetTimer) -> TaskScheduler {
    TaskScheduler::open(
        Arc::new(crate::MemoryStorage::default()),
        crate::harness::catalog::TaskCatalog::default(),
        SchedulerOptions::default(),
        SessionOptions {
            now: Arc::new(|| 0.0),
            set_timer: Some(timer),
            ..Default::default()
        },
        &Context::default(),
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn a_callback_fired_before_registration_returns_does_not_remain_armed() {
    let arms = Arc::new(AtomicU64::new(0));
    let count = arms.clone();
    let scheduler = scheduler(Arc::new(move |callback, _, background| {
        if !background && count.fetch_add(1, Ordering::SeqCst) == 0 {
            callback();
        }
        Some(Box::new(|| {}))
    }))
    .await;
    scheduler.arm_timer(Some(10.0));
    scheduler.arm_timer(Some(10.0));
    assert_eq!(
        arms.load(Ordering::SeqCst),
        2,
        "a fired timer cannot satisfy the next wake deadline"
    );
    scheduler
        .session()
        .close(&Context::default())
        .await
        .unwrap();
}

#[tokio::test]
async fn a_superseded_callback_cannot_clear_the_new_deadline() {
    let callbacks = Arc::new(Mutex::new(Vec::new()));
    let capture = callbacks.clone();
    let scheduler = scheduler(Arc::new(move |callback, _, background| {
        if !background {
            capture.lock().push(callback);
        }
        Some(Box::new(|| {}))
    }))
    .await;
    scheduler.arm_timer(Some(10.0));
    scheduler.arm_timer(Some(20.0));
    let old = callbacks.lock().remove(0);
    old();
    scheduler.arm_timer(Some(20.0));
    assert_eq!(
        callbacks.lock().len(),
        1,
        "the replacement timer must remain registered"
    );
    scheduler
        .session()
        .close(&Context::default())
        .await
        .unwrap();
}
