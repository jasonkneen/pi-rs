use crate::{Error, Result, storage::StorageFuture};
use futures::{
    FutureExt,
    future::{BoxFuture, Shared},
};
use parking_lot::Mutex;
use std::future::Future;
pub(crate) fn eager<T: Send + 'static>(
    operation: impl Future<Output = Result<T>> + Send + 'static,
) -> StorageFuture<T> {
    let (send, receive) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        let result = std::panic::AssertUnwindSafe(operation)
            .catch_unwind()
            .await
            .unwrap_or_else(|p| Err(crate::session::panic_error(p)));
        let _ = send.send(result);
    });
    Box::pin(async move { receive.await.map_err(|e| Error::message(e.to_string()))? })
}
/// Enqueue at call time and keep running if the caller drops its waiter.
pub(crate) struct MutationLine {
    tail: Mutex<Shared<BoxFuture<'static, ()>>>,
}
impl Default for MutationLine {
    fn default() -> Self {
        Self {
            tail: Mutex::new(async {}.boxed().shared()),
        }
    }
}
impl MutationLine {
    pub fn enqueue<T: Send + 'static>(
        &self,
        operation: impl Future<Output = Result<T>> + Send + 'static,
    ) -> StorageFuture<T> {
        let (finish, finished) = tokio::sync::oneshot::channel();
        let previous = std::mem::replace(
            &mut *self.tail.lock(),
            async move {
                let _ = finished.await;
            }
            .boxed()
            .shared(),
        );
        eager(async move {
            previous.await;
            let result = std::panic::AssertUnwindSafe(operation)
                .catch_unwind()
                .await
                .unwrap_or_else(|p| Err(crate::session::panic_error(p)));
            let _ = finish.send(());
            result
        })
    }
}
