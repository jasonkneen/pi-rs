//! The host can replace its model collection without reopening the scheduler.
use crate::*;
use pi_ai::models::Models;
use pi_ai::types::{CacheRetention, DeferredOption, ProviderHeaders, StreamOptions, Transport};
use serde::Deserialize;
use std::sync::Arc;

pub type ModelsReader = Arc<dyn Fn() -> Result<Models> + Send + Sync>;
pub fn read_models(reader: &ModelsReader) -> Result<Models> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| reader()))
        .unwrap_or_else(|panic| Err(crate::session::panic_error(panic)))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct StoredStreamOptions {
    transport: Option<Transport>,
    timeout_ms: Option<u64>,
    max_retries: Option<u32>,
    max_retry_delay_ms: Option<u64>,
    headers: Option<ProviderHeaders>,
    metadata: Option<serde_json::Map<String, serde_json::Value>>,
    cache_retention: Option<CacheRetention>,
    deferred: Option<DeferredOption>,
    #[serde(flatten)]
    extra: serde_json::Map<String, serde_json::Value>,
}
/// Convert only at the existing SDK's UTF8 boundary; malformed strings reject
/// instead of silently changing the durable transcript or request options.
pub fn provider_options(value: &JsonObject) -> Result<StreamOptions> {
    let parsed: StoredStreamOptions = wire::decode(value.value()).map_err(Error::message)?;
    Ok(StreamOptions {
        transport: parsed.transport,
        timeout_ms: parsed.timeout_ms,
        max_retries: parsed.max_retries,
        max_retry_delay_ms: parsed.max_retry_delay_ms,
        headers: parsed.headers,
        metadata: parsed.metadata,
        cache_retention: parsed.cache_retention,
        deferred: parsed.deferred,
        extra: parsed.extra,
        ..Default::default()
    })
}
/// Reject values the current SDK cannot represent, rather than saturating a
/// negative, fractional or overflowing JavaScript number during a Rust cast.
pub fn provider_token_limit(value: f64) -> Result<u64> {
    wire::decode(&wire::encode(&value)).map_err(Error::message)
}
/// A request owns its cancellation bridge. Settlement or a dropped caller
/// releases the watcher and cancels only this provider request.
pub struct RequestScope {
    token: tokio_util::sync::CancellationToken,
    watch: tokio::task::JoinHandle<()>,
}
impl RequestScope {
    pub fn new(
        options: &mut StreamOptions,
        signal: pi_chord::AbortSignal,
        context: &Context,
    ) -> Self {
        let token = tokio_util::sync::CancellationToken::new();
        if signal.aborted() {
            token.cancel();
        }
        let pending = token.clone();
        let watch = tokio::spawn(async move {
            signal.cancelled().await;
            pending.cancel();
        });
        options.signal = Some(token.clone());
        options.telemetry = Some(context.telemetry().clone());
        Self { token, watch }
    }
}
impl Drop for RequestScope {
    fn drop(&mut self) {
        self.token.cancel();
        self.watch.abort();
    }
}
