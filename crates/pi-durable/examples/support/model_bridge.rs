//! Original model collections are provider capabilities; Rust owns generation.
use super::*;
use pi_ai::{
    auth::types::ProviderAuth,
    models::{Models, Provider},
    types::{
        AssistantMessageEvent, AssistantMessageEventStream, DeferredHandle, Model, StreamOptions,
        TranscriptContext,
    },
    utils::event_stream::AssistantMessageEventSender,
};
use std::sync::Weak;

#[derive(Default)]
pub(super) struct ModelBridge {
    streams: Mutex<HashMap<u64, AssistantMessageEventSender>>,
    pub requests: AtomicU64,
    pub polls: AtomicU64,
    pub cancellations: AtomicU64,
}
struct HostProvider {
    hub: Weak<Hub>,
    harness: u64,
    id: String,
    models: Vec<Model>,
    auth: ProviderAuth,
}
struct HostAuth;
#[async_trait::async_trait]
impl pi_ai::auth::types::ApiKeyAuth for HostAuth {
    fn name(&self) -> &str {
        "Host model collection"
    }
    async fn resolve(
        &self,
        _: pi_ai::auth::types::ApiKeyResolveInput,
    ) -> anyhow::Result<Option<pi_ai::auth::types::AuthResult>> {
        Ok(Some(Default::default()))
    }
}
impl HostProvider {
    fn invoke(
        &self,
        method: &'static str,
        model: &Model,
        request: JsonValue,
        options: StreamOptions,
    ) -> AssistantMessageEventStream {
        let (sender, stream) = AssistantMessageEventStream::new();
        let Some(hub) = self.hub.upgrade() else {
            sender.end(Some(pi_ai::AssistantMessage {
                stop_reason: pi_ai::StopReason::Error,
                error_message: Some("Model host closed".into()),
                ..Default::default()
            }));
            return stream;
        };
        if method == "streamSimple" {
            hub.models.requests.fetch_add(1, Ordering::Relaxed);
        } else {
            hub.models.polls.fetch_add(1, Ordering::Relaxed);
        }
        let id = hub.next.fetch_add(1, Ordering::Relaxed);
        hub.models.streams.lock().insert(id, sender.clone());
        let operation = hub.call(
            "models.request",
            object([
                ("stream", encode(&id)),
                ("harness", encode(&self.harness)),
                ("method", encode(&method)),
                ("model", encode(model)),
                ("request", request),
                ("options", encode_options(&options)),
                ("aborted", encode(&options.is_aborted())),
            ]),
        );
        let model = model.clone();
        tokio::spawn(async move {
            let signal = options.signal.unwrap_or_default();
            tokio::pin!(operation);
            let result = tokio::select! {
                result=&mut operation=>result,
                _=signal.cancelled()=>{hub.emit(object([("type",encode(&"models.signal")),("stream",encode(&id))]));operation.await}
            };
            let result = result
                .and_then(|value| value.ok_or_else(|| Error::message("Model returned undefined")))
                .and_then(|value| decode::<pi_ai::AssistantMessage>(&value));
            let message = result.unwrap_or_else(|error| pi_ai::AssistantMessage {
                stop_reason: pi_ai::StopReason::Error,
                error_message: Some(error.to_string()),
                ..pi_ai::AssistantMessage::empty_for(&model, 0)
            });
            sender.end(Some(message));
            hub.models.streams.lock().remove(&id);
        });
        stream
    }
}
#[async_trait::async_trait]
impl Provider for HostProvider {
    fn id(&self) -> &str {
        &self.id
    }
    fn name(&self) -> &str {
        &self.id
    }
    fn auth(&self) -> &ProviderAuth {
        &self.auth
    }
    fn get_models(&self) -> Vec<Model> {
        self.models.clone()
    }
    fn stream(
        &self,
        model: &Model,
        context: &TranscriptContext,
        options: StreamOptions,
    ) -> AssistantMessageEventStream {
        self.stream_simple(model, context, options)
    }
    fn stream_simple(
        &self,
        model: &Model,
        context: &TranscriptContext,
        options: StreamOptions,
    ) -> AssistantMessageEventStream {
        self.invoke("streamSimple", model, encode(context), options)
    }
    fn supports_fetch_deferred(&self) -> bool {
        true
    }
    fn fetch_deferred(
        &self,
        model: &Model,
        handle: &DeferredHandle,
        options: StreamOptions,
    ) -> AssistantMessageEventStream {
        self.invoke("fetchDeferred", model, encode(handle), options)
    }
    fn supports_cancel_deferred(&self) -> bool {
        true
    }
    async fn cancel_deferred(
        &self,
        model: &Model,
        handle: &DeferredHandle,
        options: StreamOptions,
    ) -> anyhow::Result<()> {
        let hub = self
            .hub
            .upgrade()
            .ok_or_else(|| anyhow::anyhow!("Model host closed"))?;
        hub.models.cancellations.fetch_add(1, Ordering::Relaxed);
        hub.call(
            "models.cancel",
            object([
                ("harness", encode(&self.harness)),
                ("model", encode(model)),
                ("handle", encode(handle)),
                ("options", encode_options(&options)),
            ]),
        )
        .await
        .map_err(|error| anyhow::anyhow!(error.to_string()))?;
        Ok(())
    }
}
impl Hub {
    pub(super) fn host_models(self: &Arc<Self>, harness: u64) -> Result<Models> {
        let catalog: Vec<Model> = decode(&self.sync("models.catalog", encode(&harness))?)?;
        let mut providers = IndexMap::<String, Vec<Model>>::new();
        for model in catalog {
            providers
                .entry(model.provider.clone())
                .or_default()
                .push(model);
        }
        let models = Models::default();
        for (id, catalog) in providers {
            models.set_provider(Arc::new(HostProvider {
                hub: Arc::downgrade(self),
                harness,
                id,
                models: catalog,
                auth: ProviderAuth {
                    api_key: Some(Arc::new(HostAuth)),
                    oauth: None,
                },
            }));
        }
        Ok(models)
    }
    pub(super) fn model_event(&self, value: &JsonValue) -> Result<()> {
        let id = field::<u64>(value, "stream")?;
        let event: AssistantMessageEvent = field(value, "event")?;
        if let Some(sender) = self.models.streams.lock().get(&id) {
            sender.push(event);
        }
        Ok(())
    }
}
fn encode_options(options: &StreamOptions) -> JsonValue {
    let mut value = encode(&options.extra);
    macro_rules! field {
        ($name:literal,$field:ident) => {
            if let Some(field) = &options.$field {
                value
                    .as_object_mut()
                    .unwrap()
                    .insert($name.into(), encode(field));
            }
        };
    }
    field!("headers", headers);
    field!("identity", identity);
    field!("timeoutMs", timeout_ms);
    field!("maxRetries", max_retries);
    field!("maxRetryDelayMs", max_retry_delay_ms);
    field!("temperature", temperature);
    field!("samplingParams", sampling_params);
    field!("maxTokens", max_tokens);
    field!("transport", transport);
    field!("cacheRetention", cache_retention);
    field!("sessionId", session_id);
    field!("metadata", metadata);
    field!("toolChoice", tool_choice);
    field!("reasoning", reasoning);
    field!("deferred", deferred);
    field!("thinkingBudgets", thinking_budgets);
    value
}
