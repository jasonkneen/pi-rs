//! A model collection is a host capability. The native default delegates to pi-ai.
use super::*;
use crate::{harness::types::ModelRef, storage::StorageFuture};
use std::{any::Any, future::Future, pin::Pin};

#[derive(Clone)]
pub struct ResolvedModel {
    pub reference: ModelRef,
    pub context_window: f64,
    pub max_tokens: f64,
    value: Arc<dyn Any + Send + Sync>,
}
impl ResolvedModel {
    pub fn new<T: Any + Send + Sync>(reference: ModelRef, context_window: f64, value: T) -> Self {
        Self {
            reference,
            context_window,
            max_tokens: 0.0,
            value: Arc::new(value),
        }
    }
    pub fn with_max_tokens(mut self, max_tokens: f64) -> Self {
        self.max_tokens = max_tokens;
        self
    }
    pub fn value<T: Any>(&self) -> Result<&T> {
        self.value
            .downcast_ref()
            .ok_or_else(|| Error::message("Model belongs to a different collection adapter"))
    }
}
pub type ModelFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>;
pub trait ModelStream: Send {
    /// Nonterminal partials only. The terminal message belongs to `result`.
    fn next(&mut self) -> ModelFuture<'_, Option<AssistantValueRef>>;
    fn result(&mut self) -> ModelFuture<'_, AssistantValueRef>;
}
pub trait ModelCollection: Send + Sync {
    fn get_model(&self, reference: &ModelRef) -> Result<Option<ResolvedModel>>;
    fn stream_simple(
        &self,
        model: ResolvedModel,
        messages: Vec<Arc<JsonValue>>,
        options: JsonObject,
        context: Context,
    ) -> StorageFuture<Box<dyn ModelStream>>;
    fn complete_simple(
        &self,
        model: ResolvedModel,
        messages: Vec<Arc<JsonValue>>,
        options: JsonObject,
        context: Context,
    ) -> StorageFuture<AssistantValueRef> {
        let stream = self.stream_simple(model, messages, options, context);
        Box::pin(async move { stream.await?.result().await })
    }
    fn fetch_deferred(
        &self,
        model: ResolvedModel,
        handle: JsonObject,
        context: Context,
    ) -> StorageFuture<AssistantValueRef>;
    fn cancel_deferred(
        &self,
        model: ResolvedModel,
        handle: JsonObject,
        context: Context,
    ) -> StorageFuture<()>;
}
pub type ModelCollectionReader = Arc<dyn Fn() -> Result<Arc<dyn ModelCollection>> + Send + Sync>;
pub fn read_collection(reader: &ModelCollectionReader) -> Result<Arc<dyn ModelCollection>> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| reader()))
        .unwrap_or_else(|panic| Err(crate::session::panic_error(panic)))
}
struct SdkCollection(Models);
pub fn sdk_collection(models: Models) -> Arc<dyn ModelCollection> {
    Arc::new(SdkCollection(models))
}
struct SdkStream {
    stream: pi_ai::AssistantMessageEventStream,
    _scope: RequestScope,
}
fn message(message: pi_ai::AssistantMessage) -> Result<AssistantValueRef> {
    Ok(AssistantValue::json(
        JsonObject::try_from_value(wire::encode(&pi_ai::Message::Assistant(message)))
            .map_err(Error::message)?,
    ))
}
impl ModelStream for SdkStream {
    fn next(&mut self) -> ModelFuture<'_, Option<AssistantValueRef>> {
        Box::pin(async move {
            while let Some(event) = self.stream.next_event().await {
                if !event.is_terminal() {
                    return message(event.message().clone()).map(Some);
                }
            }
            Ok(None)
        })
    }
    fn result(&mut self) -> ModelFuture<'_, AssistantValueRef> {
        Box::pin(async move { message(self.stream.result().await) })
    }
}
impl ModelCollection for SdkCollection {
    fn get_model(&self, reference: &ModelRef) -> Result<Option<ResolvedModel>> {
        let provider = reference
            .provider
            .to_string_checked()
            .map_err(|error| Error::message(error.to_string()))?;
        let id = reference
            .model_id
            .to_string_checked()
            .map_err(|error| Error::message(error.to_string()))?;
        Ok(self.0.get_model(&provider, &id).map(|model| {
            let max_tokens = model.max_tokens as f64;
            ResolvedModel::new(reference.clone(), model.context_window as f64, model)
                .with_max_tokens(max_tokens)
        }))
    }
    fn stream_simple(
        &self,
        model: ResolvedModel,
        messages: Vec<Arc<JsonValue>>,
        options: JsonObject,
        context: Context,
    ) -> StorageFuture<Box<dyn ModelStream>> {
        let models = self.0.clone();
        Box::pin(async move {
            let model = model.value::<pi_ai::Model>()?;
            let messages = messages
                .iter()
                .map(|message| wire::decode(message).map_err(Error::message))
                .collect::<Result<Vec<pi_ai::Message>>>()?;
            let mut options = provider_options(&options)?;
            let signal = context
                .abort_signal()
                .cloned()
                .unwrap_or_else(|| pi_chord::AbortController::new().signal());
            let scope = RequestScope::new(&mut options, signal, &context);
            Ok(Box::new(SdkStream {
                stream: models.stream_simple(
                    model,
                    &pi_ai::Context {
                        messages,
                        ..Default::default()
                    },
                    options,
                ),
                _scope: scope,
            }) as Box<dyn ModelStream>)
        })
    }
    fn fetch_deferred(
        &self,
        model: ResolvedModel,
        handle: JsonObject,
        context: Context,
    ) -> StorageFuture<AssistantValueRef> {
        let models = self.0.clone();
        Box::pin(async move {
            let model = model.value::<pi_ai::Model>()?;
            let handle = wire::decode(handle.value()).map_err(Error::message)?;
            let mut options = StreamOptions::default();
            let signal = context
                .abort_signal()
                .cloned()
                .unwrap_or_else(|| pi_chord::AbortController::new().signal());
            let scope = RequestScope::new(&mut options, signal, &context);
            let result = models.fetch_deferred(model, &handle, options).await;
            drop(scope);
            message(result)
        })
    }
    fn cancel_deferred(
        &self,
        model: ResolvedModel,
        handle: JsonObject,
        context: Context,
    ) -> StorageFuture<()> {
        let models = self.0.clone();
        Box::pin(async move {
            let model = model.value::<pi_ai::Model>()?;
            let handle = wire::decode(handle.value()).map_err(Error::message)?;
            let mut options = StreamOptions::default();
            let signal = context
                .abort_signal()
                .cloned()
                .unwrap_or_else(|| pi_chord::AbortController::new().signal());
            let scope = RequestScope::new(&mut options, signal, &context);
            let result = models
                .cancel_deferred(model, &handle, options)
                .await
                .map_err(|error| Error::message(error.to_string()));
            drop(scope);
            result
        })
    }
}
