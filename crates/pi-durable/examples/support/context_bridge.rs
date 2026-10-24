use super::*;
use pi_durable::harness::context::{self, ContextBounds};
fn encode_view(view: &context::ContextView) -> JsonValue {
    // JSON alone loses aliasing. Preserve native Arc identity across the two
    // public message arrays, including equal messages from distinct entries.
    let positions = view
        .contributions
        .iter()
        .enumerate()
        .flat_map(|(entry, messages)| {
            messages
                .iter()
                .enumerate()
                .map(move |(message, value)| (Arc::as_ptr(value) as usize, (entry, message)))
        })
        .collect::<HashMap<_, _>>();
    let aliases = view
        .messages
        .iter()
        .map(|message| positions.get(&(Arc::as_ptr(message) as usize)).copied())
        .collect::<Vec<_>>();
    object([
        ("value", encode(view)),
        ("messageContributions", encode(&aliases)),
    ])
}
impl Hub {
    pub fn context_request(
        self: Arc<Self>,
        method: &str,
        request: JsonValue,
        context: Context,
    ) -> Result<StorageFuture<Option<JsonValue>>> {
        if method == "context.release" {
            self.ranges.lock().remove(&field::<u64>(&request, "range")?);
            return Ok(Box::pin(async { Ok(None) }));
        }
        self.context_reads.fetch_add(1, Ordering::Relaxed);
        let storage = remote_storage::RemoteStorage {
            hub: self.clone(),
            handle: field(&request, "storage")?,
        };
        let conversation = field(&request, "conversation")?;
        Ok(match method {
            "context.capture" => Box::pin(async move {
                Ok(context::capture_context_bounds(
                    &storage,
                    conversation,
                    &context,
                    field(&request, "at")?,
                )
                .await?
                .map(|bounds| encode(&bounds)))
            }),
            "context.derive" => Box::pin(async move {
                Ok(Some(encode_view(
                    &context::derive_context(
                        &storage,
                        conversation,
                        field(&request, "bounds")?,
                        &context,
                    )
                    .await?,
                )))
            }),
            "context.active" => Box::pin(async move {
                Ok(Some(encode(
                    &context::active_entries(
                        &storage,
                        conversation,
                        field(&request, "bounds")?,
                        &context,
                    )
                    .await?,
                )))
            }),
            "context.range" => {
                let previous: Option<u64> = field(&request, "previous")?;
                let previous = previous.and_then(|id| self.ranges.lock().get(&id).cloned());
                let bounds: Option<ContextBounds> = field(&request, "bounds")?;
                Box::pin(async move {
                    let read =
                        context::read_range(&storage, conversation, bounds, &context, previous)
                            .await?;
                    let range = read.range.map(|range| {
                        let id = self.next.fetch_add(1, Ordering::Relaxed);
                        let bounds = encode(&range.bounds);
                        self.ranges.lock().insert(id, range);
                        object([("handle", encode(&id)), ("bounds", bounds)])
                    });
                    Ok(Some(object([
                        ("view", encode_view(&read.view)),
                        ("range", range.unwrap_or(JsonValue::Null)),
                    ])))
                })
            }
            other => {
                return Err(Error::message(format!(
                    "Unknown context bridge method {other}"
                )));
            }
        })
    }
}
