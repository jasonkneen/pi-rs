use super::*;
use pi_durable::harness::{compaction, context::ContextView};
impl Hub {
    pub(super) fn compaction_request(&self, request: &JsonValue) -> Result<Option<JsonValue>> {
        self.compaction_reads.fetch_add(1, Ordering::Relaxed);
        Ok(match request["method"].to_utf8().as_deref() {
            Some("compaction.serialize") => {
                Some(encode(&compaction::serialize_conversation(&field::<
                    Vec<Arc<JsonValue>>,
                >(
                    request, "messages",
                )?)))
            }
            Some("compaction.select") => {
                let value = &request["view"];
                let view = ContextView {
                    head: field(value, "head")?,
                    entries: field(value, "entries")?,
                    contributions: field(value, "contributions")?,
                    messages: field(value, "messages")?,
                };
                compaction::select_cut(&view, field(request, "budget")?).map(|index| encode(&index))
            }
            other => {
                return Err(Error::message(format!(
                    "Unknown compaction method {other:?}"
                )));
            }
        })
    }
}
