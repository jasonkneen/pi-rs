//! Original stock submission handles delegate every operation to the native service.
use super::*;
#[derive(Default)]
pub(super) struct SubmissionBridge {
    pub services: Mutex<HashMap<u64, Submissions>>,
    pub admissions: AtomicU64,
    pub reads: AtomicU64,
    pub waits: AtomicU64,
    pub aborts: AtomicU64,
}
impl Hub {
    pub(super) fn submission_request(
        self: &Arc<Self>,
        method: &str,
        request: JsonValue,
        context: Context,
    ) -> Result<StorageFuture<Option<JsonValue>>> {
        let harness: u64 = field(&request, "harness")?;
        let service = self
            .submissions
            .services
            .lock()
            .get(&harness)
            .cloned()
            .ok_or_else(|| Error::message("Unknown submission service"))?;
        Ok(match method {
            "submission.submit" => {
                self.submissions.admissions.fetch_add(1, Ordering::Relaxed);
                let operation = service.submit(
                    field(&request, "conversation")?,
                    field(&request, "draft")?,
                    &context,
                );
                Box::pin(async move { Ok(Some(encode(&operation.await?.id))) })
            }
            "submission.get" => {
                self.submissions.reads.fetch_add(1, Ordering::Relaxed);
                let operation = service.get(field(&request, "submission")?, &context);
                Box::pin(
                    async move { Ok(operation.await?.map(|submission| encode(&submission.id))) },
                )
            }
            "submission.status" => {
                self.submissions.reads.fetch_add(1, Ordering::Relaxed);
                let operation = service.status(field(&request, "submission")?, &context);
                Box::pin(async move { Ok(Some(encode(&operation.await?))) })
            }
            "submission.wait" => {
                self.submissions.waits.fetch_add(1, Ordering::Relaxed);
                let operation = service.wait(field(&request, "submission")?, &context);
                Box::pin(async move { Ok(Some(encode(&operation.await?))) })
            }
            "submission.abort" => {
                self.submissions.aborts.fetch_add(1, Ordering::Relaxed);
                let id = field(&request, "submission")?;
                let operation = service.abort(id, &context, field(&request, "conversation")?);
                let handle = request["handle"].as_bool() == Some(true);
                Box::pin(async move {
                    Ok(Some(match operation.await? {
                        Some(result) => encode(&result),
                        None if handle => {
                            return Err(Error::message(format!("Submission {id} does not exist")));
                        }
                        None => encode(&"not_found"),
                    }))
                })
            }
            other => {
                return Err(Error::message(format!(
                    "Unknown submission operation {other}"
                )));
            }
        })
    }
}
