//! Backend-independent request runner used by the reference conformance bridge
//! and native tests of independently generated reference traces.

use crate::{
    storage::{Adopt, Storage},
    wire, *,
};
use serde::Serialize;
use std::sync::Arc;

fn arg<T: serde::de::DeserializeOwned>(args: &[JsonValue], index: usize) -> Result<T> {
    wire::decode(
        args.get(index)
            .ok_or_else(|| Error::request("Missing storage argument"))?,
    )
    .map_err(Error::request)
}
fn optional<T: serde::de::DeserializeOwned>(args: &[JsonValue], index: usize) -> Result<Option<T>> {
    match args.get(index) {
        None | Some(JsonValue::Null) => Ok(None),
        Some(value) => wire::decode(value).map(Some).map_err(Error::request),
    }
}
fn encoded<T: Serialize>(value: T) -> Result<Option<JsonValue>> {
    Ok(Some(
        pi_chord::json::to_value(&value).map_err(Error::message)?,
    ))
}
fn maybe<T: Serialize>(value: Option<T>) -> Result<Option<JsonValue>> {
    value
        .map(|value| pi_chord::json::to_value(&value).map_err(Error::message))
        .transpose()
}
pub async fn storage_request(
    storage: &dyn Storage,
    method: &str,
    args: &[JsonValue],
    adopt: Adopt,
) -> Result<Option<JsonValue>> {
    let context = pi_chord::context::BACKGROUND_CONTEXT.clone();
    match method {
        "commit" => encoded(
            storage
                .commit(&arg::<Vec<StorageWrite>>(args, 0)?, adopt, &context)
                .await?,
        ),
        "mintId" => encoded(storage.mint_id().await?),
        "conversation" => maybe(storage.conversation(arg(args, 0)?, &context).await?),
        "scanConversations" => encoded(
            storage
                .scan_conversations(
                    &arg(args, 0)?,
                    arg(args, 1)?,
                    optional::<Cursor>(args, 2)?.as_ref(),
                    &context,
                )
                .await?,
        ),
        "entry" => {
            if args.len() == 1 {
                maybe(storage.entry(arg(args, 0)?, &context).await?)
            } else {
                maybe(
                    storage
                        .entry_in(arg(args, 0)?, arg(args, 1)?, &context)
                        .await?,
                )
            }
        }
        "findLatestHeadMarker" => maybe(
            storage
                .find_latest_head_marker(arg(args, 0)?, optional(args, 1)?, &context)
                .await?,
        ),
        "scanEntries" => encoded(
            storage
                .scan_entries(
                    &arg(args, 0)?,
                    arg(args, 1)?,
                    optional::<Cursor>(args, 2)?.as_ref(),
                    &context,
                )
                .await?,
        ),
        "task" => maybe(storage.task(arg(args, 0)?, &context).await?),
        "scanTasks" => encoded(
            storage
                .scan_tasks(
                    &arg(args, 0)?,
                    arg(args, 1)?,
                    optional::<Cursor>(args, 2)?.as_ref(),
                    &context,
                )
                .await?,
        ),
        "submission" => maybe(storage.submission(arg(args, 0)?, &context).await?),
        "scanSubmissions" => encoded(
            storage
                .scan_submissions(
                    &arg(args, 0)?,
                    arg(args, 1)?,
                    optional::<Cursor>(args, 2)?.as_ref(),
                    &context,
                )
                .await?,
        ),
        "submissionByRequest" => maybe(
            storage
                .submission_by_request(arg(args, 0)?, &arg(args, 1)?, &context)
                .await?,
        ),
        "findDocument" => maybe(
            storage
                .find_document(&arg(args, 0)?, arg(args, 1)?, &context)
                .await?,
        ),
        "document" => maybe(
            storage
                .document(arg(args, 0)?, arg(args, 1)?, &context)
                .await?,
        ),
        "scanDocuments" => encoded(
            storage
                .scan_documents(
                    &arg(args, 0)?,
                    arg(args, 1)?,
                    optional::<Cursor>(args, 2)?.as_ref(),
                    &context,
                )
                .await?,
        ),
        "close" => {
            storage.close(&context).await?;
            Ok(None)
        }
        _ => Err(Error::request(format!("Unknown storage method {method}"))),
    }
}
pub async fn replay_case(storage: Arc<dyn Storage>, case: &JsonValue) -> Result<()> {
    for (index, request) in case["requests"]
        .as_array()
        .ok_or_else(|| Error::message("Missing requests"))?
        .iter()
        .enumerate()
    {
        let method = request["method"]
            .to_utf8()
            .ok_or_else(|| Error::message("Missing method"))?;
        let adopted = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let result = storage_request(
            storage.as_ref(),
            &method,
            request["args"]
                .as_array()
                .ok_or_else(|| Error::message("Missing args"))?,
            {
                let adopted = adopted.clone();
                Box::new(move |seq| adopted.lock().push(seq))
            },
        )
        .await;
        let expected_error = wire::get(request, "error");
        match (result, expected_error) {
            (Err(error), Some(expected)) => {
                let name = error.name();
                if !expected["name"].string_equals(name) {
                    return Err(Error::message(format!(
                        "Request {index} {method}: error kind {name}, expected {}",
                        expected["name"]
                    )));
                }
                if !expected["message"]
                    .as_string()
                    .is_some_and(|message| message.equals_str(&error.to_string()))
                {
                    return Err(Error::message(format!(
                        "Request {index} {method}: error {error}, expected {expected}"
                    )));
                }
            }
            (Ok(value), None) => {
                let expected = if request["missing"].as_bool() == Some(true) {
                    None
                } else {
                    wire::get(request, "value").cloned()
                };
                if value != expected {
                    return Err(Error::message(format!(
                        "Request {index} {method}: got {value:?}, expected {expected:?}"
                    )));
                }
            }
            (actual, expected) => {
                return Err(Error::message(format!(
                    "Request {index} {method}: result {actual:?}, expected error {expected:?}"
                )));
            }
        }
        if wire::encode(&*adopted.lock()) != request["adopted"] {
            return Err(Error::message(format!(
                "Request {index} {method}: adoption differs"
            )));
        }
    }
    Ok(())
}

pub struct StorageConformanceCase {
    pub name: JsonString,
    trace: JsonValue,
}
impl StorageConformanceCase {
    pub async fn run(&self, storage: Arc<dyn Storage>) -> Result<()> {
        replay_case(storage, &self.trace).await
    }
}
/// The pinned runner-independent storage contracts, executable with any native backend.
pub fn create_storage_conformance() -> Result<Vec<StorageConformanceCase>> {
    let fixture: JsonValue =
        serde_json::from_str(include_str!("../../tests/fixtures/storage.json"))?;
    fixture["cases"]
        .as_array()
        .ok_or_else(|| Error::message("Missing storage conformance cases"))?
        .iter()
        .map(|case| {
            Ok(StorageConformanceCase {
                name: wire::field(case, "name").map_err(Error::message)?,
                trace: case.clone(),
            })
        })
        .collect()
}
