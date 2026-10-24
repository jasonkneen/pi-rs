//! JSON-line bridge for the unmodified upstream Storage conformance suite.
use pi_durable::{storage::Storage, testing::storage_request, *};
use std::{
    io::{BufRead, Write},
    sync::{Arc, Mutex},
};

fn write(output: &Arc<Mutex<std::io::Stdout>>, value: serde_json::Value) {
    let mut output = output.lock().unwrap();
    writeln!(output, "{value}").unwrap();
    output.flush().unwrap();
}
#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let output = Arc::new(Mutex::new(std::io::stdout()));
    let mut storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new());
    for line in std::io::stdin().lock().lines() {
        let request: JsonValue = serde_json::from_str(&line?)?;
        let id = request["id"].as_u64().unwrap();
        let method = request["method"].to_utf8().unwrap();
        let result = if method == "reset" {
            storage
                .close(&pi_chord::context::BACKGROUND_CONTEXT)
                .await?;
            storage = Arc::new(MemoryStorage::new());
            Ok(None)
        } else {
            storage_request(
                storage.as_ref(),
                &method,
                request["args"].as_array().unwrap(),
                {
                    let output = output.clone();
                    Box::new(move |seq| {
                        write(
                            &output,
                            serde_json::json!({"id":id,"type":"adopt","seq":seq}),
                        )
                    })
                },
            )
            .await
        };
        let mut response = serde_json::Map::new();
        response.insert("id".into(), id.into());
        match result {
            Ok(None) => {
                response.insert("missing".into(), true.into());
            }
            Ok(Some(value)) => {
                // Compose the envelope as raw JSON to preserve lone UTF-16 units.
                let mut output = output.lock().unwrap();
                writeln!(output, "{{\"id\":{id},\"value\":{value}}}")?;
                output.flush()?;
                continue;
            }
            Err(error) => {
                response.insert(
                    "error".into(),
                    serde_json::json!({"name":error.name(),"message":error.to_string()}),
                );
            }
        }
        write(&output, serde_json::Value::Object(response));
    }
    storage
        .close(&pi_chord::context::BACKGROUND_CONTEXT)
        .await?;
    Ok(())
}
