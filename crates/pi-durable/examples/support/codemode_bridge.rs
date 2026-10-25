//! Test transport for native code-mode components and original host capabilities.
use super::*;
use pi_durable::codemode::output::*;
impl Hub {
    pub(super) fn format_codemode(
        self: &Arc<Self>,
        request: JsonValue,
    ) -> Result<StorageFuture<Option<JsonValue>>> {
        self.tool.codemode_formats.fetch_add(1, Ordering::Relaxed);
        let options = &request["options"];
        let writer: u64 = field(&request, "writer")?;
        let hub = self.clone();
        let write: OutputFile = Arc::new(move |name, data| {
            let data = match data {
                OutputData::Text(text) => object([("text", encode(&text))]),
                OutputData::Binary(bytes) => encode(&bytes),
            };
            let pending = hub.call(
                "codemode.write",
                object([
                    ("writer", encode(&writer)),
                    ("name", encode(&name)),
                    ("data", data),
                ]),
            );
            Box::pin(async move {
                let reply = pending
                    .await?
                    .ok_or_else(|| Error::message("Missing output-file result"))?;
                Ok(if reply["path"].is_string() {
                    OutputFileResult::Saved(field(&reply, "path")?)
                } else {
                    OutputFileResult::Failed(field(&reply, "error")?)
                })
            })
        });
        let options = FormatResultOptions {
            generated_images: field(options, "generatedImages")?,
            wall_ms: field(options, "wallMs")?,
            max_output_tokens: field(options, "maxOutputTokens")?,
            calls: field(options, "calls")?,
            max_bytes: field(&options["limits"], "maxBytes")?,
            max_lines: field(&options["limits"], "maxLines")?,
            write,
        };
        let pending = format_result(request["result"].clone(), options);
        Ok(Box::pin(async move {
            let result = pending.await?;
            let mut reply = object([("content", encode(&result.content))]);
            if let Some(path) = result.full_output_path {
                reply
                    .as_object_mut()
                    .unwrap()
                    .insert("fullOutputPath".into(), encode(&path));
            }
            Ok(Some(reply))
        }))
    }
}
