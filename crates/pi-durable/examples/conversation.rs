//! A complete local conversation through the native generation task and model SDK.
use pi_ai::{models::Models, providers::faux::*};
use pi_durable::{
    harness::{generation::GenerationPromptInput, registry::RegisteredSection, tool::NativeTool},
    *,
};
use std::sync::Arc;

struct Section(JsonString);
impl RegisteredSection for Section {
    fn key(&self) -> &JsonString {
        &self.0
    }
}
impl PromptSection<GenerationPromptInput<NativeTool, Section>> for Section {
    fn render(
        &self,
        _: Arc<GenerationPromptInput<NativeTool, Section>>,
        _: &Context,
    ) -> storage::StorageFuture<Option<JsonString>> {
        Box::pin(async { Ok(None) })
    }
}
#[tokio::main]
async fn main() -> Result<()> {
    let context = Context::default();
    let input = std::env::args().nth(1).unwrap_or_else(|| "Hello".into());
    let models = Models::default();
    let faux = faux_provider(RegisterFauxProviderOptions {
        tokens_per_second: Some(f64::INFINITY),
        ..Default::default()
    });
    faux.set_responses(vec![
        faux_text_message("The native durable conversation completed.").into(),
    ]);
    models.set_provider(faux.provider);
    let registry = create_registry::<NativeTool, Section>()?;
    let harness = Harness::open(
        Arc::new(MemoryStorage::new()),
        HarnessOptions::new(registry, models),
        &context,
    )
    .await?;
    let root = harness
        .root(
            ConversationOptions {
                agent: Some(
                    JsonObject::try_from_value(
                        pi_chord::json::parse(
                            r#"{"model":{"provider":"faux","modelId":"faux-1"}}"#,
                        )
                        .expect("valid agent configuration"),
                    )
                    .map_err(Error::message)?,
                ),
                ..Default::default()
            },
            &context,
        )
        .await?;
    let settled = root
        .submit(SubmissionDraft::input(input.as_str()), &context)
        .await?
        .wait(&context)
        .await?;
    println!(
        "{}",
        pi_chord::json::to_value(&settled).map_err(Error::message)?
    );
    for message in root.context(None, &context).await?.messages {
        println!("{message}");
    }
    harness.close(&context).await
}
