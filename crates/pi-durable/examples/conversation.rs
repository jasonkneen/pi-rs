//! A complete local conversation through the native generation task and model SDK.
use pi_ai::{models::Models, providers::faux::*};
use pi_durable::{
    harness::{
        agent::AGENT_DOC,
        compaction::compaction_task,
        generation::*,
        live,
        prompt::PromptSection,
        registry::*,
        scheduler::*,
        submissions::*,
        tool::*,
        types::{QueueModes, SubmissionDraft},
    },
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
fn now() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as f64
}
#[tokio::main]
async fn main() -> Result<()> {
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
    let registry = Registry::<NativeTool, Section>::new([
        generation_task::<NativeTool, Section>().erased(),
        tool_task().erased(),
        compaction_task().erased(),
    ])?;
    let storage = Arc::new(MemoryStorage::new());
    let scheduler = TaskScheduler::open(
        storage.clone(),
        registry,
        SchedulerOptions {
            models: Arc::new(move || Ok(models.clone())),
            settle_outcome: Arc::new(|tx, record, outcome| {
                Box::pin(live::settle_scheduler_outcome(tx, record, outcome))
            }),
            ..Default::default()
        },
        SessionOptions {
            conversation_created: Some(Arc::new(|tx, record| {
                Box::pin(async move { harness::create_conversation_documents(&tx, record).await })
            })),
            ..Default::default()
        },
        &Context::default(),
    )
    .await?;
    let root = scheduler
        .session()
        .commit(
            |tx| async move {
                let root = tx.create_root_conversation().await?.id;
                tx.doc(&AGENT_DOC, root).await?.update(|agent| {
                    agent.0.insert(
                        "model".into(),
                        pi_chord::json::parse(r#"{"provider":"faux","modelId":"faux-1"}"#)
                            .expect("valid model reference"),
                    );
                    Ok(())
                })?;
                Ok(root)
            },
            &Context::default(),
        )
        .await?;
    let resume = scheduler.clone();
    let submissions = Submissions::new(
        scheduler.session(),
        SubmissionsOptions {
            now: Arc::new(now),
            queue_modes: Arc::new(|| Ok(QueueModes::default())),
            resume: Arc::new(move || resume.resume()),
            telemetry: pi_telemetry::noop_telemetry_context(),
        },
    )?;
    let settled = submissions
        .submit(
            root,
            SubmissionDraft::input(input.as_str()),
            &Context::default(),
        )
        .await?
        .wait(&Context::default())
        .await?;
    println!(
        "{}",
        pi_chord::json::to_value(&settled).map_err(Error::message)?
    );
    let view = harness::context::read_context(
        &scheduler.session(),
        storage,
        root,
        &Context::default(),
        None,
    )
    .await?;
    for message in view.messages {
        println!("{message}");
    }
    scheduler.session().close(&Context::default()).await
}
