use pi_durable::harness::telemetry::DURABLE_TELEMETRY_SCHEMA;
use pi_telemetry::{InMemoryTelemetryContext, create_typed_metrics};
#[test]
fn source_vocabulary_round_trips_and_metric_factories_use_its_metadata() {
    let source: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/telemetry-schema-reference.json")).unwrap();
    assert_eq!(
        serde_json::to_value(&*DURABLE_TELEMETRY_SCHEMA).unwrap(),
        source["schema"]
    );
    let recorder = InMemoryTelemetryContext::new();
    let metrics = create_typed_metrics(recorder.clone(), &[&DURABLE_TELEMETRY_SCHEMA]);
    metrics
        .counter("pi.durable.reports")
        .add(1.0, pi_telemetry::attributes! {"type"=>"host_error"});
    metrics
        .up_down_counter("pi.durable.tasks.active")
        .add(1.0, pi_telemetry::attributes! {"task"=>"test.work"});
    metrics.histogram("pi.durable.phase.duration").record(
        4.0,
        pi_telemetry::attributes! {"task"=>"test.work","phase"=>"prepare","outcome"=>"done"},
        None,
    );
    let recorded = recorder.get_metrics();
    assert_eq!(recorded.len(), 3);
    assert_eq!(
        recorded[0].description.as_deref(),
        source["schema"]["metrics"]["pi.durable.reports"]["description"].as_str()
    );
    assert_eq!(recorded[2].unit.as_deref(), Some("ms"));
    assert!(
        recorded
            .iter()
            .all(|metric| !metric.attributes.keys().any(|key| key.contains("id")))
    );
}

#[tokio::test]
async fn committed_entry_kind_keeps_raw_utf16_in_telemetry_events() {
    use pi_durable::*;
    use pi_telemetry::{SpanOptions, SpanOutcome, TelemetryContext};
    use std::sync::Arc;
    let telemetry = InMemoryTelemetryContext::new();
    let span = telemetry.begin_span(SpanOptions::new("caller"));
    let context = pi_chord::context::with_telemetry(span.clone(), &Context::default());
    let storage = Arc::new(MemoryStorage::new());
    let session = Session::new(storage, SessionOptions::default());
    session
        .commit(
            move |tx| async move {
                let conversation = tx
                    .create_conversation(ConversationOwnership::Ownerless)
                    .await?;
                tx.append_entry(
                    conversation.id,
                    EntryDraft::new(JsonString::from_utf16(vec![0xd800])),
                )
                .await?;
                Ok(())
            },
            &context,
        )
        .await
        .unwrap();
    span.end(SpanOutcome::Ok);
    let spans = telemetry.get_spans();
    assert_eq!(spans[0].events.len(), 1);
    assert_eq!(spans[0].events[0].name, "pi.durable.entry");
    assert_eq!(
        spans[0].events[0].attributes["entry.kind"].to_json_string(),
        "\"\\ud800\""
    );
    session.close(&context).await.unwrap();
}
