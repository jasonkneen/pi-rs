use super::*;
use crate::harness::{
    hooks::HookKey,
    live::{self, CompactionReason, CompactionStatus, LIVE_DOC, RetryStatus},
    models::{RequestScope, provider_options, provider_token_limit},
    provider::ensure_provider_session_id,
    submissions::admit_submission,
    usage::record_usage,
};
use parking_lot::Mutex;
use std::sync::LazyLock;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CompactionInput {
    pub reason: CompactionReason,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instructions: Option<JsonString>,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CompactionResult {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entry_id: Option<EntryId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission_id: Option<SubmissionId>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SummaryRequest {
    pub attempt: u64,
    pub model: ModelRef,
    pub thinking_level: JsonString,
    pub stream_options: JsonObject,
    pub max_tokens: f64,
    pub tail: EntryId,
    pub first_kept: EntryId,
}
#[derive(Clone, Debug)]
pub enum CompactionCheckpoint {
    Select,
    Summarize(SummaryRequest),
    Retry { request: SummaryRequest, until: f64 },
}
impl CompactionCheckpoint {
    fn to_json(&self) -> JsonValue {
        let mut value = match self {
            Self::Select => wire::object(),
            Self::Summarize(request) | Self::Retry { request, .. } => wire::encode(request),
        };
        wire::put(
            &mut value,
            "phase",
            &match self {
                Self::Select => "select",
                Self::Summarize(_) => "summarize",
                Self::Retry { .. } => "retry",
            },
        );
        if let Self::Retry { until, .. } = self {
            wire::put(&mut value, "until", until);
        }
        value
    }
    fn from_json(value: &JsonValue) -> std::result::Result<Self, String> {
        Ok(match wire::tag(value, "phase")?.as_str() {
            "select" => Self::Select,
            "summarize" => Self::Summarize(wire::decode(value)?),
            "retry" => Self::Retry {
                request: wire::decode(value)?,
                until: wire::field(value, "until")?,
            },
            phase => return Err(format!("Unknown compaction phase {phase}")),
        })
    }
}
wire::raw_serde!(CompactionCheckpoint);
pub type CompactionRuntime =
    TaskRuntime<CompactionInput, CompactionCheckpoint, CompactionResult, CompactionHooks>;
pub type CompactionTask =
    Task<CompactionInput, CompactionCheckpoint, CompactionResult, CompactionHooks>;
type Next = NextTaskState<CompactionCheckpoint, CompactionResult>;
#[derive(Clone)]
pub struct CompactionSource {
    pub reason: CompactionReason,
    pub entries: Vec<Arc<EntryRecord>>,
    pub messages: Vec<Arc<JsonValue>>,
    pub first_kept: EntryId,
    pub instructions: Option<JsonString>,
}
pub enum CompactionDecision {
    Decline,
    Summary(JsonString),
}
pub type BeforeCompact = Arc<
    dyn Fn(
            Arc<CompactionSource>,
            CompactionRuntime,
            Context,
        ) -> crate::storage::StorageFuture<Option<CompactionDecision>>
        + Send
        + Sync,
>;
#[derive(Default)]
pub struct CompactionHooks {
    pub before_compact: Option<BeforeCompact>,
}
static BEFORE_COMPACT: LazyLock<HookKey<CompactionHooks, BeforeCompact>> = LazyLock::new(|| {
    HookKey::new("beforeCompact", |hooks: Arc<CompactionHooks>| {
        hooks.before_compact.clone()
    })
});
pub static COMPACTION_TASK: LazyLock<CompactionTask> = LazyLock::new(|| {
    define_task(TaskDefinition {
        name: "pi.compaction".into(),
        version: 1,
        initial: Arc::new(|_| Ok(CompactionCheckpoint::Select)),
        phases: indexmap::IndexMap::from_iter([
            (
                "select".into(),
                Arc::new(
                    |task, runtime, context| -> crate::storage::StorageFuture<()> {
                        Box::pin(select(task, runtime, context))
                    },
                ) as PhaseHandler<_, _, _, _>,
            ),
            (
                "summarize".into(),
                Arc::new(
                    |task, runtime, context| -> crate::storage::StorageFuture<()> {
                        Box::pin(summarize(task, runtime, context))
                    },
                ) as PhaseHandler<_, _, _, _>,
            ),
            (
                "retry".into(),
                Arc::new(
                    |task, runtime, context| -> crate::storage::StorageFuture<()> {
                        Box::pin(retry(task, runtime, context))
                    },
                ) as PhaseHandler<_, _, _, _>,
            ),
        ]),
        abort: Arc::new(|_, runtime, context| {
            Box::pin(async move {
                let id = runtime.task_id().untyped();
                let conversation = runtime.conversation_id();
                runtime
                    .commit(
                        move |tx, _| async move {
                            tx.doc(&LIVE_DOC, conversation).await?.update(|live| {
                                live::remove_compaction_status(live, id);
                                Ok(())
                            })?;
                            Ok(Some(Next::Terminal {
                                outcome: TaskOutcome::Aborted {
                                    reason: None,
                                    result: None,
                                },
                            }))
                        },
                        &context,
                    )
                    .await
            })
        }),
        migrate: None,
        hooks: None,
    })
});
pub fn compaction_task() -> CompactionTask {
    COMPACTION_TASK.clone()
}
pub async fn create_compaction(
    tx: &Transaction,
    conversation: ConversationId,
    input: CompactionInput,
    owner: Option<TaskId>,
) -> Result<TypedTaskId<CompactionResult>> {
    let mut options =
        TaskOptions::new(owner.map_or(TaskOwnership::Conversation, TaskOwnership::Task));
    options.conversation_id = Some(conversation);
    options.background = owner.is_none() && input.reason != CompactionReason::Manual;
    let reason = input.reason;
    let id = tx.create_task(&COMPACTION_TASK, input, options).await?;
    tx.doc(&LIVE_DOC, conversation).await?.update(|live| {
        live::add_compaction_status(
            live,
            CompactionStatus {
                task_id: id.untyped(),
                reason,
                blocking: owner.is_some(),
                attempt: 1,
                retry: None,
            },
        );
        Ok(())
    })?;
    Ok(id)
}
type Running = RunningTask<CompactionInput, CompactionCheckpoint, CompactionResult>;
fn resolve(runtime: &CompactionRuntime, model: &ModelRef) -> Result<Option<pi_ai::Model>> {
    let models = runtime.models()?;
    Ok(model
        .provider
        .to_string_checked()
        .ok()
        .zip(model.model_id.to_string_checked().ok())
        .and_then(|(provider, id)| models.get_model(&provider, &id)))
}
async fn select(task: Running, runtime: CompactionRuntime, context: Context) -> Result<()> {
    let agent = runtime.agent(&context).await?;
    let settings = runtime.settings()?;
    let model: Option<ModelRef> = agent
        .model()
        .map(wire::decode)
        .transpose()
        .map_err(Error::message)?;
    let Some((reference, model)) = model.as_ref().zip(match &model {
        Some(model) => resolve(&runtime, model)?,
        None => None,
    }) else {
        return fail_no_model(&runtime, model, &context).await;
    };
    let policy = compaction_policy(&settings, reference)?;
    let view = runtime
        .context(runtime.conversation_id(), None, &context)
        .await?;
    let Some(cut) = select_cut(&view, policy.keep_recent_tokens) else {
        return complete(&runtime, &context).await;
    };
    let first_kept = view.entries[cut].id;
    let source = Arc::new(CompactionSource {
        reason: task.record().info.input.reason,
        entries: view.entries[..cut].to_vec(),
        messages: summarized_messages(&view, cut),
        first_kept,
        instructions: task.record().info.input.instructions.clone(),
    });
    let decision = Arc::new(Mutex::new(None));
    let capture = decision.clone();
    let current = runtime.clone();
    let call_context = context.clone();
    runtime
        .hooks()
        .each(&BEFORE_COMPACT, move |hook| {
            let source = source.clone();
            let current = current.clone();
            let context = call_context.clone();
            let capture = capture.clone();
            async move {
                if capture.lock().is_none() {
                    let value = hook(source, current, context).await?;
                    *capture.lock() = value;
                }
                Ok(())
            }
        })
        .await?;
    let decision = decision.lock().take();
    match decision {
        Some(CompactionDecision::Decline) => return complete(&runtime, &context).await,
        Some(CompactionDecision::Summary(summary)) => {
            return place(&runtime, first_kept, summary, &context).await;
        }
        None => {}
    }
    let request = SummaryRequest {
        attempt: 1,
        model: reference.clone(),
        thinking_level: agent.thinking_level().clone(),
        stream_options: settings.stream,
        max_tokens: (0.8 * policy.reserve_tokens)
            .floor()
            .min(if model.max_tokens > 0 {
                model.max_tokens as f64
            } else {
                f64::INFINITY
            }),
        tail: view
            .entries
            .iter()
            .map(|entry| entry.id)
            .max()
            .unwrap_or(first_kept),
        first_kept,
    };
    runtime
        .commit(
            move |_, _| async move {
                Ok(Some(Next::Running {
                    checkpoint: CompactionCheckpoint::Summarize(request),
                }))
            },
            &context,
        )
        .await
}
async fn summarize(task: Running, runtime: CompactionRuntime, context: Context) -> Result<()> {
    let CompactionCheckpoint::Summarize(request) = task.checkpoint().clone() else {
        return Err(Error::message("Expected summarize checkpoint"));
    };
    let Some(model) = resolve(&runtime, &request.model)? else {
        return fail_no_model(&runtime, Some(request.model), &context).await;
    };
    let conversation = runtime.conversation_id();
    let task_id = runtime.task_id().untyped();
    let view = runtime
        .context(conversation, Some(request.tail), &context)
        .await?;
    // JS slice(0, -1) is retained if a malformed old checkpoint names no entry.
    let cut = view
        .entries
        .iter()
        .position(|entry| entry.id == request.first_kept)
        .unwrap_or_else(|| view.entries.len().saturating_sub(1));
    let now = runtime.now()?;
    let mut prompt = joined(
        [
            "<conversation>\n".into(),
            serialize_conversation(&summarized_messages(&view, cut)),
            "\n</conversation>\n\n".into(),
            SUMMARIZATION_PROMPT.into(),
        ],
        "",
    );
    if let Some(instructions) = &task.record().info.input.instructions {
        prompt.append(&"\n\nAdditional focus: ".into());
        prompt.append(instructions);
    }
    let messages = pi_ai::Context {
        messages: vec![
            provider_message("system", SUMMARIZATION_SYSTEM_PROMPT.into(), now)?,
            provider_message("user", prompt, now)?,
        ],
        ..Default::default()
    };
    let mut forwarded = request.stream_options.clone();
    forwarded.shift_remove(&JsonString::from("deferred"));
    let mut options = provider_options(&forwarded)?;
    options.cache_retention = Some(pi_ai::CacheRetention::None);
    options.max_tokens = Some(provider_token_limit(request.max_tokens)?);
    options.session_id = Some(
        ensure_provider_session_id(&runtime, &context)
            .await?
            .to_string_checked()
            .map_err(|error| Error::message(error.to_string()))?,
    );
    if !request.thinking_level.equals_str("off") {
        options.reasoning =
            Some(wire::decode(&wire::encode(&request.thinking_level)).map_err(Error::message)?);
    }
    let scope = RequestScope::new(&mut options, runtime.signal(), &context);
    let message = runtime
        .models()?
        .complete_simple(&model, &messages, options)
        .await;
    drop(scope);
    runtime.signal().check()?;
    let summary = summary_text(&message);
    let policy = runtime.settings()?.retry;
    let attempt = request.attempt;
    let retry = message.stop_reason == pi_ai::StopReason::Error
        && pi_ai::is_retryable_assistant_error(&message)
        && policy.value()["enabled"].as_bool() == Some(true)
        && attempt as f64 <= policy.value()["maxRetries"].as_f64().unwrap_or(0.0);
    let until = if retry {
        runtime.now()? + retry_delay(&policy, attempt)
    } else {
        0.0
    };
    let current = runtime.clone();
    runtime
        .commit(
            move |tx, running| async move {
                record_usage(
                    &tx,
                    conversation,
                    crate::harness::usage::UsageBucket::Models,
                    format!("{}/{}", message.provider, message.model),
                    &JsonObject::try_from_value(wire::encode(&message.usage))
                        .map_err(Error::message)?,
                )
                .await?;
                let live = tx.doc(&LIVE_DOC, conversation).await?;
                if let Some(summary) = summary {
                    return Ok(Some(
                        place_summary(&tx, &current, &running, &live, request.first_kept, summary)
                            .await?,
                    ));
                }
                if retry {
                    live.update(|live| {
                        if let Some(status) = live::compaction_status(live, task_id) {
                            status.retry = Some(RetryStatus {
                                at: until,
                                error: message.error_message.clone().unwrap_or_default().into(),
                            });
                        }
                        Ok(())
                    })?;
                    return Ok(Some(Next::Sleeping {
                        checkpoint: CompactionCheckpoint::Retry { request, until },
                        until,
                    }));
                }
                let text = summary_failure(&message);
                live.update(|live| {
                    live::fail_compaction_status(live, task_id, text.clone());
                    Ok(())
                })?;
                Ok(Some(failed(text, "model_error")))
            },
            &context,
        )
        .await
}
async fn retry(task: Running, runtime: CompactionRuntime, context: Context) -> Result<()> {
    let CompactionCheckpoint::Retry { mut request, until } = task.checkpoint().clone() else {
        return Err(Error::message("Expected retry checkpoint"));
    };
    if until > runtime.now()? {
        return runtime
            .sleep_until(until, task.checkpoint().clone(), &context)
            .await;
    }
    request.attempt += 1;
    let conversation = runtime.conversation_id();
    let task_id = runtime.task_id().untyped();
    runtime
        .commit(
            move |tx, _| async move {
                tx.doc(&LIVE_DOC, conversation).await?.update(|live| {
                    if let Some(status) = live::compaction_status(live, task_id) {
                        status.attempt = request.attempt;
                        status.retry = None;
                    }
                    Ok(())
                })?;
                Ok(Some(Next::Running {
                    checkpoint: CompactionCheckpoint::Summarize(request),
                }))
            },
            &context,
        )
        .await
}
fn provider_message(role: &str, text: JsonString, timestamp: f64) -> Result<pi_ai::Message> {
    let mut message = wire::object();
    wire::put(&mut message, "role", &role);
    if role == "user" {
        let mut block = wire::object();
        wire::put(&mut block, "type", &"text");
        wire::put(&mut block, "text", &text);
        wire::put(&mut message, "content", &vec![block]);
    } else {
        wire::put(&mut message, "content", &text);
    }
    wire::put(&mut message, "timestamp", &timestamp);
    wire::decode(&message).map_err(Error::message)
}
fn summary_text(message: &pi_ai::AssistantMessage) -> Option<JsonString> {
    if message.stop_reason != pi_ai::StopReason::Stop
        || message
            .content
            .iter()
            .any(|content| matches!(content, pi_ai::AssistantContent::ToolCall(_)))
    {
        return None;
    }
    let text = message
        .content
        .iter()
        .filter_map(|content| {
            if let pi_ai::AssistantContent::Text(text) = content {
                Some(text.text.as_str())
            } else {
                None
            }
        })
        .collect::<Vec<_>>()
        .join("\n");
    let text = text.trim_matches(|character: char| matches!(character, '\u{0009}'..='\u{000d}' | '\u{0020}' | '\u{00a0}' | '\u{1680}' | '\u{2000}'..='\u{200a}' | '\u{2028}' | '\u{2029}' | '\u{202f}' | '\u{205f}' | '\u{3000}' | '\u{feff}'));
    (!text.is_empty()).then(|| text.into())
}
fn summary_failure(message: &pi_ai::AssistantMessage) -> JsonString {
    if matches!(
        message.stop_reason,
        pi_ai::StopReason::Error | pi_ai::StopReason::Aborted
    ) {
        return format!(
            "Summarization failed: {}",
            message
                .error_message
                .as_deref()
                .unwrap_or(message.stop_reason.as_str())
        )
        .into();
    }
    if message.stop_reason == pi_ai::StopReason::Length {
        return "Summarization hit the token limit; the summary is incomplete".into();
    }
    if message
        .content
        .iter()
        .any(|content| matches!(content, pi_ai::AssistantContent::ToolCall(_)))
    {
        return "Summarization attempted to call a tool".into();
    }
    "Summarization produced no text".into()
}
fn retry_delay(policy: &JsonObject, attempt: u64) -> f64 {
    let delay = policy.value()["baseDelayMs"].as_f64().unwrap_or(0.0)
        * 2.0_f64.powf(attempt.saturating_sub(1) as f64);
    let safe =
        if delay.is_finite() && delay.fract() == 0.0 && delay.abs() <= 9_007_199_254_740_991.0 {
            delay
        } else {
            9_007_199_254_740_991.0
        };
    safe.min(
        policy.value()["maxAgentDelayMs"]
            .as_f64()
            .unwrap_or(60_000.0),
    )
}
fn failed(message: JsonString, reason: &str) -> Next {
    let mut detail = wire::object();
    wire::put(&mut detail, "reason", &reason);
    Next::Terminal {
        outcome: TaskOutcome::Failed {
            error: TaskOutcomeError {
                message,
                detail: Some(detail),
            },
            result: None,
        },
    }
}
async fn fail_no_model(
    runtime: &CompactionRuntime,
    reference: Option<ModelRef>,
    context: &Context,
) -> Result<()> {
    let text = reference.map_or_else(
        || "No model is configured".into(),
        |reference| {
            joined(
                [
                    "Model ".into(),
                    reference.provider,
                    "/".into(),
                    reference.model_id,
                    " is not available".into(),
                ],
                "",
            )
        },
    );
    let conversation = runtime.conversation_id();
    let id = runtime.task_id().untyped();
    runtime
        .commit(
            move |tx, _| async move {
                tx.doc(&LIVE_DOC, conversation).await?.update(|live| {
                    live::fail_compaction_status(live, id, text.clone());
                    Ok(())
                })?;
                Ok(Some(failed(text, "no_model")))
            },
            context,
        )
        .await
}
async fn complete(runtime: &CompactionRuntime, context: &Context) -> Result<()> {
    let conversation = runtime.conversation_id();
    let id = runtime.task_id().untyped();
    runtime
        .commit(
            move |tx, _| async move {
                tx.doc(&LIVE_DOC, conversation).await?.update(|live| {
                    live::remove_compaction_status(live, id);
                    Ok(())
                })?;
                Ok(Some(Next::Terminal {
                    outcome: TaskOutcome::Completed {
                        result: CompactionResult::default(),
                    },
                }))
            },
            context,
        )
        .await
}
async fn place(
    runtime: &CompactionRuntime,
    first_kept: EntryId,
    summary: JsonString,
    context: &Context,
) -> Result<()> {
    let current = runtime.clone();
    let conversation = runtime.conversation_id();
    runtime
        .commit(
            move |tx, task| async move {
                let live = tx.doc(&LIVE_DOC, conversation).await?;
                Ok(Some(
                    place_summary(&tx, &current, &task, &live, first_kept, summary).await?,
                ))
            },
            context,
        )
        .await
}
async fn place_summary(
    tx: &Transaction,
    runtime: &CompactionRuntime,
    current: &Running,
    live: &DocDraft<live::LiveState>,
    first_kept: EntryId,
    summary: JsonString,
) -> Result<Next> {
    live.update(|live| {
        live::remove_compaction_status(live, runtime.task_id().untyped());
        live.compaction_failure = None;
        Ok(())
    })?;
    let text = joined(["The conversation history before this point was compacted into the following summary:\n\n<summary>\n".into(), summary, "\n</summary>".into()], "");
    let mut message = wire::object();
    wire::put(&mut message, "role", &"user");
    wire::put(
        &mut message,
        "content",
        &vec![{
            let mut text_block = wire::object();
            wire::put(&mut text_block, "type", &"text");
            wire::put(&mut text_block, "text", &text);
            text_block
        }],
    );
    wire::put(&mut message, "timestamp", &runtime.now()?);
    let mut data = wire::object();
    wire::put(&mut data, "reason", &current.record().info.input.reason);
    let mut entry = EntryDraft::new("pi.compaction");
    entry.head = Some(EntryHead::Entry(first_kept));
    entry.model = Some(vec![message]);
    entry.data = Some(data);
    let result = if current.record().info.owner.is_none() {
        let admission = admit_submission(
            tx,
            runtime.conversation_id(),
            &SubmissionDraft::Write {
                request_id: Some(format!("compaction:{}", runtime.task_id().untyped()).into()),
                entry,
            },
            runtime.now()?,
            (&runtime.settings()?).into(),
            None,
        )
        .await?;
        CompactionResult {
            submission_id: Some(admission.id),
            ..Default::default()
        }
    } else {
        CompactionResult {
            entry_id: Some(tx.append_entry(runtime.conversation_id(), entry).await?.id),
            ..Default::default()
        }
    };
    Ok(Next::Terminal {
        outcome: TaskOutcome::Completed { result },
    })
}
const SUMMARIZATION_SYSTEM_PROMPT: &str = "You are a context summarization assistant. Your task is to read a conversation between a user and an AI assistant, then produce a structured summary following the exact format specified.\n\nDo NOT continue the conversation. Do NOT respond to any questions in the conversation. ONLY output the structured summary.";
const SUMMARIZATION_PROMPT: &str = "The messages above are a conversation to summarize. Create a structured context checkpoint summary that another LLM will use to continue the work. If the conversation starts with an earlier summary, preserve its information and fold the newer messages into it.\n\nUse this EXACT format:\n\n## Goal\n[What is the user trying to accomplish? Can be multiple items if the session covers different tasks.]\n\n## Constraints & Preferences\n- [Any constraints, preferences, or requirements mentioned by user]\n- [Or \"(none)\" if none were mentioned]\n\n## Progress\n### Done\n- [x] [Completed tasks/changes]\n\n### In Progress\n- [ ] [Current work]\n\n### Blocked\n- [Issues preventing progress, if any]\n\n## Key Decisions\n- **[Decision]**: [Brief rationale]\n\n## Next Steps\n1. [Ordered list of what should happen next]\n\n## Critical Context\n- [Any data, examples, or references needed to continue]\n- [Or \"(none)\" if not applicable]\n\nKeep each section concise. Preserve exact file paths, function names, and error messages.";
