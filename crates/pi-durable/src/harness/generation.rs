//! Native generation record creation and run handover.
//! Executable provider/tool phases remain tracked separately in the parity map.
use super::live::{LiveRun, LiveState};
use crate::{wire, *};
use pi_chord::delta::Op;

pub const GENERATION_KIND: &str = "pi.generation";
pub const GENERATION_VERSION: u64 = 1;
pub async fn create_generation(
    tx: &Transaction,
    conversation: ConversationId,
    submission_ids: Option<Vec<SubmissionId>>,
) -> Result<TaskId> {
    let mut options = TaskOptions::new(TaskOwnership::Conversation);
    options.conversation_id = Some(conversation);
    options.submission_ids = submission_ids;
    tx.create_task_record(
        GENERATION_KIND.into(),
        GENERATION_VERSION,
        JsonValue::Object(Default::default()),
        |_| {
            Ok(pi_chord::json::parse(r#"{"phase":"prepare","attempt":1}"#)
                .expect("valid built-in checkpoint"))
        },
        options,
    )
    .await
}
pub async fn start_run(
    tx: &Transaction,
    conversation: ConversationId,
    live: &DocDraft<LiveState>,
    inputs: Vec<SubmissionId>,
) -> Result<()> {
    let task_id = create_generation(tx, conversation, Some(inputs.clone())).await?;
    live.apply_ops(&[Op::Set(
        vec!["run".into()],
        wire::encode(&LiveRun { task_id, inputs }),
    )])
}
pub fn hand_over(live: &DocDraft<LiveState>, from: TaskId, to: TaskId) -> Result<()> {
    if live.read()?.run.is_some_and(|run| run.task_id == from) {
        live.apply_ops(&[Op::Set(
            vec!["run".into(), "taskId".into()],
            wire::encode(&to),
        )])?;
    }
    Ok(())
}
