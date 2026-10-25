//! Native generation binds original section renderers through the task read API.
use super::*;
use pi_durable::harness::{
    declaration::{DeclaredTool, ToolDeclaration},
    generation::*,
    prompt::PromptSection,
};
use registry_bridge::{SectionHandle, ToolHandle};

impl DeclaredTool for ToolHandle {
    fn declaration(&self) -> ToolDeclaration {
        self.executable
            .as_ref()
            .expect("native generation requires an executable tool registration")
            .declaration()
    }
}
impl Hub {
    pub(super) fn init_conversation(
        self: &Arc<Self>,
        callback: Option<u64>,
        tx: Transaction,
        conversation: ConversationId,
    ) -> StorageFuture<()> {
        let Some(callback) = callback else {
            return Box::pin(async { Ok(()) });
        };
        let id = self.next.fetch_add(1, Ordering::Relaxed);
        self.transactions.lock().insert(id, tx);
        let operation = self.call(
            "conversation.init",
            object([
                ("callback", encode(&callback)),
                ("tx", encode(&id)),
                ("conversation", encode(&conversation)),
            ]),
        );
        let hub = self.clone();
        Box::pin(async move {
            let result = operation.await;
            hub.transactions.lock().remove(&id);
            result.map(|_| ())
        })
    }
}
impl PromptSection<GenerationPromptInput<ToolHandle, SectionHandle>> for SectionHandle {
    fn tagged(&self) -> bool {
        !self.untagged
    }
    fn render(
        &self,
        input: Arc<GenerationPromptInput<ToolHandle, SectionHandle>>,
        context: &Context,
    ) -> StorageFuture<Option<JsonString>> {
        let Some(hub) = self.hub.upgrade() else {
            return Box::pin(async { Err(Error::message("Section host closed")) });
        };
        let runtime = input.read.erased();
        let id = hub.bind_tool_runtime(&runtime, context);
        let operation = hub.call_context(
            "generation.render",
            object([
                ("section", encode(&self.id)),
                ("runtime", encode(&id)),
                ("taskId", encode(&runtime.task_id())),
                ("conversationId", encode(&input.conversation_id)),
                (
                    "harness",
                    encode(&context.value(&HARNESS_CONTEXT).map(|id| *id)),
                ),
                ("agent", hub.encode_agent(input.agent.clone())),
                ("shown", encode(&input.shown.iter().collect::<Vec<_>>())),
                (
                    "environment",
                    encode(
                        &input
                            .env
                            .as_ref()
                            .and_then(|env| hub.environment.handle(env)),
                    ),
                ),
            ]),
            context,
        );
        Box::pin(async move { operation.await?.as_ref().map(decode).transpose() })
    }
}
