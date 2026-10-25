use futures::future::BoxFuture;
use pi_codemode::*;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

struct LegacyVm;
struct Channel;
impl VmChannel for Channel {
    fn post(&self, _: HostToWorkerMessage) {}
    fn stop(&self) -> BoxFuture<'static, ()> {
        Box::pin(async {})
    }
}
impl VmLauncher for LegacyVm {
    fn launch(
        &self,
        data: VmStartData,
        events: VmEvents,
    ) -> BoxFuture<'static, Result<Box<dyn VmChannel>, String>> {
        assert!(data.lossless_json);
        events.message(WorkerToHostMessage::Started {
            fingerprint: "legacy sandbox".into(),
        });
        events.message(WorkerToHostMessage::Call {
            id: 1,
            target: CodemodeCallTarget::Tool,
            name: "effect".into(),
            args: Some("{}".into()),
        });
        Box::pin(async { Ok(Box::new(Channel) as Box<dyn VmChannel>) })
    }
}
#[tokio::test]
async fn rejects_an_incompatible_vm_before_host_effects() {
    let sandbox = CodemodeSandbox::with_launcher(Default::default(), Arc::new(LegacyVm)).unwrap();
    let effects = Arc::new(AtomicUsize::new(0));
    let seen = effects.clone();
    let mut options = CodemodeJsonRunOptions::new(123.0, 1, move |_, _| {
        seen.fetch_add(1, Ordering::Relaxed);
    });
    options.tools.push(CodemodeDeclaredTool::new("effect"));
    let result = sandbox
        .start_json("await tools.effect({});", options)
        .unwrap()
        .result()
        .await;
    assert!(result["error"]["kind"].string_equals("sandbox"), "{result}");
    assert!(
        result["error"]["message"]
            .to_utf8()
            .unwrap()
            .contains("does not support lossless JSON")
    );
    assert_eq!(effects.load(Ordering::Relaxed), 0);
    sandbox.close().await;
}
