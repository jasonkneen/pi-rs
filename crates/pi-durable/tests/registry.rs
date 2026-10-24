use indexmap::IndexMap;
use parking_lot::Mutex;
use pi_durable::{
    harness::{agent::*, registry::*, settings::*},
    *,
};
use std::sync::Arc;

#[derive(Default)]
struct Tool {
    name: JsonString,
    callers: Option<Vec<ToolCaller>>,
    deferred: bool,
}
impl RegisteredTool for Tool {
    fn name(&self) -> &JsonString {
        &self.name
    }
    fn callers(&self) -> Option<&[ToolCaller]> {
        self.callers.as_deref()
    }
    fn deferred(&self) -> bool {
        self.deferred
    }
}
struct Section(JsonString);
impl RegisteredSection for Section {
    fn key(&self) -> &JsonString {
        &self.0
    }
}
type Ext = Extension<Tool, Section>;
fn tool(name: &str) -> Arc<Tool> {
    Arc::new(Tool {
        name: name.into(),
        ..Default::default()
    })
}
fn json(value: &str) -> JsonValue {
    pi_chord::json::parse(value).unwrap()
}
fn state(value: &str) -> AgentState {
    serde_json::from_str(value).unwrap()
}
fn names(tools: &[Arc<Tool>]) -> Vec<String> {
    tools.iter().map(|tool| tool.name.to_string()).collect()
}
fn task(name: &str, version: u64) -> AnyTask {
    let run: PhaseHandler<JsonValue, JsonValue, JsonValue> = Arc::new(|_, runtime, context| {
        Box::pin(async move {
            runtime
                .commit(
                    |_, _| async {
                        Ok(Some(NextTaskState::Terminal {
                            outcome: TaskOutcome::Completed {
                                result: JsonValue::Null,
                            },
                        }))
                    },
                    &context,
                )
                .await
        })
    });
    define_task(TaskDefinition {
        name: name.into(),
        version,
        initial: Arc::new(|_| Ok(json(r#"{"phase":"run"}"#))),
        phases: IndexMap::from([("run".into(), run.clone())]),
        abort: run,
        migrate: None,
        hooks: None,
    })
    .erased()
}

#[test]
fn publication_is_atomic_preserves_old_snapshots_and_validates_the_replacement() {
    let builtin = task("pi.generation", 1);
    let registry = Registry::<Tool, Section>::new([builtin.clone()]).unwrap();
    let events = Arc::new(Mutex::new(Vec::new()));
    let out = events.clone();
    let read = registry.clone();
    let subscription = registry.subscribe(move || {
        out.lock().push(
            read.snapshot()
                .installed()
                .iter()
                .map(|e| e.name.clone())
                .collect::<Vec<_>>(),
        )
    });
    let mut a = Ext::new("a");
    a.tools.push(tool("read"));
    a.tasks.push(task("app.index", 1));
    let a = Arc::new(a);
    registry.install(a.clone()).unwrap();
    registry.install(Arc::new(Ext::new("b"))).unwrap();
    let before = registry.snapshot();
    let mut invalid = Ext::new("x");
    invalid.tasks.push(task("app.index", 2));
    assert!(
        registry
            .install(Arc::new(invalid))
            .unwrap_err()
            .to_string()
            .contains("already installed")
    );
    assert!(registry.snapshot().same_snapshot(&before));
    assert_eq!(events.lock().len(), 2);
    let mut next = Ext::new("a");
    next.tasks.push(task("app.index", 2));
    registry.install(Arc::new(next)).unwrap();
    assert!(Arc::ptr_eq(&before.extension(&"a".into()).unwrap(), &a));
    assert_eq!(before.task(&"app.index".into()).unwrap().version(), 1);
    assert_eq!(
        registry
            .snapshot()
            .task(&"app.index".into())
            .unwrap()
            .version(),
        2
    );
    assert!(registry.snapshot().tasks()[0].same_definition(&builtin));
    registry.uninstall(&"a".into()).unwrap();
    registry.uninstall(&"a".into()).unwrap();
    registry.install(a).unwrap();
    assert_eq!(
        events.lock().last().unwrap(),
        &[JsonString::from("b"), "a".into()]
    );
    drop(subscription);
    registry.uninstall(&"a".into()).unwrap();
    assert_eq!(events.lock().len(), 5);
}

#[test]
fn invalid_names_and_duplicate_targets_do_not_publish() {
    let registry = Registry::<Tool, Section>::new([]).unwrap();
    let before = registry.snapshot();
    for keys in [
        vec!["a", "a"],
        vec!["Bad Key"],
        vec![""],
        vec!["instructions"],
        vec!["é"],
    ] {
        let mut ext = Ext::new("bad");
        ext.sections = keys
            .into_iter()
            .map(|key| Arc::new(Section(key.into())))
            .collect();
        assert!(registry.install(Arc::new(ext)).is_err());
        assert!(registry.snapshot().same_snapshot(&before));
    }
    let mut ext = Ext::new("bad");
    ext.tools = vec![tool("same"), tool("same")];
    assert!(registry.install(Arc::new(ext)).is_err());
    let mut valid = Ext::new("valid");
    valid.sections.push(Arc::new(Section("a-0_".into())));
    registry.install(Arc::new(valid)).unwrap();
}

#[test]
fn listeners_are_snapshotted_and_a_failure_retains_the_publication() {
    let registry = Registry::<Tool, Section>::new([]).unwrap();
    let later = Arc::new(Mutex::new(None));
    let remove = later.clone();
    let _first = registry.subscribe_fallible(move || {
        remove.lock().take();
        Err(Error::message("listener failed"))
    });
    let calls = Arc::new(Mutex::new(0));
    let out = calls.clone();
    *later.lock() = Some(registry.subscribe(move || *out.lock() += 1));
    assert_eq!(
        registry
            .install(Arc::new(Ext::new("published")))
            .unwrap_err()
            .to_string(),
        "listener failed"
    );
    assert!(registry.snapshot().extension(&"published".into()).is_some());
    assert_eq!(*calls.lock(), 0);
}

#[test]
fn unsubscribing_during_delivery_takes_effect_at_the_next_publication() {
    let registry = Registry::<Tool, Section>::new([]).unwrap();
    let later = Arc::new(Mutex::new(None));
    let remove = later.clone();
    let _first = registry.subscribe(move || {
        remove.lock().take();
    });
    let calls = Arc::new(Mutex::new(0));
    let out = calls.clone();
    *later.lock() = Some(registry.subscribe(move || *out.lock() += 1));
    registry.install(Arc::new(Ext::new("one"))).unwrap();
    registry.install(Arc::new(Ext::new("two"))).unwrap();
    assert_eq!(*calls.lock(), 1);
}

#[test]
fn a_failing_reporter_stops_later_wrappers() {
    let registry = Registry::<Tool, Section>::new([]).unwrap();
    let mut ext = Ext::new("broken");
    ext.tools = vec![tool("one"), tool("two")];
    ext.wraps.push(Wrap::Tool {
        name: "one".into(),
        apply: Arc::new(|_| Err(Error::message("wrapper"))),
    });
    ext.wraps.push(Wrap::Tool {
        name: "two".into(),
        apply: Arc::new(|_| panic!("reporting already failed")),
    });
    registry.install(Arc::new(ext)).unwrap();
    let result = try_resolve_agent(
        None,
        &registry.snapshot(),
        &resolve_settings(HarnessSettings::default()),
        |_, _| Err(Error::message("report failed")),
    );
    assert_eq!(result.err().unwrap().to_string(), "report failed");
}

#[test]
fn agent_composition_filters_callers_and_loads_deferred_tools_in_requested_order() {
    let registry = Registry::<Tool, Section>::new([]).unwrap();
    let mut ext = Ext::new("coding");
    ext.tools = vec![
        tool("read"),
        Arc::new(Tool {
            name: "internal".into(),
            callers: Some(vec![ToolCaller::Tools]),
            deferred: false,
        }),
        Arc::new(Tool {
            name: "search".into(),
            deferred: true,
            ..Default::default()
        }),
        Arc::new(Tool {
            name: "browse".into(),
            deferred: true,
            ..Default::default()
        }),
    ];
    registry.install(Arc::new(ext)).unwrap();
    let settings = resolve_settings(HarnessSettings::default());
    let a = resolve_agent(
        Some(&state(
            r#"{"modelTools":{"add":["browse","search","browse"],"remove":["read"]}}"#,
        )),
        &registry.snapshot(),
        &settings,
        |_, _| panic!("unexpected report"),
    )
    .unwrap();
    assert_eq!(names(&a.tools), ["browse", "search"]);
    assert_eq!(names(&a.callable), ["read", "internal", "search", "browse"]);
    assert!(a.deferred.is_empty());
    let a = resolve_agent(
        Some(&state(
            r#"{"modelTools":["search","internal","read","search"]}"#,
        )),
        &registry.snapshot(),
        &settings,
        |_, _| {},
    )
    .unwrap();
    assert_eq!(names(&a.tools), ["search", "read"]);
    assert_eq!(names(&a.deferred), ["browse"]);
}

#[test]
fn wrappers_run_before_filters_drop_failures_and_never_wrap_instructions() {
    let registry = Registry::<Tool, Section>::new([]).unwrap();
    let mut ext = Ext::new("tools");
    ext.tools = vec![tool("read"), tool("edit")];
    ext.sections.push(Arc::new(Section("cwd".into())));
    registry.install(Arc::new(ext)).unwrap();
    let mut ext = Ext::new("wrap");
    ext.wraps.push(Wrap::Tool {
        name: "read".into(),
        apply: Arc::new(|_| Err(Error::message("broken"))),
    });
    ext.wraps.push(Wrap::Tool {
        name: "edit".into(),
        apply: Arc::new(|_| Ok(tool("renamed"))),
    });
    ext.wraps.push(Wrap::Section {
        key: "instructions".into(),
        apply: Arc::new(|_| panic!("must not run")),
    });
    registry.install(Arc::new(ext)).unwrap();
    let mut errors = Vec::new();
    let a = resolve_agent(
        Some(&state(r#"{"tools":[],"instructions":""}"#)),
        &registry.snapshot(),
        &resolve_settings(HarnessSettings::default()),
        |error, ext| errors.push((error.to_string(), ext.clone())),
    )
    .unwrap();
    assert_eq!(
        errors,
        [
            ("broken".into(), "wrap".into()),
            ("Wrapper renamed edit to renamed".into(), "wrap".into())
        ]
    );
    assert!(a.tools.is_empty());
    assert_eq!(
        a.sections
            .iter()
            .map(|s| s.key().to_string())
            .collect::<Vec<_>>(),
        ["cwd", "instructions"]
    );
    assert_eq!(
        section_extension(&a, &"instructions".into()),
        Some("wrap".into())
    );
    assert_eq!(tool_extension(&a, &"read".into()), Some("wrap".into()));
}

#[test]
fn settings_defaults_merge_without_losing_unknown_policy_fields_or_nonfinite_runtime_values() {
    let input: HarnessSettings = serde_json::from_str(r#"{"retry":{"enabled":false,"custom":"\ud800"},"progress":{"partialIntervalMs":null,"outputIntervalMs":250},"restartLimit":2.9}"#).unwrap();
    let resolved = resolve_settings(input);
    assert_eq!(resolved.retry[&JsonString::from("maxRetries")], json("3"));
    assert_eq!(resolved.retry[&JsonString::from("enabled")], json("false"));
    assert_eq!(
        resolved.retry[&JsonString::from("custom")],
        json(r#""\ud800""#)
    );
    assert_eq!(resolved.progress.partial_interval_ms, 100.0);
    assert_eq!(resolved.progress.output_interval_ms, 250.0);
    assert_eq!(resolved.restart_limit, 2.0);
    let resolved = resolve_settings(HarnessSettings {
        context_retention_ms: Some(f64::NAN),
        document_retention_ms: Some(-1.0),
        restart_limit: Some(f64::INFINITY),
        ..Default::default()
    });
    assert!(resolved.context_retention_ms.is_nan());
    assert_eq!(resolved.document_retention_ms, 600_000.0);
    assert_eq!(resolved.restart_limit, f64::INFINITY);
}

#[tokio::test]
async fn configuration_is_rewindable_and_add_tools_preserves_surrogate_names() {
    let session = Session::new(Arc::new(MemoryStorage::new()), SessionOptions::default());
    let context = Context::default();
    let (parent, at) = session.commit(|tx| async move {
        let parent = tx.create_root_conversation().await?; create_agent(&tx, parent.clone()).await?;
        tx.doc(&AGENT_DOC,parent.id).await?.update(|state| { state.0.insert("custom".into(), json(r#""\udc00""#)); Ok(()) })?;
        configure(&tx, parent.id, state(r#"{"instructions":"old","tools":{"remove":[{"name":"\ud800"},{"name":"other"}]}}"#).0).await?;
        let at = tx.append_entry(parent.id, EntryDraft::new("point")).await?.id;
        Ok((parent.id,at))
    }, &context).await.unwrap();
    session
        .commit(
            move |tx| async move {
                configure(&tx, parent, state(r#"{"instructions":"new","cwd":null}"#).0).await?;
                add_tools(
                    &tx,
                    parent,
                    serde_json::from_str(r#"["\ud800","\ud800"]"#).unwrap(),
                )
                .await
            },
            &context,
        )
        .await
        .unwrap();
    let latest = session
        .snapshot(&AGENT_DOC, parent, &context)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        latest.0[&JsonString::from("tools")],
        json(r#"{"remove":["other"]}"#)
    );
    assert_eq!(
        latest.0[&JsonString::from("modelTools")],
        json(r#"{"add":["\ud800"]}"#)
    );
    assert_eq!(latest.0[&JsonString::from("custom")], json(r#""\udc00""#));
    let fork = session
        .commit(
            move |tx| async move {
                let fork = tx
                    .fork_conversation(parent, at, ConversationOwnership::Ownerless)
                    .await?;
                create_agent(&tx, fork.clone()).await?;
                Ok(fork.id)
            },
            &context,
        )
        .await
        .unwrap();
    let forked = session
        .snapshot(&AGENT_DOC, fork, &context)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        forked.0[&JsonString::from("instructions")],
        json(r#""old""#)
    );
    assert!(!forked.0.contains_key(&JsonString::from("modelTools")));
    session.close(&context).await.unwrap();
}

#[tokio::test]
async fn an_owned_conversation_copies_its_owners_stored_agent_independently() {
    let session = Session::new(Arc::new(MemoryStorage::new()), SessionOptions::default());
    let context = Context::default();
    let (parent, child) = session
        .commit(
            |tx| async move {
                let parent = tx.create_root_conversation().await?;
                create_agent(&tx, parent.clone()).await?;
                configure(
                    &tx,
                    parent.id,
                    state(r#"{"instructions":"inherited","extensions":[{"name":"coding"}]}"#).0,
                )
                .await?;
                let task = tx
                    .create_task_record(
                        "owner".into(),
                        1,
                        JsonValue::Null,
                        |_| Ok(json(r#"{"phase":"run"}"#)),
                        TaskOptions {
                            conversation_id: Some(parent.id),
                            ..TaskOptions::new(TaskOwnership::Conversation)
                        },
                    )
                    .await?;
                let child = tx
                    .create_conversation(ConversationOwnership::Task(task))
                    .await?;
                create_agent(&tx, child.clone()).await?;
                Ok((parent.id, child.id))
            },
            &context,
        )
        .await
        .unwrap();
    session
        .commit(
            move |tx| async move {
                configure(
                    &tx,
                    parent,
                    state(r#"{"instructions":null,"cwd":"/parent"}"#).0,
                )
                .await
            },
            &context,
        )
        .await
        .unwrap();
    let child = session
        .snapshot(&AGENT_DOC, child, &context)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        child.0[&JsonString::from("instructions")],
        json(r#""inherited""#)
    );
    assert_eq!(
        child.0[&JsonString::from("extensions")],
        json(r#"["coding"]"#)
    );
    assert!(!child.0.contains_key(&JsonString::from("cwd")));
    session.close(&context).await.unwrap();
}
