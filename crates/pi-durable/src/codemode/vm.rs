//! Deterministic VM capabilities; the default adapter runs the Rust sandbox.
use super::types::*;
use crate::{harness::tool::join, storage::StorageFuture, *};
use futures::{
    FutureExt,
    future::{BoxFuture, Shared},
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CallTarget {
    Tool,
    Global,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HostCall {
    pub id: u64,
    pub target: CallTarget,
    pub name: JsonString,
    #[serde(
        default,
        deserialize_with = "present_json",
        skip_serializing_if = "Option::is_none"
    )]
    pub args: Option<JsonValue>,
}
#[derive(Clone, Debug)]
pub enum Outcome {
    Success(Option<JsonValue>),
    Failure {
        message: JsonString,
        properties: Option<JsonObject>,
    },
}
impl Outcome {
    pub fn failure(message: impl Into<JsonString>) -> Self {
        Self::Failure {
            message: message.into(),
            properties: None,
        }
    }
    pub fn to_json(&self) -> JsonValue {
        let mut value = wire::object();
        match self {
            Self::Success(result) => {
                wire::put(&mut value, "ok", &true);
                wire::put_optional(&mut value, "value", result);
            }
            Self::Failure {
                message,
                properties,
            } => {
                wire::put(&mut value, "ok", &false);
                wire::put(&mut value, "message", message);
                wire::put_optional(&mut value, "properties", properties);
            }
        }
        value
    }
    pub fn from_json(value: &JsonValue) -> Result<Self> {
        if wire::field::<bool>(value, "ok").map_err(Error::message)? {
            Ok(Self::Success(
                wire::optional(value, "value").map_err(Error::message)?,
            ))
        } else {
            Ok(Self::Failure {
                message: wire::field(value, "message").map_err(Error::message)?,
                properties: wire::optional(value, "properties").map_err(Error::message)?,
            })
        }
    }
}
#[derive(Clone)]
pub struct RunOptions {
    pub start: Start,
    pub timeout_ms: Option<f64>,
    pub signal: pi_chord::AbortSignal,
    pub on_started: Arc<dyn Fn(JsonString) + Send + Sync>,
    pub on_call: Arc<dyn Fn(HostCall) + Send + Sync>,
    pub on_idle: Arc<dyn Fn(u64, u64) + Send + Sync>,
}
pub trait Run: Send + Sync {
    fn finished(&self) -> bool;
    fn deliver(&self, seq: u64, at: f64, id: u64, outcome: Outcome) -> Result<()>;
    fn result(&self) -> StorageFuture<JsonValue>;
}
pub trait Sandbox: Send + Sync {
    fn start(&self, code: JsonString, options: RunOptions) -> Result<Arc<dyn Run>>;
}
#[derive(Clone)]
pub struct NativeSandbox(pub pi_codemode::CodemodeSandbox);
impl NativeSandbox {
    pub fn new() -> Result<Self> {
        pi_codemode::CodemodeSandbox::new(Default::default())
            .map(Self)
            .map_err(|error| Error::message(error.to_string()))
    }
}
struct NativeRun {
    handle: pi_codemode::CodemodeRunHandle,
    result: Shared<BoxFuture<'static, Result<JsonValue>>>,
    watch: tokio::task::JoinHandle<()>,
    controller: pi_codemode::AbortController,
}
impl Drop for NativeRun {
    fn drop(&mut self) {
        self.watch.abort();
        if !self.handle.finished() {
            self.controller.abort();
        }
    }
}
fn utf8(value: &JsonString) -> Result<String> {
    value
        .to_string_checked()
        .map_err(|error| Error::message(error.to_string()))
}
fn sdk_value<T: serde::de::DeserializeOwned>(value: &JsonValue) -> Result<T> {
    wire::decode(value).map_err(Error::message)
}
impl Sandbox for NativeSandbox {
    fn start(&self, code: JsonString, options: RunOptions) -> Result<Arc<dyn Run>> {
        let started = options.on_started;
        let called = options.on_call;
        let idle = options.on_idle;
        let mut native = pi_codemode::CodemodeRunOptions::new(
            options.start.started_at,
            options.start.seed,
            move |call, _| {
                called(HostCall {
                    id: call.id,
                    target: if call.target == pi_codemode::CodemodeCallTarget::Tool {
                        CallTarget::Tool
                    } else {
                        CallTarget::Global
                    },
                    name: call.name.into(),
                    args: call.args.map(|value| wire::encode(&value)),
                })
            },
        );
        native.on_started = Some(Arc::new(move |fingerprint| started(fingerprint.into())));
        native.on_idle = Some(idle);
        native.tools = options
            .start
            .tools
            .iter()
            .filter(|tool| !tool.hidden)
            .map(|tool| {
                Ok(pi_codemode::CodemodeDeclaredTool {
                    name: utf8(&tool.name)?,
                    js_name: Some(utf8(&tool.js_name)?),
                    description: Some(utf8(&tool.description)?),
                })
            })
            .collect::<Result<_>>()?;
        native.globals = options
            .start
            .globals
            .iter()
            .map(|global| {
                Ok(pi_codemode::CodemodeDeclaredGlobal {
                    name: utf8(&global.name)?,
                    spread: global.spread,
                })
            })
            .collect::<Result<_>>()?;
        native.store = Some(sdk_value(options.start.store.value())?);
        native.timeout_ms = options.timeout_ms;
        let controller = pi_codemode::AbortController::new();
        native.signal = Some(controller.signal());
        let run = self
            .0
            .start(&utf8(&code)?, native)
            .map_err(|error| Error::message(error.to_string()))?;
        let handle = run.handle();
        let cancel = controller.clone();
        let signal = options.signal;
        let watch = tokio::spawn(async move {
            signal.cancelled().await;
            cancel.abort_with_reason(
                signal
                    .reason()
                    .map(|r| r.waiter_error().to_string())
                    .unwrap_or_else(|| "Operation aborted".into()),
            );
        });
        let result = async move { Ok(wire::encode(&run.result().await)) }
            .boxed()
            .shared();
        Ok(Arc::new(NativeRun {
            handle,
            result,
            watch,
            controller,
        }))
    }
}
impl Run for NativeRun {
    fn finished(&self) -> bool {
        self.handle.finished()
    }
    fn deliver(&self, seq: u64, at: f64, id: u64, outcome: Outcome) -> Result<()> {
        let outcome = match outcome {
            Outcome::Success(value) => pi_codemode::CodemodeOutcome::Ok {
                value: value.as_ref().map(sdk_value).transpose()?,
            },
            Outcome::Failure {
                message,
                properties,
            } => pi_codemode::CodemodeOutcome::Err {
                message: utf8(&message)?,
                properties: properties
                    .as_ref()
                    .map(|value| sdk_value(value.value()))
                    .transpose()?,
            },
        };
        self.handle
            .deliver(seq, at, id, outcome)
            .map_err(Error::message)
    }
    fn result(&self) -> StorageFuture<JsonValue> {
        Box::pin(self.result.clone())
    }
}
pub fn identifier(name: &JsonString) -> JsonString {
    pi_codemode::to_codemode_identifier(&name.to_string_lossy()).into()
}
pub fn describe_call(call: &HostCall) -> JsonString {
    join(
        [
            if call.target == CallTarget::Tool {
                "tools.".into()
            } else {
                JsonString::default()
            },
            call.name.clone(),
            "(".into(),
            call.args
                .as_ref()
                .map(|value| value.to_string().into())
                .unwrap_or_default(),
            ")".into(),
        ],
        "",
    )
}
