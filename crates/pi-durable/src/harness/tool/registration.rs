//! Executable registrations are host capabilities; persisted declarations stay JSON.
use super::*;
use crate::harness::{
    declaration::{DeclaredTool, ToolDeclaration},
    registry::{RegisteredTool, ToolCaller},
};
use crate::{storage::StorageFuture, *};
use pi_chord::output::OutputLimits;
use std::sync::Arc;

pub trait ExecutableTool: Send + Sync + 'static {
    fn declaration(&self) -> ToolDeclaration;
    fn replay(&self) -> ReplayPolicy {
        ReplayPolicy::Unsafe
    }
    fn execution_mode(&self) -> Option<ToolExecutionMode> {
        None
    }
    fn output_limits(&self) -> OutputLimits {
        OutputLimits {
            max_bytes: 50 * 1024,
            max_lines: 2000,
            retain: pi_chord::output::Retain::Head,
        }
    }
    fn structured_output_schema(&self) -> Option<JsonValue> {
        None
    }
    fn prepare_arguments(&self, arguments: JsonValue) -> Result<JsonValue> {
        Ok(arguments)
    }
    fn validate_arguments(&self, call: &ToolCall, arguments: JsonValue) -> Result<JsonObject> {
        validate_tool_arguments(&self.declaration(), call, arguments)
    }
    fn structured_output_error(&self, result: &ToolExecutionResult) -> Result<Option<JsonString>> {
        structured_output_error(
            &self.declaration().name,
            self.structured_output_schema().as_ref(),
            result,
        )
    }
    fn execute(
        &self,
        arguments: JsonObject,
        api: ToolExecutionApi,
        context: Context,
    ) -> StorageFuture<ToolExecutionResult>;
}

pub type ExecuteTool = Arc<
    dyn Fn(JsonObject, ToolExecutionApi, Context) -> StorageFuture<ToolExecutionResult>
        + Send
        + Sync,
>;
pub type PrepareArguments = Arc<dyn Fn(JsonValue) -> Result<JsonValue> + Send + Sync>;
pub type ValidateArguments =
    Arc<dyn Fn(&ToolDeclaration, &ToolCall, JsonValue) -> Result<JsonObject> + Send + Sync>;

pub struct NativeTool {
    pub declaration: ToolDeclaration,
    pub callers: Option<Vec<ToolCaller>>,
    pub deferred: bool,
    pub replay: ReplayPolicy,
    pub execution_mode: Option<ToolExecutionMode>,
    pub output_limits: OutputLimits,
    pub structured_output_schema: Option<JsonValue>,
    pub prepare_arguments: Option<PrepareArguments>,
    /// A registration with a lossless input representation can decode it natively.
    pub validate_arguments: Option<ValidateArguments>,
    pub execute: ExecuteTool,
}
impl NativeTool {
    pub fn new(declaration: ToolDeclaration, execute: ExecuteTool) -> Self {
        Self {
            declaration,
            callers: None,
            deferred: false,
            replay: ReplayPolicy::Unsafe,
            execution_mode: None,
            output_limits: OutputLimits {
                max_bytes: 50 * 1024,
                max_lines: 2000,
                retain: pi_chord::output::Retain::Head,
            },
            structured_output_schema: None,
            prepare_arguments: None,
            validate_arguments: None,
            execute,
        }
    }
}
impl RegisteredTool for NativeTool {
    fn name(&self) -> &JsonString {
        &self.declaration.name
    }
    fn callers(&self) -> Option<&[ToolCaller]> {
        self.callers.as_deref()
    }
    fn deferred(&self) -> bool {
        self.deferred
    }
    fn executable(self: Arc<Self>) -> Option<Arc<dyn ExecutableTool>> {
        Some(self)
    }
}
impl DeclaredTool for NativeTool {
    fn declaration(&self) -> ToolDeclaration {
        self.declaration.clone()
    }
}
impl ExecutableTool for NativeTool {
    fn declaration(&self) -> ToolDeclaration {
        self.declaration.clone()
    }
    fn replay(&self) -> ReplayPolicy {
        self.replay
    }
    fn execution_mode(&self) -> Option<ToolExecutionMode> {
        self.execution_mode
    }
    fn output_limits(&self) -> OutputLimits {
        self.output_limits
    }
    fn structured_output_schema(&self) -> Option<JsonValue> {
        self.structured_output_schema.clone()
    }
    fn prepare_arguments(&self, arguments: JsonValue) -> Result<JsonValue> {
        self.prepare_arguments
            .as_ref()
            .map_or(Ok(arguments.clone()), |prepare| prepare(arguments))
    }
    fn validate_arguments(&self, call: &ToolCall, arguments: JsonValue) -> Result<JsonObject> {
        if let Some(validate) = &self.validate_arguments {
            check_arguments_error(call)?;
            validate(&self.declaration, call, arguments)
        } else {
            validate_tool_arguments(&self.declaration, call, arguments)
        }
    }
    fn execute(
        &self,
        arguments: JsonObject,
        api: ToolExecutionApi,
        context: Context,
    ) -> StorageFuture<ToolExecutionResult> {
        (self.execute)(arguments, api, context)
    }
}

// The existing provider SDK accepts UTF8 JSON. Reject unsupported fields explicitly
// at that boundary; never replace lone UTF16 units in durable data with U+FFFD.
fn sdk_json(value: &JsonValue) -> Result<serde_json::Value> {
    serde_json::from_str(&value.to_string())
        .map_err(|error| Error::message(format!("Unsupported SDK JSON: {error}")))
}
fn check_arguments_error(call: &ToolCall) -> Result<()> {
    if let Some(error) = call
        .extra
        .get(&JsonString::from("argumentsError"))
        .and_then(JsonValue::as_string)
        && !error.units().is_empty()
    {
        return Err(Error::host(
            "Error",
            result::join(
                [
                    "Invalid arguments for tool \"".into(),
                    call.name.clone(),
                    "\": ".into(),
                    error.clone(),
                ],
                "",
            ),
        ));
    }
    Ok(())
}
pub fn validate_tool_arguments(
    declaration: &ToolDeclaration,
    call: &ToolCall,
    arguments: JsonValue,
) -> Result<JsonObject> {
    check_arguments_error(call)?;
    let parameters = sdk_json(&declaration.parameters)?;
    let args = sdk_json(&arguments)?;
    let serde_json::Value::Object(args) = args else {
        return Err(Error::message("Tool arguments must be an object"));
    };
    let name = declaration
        .name
        .to_string_checked()
        .map_err(|e| Error::message(e.to_string()))?;
    let tool = pi_ai::Tool::new(&name, "", parameters);
    let call = pi_ai::ToolCall {
        name,
        arguments: args,
        ..Default::default()
    };
    let result = pi_ai::utils::validation::validate_tool_arguments(&tool, &call)
        .map_err(|error| Error::message(error.to_string()))?;
    let value = pi_chord::json::parse(&result.to_string())
        .map_err(|error| Error::message(error.to_string()))?;
    JsonObject::try_from_value(value).map_err(Error::message)
}
fn structured_output_error(
    name: &JsonString,
    schema: Option<&JsonValue>,
    result: &ToolExecutionResult,
) -> Result<Option<JsonString>> {
    let prefix = result::join(["Tool ".into(), name.clone()], "");
    let message = match (schema, result.structured_output.as_ref()) {
        (None, None) => return Ok(None),
        (None, Some(_)) => {
            " returned structuredOutput but declares no structuredOutputSchema".into()
        }
        (Some(_), None) if result.is_error == Some(true) => return Ok(None),
        (Some(_), None) => " returned no structuredOutput".into(),
        (Some(schema), Some(value)) => {
            let schema = sdk_json(schema)?;
            let validator = pi_ai::utils::validation::SchemaValidator::new(&schema);
            let value = sdk_json(&value.copy_json()?)?;
            if validator.check(&value) {
                return Ok(None);
            }
            let errors = validator.errors(&value);
            let first = errors.first();
            let path = first
                .map(|error| {
                    error
                        .instance_path
                        .trim_start_matches('/')
                        .replace('/', ".")
                })
                .filter(|path| !path.is_empty())
                .unwrap_or_else(|| "root".into());
            format!(
                " returned structuredOutput that does not match its schema: {path}: {}",
                first.map_or("invalid", |error| &error.message)
            )
            .into()
        }
    };
    Ok(Some(result::join([prefix, message], "")))
}
