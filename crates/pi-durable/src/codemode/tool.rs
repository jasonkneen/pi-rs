use super::{call, describe::*, models::*, types::*};
use crate::{
    harness::{
        declaration::ToolDeclaration,
        prompt::NativeSection,
        registry::{Extension, ToolCaller},
        tool::*,
    },
    *,
};
use std::sync::Arc;

pub fn create_codemode_tool(options: CodemodeToolOptions) -> NativeTool {
    let options = Arc::new(options);
    let mut declaration = ToolDeclaration::new(
        CODEMODE_TOOL_NAME,
        codemode_description(options.models),
        wire::encode(
            &serde_json::json!({"type":"object","properties":{"code":{"type":"string","description":"Raw JavaScript source."}},"required":["code"]}),
        ),
    );
    declaration.constrained_sampling = Some(wire::encode(
        &serde_json::json!({"type":"grammar","variants":{"openai_lark":pi_codemode::CODEMODE_SOURCE_GRAMMAR}}),
    ));
    let mut tool = NativeTool::new(
        declaration,
        Arc::new(move |arguments, api, context| {
            let code = wire::field(arguments.value(), "code").map_err(Error::message);
            match code {
                Ok(code) => call::execute(options.clone(), api, context, code),
                Err(error) => Box::pin(async { Err(error) }),
            }
        }),
    );
    tool.callers = Some(vec![ToolCaller::Model]);
    tool.replay = ReplayPolicy::Safe;
    tool.output_limits = pi_chord::output::OutputLimits {
        max_bytes: call::OUTPUT_BYTES,
        max_lines: call::OUTPUT_LINES,
        retain: pi_chord::output::Retain::Head,
    };
    tool
}
pub fn create_codemode_extension(
    options: CodemodeToolOptions,
) -> Extension<NativeTool, NativeSection> {
    let models = options.models;
    let namespaces = options.namespaces.clone();
    let budget = options.inline_budget;
    let mut extension = Extension::new("codemode");
    extension
        .tools
        .push(Arc::new(create_codemode_tool(options)));
    if models {
        extension
            .tools
            .extend(create_model_tools().into_iter().map(Arc::new));
    }
    extension.sections.push(Arc::new(NativeSection {
        key: "codemode".into(),
        tagged: true,
        render: Arc::new(move |input, _| {
            let tools = input
                .agent
                .callable
                .iter()
                .filter(|tool| {
                    !tool.deferred
                        && !tool.declaration.name.units().starts_with(
                            &CODEMODE_INTERNAL_PREFIX.encode_utf16().collect::<Vec<_>>(),
                        )
                })
                .map(|tool| ToolMetadata::of(tool.as_ref()))
                .collect::<Vec<_>>();
            let result = codemode_section(&tools, &namespaces, budget);
            Box::pin(async { result })
        }),
    }));
    extension
}
