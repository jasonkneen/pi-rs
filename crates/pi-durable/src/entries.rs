use crate::{EntryRecord, Error, Result};
use pi_chord::{JsonString, JsonValue};
use std::marker::PhantomData;

#[derive(Clone, Debug)]
pub struct Entry<D = JsonValue> {
    kind: JsonString,
    marker: PhantomData<fn() -> D>,
}
impl<D> Entry<D> {
    pub fn kind(&self) -> &JsonString {
        &self.kind
    }
    pub fn is(&self, entry: Option<&EntryRecord>) -> bool {
        entry.is_some_and(|entry| entry.kind == self.kind)
    }
}
pub fn define_entry<D>(kind: impl Into<JsonString>) -> Result<Entry<D>> {
    let kind = kind.into();
    if kind.units().is_empty() {
        return Err(Error::message("Entry kind must be a non-empty string"));
    }
    Ok(Entry {
        kind,
        marker: PhantomData,
    })
}
pub static USER_ENTRY: std::sync::LazyLock<Entry> =
    std::sync::LazyLock::new(|| define_entry("pi.user").unwrap());
pub static ASSISTANT_ENTRY: std::sync::LazyLock<Entry> =
    std::sync::LazyLock::new(|| define_entry("pi.assistant").unwrap());
pub static SYSTEM_ENTRY: std::sync::LazyLock<Entry> =
    std::sync::LazyLock::new(|| define_entry("pi.system").unwrap());
pub static TOOL_RESULT_ENTRY: std::sync::LazyLock<Entry> =
    std::sync::LazyLock::new(|| define_entry("pi.tool-result").unwrap());
pub static RESET_ENTRY: std::sync::LazyLock<Entry> =
    std::sync::LazyLock::new(|| define_entry("pi.reset").unwrap());
pub static COMPACTION_ENTRY: std::sync::LazyLock<Entry> =
    std::sync::LazyLock::new(|| define_entry("pi.compaction").unwrap());
